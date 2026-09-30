#include "check.hpp"
#include "manual_transport.hpp"
#include <rpc/stream.hpp>
#include <net/run_async.hpp>
#include <thread>

namespace {
struct fixture {
    fixture() {
        rpc::client_options config; config.connection.receive.features |= rpc::wire::streaming;
        config.connection.receive.initial_stream_window = 1024; config.connection.receive.max_frame_size = 1024;
        config.connection.receive.max_message_size = 1024; client = rpc::make_client(context, config);
        std::vector<std::uint8_t> data(96);
        auto settings = config.connection.receive;
        auto head = rpc::wire::encode_settings(settings, {data.data() + 16, 80});
        rpc::wire::encode_header({static_cast<std::uint32_t>(head.written), 0, rpc::wire::frame_type::settings, 0, static_cast<std::uint16_t>(head.written), 0}, {data.data(), 16});
        data.resize(head.written + 16); settings_frame = data; pipe->feed(data);
        rpc::status_code connected = rpc::status_code::unknown;
        net::run_async(context.get_executor(), [&](rpc::status_code c) { connected = c; }, [](std::exception_ptr e) { std::rethrow_exception(e); })(
            [&] { return client->attach(std::make_unique<manual_transport>(pipe)); });
        context.poll(); CHECK(connected == rpc::status_code::ok); pipe->output.clear(); pipe->block_writes = true;
    }
    ~fixture() { client->close(); pipe->delay_close_write = false; pipe->abort(true); context.run(); }
    void flush() {
        for (unsigned i = 0; pipe->writer && i < 100; ++i) { pipe->finish_write(1024); context.poll(); }
        CHECK(!pipe->writer);
    }
    net::io_context context{net::default_backend, net::single_thread_hint};
    std::shared_ptr<manual_pipe> pipe = std::make_shared<manual_pipe>();
    std::unique_ptr<rpc::client> client;
    std::vector<std::uint8_t> settings_frame;
};
void deadline_during_write() {
    fixture f; rpc::call_options options; options.timeout = std::chrono::milliseconds{1};
    unsigned done = 0; rpc::stream_open_result result;
    net::run_async(f.context.get_executor(), [&](rpc::stream_open_result r) { result = std::move(r); ++done; }, [](std::exception_ptr e) { std::rethrow_exception(e); })(
        [&] { return f.client->open_stream("Chat", rpc::method_kind::bidirectional, options); });
    f.context.run_for(std::chrono::milliseconds{5}); CHECK(done == 1 && result.code == rpc::status_code::deadline_exceeded);
    CHECK(f.client->stats().active_calls == 0 && f.pipe->writer);
    f.flush(); CHECK(f.client->ready());
    auto first = rpc::wire::decode_frame({f.pipe->output.data(), f.pipe->output.size()}, {1024, 1024, rpc::wire::streaming});
    CHECK(first.code == rpc::wire::error::none && first.value.header.type == rpc::wire::frame_type::request);
    auto second = rpc::wire::decode_frame({f.pipe->output.data() + first.consumed, f.pipe->output.size() - first.consumed}, {1024, 1024, rpc::wire::streaming});
    CHECK(second.code == rpc::wire::error::none && second.value.header.type == rpc::wire::frame_type::cancel);
}
void foreign_cancel() {
    fixture f; net::stop_source stop; unsigned done = 0; rpc::stream_open_result result;
    net::run_async(f.context.get_executor(), stop.get_token(), nullptr,
        [&](rpc::stream_open_result r) { result = std::move(r); ++done; }, [](std::exception_ptr e) { std::rethrow_exception(e); })(
        [&] { return f.client->open_stream("Chat", rpc::method_kind::bidirectional); });
    f.context.poll(); CHECK(f.pipe->writer && done == 0);
    std::thread cancel([&] { stop.request_stop(); }); cancel.join(); f.context.poll();
    CHECK(done == 1 && result.code == rpc::status_code::cancelled); f.flush(); CHECK(f.client->ready());
}
void invalid_success_and_repeated_settings() {
    {
        fixture f; rpc::stream_open_result opened;
        net::run_async(f.context.get_executor(), [&](rpc::stream_open_result r) { opened = std::move(r); }, [](std::exception_ptr e) { std::rethrow_exception(e); })(
            [&] { return f.client->open_stream("Upload", rpc::method_kind::client_streaming); });
        f.context.poll(); f.flush(); CHECK(opened.code == rpc::status_code::ok && opened.stream);
        std::vector<std::uint8_t> end(18, 0);
        CHECK(rpc::wire::encode_header({2, 1, rpc::wire::frame_type::end, 0, 2, 0}, {end.data(), 16}, {1024, 1024, rpc::wire::streaming}).code == rpc::wire::error::none);
        f.pipe->feed(end); f.context.poll();
        CHECK(!f.client->ready() && f.client->stats().active_calls == 0);
        rpc::call_result result;
        net::run_async(f.context.get_executor(), [&](rpc::call_result r) { result = std::move(r); }, [](std::exception_ptr e) { std::rethrow_exception(e); })(
            [&] { return opened.stream->finish(); });
        f.context.poll(); CHECK(result.code == rpc::status_code::unavailable);
    }
    {
        fixture f; f.pipe->feed(f.settings_frame); f.context.poll(); CHECK(!f.client->ready());
    }
}
struct encoding_action { net::stop_source *stop; rpc::clock::time_point deadline; bool cancel; };
void stop_during_encoding(bool cancel) {
    fixture f; net::stop_source stop; rpc::stream_open_result opened; rpc::call_options options;
    if (!cancel) options.deadline = rpc::clock::now() + std::chrono::milliseconds{20};
    net::run_async(f.context.get_executor(), stop.get_token(), nullptr,
        [&](rpc::stream_open_result r) { opened = std::move(r); }, [](std::exception_ptr e) { std::rethrow_exception(e); })
        ([&] { return f.client->open_stream("Upload", rpc::method_kind::client_streaming, options); });
    f.context.poll(); f.flush(); CHECK(opened.code == rpc::status_code::ok); f.pipe->output.clear();
    encoding_action action{&stop, options.deadline, cancel};
    rpc::codec_ops ops{
        [](void const *) -> std::size_t { return 1; },
        [](void const *, rpc::wire::mutable_bytes_view) { return false; }, nullptr, nullptr,
        [](void const *p, rpc::wire::mutable_bytes_view bytes) {
            auto const &a = *static_cast<encoding_action const *>(p);
            if (a.cancel) a.stop->request_stop(); else std::this_thread::sleep_until(a.deadline);
            bytes.data[0] = 42; return rpc::wire::encode_result{rpc::wire::error::none, 1};
        }, [](void const *) -> std::size_t { return 1; }};
    rpc::status_code result = rpc::status_code::unknown;
    net::run_async(f.context.get_executor(), [&](rpc::status_code r) { result = r; }, [](std::exception_ptr e) { std::rethrow_exception(e); })
        ([&] { return opened.stream->write_encoded({&action, &ops}); });
    f.context.poll(); f.flush();
    CHECK(result == (cancel ? rpc::status_code::cancelled : rpc::status_code::deadline_exceeded));
    std::size_t offset = 0;
    while (offset < f.pipe->output.size()) {
        auto frame = rpc::wire::decode_frame({f.pipe->output.data() + offset, f.pipe->output.size() - offset}, {1024, 1024, rpc::wire::streaming});
        CHECK(frame.code == rpc::wire::error::none && frame.value.header.type != rpc::wire::frame_type::message); offset += frame.consumed;
    }
    CHECK(f.client->stats().active_calls == 0 && f.client->stats().request_bytes_in_use == 0);
}
}
int main() { deadline_during_write(); foreign_cancel(); invalid_success_and_repeated_settings(); stop_during_encoding(true); stop_during_encoding(false); }
