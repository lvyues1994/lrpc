#include "check.hpp"
#include "backend.hpp"
#include <rpc/stream.hpp>
#include <net/run_async.hpp>
#include <net/test/run_blocking.hpp>
#include <net/timeout.hpp>
#include <iostream>
#include <thread>

namespace {
using namespace rpc;
struct echo_handler final : method_handler {
    net::task<status_code> invoke(server_context &, wire::bytes_view request, response_writer &response) override {
        return run(request, &response);
    }
    static auto run(wire::bytes_view request, response_writer *response)
        CO2_BEG(net::task<status_code>, (request, response)) { response->assign(request); CO2_RETURN(status_code::ok); } CO2_END
};
struct chat_service {
    using stream_type = server_stream<byte_buffer, byte_buffer>;
    unsigned messages = 0;
    net::task<status_code> chat(server_context &, stream_type &stream) { return run(this, &stream); }
    static auto run(chat_service *service, stream_type *stream)
        CO2_BEG(net::task<status_code>, (service, stream), byte_buffer message; stream_read_result read; status_code sent;) {
        for (;;) {
            CO2_AWAIT_SET(read, stream->read(message));
            if (read.code != status_code::ok) CO2_RETURN(read.code);
            if (read.ended) break;
            ++service->messages; CO2_AWAIT_SET(sent, stream->write(message));
            if (sent != status_code::ok) CO2_RETURN(sent);
        }
        CO2_RETURN(status_code::ok);
    }
    CO2_END
};
struct throwing_handler final : stream_method_handler {
    explicit throwing_handler(bool synchronous) : synchronous(synchronous) {}
    net::task<status_code> invoke(server_context &, std::shared_ptr<byte_stream>) override {
        if (synchronous) throw std::runtime_error{"synchronous handler failure"};
        return run();
    }
    static auto run() CO2_BEG(net::task<status_code>, ()) {
        throw std::runtime_error{"coroutine handler failure"}; CO2_RETURN(status_code::ok);
    } CO2_END
    bool synchronous;
};
struct invalid_status_handler final : stream_method_handler {
    net::task<status_code> invoke(server_context &context, std::shared_ptr<byte_stream>) override {
        set_status(context, {status_code::invalid_argument, "invalid OK description"}); return run();
    }
    static auto run() CO2_BEG(net::task<status_code>, ()) { CO2_RETURN(status_code::ok); } CO2_END
};
using call_type = stream_call<byte_buffer, byte_buffer>;
auto exercise(client *client, server *server, net::ip::tcp::endpoint endpoint, method_handle chat, std::size_t size, byte_buffer *survivor)
    CO2_BEG(net::task<>, (client, server, endpoint, chat, size, survivor),
            status_code connected; call_options options; call_result result; std::vector<std::uint8_t> input, output;
            byte_buffer request, reply, pin; call_type call; stream_open_result failed; stream_read_result read; unsigned turn = 0;) {
    CO2_AWAIT_SET(connected, client->connect(endpoint)); CHECK(connected == status_code::ok);
    input.assign(size, 0x5a); output.assign(size, 0); options.timeout = std::chrono::seconds{2};
    CO2_AWAIT_SET(result, client->call("Echo", {input.data(), input.size()}, {output.data(), output.size()}, options));
    CHECK(result.code == status_code::ok && input == output);
    request = byte_buffer::copy({input.data(), input.size()});
    for (turn = 0; turn < 3; ++turn) {
        CO2_AWAIT_SET(call, (open_stream<byte_buffer, byte_buffer>(*client, chat, options))); if (!call) throw std::runtime_error{"open code=" + std::to_string(static_cast<unsigned>(call.code())) + " turn=" + std::to_string(turn)};
        CO2_AWAIT_SET(connected, call.write(request)); CHECK(connected == status_code::ok);
        CO2_AWAIT_SET(read, call.read(reply)); CHECK(read.code == status_code::ok && !read.ended && reply.size() == size);
        CHECK(reply.copy_to({output.data(), output.size()}) && input == output);
        CO2_AWAIT_SET(connected, call.write(request)); CHECK(connected == status_code::ok);
        CO2_AWAIT_SET(read, call.read(reply)); CHECK(read.code == status_code::ok && !read.ended && reply.size() == size);
        // Empty messages are distinct from half-close and preserve receipts.
        request.clear(); CO2_AWAIT_SET(connected, call.write(request)); CHECK(connected == status_code::ok);
        CO2_AWAIT_SET(read, call.read(reply)); CHECK(read.code == status_code::ok && !read.ended && reply.size() == 0);
        CO2_AWAIT_SET(connected, call.writes_done()); CHECK(connected == status_code::ok);
        CO2_AWAIT_SET(read, call.read(reply)); CHECK(read.code == status_code::ok && read.ended);
        CO2_AWAIT_SET(result, call.finish()); CHECK(result.code == status_code::ok);
        request = byte_buffer::copy({input.data(), input.size()});
    }
    // A failing handler must leave another admitted stream usable on the same connection.
    for (turn = 0; turn < 3; ++turn) {
        CO2_AWAIT_SET(call, (open_stream<byte_buffer, byte_buffer>(*client, chat, options))); CHECK(call);
        CO2_AWAIT_SET(failed, client->open_stream(turn == 0 ? "ThrowSync" : turn == 1 ? "ThrowAsync" : "BadStatus", method_kind::bidirectional, options)); CHECK(failed.code == status_code::ok && failed.stream);
        CO2_AWAIT_SET(result, failed.stream->finish()); CHECK(result.code == status_code::internal && client->ready());
        CO2_AWAIT_SET(connected, call.write(request)); CHECK(connected == status_code::ok);
        CO2_AWAIT_SET(read, call.read(reply)); CHECK(read.code == status_code::ok && !read.ended && reply.size() == size);
        CO2_AWAIT_SET(connected, call.writes_done()); CHECK(connected == status_code::ok);
        CO2_AWAIT_SET(result, call.finish()); CHECK(result.code == status_code::ok);
    }
    CO2_AWAIT_SET(call, (open_stream<byte_buffer, byte_buffer>(*client, chat, options))); CHECK(call);
    CO2_AWAIT_SET(connected, call.write(request)); CHECK(connected == status_code::ok);
    CO2_AWAIT_SET(read, call.read(reply)); CHECK(read.code == status_code::ok && !read.ended);
    *survivor = reply.slice(0, reply.size());
    CO2_AWAIT_SET(connected, call.writes_done()); CHECK(connected == status_code::ok);
    CO2_AWAIT_SET(result, call.finish()); CHECK(result.code == status_code::ok);
    client->close(); CO2_AWAIT(server->shutdown(std::chrono::milliseconds{0})); CO2_RETURN();
}
CO2_END
}
int main(int argc, char **argv) {
    auto backend = test_backend(argc, argv);
    byte_buffer survivor;
    try {
        for (unsigned compression = 0; compression <= 2; ++compression) {
            if (compression && !(compression_algorithms() & (1U << (compression - 1)))) continue;
            for (bool source : {false, true}) {
                std::thread release([owned = std::move(survivor)]() mutable { owned.clear(); }); release.join();
                net::io_context context{backend, net::single_thread_hint}; echo_handler echo; chat_service chat;
                throwing_handler throw_sync{true}, throw_async{false}; invalid_status_handler bad_status;
                connection_options connection; connection.receive.max_frame_size = 1024; connection.receive.max_message_size = 65536;
                connection.receive.max_method_ids = 5; connection.receive.features |= wire::streaming;
                connection.receive.initial_stream_window = 65536; connection.use_receive_source = source;
                if (compression) { connection.receive.features |= wire::message_compression; connection.receive.compression = compression_algorithms(); connection.preferred_compression = static_cast<std::uint8_t>(compression); }
                server_options options; options.connection = connection;
                std::vector<method_binding> bindings{{"Echo", 65536, &echo}};
                bindings.push_back(bind_stream_method(method<byte_buffer, byte_buffer>{"Chat"}, method_kind::bidirectional, chat, &chat_service::chat, {65536, 2}));
                bindings.push_back({"ThrowSync", 65536, nullptr, 2, {}, method_kind::bidirectional, &throw_sync, {}});
                bindings.push_back({"ThrowAsync", 65536, nullptr, 2, {}, method_kind::bidirectional, &throw_async, {}});
                bindings.push_back({"BadStatus", 65536, nullptr, 128, {}, method_kind::bidirectional, &bad_status, {}});
                auto server = make_server(context, std::move(bindings), options);
                auto endpoint = server->listen({net::ip::address_v4::loopback(), 0});
                client_options config; config.connection = connection; auto client = make_client(context, config);
                method_descriptor descriptor{"Chat", method_kind::bidirectional, idempotency::unknown, &codec_for<byte_buffer>(), &codec_for<byte_buffer>(), 0};
                auto handle = client->bind(descriptor);
                std::exception_ptr failure;
                net::run_async(context.get_executor(), [] {}, [&](std::exception_ptr e) { failure = e; client->close(); server->close(); })(
                    [&] { return exercise(client.get(), server.get(), endpoint, handle, 65536, &survivor); });
                context.run();
                if (failure) std::rethrow_exception(failure);
                CHECK(client->quiescent() && chat.messages == 13);
                CHECK(client->stats().request_bytes_in_use == 0 && client->stats().response_bytes_in_use >= survivor.size());
            }
        }
    } catch (std::system_error const &error) { if (backend == net::backend_kind::io_uring && (error.code() == std::errc::operation_not_permitted || error.code() == std::errc::function_not_supported)) return 77; throw; }
    std::thread release([owned = std::move(survivor)]() mutable { owned.clear(); }); release.join();
    std::cout << "PASS fragmented unary, bidirectional flow, empty messages, method reuse, compression and receive source\n";
}
