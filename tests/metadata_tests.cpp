#include "client_fixture.hpp"

#include <array>
#include <iostream>

namespace {
namespace w = rpc::wire;

w::bytes_view bytes(std::string const &value) {
    return {reinterpret_cast<std::uint8_t const *>(value.data()), value.size()};
}
std::string text(w::bytes_view value) { return {reinterpret_cast<char const *>(value.data), value.size}; }

void check_metadata(w::metadata_view value) {
    CHECK(value.count == 3);
    auto a = w::decode_metadata_entry(value.entries); CHECK(a.code == w::error::none);
    CHECK(text(a.value.key) == "k" && text(a.value.value) == std::string("a\0b", 3));
    value.entries.data += a.consumed; value.entries.size -= a.consumed;
    auto b = w::decode_metadata_entry(value.entries); CHECK(b.code == w::error::none);
    CHECK(text(b.value.key) == "k" && b.value.value.size == 0);
    value.entries.data += b.consumed; value.entries.size -= b.consumed;
    auto c = w::decode_metadata_entry(value.entries); CHECK(c.code == w::error::none);
    CHECK(c.value.key.size == 0 && text(c.value.value) == "z");
    CHECK(c.consumed == value.entries.size);
}

struct metadata {
    std::string key = "k", value = std::string("a\0b", 3), last = "z";
    std::array<w::metadata_entry, 3> entries{{{bytes(key), bytes(value)}, {bytes(key), {}}, {{}, bytes(last)}}};
    w::metadata_list view() const { return {entries.data(), entries.size()}; }
};

std::vector<std::uint8_t> end(std::uint32_t id, w::metadata_list values, rpc::status_code code = rpc::status_code::ok) {
    std::vector<std::uint8_t> data(256);
    auto encoded = w::encode_end_head({{}, values}, {data.data() + 16, data.size() - 16});
    CHECK(encoded.code == w::error::none);
    auto const body = code == rpc::status_code::ok ? 1U : 0U;
    CHECK(w::encode_header({static_cast<std::uint32_t>(encoded.written + body), id, w::frame_type::end,
        0, static_cast<std::uint16_t>(encoded.written), static_cast<std::uint32_t>(code)}, {data.data(), 16}).code == w::error::none);
    data[16 + encoded.written] = 42; data.resize(16 + encoded.written + body); return data;
}

void client_metadata() {
    metadata values;
    test_client::call a, b; test_client::fixture f;
    std::array<std::uint8_t, 64> output{};
    rpc::call_options options{}; options.metadata = values.view(); options.response_metadata = {output.data(), output.size()};
    f.start(a, "Echo", options); f.start(b, "Echo", options); f.context.poll();
    w::bytes_view data{f.pipe->output.data(), f.pipe->output.size()};
    for (unsigned i = 0; i < 2; ++i) {
        auto frame = w::decode_frame(data); CHECK(frame.code == w::error::none);
        CHECK(((frame.value.header.flags & w::new_method) != 0) == (i == 0));
        auto head = w::decode_request_head(frame.value.head, i == 0);
        check_metadata(head.value.metadata); CHECK(frame.value.body.size == 1 && frame.value.body.data[0] == 42);
        data.data += frame.consumed; data.size -= frame.consumed;
    }
    CHECK(data.size == 0);
    f.pipe->feed(end(1, values.view())); f.context.poll();
    CHECK(a.completions == 1 && a.result.code == rpc::status_code::ok); check_metadata(a.result.response_metadata);
    f.pipe->feed(end(2, values.view(), rpc::status_code::permission_denied)); f.context.poll();
    CHECK(b.result.code == rpc::status_code::permission_denied); check_metadata(b.result.response_metadata);
    // Force rx reuse after completion; returned metadata belongs to the caller.
    for (unsigned i = 0; i < 100; ++i) f.pipe->feed(test_client::reply(2, 99));
    f.context.poll(); check_metadata(a.result.response_metadata); f.zero();
}

void cancelled_definition() {
    metadata values;
    test_client::call hold, cancelled, kept; test_client::fixture f;
    f.pipe->block_writes = true; f.start(hold, "Hold"); f.context.poll();
    rpc::call_options options{}; options.metadata = values.view();
    f.start(cancelled, "Echo", options); f.start(kept, "Echo", options); f.context.poll();
    cancelled.stop.request_stop(); f.context.poll();
    CHECK(cancelled.completions == 1 && cancelled.result.code == rpc::status_code::cancelled);
    f.flush();
    w::bytes_view data{f.pipe->output.data(), f.pipe->output.size()}; bool seen = false;
    while (data.size != 0) {
        auto frame = w::decode_frame(data); CHECK(frame.code == w::error::none);
        if (frame.value.header.stream_id == 3) {
            CHECK((frame.value.header.flags & w::new_method) != 0);
            auto head = w::decode_request_head(frame.value.head, true);
            CHECK(text(head.value.method_name) == "Echo"); check_metadata(head.value.metadata); seen = true;
        }
        CHECK(frame.value.header.stream_id != 2);
        data.data += frame.consumed; data.size -= frame.consumed;
    }
    CHECK(seen); f.pipe->feed(test_client::reply(1)); f.pipe->feed(test_client::reply(3)); f.context.poll(); f.zero();
}

void client_limits() {
    metadata values; test_client::fixture f; test_client::call a, b, c, d, e, oversized;
    std::uint8_t small = 0; rpc::call_options options{}; options.response_metadata = {&small, 1};
    f.start(a, "Echo", options); f.context.poll(); f.pipe->feed(end(1, values.view())); f.context.poll();
    CHECK(a.result.code == rpc::status_code::resource_exhausted && a.result.response_metadata.count == 0);
    options = {}; options.metadata = {nullptr, 1}; f.start(b, "Echo", options); f.context.poll();
    CHECK(b.result.code == rpc::status_code::invalid_argument);
    options = {}; options.response_metadata = {nullptr, 1}; f.start(c, "Echo", options); f.context.poll();
    CHECK(c.result.code == rpc::status_code::invalid_argument);
    options = {}; options.response_metadata = {&e.output, 1}; f.start(e, "Echo", options); f.context.poll();
    CHECK(e.result.code == rpc::status_code::invalid_argument);
    std::string huge(2048, 'x'); w::metadata_entry large{{}, bytes(huge)};
    options = {}; options.metadata = {&large, 1}; f.start(oversized, "Echo", options); f.context.poll();
    CHECK(oversized.result.code == rpc::status_code::resource_exhausted);
    f.start(d); f.context.poll(); f.pipe->feed(end(2, values.view())); f.context.poll();
    CHECK(d.result.code == rpc::status_code::ok && d.result.response_metadata.count == 0); f.zero();
}

struct metadata_handler final : rpc::method_handler {
    net::task<rpc::status_code> invoke(rpc::server_context &, w::bytes_view, rpc::response_writer &) override;
    unsigned calls = 0;
    bool overflow_body = false;
    rpc::status_code result = rpc::status_code::ok;
};
auto respond(metadata_handler *self, rpc::server_context *context, w::bytes_view request, rpc::response_writer *writer)
    CO2_BEG(net::task<rpc::status_code>, (self, context, request, writer)) {
    ++self->calls; check_metadata(context->metadata);
    {
        metadata local; context->response_metadata.assign(local.view());
        local.value.assign(200, 'x'); // The setter must already have copied it.
    }
    if (self->overflow_body) writer->commit(writer->buffer().size + 1);
    else writer->assign(request);
    CO2_RETURN(self->result);
}
CO2_END
net::task<rpc::status_code> metadata_handler::invoke(rpc::server_context &ctx, w::bytes_view req, rpc::response_writer &out) {
    return respond(this, &ctx, req, &out);
}

std::vector<std::uint8_t> request(metadata const &values) {
    std::vector<std::uint8_t> data(256); std::string name = "Echo";
    auto h = w::encode_request_head({0, bytes(name), values.view()}, true, {data.data() + 16, data.size() - 16});
    CHECK(h.code == w::error::none);
    CHECK(w::encode_header({static_cast<std::uint32_t>(h.written + 1), 1, w::frame_type::request,
        static_cast<std::uint8_t>(w::end_stream | w::new_method), static_cast<std::uint16_t>(h.written), 1},
        {data.data(), 16}).code == w::error::none);
    data[16 + h.written] = 42; data.resize(17 + h.written); return data;
}

void server_metadata(std::size_t head_capacity, rpc::status_code status, bool overflow_body = false, bool delayed_close = false) {
    metadata values; metadata_handler handler; handler.result = status; handler.overflow_body = overflow_body;
    net::io_context context{net::default_backend, net::single_thread_hint};
    auto server = rpc::make_server(context, {{"Echo", 16, &handler, head_capacity}});
    auto pipe = std::make_shared<manual_pipe>();
    std::vector<std::uint8_t> preface(8); w::encode_preface({preface.data(), preface.size()});
    pipe->feed(preface); pipe->feed(test_client::peer_settings());
    CHECK(server->attach(std::make_unique<manual_transport>(pipe))); context.poll(); pipe->output.clear();
    pipe->block_writes = true; pipe->delay_close_write = delayed_close;
    pipe->feed(request(values)); context.poll();
    if (head_capacity > 1008) {
        CHECK(handler.calls == 0 && server->stats().response_bytes_in_use == 0);
        while (pipe->writer) { pipe->finish_write(10000); context.poll(); }
        auto frame = w::decode_frame({pipe->output.data(), pipe->output.size()});
        CHECK(frame.code == w::error::none && frame.value.header.aux == static_cast<unsigned>(rpc::status_code::resource_exhausted));
        server->close(); context.run(); return;
    }
    CHECK(handler.calls == 1 && server->stats().active_calls == 0);
    CHECK(server->stats().response_bytes_in_use != 0 && server->stats().control_bytes_in_use == 0);
    if (delayed_close) {
        server->close(); context.poll(); CHECK(server->stats().response_bytes_in_use != 0);
        pipe->abort(true); context.run();
    } else {
        pipe->finish_write(1); context.poll();
        while (pipe->writer) { pipe->finish_write(10000); context.poll(); }
        auto result = w::decode_frame({pipe->output.data(), pipe->output.size()}); CHECK(result.code == w::error::none);
        auto head = w::decode_end_head(result.value.head); CHECK(head.code == w::error::none);
        if (head_capacity == 2 || overflow_body) {
            CHECK(result.value.header.aux == static_cast<unsigned>(rpc::status_code::internal));
            CHECK(result.value.body.size == 0 && head.value.metadata.count == 0);
        } else {
            CHECK(result.value.header.aux == static_cast<unsigned>(status)); check_metadata(head.value.metadata);
            CHECK(result.value.body.size == (status == rpc::status_code::ok ? 1U : 0U));
            if (result.value.body.size) CHECK(result.value.body.data[0] == 42);
        }
        server->close(); context.run();
    }
    CHECK(server->stats().response_bytes_in_use == 0 && server->stats().connections == 0);
}

void binding_limits() {
    metadata_handler handler; net::io_context context{net::default_backend, net::single_thread_hint};
    for (auto cap : {std::size_t{0}, std::size_t{1}, std::size_t{65536}}) {
        bool rejected = false;
        try { rpc::make_server(context, {{"Echo", 16, &handler, cap}}); }
        catch (std::invalid_argument const &) { rejected = true; }
        CHECK(rejected);
    }
    bool duplicate = false;
    try { rpc::make_server(context, {{"Echo", 16, &handler}, {"Echo", 16, &handler}}); }
    catch (std::invalid_argument const &) { duplicate = true; }
    CHECK(duplicate);
}
} // namespace

int main() {
    try {
        client_metadata(); cancelled_definition(); client_limits(); binding_limits();
        server_metadata(64, rpc::status_code::ok); server_metadata(64, rpc::status_code::permission_denied);
        server_metadata(2, rpc::status_code::ok); server_metadata(64, rpc::status_code::ok, true);
        server_metadata(64, rpc::status_code::ok, false, true);
        server_metadata(1008, rpc::status_code::ok); server_metadata(1009, rpc::status_code::ok);
        std::cout << "PASS metadata lifetime, method publication, errors and budgets\n";
    } catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
