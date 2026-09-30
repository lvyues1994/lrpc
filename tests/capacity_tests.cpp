#include "client_fixture.hpp"
#include <iostream>

namespace {
namespace wire = rpc::wire;
constexpr std::size_t request_charge = 1024 + sizeof(rpc::detail::block);
constexpr std::size_t response_charge = 32 + 18 + sizeof(rpc::detail::block);
constexpr std::size_t control_charge = 64 + sizeof(rpc::detail::block);

struct held_handler final : rpc::method_handler {
    net::task<rpc::status_code> invoke(rpc::server_context &, wire::bytes_view, rpc::response_writer &) override;
    std::array<rpc::detail::event, 16> gates{};
    std::array<bool, 16> finished{};
    std::size_t called = 0;
    bool hold = true;
};
auto handle(held_handler &self, wire::bytes_view input, rpc::response_writer &output)
    CO2_BEG(net::task<rpc::status_code>, (self, input, output), std::size_t index = 0;) {
    index = self.called++; CHECK(index < self.gates.size());
    if (self.hold) CO2_AWAIT(self.gates[index].wait());
    CHECK(output.assign(input)); self.finished[index] = true;
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END
net::task<rpc::status_code> held_handler::invoke(rpc::server_context &, wire::bytes_view in, rpc::response_writer &out) {
    return handle(*this, in, out);
}
rpc::server_options options() {
    rpc::server_options result{}; result.connection.receive = {1024, 1024, 8, 0, 16, 0};
    result.max_connections = 2; result.max_active_calls = 8;
    result.request_bytes = request_charge * 8; result.request_bytes_per_connection = request_charge * 8;
    result.response_bytes = response_charge * 8; result.response_bytes_per_connection = response_charge * 8;
    result.control_bytes = control_charge * 8; result.control_bytes_per_connection = control_charge * 8;
    return result;
}
struct server_fixture {
    explicit server_fixture(rpc::server_options config) : server(rpc::make_server(context, {{"Echo", 32, &handler}}, config)) {
        for (auto &pipe : pipes) {
            pipe = std::make_shared<manual_pipe>();
            pipe->input.reserve(65536); pipe->output.reserve(65536);
            std::vector<std::uint8_t> bytes(wire::preface_size);
            CHECK(wire::encode_preface({bytes.data(), bytes.size()}).code == wire::error::none);
            auto settings = test_client::peer_settings(); bytes.insert(bytes.end(), settings.begin(), settings.end());
            pipe->feed(bytes); CHECK(server->attach(std::make_unique<manual_transport>(pipe))); context.poll();
            CHECK(!pipe->closed); pipe->output.clear();
        }
    }
    ~server_fixture() { server->close(); for (auto &gate : handler.gates) gate.signal(); context.run(); }
    std::uint32_t request(std::size_t connection, std::uint64_t timeout = 0) {
        auto id = ++ids[connection]; auto fresh = id == 1;
        std::vector<std::uint8_t> bytes(128);
        wire::bytes_view name = fresh ? wire::bytes_view{reinterpret_cast<std::uint8_t const *>("Echo"), 4} : wire::bytes_view{};
        auto head = wire::encode_request_head({timeout, name, {}}, fresh, {bytes.data() + 16, 112});
        CHECK(head.code == wire::error::none);
        CHECK(wire::encode_header({static_cast<std::uint32_t>(head.written + 1), id, wire::frame_type::request,
            static_cast<std::uint8_t>(wire::end_stream | (fresh ? wire::new_method : 0)),
            static_cast<std::uint16_t>(head.written), 1}, {bytes.data(), 16}).code == wire::error::none);
        bytes.resize(16 + head.written + 1); bytes.back() = 42;
        pipes[connection]->feed(bytes); context.poll(); return id;
    }
    void ping(std::size_t connection) {
        std::vector<std::uint8_t> bytes(16);
        CHECK(wire::encode_header({0, 0, wire::frame_type::ping, 0, 0, 0}, {bytes.data(), 16}).code == wire::error::none);
        pipes[connection]->feed(bytes); context.poll();
    }
    bool rejected(std::size_t connection, std::uint32_t id) {
        auto &data = pipes[connection]->output;
        for (std::size_t offset = 0; offset < data.size();) {
            auto frame = wire::decode_frame({data.data() + offset, data.size() - offset});
            CHECK(frame.code == wire::error::none); offset += frame.consumed;
            if (frame.value.header.stream_id == id && frame.value.header.type == wire::frame_type::end)
                return frame.value.header.aux == static_cast<std::uint32_t>(rpc::status_code::resource_exhausted);
        }
        return false;
    }
    void flush(std::size_t connection) {
        auto &pipe = pipes[connection];
        for (unsigned i = 0; pipe->writer && i < 100; ++i) { pipe->finish_write(65536); context.poll(); }
        CHECK(!pipe->writer);
    }
    void release(std::size_t call) { handler.gates[call].signal(); context.poll(); }
    void zero() {
        server->close(); for (auto &gate : handler.gates) gate.signal(); context.run();
        auto s = server->stats();
        CHECK(s.connections == 0 && s.active_calls == 0 && s.request_bytes_in_use == 0 &&
              s.response_bytes_in_use == 0 && s.control_bytes_in_use == 0 && context.run() == 0);
    }
    net::io_context context{net::default_backend, net::single_thread_hint};
    held_handler handler;
    std::array<std::shared_ptr<manual_pipe>, 2> pipes{};
    std::array<std::uint32_t, 2> ids{};
    std::unique_ptr<rpc::server> server;
};

void server_capacity() {
    for (unsigned limit = 0; limit < 6; ++limit) {
        auto config = options();
        if (limit == 0) config.request_bytes_per_connection = request_charge;
        if (limit == 1) config.connection.receive.max_concurrent_streams = 1;
        if (limit == 2) config.request_bytes = 2 * request_charge;
        if (limit == 3) config.max_active_calls = 2;
        if (limit == 4) config.response_bytes_per_connection = response_charge;
        if (limit == 5) config.response_bytes = 2 * response_charge;
        server_fixture f{config};
        auto const responses = limit >= 4;
        if (responses) { f.handler.hold = false; f.pipes[0]->block_writes = true; f.pipes[1]->block_writes = true; }
        f.request(0); CHECK(f.handler.called == 1);
        auto const per_connection = limit == 0 || limit == 1 || limit == 4;
        if (!per_connection) { f.request(1); CHECK(f.handler.called == 2); }
        auto before = f.server->stats(); auto rejected = f.request(0);
        CHECK(f.handler.called == (per_connection ? 1 : 2));
        auto after = f.server->stats();
        CHECK(after.request_bytes_in_use == before.request_bytes_in_use && after.response_bytes_in_use == before.response_bytes_in_use);
        if (per_connection) { f.request(1); CHECK(f.handler.called == 2); }
        if (responses) {
            CHECK(f.server->stats().active_calls == 0 && f.server->stats().response_bytes_in_use == response_charge * 2);
            f.flush(0);
        } else f.release(0);
        CHECK(f.rejected(0, rejected));
        f.request(0); CHECK(f.handler.called == 3);
        f.zero();
    }
    for (bool shared : {false, true}) {
        auto config = options();
        if (shared) config.control_bytes = control_charge * 2;
        else config.control_bytes_per_connection = control_charge;
        server_fixture f{config};
        f.pipes[0]->block_writes = true; f.pipes[1]->block_writes = true;
        f.ping(0); f.ping(1); CHECK(!f.pipes[0]->closed && !f.pipes[1]->closed);
        CHECK(f.server->stats().control_bytes_in_use == control_charge * 2);
        f.ping(0); CHECK(f.pipes[0]->closed && !f.pipes[1]->closed);
        f.flush(1); f.pipes[1]->block_writes = false; f.ping(1);
        CHECK(!f.pipes[1]->closed); f.zero();
    }
}

void client_capacity() {
    for (bool request_pool : {false, true}) {
        test_client::call a, b, c, d;
        rpc::client_options config{}; config.connection.receive.max_frame_size = 1024;
        config.connection.receive.max_concurrent_streams = request_pool ? 8 : 2;
        if (request_pool) config.request_bytes = 2 * (1024 + 16 + sizeof(rpc::detail::block));
        test_client::fixture f{config}; f.pipe->block_writes = request_pool;
        f.start(a); f.start(b); f.context.poll();
        CHECK(f.client->stats().active_calls == 2);
        CHECK((f.client->stats().request_bytes_in_use != 0) == request_pool);
        f.start(c); f.context.poll();
        CHECK(c.completions == 1 && c.result.code == rpc::status_code::resource_exhausted);
        f.flush(); f.pipe->feed(test_client::reply(1)); f.context.poll();
        CHECK(a.completions == 1 && a.result.code == rpc::status_code::ok);
        f.start(d); f.context.poll(); f.flush();
        f.pipe->feed(test_client::reply(2)); f.pipe->feed(test_client::reply(3)); f.context.poll();
        CHECK(b.completions == 1 && d.completions == 1 && d.result.code == rpc::status_code::ok); f.zero();
    }
    for (bool local : {false, true}) {
        test_client::call a, b, c, d;
        rpc::client_options config{}; config.connection.receive.max_method_ids = local ? 2 : 8;
        test_client::fixture f{config, 8, local ? 8U : 2U};
        f.start(a, "A"); f.context.poll(); f.pipe->feed(test_client::reply(1)); f.context.poll();
        f.start(b, "B"); f.context.poll(); f.pipe->feed(test_client::reply(2)); f.context.poll();
        f.start(c, "C"); f.context.poll();
        CHECK(c.completions == 1 && c.result.code == rpc::status_code::resource_exhausted);
        f.start(d, "A"); f.context.poll(); f.pipe->feed(test_client::reply(3)); f.context.poll();
        CHECK(d.completions == 1 && d.result.code == rpc::status_code::ok); f.zero();
    }
}
void server_queue() {
    auto config = options(); config.max_active_calls = 1;
    config.max_queued_calls = 1; config.max_queued_bytes = 128;
    server_fixture f{config};
    f.request(0, 1000000); f.request(1, 1000000);
    CHECK(f.handler.called == 1 && f.server->stats().active_calls == 1 && f.server->stats().queued_calls == 1);
    auto rejected = f.request(0, 1000000); CHECK(f.rejected(0, rejected));
    f.release(0);
    CHECK(f.handler.called == 2 && f.server->stats().queued_calls == 0 && f.server->stats().active_calls == 1);
    f.release(1);
    f.request(0, 1000000); f.request(1, 1000);
    f.context.run_for(std::chrono::milliseconds{5});
    CHECK(f.handler.called == 3 && f.server->stats().queued_calls == 0);
    f.release(2); f.zero();
    server_fixture infinite{config}; infinite.request(0, 1000000);
    auto refused = infinite.request(1); CHECK(infinite.rejected(1, refused)); infinite.zero();
    server_fixture draining{config}; draining.request(0, 1000000); draining.request(1, 1000000);
    draining.server->drain(); draining.context.poll(); draining.release(0);
    CHECK(draining.handler.called == 1 && draining.server->stats().queued_calls == 0); draining.zero();
}
}
int main() {
    try { server_capacity(); client_capacity(); server_queue(); std::cout << "PASS independent quotas, tx retention, deadline-bounded server queue and client limits\n"; }
    catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
