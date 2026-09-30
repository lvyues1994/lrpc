#include "check.hpp"
#include "manual_transport.hpp"
#include "../unary/src/connection.hpp"

#include <net/run_async.hpp>

#include <iostream>

namespace {

namespace wire = rpc::wire;

std::vector<std::uint8_t> settings(bool preface) {
    std::vector<std::uint8_t> bytes(80);
    std::size_t const offset = preface ? wire::preface_size : 0;
    if (preface) CHECK(wire::encode_preface({bytes.data(), offset}).code == wire::error::none);
    auto const head = wire::encode_settings({1024, 1024, 8, 0, 16, 0},
        {bytes.data() + offset + wire::header_size, bytes.size() - offset - wire::header_size});
    CHECK(head.code == wire::error::none);
    CHECK(wire::encode_header({static_cast<std::uint32_t>(head.written), 0, wire::frame_type::settings,
        0, static_cast<std::uint16_t>(head.written), 0}, {bytes.data() + offset, wire::header_size}).code == wire::error::none);
    bytes.resize(offset + wire::header_size + head.written);
    return bytes;
}

std::vector<std::uint8_t> request(std::uint32_t id, bool fresh = true, std::uint64_t timeout = 0) {
    std::vector<std::uint8_t> bytes(128);
    wire::bytes_view const name = fresh ? wire::bytes_view{reinterpret_cast<std::uint8_t const *>("Echo"), 4} : wire::bytes_view{};
    auto const head = wire::encode_request_head({timeout, name, {}},
        fresh, {bytes.data() + wire::header_size, bytes.size() - wire::header_size});
    CHECK(head.code == wire::error::none);
    CHECK(wire::encode_header({static_cast<std::uint32_t>(head.written + 3), id, wire::frame_type::request,
        static_cast<std::uint8_t>(wire::end_stream | (fresh ? wire::new_method : 0)),
        static_cast<std::uint16_t>(head.written), 1}, {bytes.data(), wire::header_size}).code == wire::error::none);
    bytes.resize(wire::header_size + head.written + 3);
    std::memcpy(bytes.data() + wire::header_size + head.written, "abc", 3);
    return bytes;
}

std::vector<std::uint8_t> control(wire::frame_type type, std::uint32_t id = 0, std::uint32_t aux = 0) {
    std::vector<std::uint8_t> bytes(wire::header_size);
    CHECK(wire::encode_header({0, id, type, 0, 0, aux}, {bytes.data(), bytes.size()}).code == wire::error::none);
    return bytes;
}

struct handler final : rpc::method_handler {
    net::task<rpc::status_code> invoke(rpc::server_context &, wire::bytes_view, rpc::response_writer &) override;
    rpc::detail::event gate{};
    bool blocked = false;
    unsigned called = 0;
    unsigned finished = 0;
};

auto handle(handler &self, wire::bytes_view input, rpc::response_writer &output)
    CO2_BEG(net::task<rpc::status_code>, (self, input, output)) {
    ++self.called;
    if (self.blocked) CO2_AWAIT(self.gate.wait());
    ++self.finished;
    CHECK(output.assign(input));
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

net::task<rpc::status_code> handler::invoke(rpc::server_context &, wire::bytes_view input,
                                          rpc::response_writer &output) { return handle(*this, input, output); }

rpc::server_options config() {
    rpc::server_options value{};
    value.connection.receive = {1024, 1024, 8, 0, 16, 0};
    value.max_connections = 2; value.max_active_calls = 8;
    value.request_bytes = 32 * 1024; value.response_bytes = 8192; value.control_bytes = 4096;
    return value;
}

struct fixture {
    explicit fixture(rpc::server_options options = config())
        : server(rpc::make_server(context, {{"Echo", 32, &service}}, options)) {}
    ~fixture() {
        if (server) server->close();
        pipe->delay_close_write = false; pipe->close();
        service.gate.signal();
        context.run();
    }
    void start(std::size_t read_chunk = 1024) {
        pipe->read_chunk = read_chunk;
        pipe->feed(settings(true));
        CHECK(server->attach(std::make_unique<manual_transport>(pipe)));
        context.poll();
        CHECK(!pipe->closed && server->stats().connections == 1);
        auto const result = wire::decode_frame({pipe->output.data(), pipe->output.size()});
        CHECK(result.code == wire::error::none && result.value.header.type == wire::frame_type::settings);
        CHECK(wire::decode_settings(result.value.head).value.max_frame_size == 1024);
        pipe->output.clear();
    }
    void close() {
        server->close(); context.run();
        auto const stats = server->stats();
        CHECK(stats.connections == 0 && stats.active_calls == 0);
        CHECK(stats.request_bytes_in_use == 0 && stats.response_bytes_in_use == 0 && stats.control_bytes_in_use == 0);
    }
    net::io_context context{net::default_backend, net::single_thread_hint};
    handler service{};
    std::shared_ptr<manual_pipe> pipe = std::make_shared<manual_pipe>();
    std::unique_ptr<rpc::server> server;
};

void partial_write_close() {
    fixture f; f.start();
    f.pipe->block_writes = true;
    f.pipe->feed(request(1)); f.context.poll();
    CHECK(f.service.finished == 1 && f.pipe->writer);
    auto const retained = f.server->stats().response_bytes_in_use;
    CHECK(retained != 0);
    f.pipe->finish_write(3); f.context.poll();
    CHECK(f.pipe->output.size() == 3 && f.pipe->writer);
    CHECK(f.server->stats().response_bytes_in_use == retained);
    f.server->close();
    CHECK(f.pipe->closed && f.server->stats().response_bytes_in_use == retained);
    f.close();
}

void split_and_coalesced_frames() {
    fixture f; f.start(1);
    auto bytes = request(1); auto next = request(2, false);
    bytes.insert(bytes.end(), next.begin(), next.end());
    f.pipe->feed(bytes); f.context.poll();
    CHECK(f.service.finished == 2 && !f.pipe->closed);
    std::size_t offset = 0;
    for (unsigned i = 1; i <= 2; ++i) {
        auto const response = wire::decode_frame({f.pipe->output.data() + offset, f.pipe->output.size() - offset});
        CHECK(response.code == wire::error::none);
        CHECK(response.value.header.type == wire::frame_type::end && response.value.header.stream_id == i);
        CHECK(response.value.header.aux == 0 && response.value.body.size == 3);
        CHECK(std::memcmp(response.value.body.data, "abc", 3) == 0);
        offset += response.consumed;
    }
    CHECK(offset == f.pipe->output.size()); f.close();
}

void protocol_errors() {
    { fixture f; f.start(); f.pipe->feed(settings(false)); f.context.poll(); CHECK(f.pipe->closed); f.close(); }
    { fixture f; f.start(); f.pipe->feed(control(wire::frame_type::goaway)); f.context.poll(); CHECK(f.pipe->closed); f.close(); }
    { fixture f; f.start(); f.pipe->feed(control(wire::frame_type::cancel, 1)); f.context.poll(); CHECK(f.pipe->closed); f.close(); }
    {
        fixture f; f.start(); f.pipe->feed(request(1)); f.context.poll();
        f.pipe->feed(request(1, false)); f.context.poll();
        CHECK(f.pipe->closed && f.service.called == 1); f.close();
    }
    {
        fixture f; f.start(); auto bytes = request(1); bytes[wire::header_size + 8] = 127;
        f.pipe->feed(bytes); f.context.poll(); CHECK(f.pipe->closed && f.service.called == 0); f.close();
    }
}

void control_pool_exhaustion() {
    auto options = config(); options.control_bytes = 64 + sizeof(rpc::detail::block);
    fixture f{options}; f.start(); f.pipe->block_writes = true;
    f.pipe->feed(control(wire::frame_type::ping)); f.context.poll();
    CHECK(f.pipe->writer && f.server->stats().control_bytes_in_use == options.control_bytes);
    f.pipe->feed(control(wire::frame_type::ping)); f.context.poll();
    CHECK(f.pipe->closed && f.service.called == 0); f.close();
}
void keepalive_waits_for_sent_ping() {
    auto options = config(); options.connection.keepalive_interval = std::chrono::milliseconds{1};
    options.connection.keepalive_timeout = std::chrono::milliseconds{2};
    fixture f{options}; f.start(); f.pipe->block_writes = true;
    f.context.run_for(std::chrono::milliseconds{10});
    CHECK(!f.pipe->closed && f.pipe->writer && f.pipe->output.empty());
    f.pipe->finish_write(65536); f.context.poll();
    auto ping = wire::decode_frame({f.pipe->output.data(), f.pipe->output.size()});
    CHECK(ping.code == wire::error::none && ping.value.header.type == wire::frame_type::ping);
    f.context.run_for(std::chrono::milliseconds{5}); CHECK(f.pipe->closed); f.close();
}

void deadline_owner_lifetime() {
    fixture f; f.start(); f.service.blocked = true;
    f.pipe->feed(request(1, true, 3600000000ULL)); f.context.poll();
    CHECK(f.service.called == 1 && f.service.finished == 0);
    f.server.reset(); f.context.poll(); // Reader/writer finish before handler and cancelled deadline task.
    CHECK(f.pipe->closed && f.service.finished == 0);
    f.service.gate.signal(); f.context.run();
    CHECK(f.service.finished == 1 && f.context.run() == 0);
}

void handshake_exception_cleanup() {
    net::io_context context{net::default_backend, net::single_thread_hint};
    auto client = rpc::make_client(context);
    auto pipe = std::make_shared<manual_pipe>(); pipe->throw_reads = true;
    bool failed = false;
    net::run_async(context.get_executor(), [](rpc::status_code) {},
        [&](std::exception_ptr error) { failed = static_cast<bool>(error); })([&] {
            return client->attach(std::make_unique<manual_transport>(pipe));
        });
    context.run();
    CHECK(failed && pipe->closed && !client->ready());
    CHECK(client->stats().control_bytes_in_use == 0);
}

void connection_capacity_lifetime(std::size_t receive_buffer_bytes = 0) {
    auto options = config(); options.max_connections = 1;
    options.connection.receive_buffer_bytes = receive_buffer_bytes;
    {
        fixture f{options}; f.start(); f.service.blocked = true;
        f.pipe->feed(request(1, true, 3600000000ULL)); f.context.poll();
        CHECK(f.service.called == 1);
        f.pipe->close(); f.context.poll();
        CHECK(f.server->stats().connections == 1 && f.server->stats().active_calls == 1);
        auto refused = std::make_shared<manual_pipe>();
        CHECK(!f.server->attach(std::make_unique<manual_transport>(refused)) && refused->closed);
        f.service.gate.signal(); f.context.run();
        CHECK(f.server->stats().connections == 0 && f.server->stats().active_calls == 0);
        f.pipe = std::make_shared<manual_pipe>(); f.start(); f.close();
    }
    {
        fixture f{options}; f.start();
        f.pipe->block_writes = true; f.pipe->delay_close_write = true;
        f.pipe->feed(request(1)); f.context.poll(); CHECK(f.pipe->writer);
        f.pipe->close(); f.context.poll();
        CHECK(f.server->stats().connections == 1 && f.server->stats().response_bytes_in_use != 0);
        auto refused = std::make_shared<manual_pipe>();
        CHECK(!f.server->attach(std::make_unique<manual_transport>(refused)) && refused->closed);
        f.pipe->abort(true); f.context.run();
        CHECK(f.server->stats().connections == 0 && f.server->stats().response_bytes_in_use == 0);
        f.pipe = std::make_shared<manual_pipe>(); f.start(); f.close();
    }
    {
        fixture f{options}; f.pipe->throw_reads = true;
        CHECK(f.server->attach(std::make_unique<manual_transport>(f.pipe)));
        f.context.run(); CHECK(f.pipe->closed && f.server->stats().connections == 0);
        f.pipe = std::make_shared<manual_pipe>(); f.start(); f.close();
    }
}

void pool_leases() {
    auto const charge = 32 + sizeof(rpc::detail::block);
    rpc::detail::block_pool pool{32, charge * 2};
    rpc::detail::byte_budget connection{charge, 0};
    rpc::detail::byte_budget other{charge * 2, 0};
    auto first = pool.acquire(connection); CHECK(first.get() != nullptr);
    CHECK(pool.acquire(connection).get() == nullptr);
    auto second = pool.acquire(other); CHECK(second.get() != nullptr);
    CHECK(pool.acquire(other).get() == nullptr);
    CHECK(pool.allocated() == charge * 2 && pool.in_use() == charge * 2);
    rpc::detail::tx_queue queue;
    auto *original = first.get(); queue.push(std::move(first)); queue.push(std::move(second));
    auto popped = queue.pop(); CHECK(popped.get() == original); queue.push_front(std::move(popped));
    CHECK(connection.used == charge && other.used == charge);
    queue.clear(); CHECK(connection.used == 0 && other.used == 0 && pool.in_use() == 0);
    CHECK(pool.allocated() == charge * 2); // Cached blocks remain charged to physical storage.
}

void configuration_limits() {
    net::io_context context{net::default_backend, net::single_thread_hint};
    handler service;
    rpc::method_binding method{}; method.name = "Echo"; method.handler = &service;
    bool rejected = false;
    try { auto server = rpc::make_server(context, {method}, config()); }
    catch (std::invalid_argument const &) { rejected = true; }
    CHECK(rejected); // Omitted response reservation is distinct from explicitly empty.
    method.max_response_bytes = 0;
    auto server = rpc::make_server(context, {method}, config());
    server->close();
    auto options = config(); options.response_bytes_per_connection = 1;
    rejected = false;
    try { auto invalid = rpc::make_server(context, {method}, options); }
    catch (std::invalid_argument const &) { rejected = true; }
    CHECK(rejected);
}

void receive_windows() {
    auto largest = config().connection;
    largest.receive_buffer_bytes = 16U * 1024U * 1024U + 16;
    rpc::detail::validate_options(largest); // Legal upper endpoint without allocating its read window.
    largest.receive.max_frame_size = 16U * 1024U * 1024U;
    rpc::detail::validate_options(largest);
    std::size_t pool_bytes = 0;
    for (auto window : {std::size_t{0}, std::size_t{1040}, std::size_t{65536}}) {
        auto options = config(); options.connection.receive_buffer_bytes = window;
        fixture f{options}; f.start(65536);
        CHECK(f.pipe->read_buffers.to_span()[0].size() == (window ? window : 1040));
        if (pool_bytes == 0) pool_bytes = f.server->stats().storage_bytes;
        CHECK(f.server->stats().storage_bytes == pool_bytes);
        auto input = request(1); auto second = request(2, false); auto third = request(3, false);
        input.insert(input.end(), second.begin(), second.end());
        input.insert(input.end(), third.begin(), third.begin() + 18);
        f.pipe->feed(input); f.context.poll(); CHECK(f.service.finished == 2);
        f.pipe->feed({third.begin() + 18, third.end()}); f.context.poll(); CHECK(f.service.finished == 3);
        std::size_t offset = 0;
        for (std::uint32_t id = 1; id <= 3; ++id) {
            auto reply = wire::decode_frame({f.pipe->output.data() + offset, f.pipe->output.size() - offset});
            CHECK(reply.code == wire::error::none && reply.value.header.stream_id == id);
            CHECK(reply.value.header.aux == 0 && reply.value.body.size == 3);
            CHECK(std::memcmp(reply.value.body.data, "abc", 3) == 0); offset += reply.consumed;
        }
        CHECK(offset == f.pipe->output.size());
        std::vector<std::uint8_t> oversized(1041);
        CHECK(wire::encode_header({1025, 4, wire::frame_type::request, wire::end_stream, 9, 1},
            {oversized.data(), 16}).code == wire::error::none);
        f.pipe->feed(oversized); f.context.poll(); CHECK(f.pipe->closed && f.service.called == 3); f.close();
    }
    for (auto window : {std::size_t{1039}, std::size_t{16U * 1024U * 1024U + 17}}) {
        auto options = config(); options.connection.receive_buffer_bytes = window;
        net::io_context context{net::default_backend, net::single_thread_hint};
        handler service; bool client_rejected = false, server_rejected = false;
        rpc::client_options client_config{}; client_config.connection = options.connection;
        try { auto client = rpc::make_client(context, client_config); }
        catch (std::invalid_argument const &) { client_rejected = true; }
        try { auto server = rpc::make_server(context, {{"Echo", 32, &service}}, options); }
        catch (std::invalid_argument const &) { server_rejected = true; }
        CHECK(client_rejected && server_rejected);
    }
    connection_capacity_lifetime(65536);
}

} // namespace

int main() {
    try {
        configuration_limits(); std::cout << "PASS explicit response limits and startup feasibility\n";
        receive_windows(); std::cout << "PASS receive windows preserve wire limits, pool budgets and fragmented frames\n";
        pool_leases(); std::cout << "PASS pool quotas/lease transfer/cache accounting\n";
        partial_write_close(); std::cout << "PASS partial write retains storage through close completion\n";
        split_and_coalesced_frames(); std::cout << "PASS bytewise and coalesced frames\n";
        protocol_errors(); std::cout << "PASS connection protocol errors\n";
        control_pool_exhaustion(); std::cout << "PASS independent control pool exhaustion\n";
        keepalive_waits_for_sent_ping(); std::cout << "PASS keepalive timeout starts after PING write\n";
        deadline_owner_lifetime(); std::cout << "PASS deadline owner outlives I/O and facade\n";
        connection_capacity_lifetime(); std::cout << "PASS connection capacity retained until physical release\n";
        handshake_exception_cleanup(); std::cout << "PASS handshake exception cleanup\n";
    } catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
