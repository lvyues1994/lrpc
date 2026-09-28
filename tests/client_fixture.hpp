#pragma once

#include "check.hpp"
#include "manual_transport.hpp"
#include "../unary/src/connection.hpp"
#include <net/run_async.hpp>

namespace test_client {
namespace wire = rpc::wire;

inline std::vector<std::uint8_t> peer_settings(std::uint32_t streams = 8, std::uint32_t methods = 16) {
    std::vector<std::uint8_t> data(80);
    auto head = wire::encode_settings({1024, 1024, streams, 0, methods, 0}, {data.data() + 16, 64});
    CHECK(head.code == wire::error::none);
    CHECK(wire::encode_header({static_cast<std::uint32_t>(head.written), 0, wire::frame_type::settings,
        0, static_cast<std::uint16_t>(head.written), 0}, {data.data(), 16}).code == wire::error::none);
    data.resize(16 + head.written); return data;
}
inline std::vector<std::uint8_t> reply(std::uint32_t id, std::uint8_t value = 42) {
    std::vector<std::uint8_t> data(19, 0);
    CHECK(wire::encode_header({3, id, wire::frame_type::end, 0, 2, 0}, {data.data(), 16}).code == wire::error::none);
    data[18] = value; return data;
}
struct call {
    net::stop_source stop{};
    std::uint8_t input = 42;
    std::uint8_t output = 0;
    unsigned completions = 0;
    rpc::call_result result{};
    std::exception_ptr failure{};
};
struct fixture {
    explicit fixture(rpc::client_options config = {}, std::uint32_t streams = 8, std::uint32_t methods = 16)
        : client(rpc::make_client(context, config)) {
        pipe->input.reserve(1024 * 1024); pipe->output.reserve(1024 * 1024);
        pipe->feed(peer_settings(streams, methods));
        rpc::status_code connected = rpc::status_code::unknown;
        std::exception_ptr error;
        net::run_async(context.get_executor(), [&](rpc::status_code value) { connected = value; },
            [&](std::exception_ptr e) { error = e; })([&] { return client->attach(std::make_unique<manual_transport>(pipe)); });
        context.poll();
        if (error) std::rethrow_exception(error);
        CHECK(connected == rpc::status_code::ok); pipe->output.clear();
    }
    ~fixture() { client->close(); pipe->delay_close_write = false; pipe->abort(true); context.run(); }
    void start(call &value, std::string name = "Echo", rpc::call_options options = {}) {
        net::run_async(context.get_executor(), value.stop.get_token(), nullptr,
            [&value](rpc::call_result result) { value.result = result; ++value.completions; },
            [&value](std::exception_ptr error) { value.failure = error; ++value.completions; })(
            [this, &value, name = std::move(name), options] {
                return client->call(name, {&value.input, 1}, {&value.output, 1}, options);
            });
    }
    void flush() {
        for (unsigned i = 0; pipe->writer && i < 100; ++i) {
            pipe->finish_write(std::numeric_limits<std::size_t>::max()); context.poll();
        }
        CHECK(!pipe->writer);
    }
    void zero() {
        client->close(); context.run();
        auto s = client->stats();
        CHECK(s.active_calls == 0 && s.request_bytes_in_use == 0 && s.control_bytes_in_use == 0);
        CHECK(context.run() == 0);
    }
    net::io_context context{net::default_backend, net::single_thread_hint};
    std::shared_ptr<manual_pipe> pipe = std::make_shared<manual_pipe>();
    std::unique_ptr<rpc::client> client;
};
} // namespace test_client
