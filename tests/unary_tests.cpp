#include "check.hpp"
#include "backend.hpp"
#include <rpc/unary.hpp>
#include "../unary/src/connection.hpp"

#include <net/run_async.hpp>
#include <net/timeout.hpp>

#include <array>
#include <cstring>
#include <iostream>
#include <thread>

namespace {
net::backend_kind selected_backend = net::default_backend_t::kind;

struct test_handler final : rpc::method_handler {
    net::task<rpc::status_code> invoke(rpc::server_context &, rpc::wire::bytes_view request,
                                      rpc::response_writer &response) override;
    unsigned calls = 0;
    unsigned finished = 0;
    bool delayed = false;
    bool oversized = false;
    bool throws = false;
    std::vector<unsigned> order{};
};

auto handle(test_handler &self, rpc::wire::bytes_view request, rpc::response_writer &response)
    CO2_BEG(net::task<rpc::status_code>, (self, request, response), net::io_result<> slept;) {
    ++self.calls;
    if (self.delayed && request.size != 0 && request.data[0] != 0)
        CO2_AWAIT_SET(slept, net::delay(std::chrono::milliseconds{request.data[0]}));
    ++self.finished;
    if (self.throws) throw std::runtime_error{"handler failure must not escape"};
    if (slept.ec) CO2_RETURN(rpc::status_code::cancelled);
    if (request.size != 0) self.order.push_back(request.data[0]);
    if (self.oversized) response.commit(response.buffer().size + 1);
    else response.assign(request);
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

net::task<rpc::status_code> test_handler::invoke(rpc::server_context &, rpc::wire::bytes_view request,
                                               rpc::response_writer &response) { return handle(*this, request, response); }

rpc::server_options server_config() {
    rpc::server_options config{};
    config.connection.receive = {8192, 8192, 8, 0, 16, 0};
    config.max_connections = 4; config.max_active_calls = 16;
    config.request_bytes = 512 * 1024; config.response_bytes = 64 * 1024;
    config.control_bytes = 16 * 1024;
    config.request_bytes_per_connection = 128 * 1024;
    config.response_bytes_per_connection = 16 * 1024;
    config.control_bytes_per_connection = 4096;
    return config;
}
rpc::client_options client_config() {
    rpc::client_options config{};
    config.connection = server_config().connection;
    config.request_bytes = 128 * 1024; config.control_bytes = 4096;
    return config;
}

struct fixture {
    explicit fixture(rpc::server_options sc = server_config(), rpc::client_options cc = client_config(),
                     std::size_t response_limit = 256, std::string method = "test/Echo")
        : server(rpc::make_server(context, {{std::move(method), response_limit, &handler}}, sc)),
          client(rpc::make_client(context, cc)), endpoint(server->listen({net::ip::address_v4::loopback(), 0})) {}

    template <class F> void run(F factory) {
        std::exception_ptr failure;
        auto close = [&] { client->close(); server->close(); };
        net::run_async(context.get_executor(), close, [&](std::exception_ptr error) {
            failure = error; close();
        })(factory);
        context.run();
        if (failure) std::rethrow_exception(failure);
        CHECK(server->stats().active_calls == 0);
        CHECK(server->stats().request_bytes_in_use == 0);
        CHECK(server->stats().response_bytes_in_use == 0);
        CHECK(server->stats().control_bytes_in_use == 0);
        CHECK(client->stats().active_calls == 0);
        CHECK(client->stats().request_bytes_in_use == 0);
        CHECK(client->stats().control_bytes_in_use == 0);
        CHECK(context.run() == 0);
    }

    net::io_context context{selected_backend, net::single_thread_hint};
    test_handler handler{};
    std::unique_ptr<rpc::server> server;
    std::unique_ptr<rpc::client> client;
    net::ip::tcp::endpoint endpoint;
};

auto basic(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::call_result result;
            std::array<std::uint8_t, 64> request{}; std::array<std::uint8_t, 64> response{}; int i = 0;) {
    CO2_AWAIT_SET(result, f.client->call("test/Echo", {}, {}));
    CHECK(result.code == rpc::status_code::unavailable);
    CO2_AWAIT_SET(connected, f.client->connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok && f.client->ready());
    request.fill(0x5a);
    for (i = 0; i < 3; ++i) {
        CO2_AWAIT_SET(result, f.client->call("test/Echo", {request.data(), request.size()}, {response.data(), response.size()}));
        CHECK(result.code == rpc::status_code::ok && result.response_size == request.size());
        CHECK(request == response);
    }
    for (i = 0; i < 2; ++i) {
        CO2_AWAIT_SET(result, f.client->call("missing/Method", {}, {}));
        CHECK(result.code == rpc::status_code::unimplemented);
    }
    CHECK(f.handler.calls == 3);
    f.handler.oversized = true;
    CO2_AWAIT_SET(result, f.client->call("test/Echo", {}, {}));
    CHECK(result.code == rpc::status_code::internal);
    f.handler.oversized = false; f.handler.throws = true;
    CO2_AWAIT_SET(result, f.client->call("test/Echo", {}, {}));
    CHECK(result.code == rpc::status_code::internal);
    f.handler.throws = false;
    CO2_AWAIT_SET(result, f.client->call("test/Echo", {}, {}));
    CHECK(result.code == rpc::status_code::ok);
    CO2_RETURN();
}
CO2_END

auto concurrent(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; unsigned done = 0;
            std::array<std::uint8_t, 8> requests{};
            std::array<std::uint8_t, 8> replies{}; std::array<rpc::call_result, 8> results{};
            std::exception_ptr failure;) {
    f.handler.delayed = true;
    requests = {{32,28,24,20,16,12,8,4}};
    CO2_AWAIT_SET(connected, f.client->connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    for (std::size_t i = 0; i < requests.size(); ++i) {
        net::run_async(f.context.get_executor(), [&, i](rpc::call_result value) { results[i] = value; ++done; },
            [&](std::exception_ptr error) { failure = error; ++done; })([&, i] {
                return f.client->call("test/Echo", {&requests[i], 1}, {&replies[i], 1});
            });
    }
    while (done != requests.size()) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    if (failure) std::rethrow_exception(failure);
    CHECK(requests == replies);
    for (auto const &result : results) CHECK(result.code == rpc::status_code::ok && result.response_size == 1);
    CHECK(f.handler.order.front() == 4 && f.handler.order.back() == 32);
    CO2_RETURN();
}
CO2_END

auto cancellation(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::call_result result;
            net::stop_source stop; std::uint8_t slow = 100; std::uint8_t fast = 0;
            std::uint8_t response = 0; bool done = false; rpc::call_result stopped; std::exception_ptr failure;) {
    f.handler.delayed = true;
    CO2_AWAIT_SET(connected, f.client->connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    net::run_async(f.context.get_executor(), stop.get_token(), nullptr,
        [&](rpc::call_result value) { stopped = value; done = true; },
        [&](std::exception_ptr error) { failure = error; done = true; })([&] {
            return f.client->call("test/Echo", {&slow, 1}, {&response, 1});
        });
    while (f.handler.calls == 0) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    { std::thread requester{[&] { stop.request_stop(); }}; requester.join(); }
    while (!done) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    if (failure) std::rethrow_exception(failure);
    CHECK(stopped.code == rpc::status_code::cancelled);
    CO2_AWAIT_SET(result, f.client->call("test/Echo", {&slow, 1}, {&response, 1},
        {rpc::clock::time_point::max(), std::chrono::milliseconds{2}}));
    CHECK(result.code == rpc::status_code::deadline_exceeded);
    CO2_AWAIT_SET(result, f.client->call("test/Echo", {&fast, 1}, {&response, 1}));
    CHECK(result.code == rpc::status_code::ok);
    CO2_AWAIT_SET(result, f.client->call("test/Echo", {}, {}, {rpc::clock::now() - std::chrono::seconds{1}, {}}));
    CHECK(result.code == rpc::status_code::deadline_exceeded);
    CO2_RETURN();
}
CO2_END

auto cancelled_definition(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::call_result result;
            net::stop_source stop; bool done = false; rpc::call_result stopped; std::exception_ptr failure;) {
    CO2_AWAIT_SET(connected, f.client->connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    CO2_AWAIT((rpc::detail::yield_awaiter{})); // Let the writer enter its empty wait.
    net::run_async(f.context.get_executor(), stop.get_token(), nullptr,
        [&](rpc::call_result value) { stopped = value; done = true; },
        [&](std::exception_ptr error) { failure = error; done = true; })([&] { return f.client->call("test/Echo", {}, {}); });
    CO2_AWAIT((rpc::detail::yield_awaiter{})); // Call starts, its writer wake is behind this continuation.
    stop.request_stop();
    CO2_AWAIT_SET(result, f.client->call("test/Echo", {}, {}));
    CHECK(result.code == rpc::status_code::ok);
    while (!done) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    if (failure) std::rethrow_exception(failure);
    CHECK(stopped.code == rpc::status_code::cancelled && f.handler.calls == 1);
    CO2_RETURN();
}
CO2_END

auto response_budget(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::call_result result;
            std::uint8_t slow = 20; std::uint8_t response = 0; bool done = false;
            rpc::call_result first; std::exception_ptr failure;) {
    f.handler.delayed = true;
    CO2_AWAIT_SET(connected, f.client->connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    net::run_async(f.context.get_executor(), [&](rpc::call_result value) { first = value; done = true; },
        [&](std::exception_ptr error) { failure = error; done = true; })([&] {
            return f.client->call("test/Echo", {&slow, 1}, {&response, 1});
        });
    while (f.handler.calls == 0) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    CHECK(f.server->stats().response_bytes_in_use == 18 + 32 + sizeof(rpc::detail::block));
    CO2_AWAIT_SET(result, f.client->call("test/Echo", {}, {}));
    CHECK(result.code == rpc::status_code::resource_exhausted && f.handler.calls == 1);
    while (!done) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    if (failure) std::rethrow_exception(failure);
    CHECK(first.code == rpc::status_code::ok);
    CO2_AWAIT_SET(result, f.client->call("test/Echo", {}, {}));
    CHECK(result.code == rpc::status_code::ok && f.handler.calls == 2);
    CO2_RETURN();
}
CO2_END

auto peer_response_limit(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::call_result result;) {
    CO2_AWAIT_SET(connected, f.client->connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    CO2_AWAIT_SET(result, f.client->call("test/Echo", {}, {}));
    CHECK(result.code == rpc::status_code::resource_exhausted && f.handler.calls == 0);
    CO2_RETURN();
}
CO2_END

auto compact_method(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::call_result result;
            std::array<std::uint8_t, 27> request{}; std::array<std::uint8_t, 27> response{};) {
    CO2_AWAIT_SET(connected, f.client->connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    CO2_AWAIT_SET(result, f.client->call("abcdefghijklmnopqrstuvwx", {}, {}));
    CHECK(result.code == rpc::status_code::ok);
    CO2_AWAIT_SET(result, f.client->call("abcdefghijklmnopqrstuvwx", {request.data(), request.size()}, {response.data(), response.size()}));
    CHECK(result.code == rpc::status_code::ok && result.response_size == 27);
    CO2_RETURN();
}
CO2_END

auto graceful_drain(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; bool done = false;
            std::uint8_t slow = 10; std::uint8_t response = 0; rpc::call_result result; std::exception_ptr failure;) {
    f.handler.delayed = true;
    CO2_AWAIT_SET(connected, f.client->connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    net::run_async(f.context.get_executor(), [&](rpc::call_result value) { result = value; done = true; },
        [&](std::exception_ptr error) { failure = error; done = true; })([&] {
            return f.client->call("test/Echo", {&slow, 1}, {&response, 1});
        });
    while (f.handler.calls == 0) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    f.server->drain();
    while (!done) CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    if (failure) std::rethrow_exception(failure);
    CHECK(result.code == rpc::status_code::ok && response == slow);
    CO2_AWAIT_SET(result, f.client->call("test/Echo", {}, {}));
    CHECK(result.code == rpc::status_code::unavailable);
    CO2_RETURN();
}
CO2_END

} // namespace

int main(int argc, char **argv) {
    try {
        selected_backend = test_backend(argc, argv);
        if (!net::backend_available(selected_backend)) { std::cout << "SKIP backend unavailable\n"; return 77; }
        { fixture f; f.run([&] { return basic(f); }); std::cout << "PASS basic/unknown/oversized/exception\n"; }
        { fixture f; f.run([&] { return concurrent(f); }); std::cout << "PASS multiplexed reverse completion\n"; }
        { fixture f; f.run([&] { return cancellation(f); }); std::cout << "PASS cross-thread cancel/deadline\n"; }
        { fixture f; f.run([&] { return cancelled_definition(f); }); std::cout << "PASS cancelled first method definition\n"; }
        {
            auto config = server_config(); config.response_bytes = 18 + 32 + sizeof(rpc::detail::block);
            fixture f{config, client_config(), 32}; f.run([&] { return response_budget(f); });
            std::cout << "PASS response reservation/refusal/recovery\n";
        }
        {
            auto config = client_config(); config.connection.receive.max_message_size = 16;
            fixture f{server_config(), config}; f.run([&] { return peer_response_limit(f); });
            std::cout << "PASS peer response cap before handler\n";
        }
        {
            auto sc = server_config(); sc.connection.receive.max_frame_size = 36;
            fixture f{sc, client_config(), 27, "abcdefghijklmnopqrstuvwx"}; f.run([&] { return compact_method(f); });
            std::cout << "PASS interned method frame boundary\n";
        }
        { fixture f; f.run([&] { return graceful_drain(f); }); std::cout << "PASS GOAWAY drain\n"; }
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
