#include "check.hpp"
#include "backend.hpp"
#include <rpc/unary.hpp>
#include <net/run_async.hpp>
#include <net/timeout.hpp>
#include <array>
#include <atomic>
#include <iostream>
#include <thread>

namespace {
struct handler final : rpc::method_handler {
    net::task<rpc::status_code> invoke(rpc::server_context &, rpc::wire::bytes_view, rpc::response_writer &) override;
    std::atomic<bool> started{false};
};
auto echo(handler &owner, rpc::wire::bytes_view input, rpc::response_writer &output)
    CO2_BEG(net::task<rpc::status_code>, (owner, input, output), net::io_result<> waited;) {
    owner.started.store(true, std::memory_order_release);
    CO2_AWAIT_SET(waited, net::delay(std::chrono::microseconds{100 + input.data[0] * 20}));
    if (waited.ec) CO2_RETURN(rpc::status_code::cancelled);
    CHECK(output.assign(input)); CO2_RETURN(rpc::status_code::ok);
}
CO2_END
net::task<rpc::status_code> handler::invoke(rpc::server_context &, rpc::wire::bytes_view input, rpc::response_writer &output) {
    return echo(*this, input, output);
}
struct invocation {
    net::stop_source stop{};
    std::uint8_t request = 0, response = 255;
    unsigned completed = 0;
    rpc::call_result result{};
};
struct wave {
    explicit wave(net::backend_kind backend, unsigned seed) : context(backend, net::single_thread_hint) {
        rpc::server_options so{}; so.connection.receive = {1024, 1024, 64, 0, 16, 0};
        so.max_connections = 1; so.max_active_calls = 64;
        so.request_bytes = 128 * 1024; so.response_bytes = 16 * 1024;
        rpc::client_options co{}; co.connection = so.connection; co.request_bytes = 128 * 1024;
        server = rpc::make_server(context, {{"Echo", 1, &service}}, so); client = rpc::make_client(context, co);
        endpoint = server->listen({net::ip::address_v4::loopback(), 0});
        for (std::size_t i = 0; i < calls.size(); ++i) calls[i].request = static_cast<std::uint8_t>((i * 17 + seed * 13) % 64);
    }
    ~wave() { stop_thread(); close(); context.run(); }
    void close() noexcept { client->close(); server->close(); }
    void stop_thread() { done.store(true, std::memory_order_release); if (requester.joinable()) requester.join(); }
    void complete(invocation &call, rpc::call_result result) {
        call.result = result; ++call.completed; ++completed;
        if (requesting.load(std::memory_order_acquire)) ++overlapping;
        if (completed == calls.size()) { done.store(true, std::memory_order_release); close(); }
    }
    net::io_context context;
    handler service;
    std::array<invocation, 64> calls{};
    std::unique_ptr<rpc::server> server;
    std::unique_ptr<rpc::client> client;
    net::ip::tcp::endpoint endpoint;
    std::thread requester;
    std::atomic<bool> done{false}, requesting{false};
    std::atomic<std::uint64_t> requests{0};
    std::size_t completed = 0, overlapping = 0;
    std::exception_ptr failure;
};
auto start(wave &self, unsigned round)
    CO2_BEG(net::task<>, (self, round), rpc::status_code connected; net::io_result<> waited;) {
    CO2_AWAIT_SET(connected, self.client->connect(self.endpoint)); CHECK(connected == rpc::status_code::ok);
    for (auto &entry : self.calls) {
        auto *call = &entry;
        auto timeout = call->request % 4 == 0 ? std::chrono::microseconds{250} : std::chrono::microseconds{50000};
        net::run_async(self.context.get_executor(), call->stop.get_token(), nullptr,
            [owner = &self, call](rpc::call_result result) { owner->complete(*call, result); },
            [owner = &self](std::exception_ptr e) { owner->failure = e; owner->close(); })(
            [owner = &self, call, timeout] { return owner->client->call("Echo", {&call->request, 1}, {&call->response, 1}, {rpc::clock::time_point::max(), timeout}); });
    }
    if (round % 3 == 0) {
        CO2_AWAIT_SET(waited, net::delay(std::chrono::microseconds{400})); self.server->close();
    }
    CO2_RETURN();
}
CO2_END

void run_wave(net::backend_kind backend, unsigned round, std::uint64_t &requests, std::size_t &overlap) {
    wave state{backend, round};
    state.requester = std::thread{[&] {
        while (!state.done.load(std::memory_order_acquire) && !state.service.started.load(std::memory_order_acquire)) std::this_thread::yield();
        state.requesting.store(true, std::memory_order_release);
        while (!state.done.load(std::memory_order_acquire)) {
            for (std::size_t i = round % 3; i < state.calls.size(); i += 3) {
                state.calls[i].stop.request_stop(); state.requests.fetch_add(1, std::memory_order_relaxed);
            }
            std::this_thread::yield();
        }
        state.requesting.store(false, std::memory_order_release);
    }};
    net::run_async(state.context.get_executor(), [] {}, [&](std::exception_ptr error) { state.failure = error; state.close(); })([&] { return start(state, round); });
    state.context.run_for(std::chrono::seconds{5});
    auto const timed_out = state.completed != state.calls.size() && !state.failure;
    state.stop_thread(); state.close(); state.context.run();
    if (state.failure) std::rethrow_exception(state.failure);
    CHECK(!timed_out);
    CHECK(state.completed == state.calls.size());
    for (auto const &call : state.calls) {
        CHECK(call.completed == 1);
        if (call.result.code == rpc::status_code::ok) CHECK(call.result.response_size == 1 && call.request == call.response);
        else CHECK(call.result.code == rpc::status_code::cancelled || call.result.code == rpc::status_code::deadline_exceeded ||
                   call.result.code == rpc::status_code::unavailable);
    }
    auto c = state.client->stats(); auto s = state.server->stats();
    CHECK(c.active_calls == 0 && c.request_bytes_in_use == 0 && c.control_bytes_in_use == 0);
    CHECK(s.connections == 0 && s.active_calls == 0 && s.request_bytes_in_use == 0 && s.response_bytes_in_use == 0 && s.control_bytes_in_use == 0);
    CHECK(state.context.run() == 0);
    requests += state.requests.load(); overlap += state.overlapping;
}
}
int main(int argc, char **argv) {
    try {
        auto backend = test_backend(argc, argv);
        if (!net::backend_available(backend)) { std::cout << "SKIP backend unavailable\n"; return 77; }
        std::uint64_t requests = 0; std::size_t overlap = 0;
        for (unsigned round = 0; round < 32; ++round) run_wave(backend, round, requests, overlap);
        CHECK(requests > 0 && overlap > 0);
        std::cout << "PASS 2048 calls, seed sequence 0..31, concurrent stop requests=" << requests << ", overlapping completions=" << overlap << '\n';
    } catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
