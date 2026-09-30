#include "common.hpp"
#include <asio.hpp>
#include <async_simple/coro/Collect.h>
#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <thread>

namespace cb = comparison_bench;
using async_simple::coro::Lazy;
namespace {
// Owning strings include ordinary client decoding and a server response copy.
std::string echo(std::string const &request) { return request; }
void serve() {
    coro_rpc::coro_rpc_server server{1, 0, "127.0.0.1", std::chrono::seconds{0}, true};
    server.register_handler<echo>();
    auto stopped = server.async_start();
    if (stopped.hasResult()) throw std::runtime_error{"coro_rpc server startup failed"};
    cb::ready(server.port()); cb::await_shutdown();
    server.stop(); std::move(stopped).get();
}
Lazy<void> run_lane(cb::options const &config, cb::phase &state, coro_rpc::coro_rpc_client &client) {
    auto body = cb::payload(config);
    for (;;) {
        auto const index = state.next.fetch_add(1, std::memory_order_relaxed);
        if (index >= state.samples.size()) break;
        cb::sequence(body, index);
        auto &sample = state.samples[index];
        sample.started_ns = cb::now_ns(state);
        auto pending = co_await client.send_request<echo>(body);
        auto result = co_await std::move(pending);
        sample.completed_ns = cb::now_ns(state);
        sample.status = result and result->result() == body ? 0 : 1;
    }
}
Lazy<std::int64_t> run_phase(cb::options const &config, cb::phase &state, coro_rpc::coro_rpc_client &client) {
    std::vector<Lazy<void>> lanes;
    lanes.reserve(config.inflight);
    for (std::size_t i = 0; i < config.inflight; ++i) lanes.push_back(run_lane(config, state, client));
    state.epoch = cb::clock::now();
    auto results = co_await async_simple::coro::collectAll(std::move(lanes));
    for (auto const &result : results) if (result.hasError()) std::rethrow_exception(result.getException());
    co_return cb::now_ns(state);
}
Lazy<void> shutdown(coro_rpc::coro_rpc_client &client) { client.close(); co_return; }
void client(cb::options const &config) {
    asio::io_context io{1};
    coro_io::ExecutorWrapper<> executor{io.get_executor()};
    auto work = asio::make_work_guard(io);
    auto worker = std::thread{[&io] { io.run(); }};
    try {
        coro_rpc::coro_rpc_client::config options;
        options.connect_timeout_duration = std::chrono::milliseconds{-1};
        options.request_timeout_duration = std::chrono::milliseconds{-1};
        coro_rpc::coro_rpc_client client{&executor, std::move(options)};
        auto const ec = async_simple::coro::syncAwait(client.connect("127.0.0.1", std::to_string(config.port)).via(&executor));
        if (ec) throw std::runtime_error{"coro_rpc connect failed"};
        cb::phase warm; warm.samples.resize(config.warmup);
        async_simple::coro::syncAwait(run_phase(config, warm, client).via(&executor));
        if (std::any_of(warm.samples.begin(), warm.samples.end(), [](auto const &s) { return s.status != 0; }))
            throw std::runtime_error{"warmup failed"};
        cb::phase measured; measured.samples.resize(config.iterations);
        rusage before{}, after{}; getrusage(RUSAGE_SELF, &before);
        auto const elapsed = async_simple::coro::syncAwait(run_phase(config, measured, client).via(&executor));
        getrusage(RUSAGE_SELF, &after);
        async_simple::coro::syncAwait(shutdown(client).via(&executor));
        work.reset(); worker.join();
        cb::finish(config, measured, elapsed, cb::cpu_seconds(after) - cb::cpu_seconds(before), "coro_rpc");
    } catch (...) { io.stop(); if (worker.joinable()) worker.join(); throw; }
}
} // namespace
int main(int argc, char **argv) {
    try {
        auto const config = cb::parse(argc, argv);
        if (config.server) cb::block_shutdown_signals();
        easylog::set_min_severity(easylog::Severity::CRITICAL);
        easylog::set_async(false); easylog::set_console(false);
        if (config.server) serve(); else client(config);
        return 0;
    } catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
