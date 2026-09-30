#include "bench.hpp"
#include <net/run_async.hpp>
#include <algorithm>
#include <iostream>

namespace {
// Same owned captures as today's raw call, with no network or codec work.
// This measures only the proposed coroutine/awaiter representation.
struct arguments {
    std::shared_ptr<int> owner;
    std::string method;
    rpc::wire::bytes_view request;
    rpc::wire::mutable_bytes_view response;
    rpc::call_options options;
};
struct direct_call {
    arguments args;
    net::continuation continuation{};
    bool await_ready() const noexcept { return false; }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
        continuation.h = handle;
        env->executor.post(continuation);
        return net::noop_coroutine();
    }
    rpc::call_result await_resume() const noexcept { return {rpc::status_code::ok, args.request.size}; }
};
auto wrapped_call(arguments args)
    CO2_BEG(net::task<rpc::call_result>, (args)) {
    CO2_AWAIT((bench::yield{}));
    CO2_RETURN((rpc::call_result{rpc::status_code::ok, args.request.size}));
}
CO2_END
struct result {
    std::vector<std::int64_t> latency = std::vector<std::int64_t>(100000);
    std::uint64_t count = 0;
};
auto run_probe(bool direct, arguments args, result &out)
    CO2_BEG(net::task<>, (direct, args, out), std::size_t i = 0; rpc::clock::time_point start;
            rpc::call_result reply; std::uint64_t before = 0;) {
    before = bench::allocations();
    for (; i < out.latency.size(); ++i) {
        start = rpc::clock::now();
        if (direct) { CO2_AWAIT_SET(reply, (direct_call{args, {}})); }
        else { CO2_AWAIT_SET(reply, wrapped_call(args)); }
        out.latency[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(rpc::clock::now() - start).count();
        if (reply.code != rpc::status_code::ok || reply.response_size != args.request.size) throw std::logic_error{"probe mismatch"};
    }
    out.count = bench::allocations() - before;
    CO2_RETURN();
}
CO2_END
}
int main() {
    for (bool direct : {false, true, true, false}) {
        net::io_context context{net::epoll, net::single_thread_hint};
        result measured;
        std::uint8_t byte = 0;
        arguments args{std::make_shared<int>(0), "bench/Echo", {&byte, 1}, {&byte, 1}, {}};
        std::exception_ptr failure;
        net::run_async(context.get_executor(), [] {}, [&](std::exception_ptr error) { failure = error; })
            ([&] { return run_probe(direct, args, measured); });
        context.run();
        if (failure) std::rethrow_exception(failure);
        std::sort(measured.latency.begin(), measured.latency.end());
        std::cout << "{\"api\":\"" << (direct ? "B_owned_awaiter" : "A_task")
            << "\",\"direct_awaiter_bytes\":" << sizeof(direct_call)
            << ",\"co2_inline_bytes\":" << CO2_AWAIT_STORAGE_SIZE
            << ",\"new_per_call\":" << static_cast<double>(measured.count) / static_cast<double>(measured.latency.size())
            << ",\"p50_ns\":" << measured.latency[measured.latency.size() / 2]
            << ",\"p99_ns\":" << measured.latency[measured.latency.size() * 99 / 100] << "}\n";
    }
}
