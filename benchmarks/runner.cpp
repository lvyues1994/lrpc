#include "bench.hpp"

#include <net/run_async.hpp>
#include <net/timer.hpp>

#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace bench {
namespace {

struct trial_state {
    net::io_context &context;
    channel &transport;
    options const &config;
    measurements &result;
    net::steady_timer &schedule_timer;
    std::vector<slot> slots;
    event phase_done{};
    std::exception_ptr failure{};
    std::size_t total = 0;
    std::size_t offered = 0;
    std::size_t completed = 0;
    std::size_t active = 0;
    bool measuring = false;
    bool finished = false;
};

void fail(trial_state &state, std::exception_ptr error) {
    if (!state.failure) state.failure = error;
    state.transport.close(); state.schedule_timer.cancel(); state.phase_done.signal();
}

void launch(trial_state &state, slot &storage, rpc::clock::time_point scheduled);

auto perform(trial_state &state, slot &storage)
    CO2_BEG(net::task<>, (state, storage), rpc::call_result reply;) {
    state.result.samples[storage.sample_index].started = rpc::clock::now();
    CO2_AWAIT_SET(reply, state.transport.call(storage));
    auto &record = state.result.samples[storage.sample_index];
    record.completed = rpc::clock::now(); record.code = reply.code;
    if (reply.code == rpc::status_code::ok) {
        if (reply.response_size != storage.request.size() || storage.request != storage.reply)
            throw std::runtime_error{"response validation failed; discard trial"};
    } else if (!state.measuring || state.config.transport == "net") {
        throw std::runtime_error{"warmup or raw transport failed; discard trial"};
    }
    CO2_RETURN();
}
CO2_END

void launch(trial_state &state, slot &storage, rpc::clock::time_point scheduled) {
    storage.busy = true; storage.sample_index = state.offered++;
    auto &record = state.result.samples[storage.sample_index];
    record = {}; record.scheduled = scheduled; record.offered = rpc::clock::now();
    state.result.last_offer = record.offered;
    auto const sequence = static_cast<std::uint64_t>(storage.sample_index);
    std::memcpy(storage.request.data(), &sequence, sizeof(sequence));
    ++state.active;
    if (state.measuring) state.result.maximum_inflight = std::max(state.result.maximum_inflight, state.active);
    net::run_async(state.context.get_executor(), [&state, &storage] {
        storage.busy = false; --state.active; ++state.completed;
        if (state.failure) return;
        if ((!state.measuring || state.config.rate == 0) && state.offered < state.total) {
            try { launch(state, storage, rpc::clock::now()); }
            catch (...) { fail(state, std::current_exception()); }
        }
        if (state.completed == state.total) state.phase_done.signal();
    }, [&state, &storage](std::exception_ptr error) {
        storage.busy = false; --state.active; fail(state, error);
    })([&state, &storage] { return perform(state, storage); });
}

void begin_closed(trial_state &state) {
    auto const limit = !state.measuring && state.config.transport == "rpc" ?
        std::min(state.slots.size(), state.config.rpc_streams) : state.slots.size();
    std::size_t count = 0;
    for (auto &storage : state.slots) {
        if (state.offered == state.total || count++ == limit) break;
        launch(state, storage, rpc::clock::now());
    }
}

auto offer_open(trial_state &state)
    CO2_BEG(net::task<>, (state), rpc::clock::time_point scheduled; net::io_result<> waited;
            std::size_t next_slot = 0; unsigned turn = 0; slot *available = nullptr;) {
    while (state.offered < state.total && !state.failure) {
        {
            auto const group = state.offered / state.config.burst * state.config.burst;
            auto const ns = static_cast<std::uint64_t>(group) * 1000000000ULL / state.config.rate;
            scheduled = state.result.epoch + std::chrono::nanoseconds{static_cast<std::int64_t>(ns)};
        }
        if (rpc::clock::now() < scheduled) {
            state.schedule_timer.expires_at(scheduled);
            CO2_AWAIT_SET(waited, state.schedule_timer.wait());
            if (waited.ec) break;
        }
        available = nullptr;
        for (std::size_t i = 0; i < state.slots.size(); ++i) {
            auto &candidate = state.slots[next_slot];
            next_slot = (next_slot + 1) % state.slots.size();
            if (!candidate.busy) { available = &candidate; break; }
        }
        if (available) launch(state, *available, scheduled);
        else {
            auto &record = state.result.samples[state.offered++];
            record = {}; record.scheduled = scheduled; record.offered = rpc::clock::now();
            record.completed = record.offered; record.generator_drop = true;
            state.result.last_offer = record.offered; ++state.completed;
        }
        // Catch-up remains visible in scheduled-to-start lag; never reset epoch.
        if (++turn == 64) { turn = 0; CO2_AWAIT((yield{})); }
    }
    if (state.completed == state.total) state.phase_done.signal();
    CO2_RETURN();
}
CO2_END

auto trial(trial_state &state)
    CO2_BEG(net::task<>, (state), rpc::status_code connected; cpu_times cpu_start;
            std::uint64_t alloc_start = 0;) {
    CO2_AWAIT_SET(connected, state.transport.connect());
    if (connected != rpc::status_code::ok) throw std::runtime_error{"benchmark connect failed"};
    state.total = state.config.warmup; begin_closed(state);
    CO2_AWAIT(state.phase_done.wait());
    if (state.failure) std::rethrow_exception(state.failure);
    state.measuring = true; state.total = state.config.iterations; state.offered = 0; state.completed = 0;
    state.transport.begin_measurement();
    std::fill(state.result.samples.begin(), state.result.samples.end(), sample{});
#ifdef LRPC_ENABLE_DIAGNOSTICS
    rpc::detail::local_queue_capture() = {};
    rpc::detail::local_queue_capture().active = true;
#endif
#ifdef LRPC_BENCH_COUNT_ALLOCATIONS
    if (!state.config.allocation_trace.empty()) begin_allocation_trace();
#endif
    cpu_start = cpu_now(); alloc_start = allocations(); state.result.epoch = rpc::clock::now();
    if (state.config.rate == 0) begin_closed(state);
    else CO2_AWAIT(offer_open(state));
    CO2_AWAIT(state.phase_done.wait());
    state.result.finished = rpc::clock::now(); state.result.new_calls = allocations() - alloc_start;
#ifdef LRPC_BENCH_COUNT_ALLOCATIONS
    end_allocation_trace();
#endif
#ifdef LRPC_ENABLE_DIAGNOSTICS
    state.result.queues = rpc::detail::local_queue_capture().metrics;
    rpc::detail::local_queue_capture().active = false;
#endif
    state.result.cpu = cpu_now(); state.result.cpu.user_ns -= cpu_start.user_ns; state.result.cpu.system_ns -= cpu_start.system_ns;
    if (state.failure) std::rethrow_exception(state.failure);
    CO2_RETURN();
}
CO2_END

} // namespace

measurements run(options const &config) {
    net::recycling_memory_resource frames;
    net::io_context context{config.backend, net::single_thread_hint};
    context.set_frame_allocator(config.recycle_frames ? static_cast<net::memory_resource *>(&frames) : net::new_delete_resource());
    auto transport = make_channel(context, config);
    net::steady_timer schedule_timer{context};
    measurements result{}; result.samples.resize(std::max(config.iterations, config.warmup));
    trial_state state{context, *transport, config, result, schedule_timer, std::vector<slot>(config.inflight)};
    for (auto &storage : state.slots) { storage.request.assign(config.bytes, 0x5a); storage.reply.resize(config.bytes); }
    try {
        net::run_async(context.get_executor(), [&] { state.finished = true; transport->close(); },
            [&](std::exception_ptr error) { state.finished = true; fail(state, error); })([&] { return trial(state); });
        context.run_for(std::chrono::seconds{30});
        if (!state.finished) fail(state, std::make_exception_ptr(std::runtime_error{"benchmark exceeded 30 seconds"}));
    } catch (...) { fail(state, std::current_exception()); }
    transport->close(); schedule_timer.cancel();
#ifdef LRPC_BENCH_COUNT_ALLOCATIONS
    end_allocation_trace();
#endif
#ifdef LRPC_ENABLE_DIAGNOSTICS
    rpc::detail::local_queue_capture().active = false;
#endif
    // A callback exception must not unwind driver-owned slots before pending
    // continuations have consumed their cancellation completions.
    for (;;) {
        try { context.run(); break; }
        catch (...) { fail(state, std::current_exception()); }
    }
    if (state.failure) std::rethrow_exception(state.failure);
    if (state.completed != config.iterations || state.active != 0) throw std::runtime_error{"incomplete trial"};
    result.samples.resize(config.iterations);
    return result;
}

} // namespace bench
