#pragma once

#include "event.hpp"
#include <rpc/unary.hpp>
#include "../unary/src/diagnostics.hpp"

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace bench {

struct options {
    std::string transport = "rpc";
    net::backend_kind backend = net::backend_kind::epoll;
    std::size_t bytes = 64;
    std::size_t inflight = 1;
    std::size_t iterations = 100000;
    std::size_t warmup = 5000;
    std::size_t rpc_streams = 64;
    std::uint64_t rate = 0; // Zero selects closed-loop; otherwise fixed offered calls/s.
    std::size_t burst = 1;
    std::size_t receive_buffer_bytes = 65536;
    std::uint64_t deadline_us = 0;
    std::uint64_t max_schedule_lag_us = 100;
    bool recycle_frames = false;
    std::string samples{};
    std::string allocation_trace{};
    std::string role = "both";
    std::uint16_t port = 0;
};

struct sample {
    rpc::clock::time_point scheduled{};
    rpc::clock::time_point offered{};
    rpc::clock::time_point started{};
    rpc::clock::time_point completed{};
    rpc::status_code code = rpc::status_code::unknown;
    bool generator_drop = false;
};

struct slot {
    std::vector<std::uint8_t> request{};
    std::vector<std::uint8_t> reply{};
    event ready{};
    std::size_t sample_index = 0;
    bool busy = false;
    bool sent = false;
    bool received = false;
};

struct channel {
    virtual ~channel() = default;
    virtual net::task<rpc::status_code> connect() = 0;
    virtual net::task<rpc::call_result> call(slot &storage) = 0;
    virtual void begin_measurement() noexcept = 0;
    virtual void close() noexcept = 0;
};

std::unique_ptr<channel> make_channel(net::io_context &context, options const &config);

struct cpu_times { double user_ns = 0; double system_ns = 0; };
cpu_times cpu_now();

#ifdef LRPC_BENCH_COUNT_ALLOCATIONS
std::uint64_t allocations() noexcept;
void begin_allocation_trace();
void end_allocation_trace() noexcept;
void write_allocation_trace(std::string const &path);
#else
inline std::uint64_t allocations() noexcept { return 0; }
#endif

struct measurements {
    std::vector<sample> samples{};
    rpc::clock::time_point epoch{};
    rpc::clock::time_point last_offer{};
    rpc::clock::time_point finished{};
    cpu_times cpu{};
    std::uint64_t new_calls = 0;
    std::size_t maximum_inflight = 0;
    rpc::detail::queue_metrics queues{};
};

measurements run(options const &config);
void report(options const &config, measurements const &result);
void serve(options const &config);

} // namespace bench
