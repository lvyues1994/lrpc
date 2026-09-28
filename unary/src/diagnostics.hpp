#pragma once

#include <array>
#include <cstdint>

namespace rpc {
namespace detail {

// Internal, single-thread diagnostics. No storage or clock reads are added to
// the normal library build. Buckets contain [0,1], (1,2], (2,4], ... nanoseconds.
struct queue_distribution {
    std::array<std::uint64_t, 64> buckets{};
    std::uint64_t count = 0;
    std::uint64_t total_ns = 0;
    std::uint64_t maximum_ns = 0;
};
struct queue_metrics {
    queue_distribution requests{};
    queue_distribution responses{};
    queue_distribution controls{};
};
#ifdef LRPC_ENABLE_DIAGNOSTICS
struct queue_capture {
    queue_metrics metrics{};
    bool active = false;
};
inline queue_capture &local_queue_capture() {
    static thread_local queue_capture capture;
    return capture;
}
inline void record_queue_wait(queue_distribution &out, std::uint64_t ns) noexcept {
    std::size_t bucket = 0;
    auto value = ns == 0 ? 0 : ns - 1;
    while (value != 0) { ++bucket; value >>= 1; }
    if (bucket >= out.buckets.size()) bucket = out.buckets.size() - 1;
    ++out.buckets[bucket]; ++out.count; out.total_ns += ns;
    if (ns > out.maximum_ns) out.maximum_ns = ns;
}
#endif

} // namespace detail
} // namespace rpc
