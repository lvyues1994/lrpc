#include "bench.hpp"

#include <atomic>
#include <cstdlib>
#include <new>
#include <execinfo.h>
#include <fstream>
#include <iomanip>
#include <stdexcept>

namespace {
std::atomic<std::uint64_t> count{0};
struct allocation_site {
    std::array<void *, 24> stack{};
    int depth = 0;
    std::uint64_t calls = 0;
    std::uint64_t bytes = 0;
};
std::array<allocation_site, 2048> sites{};
std::uint64_t lost = 0;
thread_local bool tracing = false;
thread_local bool capturing = false;

void capture(std::size_t size) noexcept {
    if (!tracing || capturing) return;
    capturing = true;
    std::array<void *, 24> stack{};
    auto const depth = ::backtrace(stack.data(), static_cast<int>(stack.size()));
    std::uintptr_t hash = 0;
    for (int i = 0; i < depth; ++i) hash = hash * 131U + reinterpret_cast<std::uintptr_t>(stack[static_cast<std::size_t>(i)]);
    bool recorded = false;
    for (std::size_t probe = 0; probe < sites.size(); ++probe) {
        auto &site = sites[(hash + probe) % sites.size()];
        if (site.calls != 0 && (site.depth != depth || site.stack != stack)) continue;
        site.stack = stack; site.depth = depth; ++site.calls; site.bytes += size;
        recorded = true; break;
    }
    if (!recorded) ++lost;
    capturing = false;
}
void *counted_allocate(std::size_t size) {
    static auto const fail_at = [] {
        auto const *value = std::getenv("LRPC_BENCH_FAIL_NEW_AT");
        return value ? std::strtoull(value, nullptr, 10) : 0ULL;
    }();
    auto const current = count.fetch_add(1, std::memory_order_relaxed) + 1;
    if (fail_at != 0 && current == fail_at) throw std::bad_alloc{};
    capture(size);
    if (auto *pointer = std::malloc(size == 0 ? 1 : size)) return pointer;
    throw std::bad_alloc{};
}
} // namespace

namespace bench {
std::uint64_t allocations() noexcept { return count.load(std::memory_order_relaxed); }
void begin_allocation_trace() {
    sites = {}; lost = 0;
    void *warmup[24]; static_cast<void>(::backtrace(warmup, 24));
    tracing = true;
}
void end_allocation_trace() noexcept { tracing = false; }
void write_allocation_trace(std::string const &path) {
    std::ofstream output{path};
    if (!output) throw std::runtime_error{"cannot open allocation trace"};
    output << "{\"lost\":" << lost << ",\"stack_capacity\":24,\"sites\":[";
    bool comma = false;
    for (auto const &site : sites) {
        if (site.calls == 0) continue;
        if (comma) output << ',';
        comma = true;
        output << "{\"calls\":" << site.calls << ",\"bytes\":" << site.bytes << ",\"return_addresses\":[";
        for (int i = 0; i < site.depth; ++i) {
            if (i) output << ',';
            output << '"' << std::hex << reinterpret_cast<std::uintptr_t>(site.stack[static_cast<std::size_t>(i)]) << std::dec << '"';
        }
        output << "]}";
    }
    output << "]}\n"; output.flush();
    if (!output) throw std::runtime_error{"allocation trace write failed"};
}
} // namespace bench

void *operator new(std::size_t size) { return counted_allocate(size); }
void *operator new[](std::size_t size) { return counted_allocate(size); }
void operator delete(void *pointer) noexcept { std::free(pointer); }
void operator delete[](void *pointer) noexcept { std::free(pointer); }
void operator delete(void *pointer, std::size_t) noexcept { std::free(pointer); }
void operator delete[](void *pointer, std::size_t) noexcept { std::free(pointer); }
