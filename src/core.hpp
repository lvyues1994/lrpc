#pragma once

#include <rpc/detail/stream_state.hpp>
#include <rpc/options.hpp>
#include <rpc/shard.hpp>

#include <net/continuation.hpp>
#include <net/coroutine.hpp>
#include <net/detail/completion_frame.hpp>
#include <net/io_context.hpp>
#include <net/timer.hpp>

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

namespace rpc {
struct message_compressor;
namespace detail {

// Every clock read in the library goes through here, so tests can assert that
// a call path without a deadline reads none.
std::uint64_t &clock_counter() noexcept;
inline clock::time_point now() noexcept {
    ++clock_counter();
    return clock::now();
}

template <class T> class late_init {
public:
    late_init() noexcept = default;
    late_init(late_init const &) = delete;
    late_init &operator=(late_init const &) = delete;
    ~late_init() { reset(); }

    template <class... Args> T &emplace(Args &&...args) {
        reset();
        auto *value = ::new (static_cast<void *>(&storage_)) T(std::forward<Args>(args)...);
        live_ = true;
        return *value;
    }
    void reset() noexcept {
        if (!live_) return;
        live_ = false;
        get().~T();
    }
    bool has_value() const noexcept { return live_; }
    T &get() noexcept { return *reinterpret_cast<T *>(&storage_); }

private:
    typename std::aligned_storage<sizeof(T), alignof(T)>::type storage_;
    bool live_ = false;
};

// Single waiter, single thread. A signal without a waiter is remembered once.
class event {
public:
    struct awaiter {
        event *owner;
        net::continuation continuation{};
        bool await_ready() noexcept { return std::exchange(owner->signaled_, false); }
        net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
            assert(owner->waiter_ == nullptr);
            continuation.h = handle;
            owner->waiter_ = &continuation;
            owner->executor_ = env->executor;
            return net::noop_coroutine();
        }
        void await_resume() noexcept {}
    };
    awaiter wait() noexcept { return awaiter{this}; }
    bool waiting() const noexcept { return waiter_ != nullptr; }
    void signal() noexcept {
        if (auto *waiter = std::exchange(waiter_, nullptr)) executor_.post(*waiter);
        else signaled_ = true;
    }

private:
    bool signaled_ = false;
    net::continuation *waiter_ = nullptr;
    net::executor_ref executor_{};
};

struct yield_awaiter {
    net::continuation continuation{};
    bool await_ready() const noexcept { return false; }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
        continuation.h = handle;
        env->executor.post(continuation);
        return net::noop_coroutine();
    }
    void await_resume() const noexcept {}
};

// Size-classed blocks, 64 B .. 64 KiB in powers of two; larger requests go
// straight to the allocator. Blocks are taken on demand and idle ones are
// cached up to a per-class limit, so memory follows load. Single-threaded.
class slab {
public:
    static constexpr std::size_t min_block = 64;
    static constexpr std::size_t max_block = 64U * 1024U;
    static constexpr std::size_t classes = 11;

    explicit slab(std::size_t cache_bytes_per_class) noexcept : limit_(cache_bytes_per_class) {}
    ~slab();
    slab(slab const &) = delete;
    slab &operator=(slab const &) = delete;

    // The block holds block_size(size) bytes. Throws std::bad_alloc.
    std::uint8_t *allocate(std::size_t size);
    void deallocate(std::uint8_t *block, std::size_t size) noexcept;
    static std::size_t block_size(std::size_t size) noexcept;
    std::size_t cached_bytes() const noexcept;

private:
    static std::size_t class_of(std::size_t size) noexcept;
    struct node {
        node *next;
    };
    std::array<node *, classes> free_{};
    std::array<std::size_t, classes> cached_{};
    std::size_t limit_;
};

// An object in a slab block, so steady-state churn reuses cached blocks.
// Destroy it as its own (final) type.
template <class T, class... Args> T *make_in(slab &memory, Args &&...args) {
    static_assert(alignof(T) <= alignof(std::max_align_t), "slab blocks are max_align_t aligned");
    auto *const block = memory.allocate(sizeof(T));
    try {
        return ::new (static_cast<void *>(block)) T(std::forward<Args>(args)...);
    } catch (...) {
        memory.deallocate(block, sizeof(T));
        throw;
    }
}

template <class T> void destroy_in(slab &memory, T *const object) noexcept {
    if (object == nullptr) return;
    object->~T();
    memory.deallocate(reinterpret_cast<std::uint8_t *>(object), sizeof(T));
}

// Fixed capacity, Robin Hood open addressing at load factor <= 1/2, with
// backward-shift deletion. Stream IDs are monotonic, so the low bits are a
// good hash and nearly every entry sits in its home slot: in-flight streams
// form one long run, and deletion stops at the first entry already at home.
class stream_table {
public:
    explicit stream_table(std::size_t limit = 0);
    stream_state *find(std::uint32_t id) const noexcept;
    bool insert(stream_state &value) noexcept; // False when full.
    void erase(std::uint32_t id) noexcept;
    stream_state *any() const noexcept;
    std::vector<stream_state *> snapshot() const; // Cold paths that must filter streams.
    std::size_t size() const noexcept { return size_; }
    std::size_t limit() const noexcept { return limit_; }

private:
    struct entry {
        std::uint32_t id = 0;
        stream_state *value = nullptr;
    };
    std::size_t mask() const noexcept { return entries_.size() - 1; }
    std::size_t distance(std::size_t index, std::uint32_t id) const noexcept { return (index - id) & mask(); }
    std::vector<entry> entries_;
    std::size_t limit_ = 0;
    std::size_t size_ = 0;
};

// Hashed wheel of 1 ms ticks. Insertion and removal are O(1); most deadlines
// are removed long before they expire. Ticks beyond one revolution wait in an
// overflow list and are moved into slots as the wheel turns.
class timing_wheel {
public:
    static constexpr std::size_t slots = 4096;

    timing_wheel() noexcept;
    timing_wheel(timing_wheel const &) = delete;
    timing_wheel &operator=(timing_wheel const &) = delete;

    // A due tick is moved past the tick being expired, so an expiry callback
    // that schedules again cannot livelock the running advance.
    void insert(wheel_node &node, std::uint64_t tick) noexcept;
    void erase(wheel_node &node) noexcept;
    bool empty() const noexcept { return size_ == 0; }
    std::uint64_t next_tick() const noexcept; // Requires !empty(); never later than the earliest node.
    // Unlinks each node with tick <= now_tick before passing it to expire,
    // which may insert or erase other nodes.
    template <class Expire> void advance(std::uint64_t now_tick, Expire expire);

private:
    static void link(wheel_node &head, wheel_node &node) noexcept;
    static void unlink(wheel_node &node) noexcept;
    void mark(std::size_t slot) noexcept { bits_[slot / 64] |= std::uint64_t{1} << (slot % 64); }
    void clear(std::size_t slot) noexcept { bits_[slot / 64] &= ~(std::uint64_t{1} << (slot % 64)); }
    bool marked(std::size_t slot) const noexcept { return ((bits_[slot / 64] >> (slot % 64)) & 1U) != 0; }
    void rebucket() noexcept;

    std::array<wheel_node, slots> heads_{};
    std::array<std::uint64_t, slots / 64> bits_{};
    wheel_node overflow_{};
    std::uint64_t overflow_min_ = std::numeric_limits<std::uint64_t>::max(); // A lower bound.
    std::uint64_t current_ = 0; // First tick not yet processed.
    std::size_t size_ = 0;
    std::size_t overflow_size_ = 0;
    bool advancing_ = false; // The slot of current_ is being expired.
};

template <class Expire> void timing_wheel::advance(std::uint64_t const now_tick, Expire expire) {
    advancing_ = true;
    while (current_ <= now_tick && size_ != 0) {
        auto const slot = static_cast<std::size_t>(current_ % slots);
        if (marked(slot)) {
            auto &head = heads_[slot];
            while (head.next != &head) {
                auto &node = *head.next;
                unlink(node);
                --size_;
                expire(node);
            }
            clear(slot);
        }
        ++current_;
        if (overflow_size_ != 0 && current_ + slots > overflow_min_) rebucket();
    }
    advancing_ = false;
    if (size_ == 0 && current_ <= now_tick) current_ = now_tick + 1;
}

struct shard_state;

// Forwards a stop request from any thread to the shard, where the target
// stream is cancelled. The hook outlives its call if a forwarded request is
// still queued, and returns to the pool when that request runs.
struct stop_hook {
    struct forward {
        stop_hook *self;
        void operator()() const noexcept;
    };

    explicit stop_hook(shard_state &owner) noexcept;
    stop_hook(stop_hook const &) = delete;
    stop_hook &operator=(stop_hook const &) = delete;

    void attach(stream_state &target, net::stop_token const &token) noexcept;
    void detach() noexcept; // Shard thread; may return the hook to the pool.

    shard_state *shard;
    stream_state *target = nullptr;
    std::atomic<bool> queued{false};
    late_init<net::stop_callback<forward>> callback;
    net::detail::completion_frame frame;
    net::continuation start{};
    stop_hook *next = nullptr;

private:
    static net::coroutine_handle<> on_forwarded(void *self) noexcept;
};

// Owns the shard's only steady_timer. Heap-allocated so that a shard can be
// destroyed while a cancelled wait is still queued; the wait then frees it.
struct shard_ticker {
    explicit shard_ticker(shard_state &owner);
    shard_state *owner;
    net::io_context::executor_type executor; // env refers to this copy.
    net::steady_timer timer;
    net::timer_wait_awaitable wait{nullptr};
    net::detail::completion_frame frame;
    net::continuation start{};
    net::io_env env{};
    std::uint64_t tick = 0;
    bool armed = false;
};

struct shard_state {
    shard_state(net::io_context &context, shard_options options);
    ~shard_state();
    shard_state(shard_state const &) = delete;
    shard_state &operator=(shard_state const &) = delete;

    // Expiry is never early and at most about one tick late.
    void schedule(stream_state &stream, clock::time_point deadline) noexcept;
    void unschedule(stream_state &stream) noexcept {
        if (stream.scheduled()) wheel_.erase(stream);
    }
    void endpoint_opened() noexcept { ++endpoints_; }
    void endpoint_closed() noexcept;

    stop_hook *acquire_hook(); // Throws std::bad_alloc.
    void release_hook(stop_hook &hook) noexcept;

    // Created on first use and shared by the shard's connections; its
    // workspace is not reentrant, which one thread guarantees. Null when the
    // build lacks the algorithm.
    message_compressor *compressor(std::uint8_t algorithm) noexcept;

    net::io_context &context;
    net::io_context::executor_type executor;
    slab memory;

private:
    std::array<std::unique_ptr<message_compressor>, 2> compressors_;
    friend struct shard_ticker;
    void arm(std::uint64_t tick) noexcept;
    static net::coroutine_handle<> on_timer(void *ticker) noexcept;

    timing_wheel wheel_;
    clock::time_point epoch_;
    shard_ticker *ticker_;
    std::size_t endpoints_ = 0;
    stop_hook *free_hooks_ = nullptr;
};

} // namespace detail
} // namespace rpc
