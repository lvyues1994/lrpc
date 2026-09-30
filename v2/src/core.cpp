#include "core.hpp"

#include <algorithm>

namespace rpc {
namespace v2 {
namespace detail {
namespace {

unsigned lowest_bit(std::uint64_t value) noexcept {
#if defined(__GNUC__)
    return static_cast<unsigned>(__builtin_ctzll(value));
#else
    unsigned index = 0;
    while ((value & 1U) == 0) { value >>= 1; ++index; }
    return index;
#endif
}

} // namespace

std::uint64_t &clock_counter() noexcept {
    thread_local std::uint64_t reads = 0;
    return reads;
}

// ---- slab ----

std::size_t slab::class_of(std::size_t const size) noexcept {
    if (size <= min_block) return 0;
#if defined(__GNUC__)
    return static_cast<std::size_t>(64 - __builtin_clzll(static_cast<unsigned long long>(size - 1))) - 6;
#else
    std::size_t index = 0;
    for (auto block = min_block; block < size; block <<= 1) ++index;
    return index;
#endif
}

std::size_t slab::block_size(std::size_t const size) noexcept {
    return size > max_block ? size : min_block << class_of(size);
}

std::uint8_t *slab::allocate(std::size_t const size) {
    if (size > max_block) return static_cast<std::uint8_t *>(::operator new(size));
    auto const index = class_of(size);
    if (auto *block = free_[index]) {
        free_[index] = block->next;
        cached_[index] -= min_block << index;
        return reinterpret_cast<std::uint8_t *>(block);
    }
    return static_cast<std::uint8_t *>(::operator new(min_block << index));
}

void slab::deallocate(std::uint8_t *const block, std::size_t const size) noexcept {
    if (block == nullptr) return;
    if (size > max_block) { ::operator delete(block); return; }
    auto const index = class_of(size);
    auto const bytes = min_block << index;
    if (cached_[index] + bytes > limit_) { ::operator delete(block); return; }
    auto *node = reinterpret_cast<slab::node *>(block);
    node->next = free_[index];
    free_[index] = node;
    cached_[index] += bytes;
}

std::size_t slab::cached_bytes() const noexcept {
    std::size_t total = 0;
    for (auto const bytes : cached_) total += bytes;
    return total;
}

slab::~slab() {
    for (auto *&head : free_) {
        while (head != nullptr) {
            auto *next = head->next;
            ::operator delete(head);
            head = next;
        }
    }
}

// ---- stream_table ----

stream_table::stream_table(std::size_t const limit) : limit_(limit) {
    std::size_t capacity = 2;
    while (capacity < limit * 2) capacity <<= 1;
    entries_.resize(capacity);
}

stream_state *stream_table::find(std::uint32_t const id) const noexcept {
    std::size_t index = id & mask();
    for (std::size_t probe = 0;; ++probe, index = (index + 1) & mask()) {
        auto const &slot = entries_[index];
        // Robin Hood order: past a richer entry, the key cannot follow.
        if (slot.value == nullptr || distance(index, slot.id) < probe) return nullptr;
        if (slot.id == id) return slot.value;
    }
}

bool stream_table::insert(stream_state &value) noexcept {
    if (size_ >= limit_ || find(value.id) != nullptr) return false;
    entry carried{value.id, &value};
    std::size_t index = carried.id & mask();
    for (std::size_t probe = 0;; ++probe, index = (index + 1) & mask()) {
        auto &slot = entries_[index];
        if (slot.value == nullptr) {
            slot = carried;
            ++size_;
            return true;
        }
        auto const resident = distance(index, slot.id);
        if (resident < probe) {
            std::swap(slot, carried);
            probe = resident;
        }
    }
}

void stream_table::erase(std::uint32_t const id) noexcept {
    std::size_t hole = id & mask();
    for (std::size_t probe = 0;; ++probe, hole = (hole + 1) & mask()) {
        auto const &slot = entries_[hole];
        if (slot.value == nullptr || distance(hole, slot.id) < probe) return;
        if (slot.id == id) break;
    }
    --size_;
    for (auto next = (hole + 1) & mask();; next = (next + 1) & mask()) {
        auto const &candidate = entries_[next];
        if (candidate.value == nullptr || distance(next, candidate.id) == 0) break;
        entries_[hole] = candidate;
        hole = next;
    }
    entries_[hole] = {};
}

stream_state *stream_table::any() const noexcept {
    if (size_ == 0) return nullptr;
    for (auto const &slot : entries_)
        if (slot.value != nullptr) return slot.value;
    return nullptr;
}

std::vector<stream_state *> stream_table::snapshot() const {
    std::vector<stream_state *> streams;
    streams.reserve(size_);
    for (auto const &slot : entries_)
        if (slot.value != nullptr) streams.push_back(slot.value);
    return streams;
}

// ---- timing_wheel ----

timing_wheel::timing_wheel() noexcept {
    for (auto &head : heads_) head.prev = head.next = &head;
    overflow_.prev = overflow_.next = &overflow_;
}

void timing_wheel::link(wheel_node &head, wheel_node &node) noexcept {
    node.prev = head.prev;
    node.next = &head;
    head.prev->next = &node;
    head.prev = &node;
}

void timing_wheel::unlink(wheel_node &node) noexcept {
    node.prev->next = node.next;
    node.next->prev = node.prev;
    node.prev = node.next = nullptr;
}

void timing_wheel::insert(wheel_node &node, std::uint64_t tick) noexcept {
    assert(!node.scheduled());
    tick = std::max(tick, advancing_ ? current_ + 1 : current_);
    node.tick = tick;
    ++size_;
    if (tick - current_ < slots) {
        auto const slot = static_cast<std::size_t>(tick % slots);
        link(heads_[slot], node);
        mark(slot);
        return;
    }
    link(overflow_, node);
    ++overflow_size_;
    overflow_min_ = std::min(overflow_min_, tick);
}

void timing_wheel::erase(wheel_node &node) noexcept {
    assert(node.scheduled());
    auto const in_overflow = node.tick - current_ >= slots;
    auto const slot = static_cast<std::size_t>(node.tick % slots);
    unlink(node);
    --size_;
    if (in_overflow) {
        if (--overflow_size_ == 0) overflow_min_ = std::numeric_limits<std::uint64_t>::max();
    } else if (heads_[slot].next == &heads_[slot]) {
        clear(slot);
    }
}

void timing_wheel::rebucket() noexcept {
    auto lowest = std::numeric_limits<std::uint64_t>::max();
    for (auto *node = overflow_.next; node != &overflow_;) {
        auto *next = node->next;
        if (node->tick - current_ < slots) {
            unlink(*node);
            --overflow_size_;
            auto const slot = static_cast<std::size_t>(node->tick % slots);
            link(heads_[slot], *node);
            mark(slot);
        } else {
            lowest = std::min(lowest, node->tick);
        }
        node = next;
    }
    overflow_min_ = lowest;
}

std::uint64_t timing_wheel::next_tick() const noexcept {
    auto best = overflow_size_ != 0 ? overflow_min_ : std::numeric_limits<std::uint64_t>::max();
    if (size_ == overflow_size_) return best;
    auto const start = static_cast<std::size_t>(current_ % slots);
    constexpr std::size_t words = slots / 64;
    for (std::size_t step = 0; step <= words; ++step) {
        auto const word = (start / 64 + step) % words;
        auto bits = bits_[word];
        if (step == 0) bits &= ~std::uint64_t{0} << (start % 64);
        if (step == words) bits &= (std::uint64_t{1} << (start % 64)) - 1;
        if (bits == 0) continue;
        auto const slot = word * 64 + lowest_bit(bits);
        auto const distance = (slot + slots - start) % slots;
        return std::min(best, current_ + distance);
    }
    return best;
}

// ---- stop_hook ----

stop_hook::stop_hook(shard_state &owner) noexcept : shard(&owner), frame(&on_forwarded, this) {
    start.h = frame.handle();
}

void stop_hook::forward::operator()() const noexcept {
    self->queued.store(true, std::memory_order_release);
    self->shard->executor.post(self->start);
}

void stop_hook::attach(stream_state &stream, net::stop_token const &token) noexcept {
    target = &stream;
    callback.emplace(token, forward{this});
}

void stop_hook::detach() noexcept {
    callback.reset(); // Waits for a callback running on another thread.
    if (queued.load(std::memory_order_acquire)) target = nullptr;
    else shard->release_hook(*this);
}

net::coroutine_handle<> stop_hook::on_forwarded(void *const self) noexcept {
    auto &hook = *static_cast<stop_hook *>(self);
    hook.queued.store(false, std::memory_order_relaxed);
    if (hook.target != nullptr) hook.target->ops->on_cancel(*hook.target);
    else hook.shard->release_hook(hook);
    return net::noop_coroutine();
}

// ---- shard_state ----

shard_ticker::shard_ticker(shard_state &state)
    : owner(&state), executor(state.executor), timer(state.context), frame(&shard_state::on_timer, this) {
    start.h = frame.handle();
    env.executor = net::executor_ref{executor};
}

shard_state::shard_state(net::io_context &io, shard_options const options)
    : context(io), executor(io.get_executor()), memory(options.slab_cache_bytes_per_class), epoch_(now()),
      ticker_(new shard_ticker(*this)) {}

shard_state::~shard_state() {
    while (free_hooks_ != nullptr) {
        auto *hook = free_hooks_;
        free_hooks_ = hook->next;
        delete hook;
    }
    if (ticker_->armed) {
        ticker_->owner = nullptr;
        ticker_->timer.cancel();
    } else {
        delete ticker_;
    }
}

void shard_state::schedule(stream_state &stream, clock::time_point const deadline) noexcept {
    using std::chrono::milliseconds;
    constexpr auto max_offset = std::chrono::hours{24 * 365 * 100};
    auto const offset = deadline - epoch_;
    std::uint64_t tick = 0;
    if (offset > max_offset) tick = static_cast<std::uint64_t>(std::chrono::duration_cast<milliseconds>(max_offset).count());
    else if (offset.count() > 0) {
        auto const whole = std::chrono::duration_cast<milliseconds>(offset);
        tick = static_cast<std::uint64_t>(whole.count()) + (whole < offset ? 1U : 0U);
    }
    wheel_.insert(stream, tick);
    if (!ticker_->armed) arm(tick);
    else if (tick < ticker_->tick) ticker_->timer.cancel(); // on_timer re-arms for the earliest tick.
}

void shard_state::arm(std::uint64_t const tick) noexcept {
    auto &ticker = *ticker_;
    ticker.tick = tick;
    ticker.armed = true;
    ticker.timer.expires_at(epoch_ + std::chrono::milliseconds{static_cast<std::int64_t>(tick)});
    ticker.wait = ticker.timer.wait();
    if (ticker.wait.await_ready() || ticker.wait.await_suspend(ticker.frame.handle(), &ticker.env) != net::noop_coroutine())
        ticker.executor.post(ticker.start);
}

net::coroutine_handle<> shard_state::on_timer(void *const pointer) noexcept {
    auto &ticker = *static_cast<shard_ticker *>(pointer);
    ticker.armed = false;
    static_cast<void>(ticker.wait.await_resume());
    auto *const self = ticker.owner;
    if (self == nullptr) {
        delete &ticker;
        return net::noop_coroutine();
    }
    if (!self->wheel_.empty()) {
        auto const elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now() - self->epoch_);
        auto const now_tick = elapsed.count() > 0 ? static_cast<std::uint64_t>(elapsed.count()) : 0U;
        self->wheel_.advance(now_tick, [](wheel_node &node) {
            auto &stream = static_cast<stream_state &>(node);
            stream.ops->on_deadline(stream);
        });
    }
    if (!self->wheel_.empty() && !ticker.armed) self->arm(self->wheel_.next_tick());
    return net::noop_coroutine();
}

void shard_state::endpoint_closed() noexcept {
    assert(endpoints_ != 0);
    // A lazily disarmed timer would otherwise keep run() from returning.
    if (--endpoints_ == 0 && wheel_.empty() && ticker_->armed) ticker_->timer.cancel();
}

stop_hook *shard_state::acquire_hook() {
    if (auto *hook = free_hooks_) {
        free_hooks_ = hook->next;
        hook->next = nullptr;
        return hook;
    }
    return new stop_hook(*this);
}

void shard_state::release_hook(stop_hook &hook) noexcept {
    hook.target = nullptr;
    hook.next = free_hooks_;
    free_hooks_ = &hook;
}

} // namespace detail

shard::shard(net::io_context &context, shard_options const options)
    : state_(new detail::shard_state(context, options)) {}
shard::~shard() = default;
net::io_context &shard::context() const noexcept { return state_->context; }

namespace debug {
std::uint64_t clock_reads() noexcept { return detail::clock_counter(); }
} // namespace debug

char const *to_string(status_code const code) noexcept {
    switch (code) {
    case status_code::ok: return "ok";
    case status_code::cancelled: return "cancelled";
    case status_code::unknown: return "unknown";
    case status_code::invalid_argument: return "invalid_argument";
    case status_code::deadline_exceeded: return "deadline_exceeded";
    case status_code::not_found: return "not_found";
    case status_code::already_exists: return "already_exists";
    case status_code::permission_denied: return "permission_denied";
    case status_code::resource_exhausted: return "resource_exhausted";
    case status_code::failed_precondition: return "failed_precondition";
    case status_code::aborted: return "aborted";
    case status_code::out_of_range: return "out_of_range";
    case status_code::unimplemented: return "unimplemented";
    case status_code::internal: return "internal";
    case status_code::unavailable: return "unavailable";
    case status_code::data_loss: return "data_loss";
    case status_code::unauthenticated: return "unauthenticated";
    }
    return "invalid";
}

} // namespace v2
} // namespace rpc
