#include "runtime.hpp"
#include "connection.hpp"

#include <net/run_async.hpp>
#include <net/error.hpp>
#include <algorithm>

namespace rpc {
namespace detail {

struct deadline_scheduler {
    explicit deadline_scheduler(net::io_context &context) : context(context), timer(context) {}
    std::size_t absent() const noexcept { return std::numeric_limits<std::size_t>::max(); }
    void swap_nodes(std::size_t a, std::size_t b) noexcept {
        std::swap(heap[a], heap[b]); heap[a]->index = a; heap[b]->index = b;
    }
    void up(std::size_t i) noexcept {
        while (i != 0 && heap[i]->when < heap[(i - 1) / 2]->when) {
            auto const parent = (i - 1) / 2; swap_nodes(i, parent); i = parent;
        }
    }
    void down(std::size_t i) noexcept {
        for (;;) {
            auto child = i * 2 + 1;
            if (child >= heap.size()) return;
            if (child + 1 < heap.size() && heap[child + 1]->when < heap[child]->when) ++child;
            if (heap[i]->when <= heap[child]->when) return;
            swap_nodes(i, child); i = child;
        }
    }
    void erase(deadline_node &node) noexcept {
        if (node.index == absent()) return;
        auto const index = node.index;
        assert(index < heap.size() && heap[index] == &node);
        swap_nodes(index, heap.size() - 1); heap.pop_back(); node.index = absent();
        if (index < heap.size()) {
            if (index != 0 && heap[index]->when < heap[(index - 1) / 2]->when) up(index);
            else down(index);
        }
        if (heap.empty() && waiting) timer.cancel();
    }
    net::io_context &context;
    net::steady_timer timer;
    std::vector<deadline_node *> heap{};
    event changed{};
    std::size_t capacity = 0;
    std::size_t users = 0;
    clock::time_point armed = clock::time_point::max();
    bool running = false;
    bool waiting = false;
    bool failed = false;
};

// One service per execution context. The service owns no active work; endpoint
// activation owns the driver, and close/drain explicitly releases that work.
struct runtime_service final : net::execution_context::service {
    explicit runtime_service(net::execution_context &) {}
    void shutdown() override {}
    std::weak_ptr<deadline_scheduler> deadlines{};
};

std::shared_ptr<deadline_scheduler> shard_deadlines(net::io_context &context) {
    auto &service = context.use_service<runtime_service>();
    auto result = service.deadlines.lock();
    if (!result) { result = std::make_shared<deadline_scheduler>(context); service.deadlines = result; }
    return result;
}
std::size_t deadline_capacity(deadline_scheduler const &scheduler) noexcept { return scheduler.capacity; }
void reserve_deadlines(deadline_scheduler &scheduler, std::size_t capacity) {
    if (capacity > scheduler.heap.max_size() - scheduler.capacity) throw std::invalid_argument{"deadline capacity overflow"};
    scheduler.heap.reserve(scheduler.capacity + capacity);
    scheduler.capacity += capacity;
}
void release_deadlines(deadline_scheduler &scheduler, std::size_t capacity) noexcept {
    assert(capacity <= scheduler.capacity);
    scheduler.capacity -= capacity;
}

auto deadline_driver(std::shared_ptr<deadline_scheduler> scheduler)
    CO2_BEG(net::task<>, (scheduler), net::io_result<> waited; unsigned processed = 0;) {
    while (!scheduler->failed && (scheduler->users != 0 || !scheduler->heap.empty())) {
        if (scheduler->heap.empty()) { CO2_AWAIT(scheduler->changed.wait()); continue; }
        if (scheduler->heap.front()->when > clock::now()) {
            scheduler->armed = scheduler->heap.front()->when;
            scheduler->timer.expires_at(scheduler->armed);
            scheduler->waiting = true;
            CO2_AWAIT_SET(waited, scheduler->timer.wait());
            scheduler->waiting = false;
            if (waited.ec && waited.ec != net::cond::canceled) { fail_deadlines(*scheduler); break; }
            // Only this driver re-arms, after the previous wait was consumed.
            continue;
        }
        {
            auto *node = scheduler->heap.front();
            scheduler->erase(*node);
            auto const expire = node->expire;
            auto *user = node->user;
            expire(user, status_code::deadline_exceeded); // Callback may reclaim node.
        }
        if (++processed == 64) { processed = 0; CO2_AWAIT((yield_awaiter{})); }
    }
    CO2_RETURN();
}
CO2_END

void activate_deadlines(std::shared_ptr<deadline_scheduler> const &scheduler) {
    if (scheduler->failed) throw std::runtime_error{"deadline scheduler failed"};
    ++scheduler->users;
    if (scheduler->running) { scheduler->changed.signal(); return; }
    scheduler->running = true;
    try {
        net::run_async(scheduler->context.get_executor(), [scheduler] { scheduler->running = false; },
            [scheduler](std::exception_ptr) {
                scheduler->waiting = false; scheduler->running = false;
                fail_deadlines(*scheduler);
            })([scheduler] { return deadline_driver(scheduler); });
    } catch (...) { scheduler->running = false; --scheduler->users; throw; }
}
void deactivate_deadlines(deadline_scheduler &scheduler) noexcept {
    assert(scheduler.users != 0);
    --scheduler.users;
    if (scheduler.users == 0) {
        if (scheduler.waiting) scheduler.timer.cancel();
        scheduler.changed.signal();
    }
}
bool schedule(deadline_scheduler &scheduler, deadline_node &node, clock::time_point when,
              void (*expire)(void *, status_code), void *user) noexcept {
    assert(node.index == scheduler.absent());
    if (when == clock::time_point::max()) return true;
    if (scheduler.heap.size() == scheduler.capacity || !scheduler.running || scheduler.failed) return false;
    node.when = when; node.expire = expire; node.user = user; node.index = scheduler.heap.size();
    scheduler.heap.push_back(&node); scheduler.up(node.index);
    if (scheduler.waiting && when < scheduler.armed) scheduler.timer.cancel();
    scheduler.changed.signal();
    return true;
}
void unschedule(deadline_scheduler &scheduler, deadline_node &node) noexcept { scheduler.erase(node); }
void fail_deadlines(deadline_scheduler &scheduler) noexcept {
    scheduler.failed = true;
    if (scheduler.waiting) scheduler.timer.cancel();
    scheduler.changed.signal();
    while (!scheduler.heap.empty()) {
        auto *node = scheduler.heap.front(); scheduler.erase(*node);
        auto const expire = node->expire; auto *user = node->user;
        expire(user, status_code::unavailable);
    }
}

} // namespace detail
} // namespace rpc
