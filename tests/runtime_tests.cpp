#include "check.hpp"
#include "../unary/src/runtime.hpp"
#include <array>
#include <iostream>

namespace {
using namespace rpc;
using namespace rpc::detail;

void slots_and_index() {
    slot_pool<int> slots{2};
    auto old = slots.acquire(); auto other = slots.acquire();
    CHECK(slots.get(old) && slots.get(other) && !slots.get(slots.acquire()));
    slots.release(old); auto fresh = slots.acquire();
    CHECK(fresh.index == old.index && fresh.generation != old.generation && !slots.get(old));
    slots.release(fresh); slots.release(other); CHECK(slots.size() == 0);
    stream_index<int> index{8}; std::array<int, 8> values{};
    for (std::size_t i = 0; i < values.size(); ++i) CHECK(index.insert(static_cast<std::uint32_t>(15 + i * 16), values[i]));
    CHECK(!index.insert(200, values[0]));
    index.erase(47); index.erase(15); index.erase(127);
    for (std::size_t i = 1; i < 7; ++i) CHECK(index.find(static_cast<std::uint32_t>(15 + i * 16)) == (i == 2 ? nullptr : &values[i]));
    CHECK(index.size() == 5 && index.insert(200, values[0]) && index.insert(216, values[2]));
    while (!index.empty()) {
        bool removed = false;
        for (std::uint32_t id = 1; id <= 216; ++id) if (index.find(id)) { index.erase(id); removed = true; break; }
        CHECK(removed);
    }
}
struct expiry {
    unsigned count = 0;
    status_code code = status_code::unknown;
    clock::time_point observed{};
    static void finish(void *value, status_code code) noexcept {
        auto &self = *static_cast<expiry *>(value);
        ++self.count; self.code = code; self.observed = clock::now();
    }
};
void deadlines() {
    net::io_context context{net::epoll, net::single_thread_hint};
    auto scheduler = shard_deadlines(context);
    CHECK(scheduler == shard_deadlines(context) && context.run() == 0);
    deadline_reservation capacity{*scheduler, 2};
    CHECK(deadline_capacity(*scheduler) == 2);
    activate_deadlines(scheduler);
    expiry a, b, c; deadline_node first, second, third;
    auto far = clock::now() + std::chrono::seconds{5};
    CHECK(schedule(*scheduler, first, far, &expiry::finish, &a)); context.poll();
    auto soon = clock::now() + std::chrono::milliseconds{2};
    CHECK(schedule(*scheduler, second, soon, &expiry::finish, &b));
    CHECK(!schedule(*scheduler, third, far, &expiry::finish, &c));
    context.run_for(std::chrono::milliseconds{10});
    CHECK(b.count == 1 && b.code == status_code::deadline_exceeded && b.observed >= soon && a.count == 0);
    // Remove/reuse a node while its old timer cancellation is still pending.
    unschedule(*scheduler, first);
    soon = clock::now() + std::chrono::milliseconds{2};
    CHECK(schedule(*scheduler, first, soon, &expiry::finish, &a));
    CHECK(schedule(*scheduler, third, soon, &expiry::finish, &c));
    context.run_for(std::chrono::milliseconds{10});
    CHECK(a.count == 1 && c.count == 1 && a.observed >= soon && c.observed >= soon);
    CHECK(schedule(*scheduler, first, far, &expiry::finish, &a)); context.poll();
    unschedule(*scheduler, first); deactivate_deadlines(*scheduler); context.run();
    CHECK(context.run() == 0 && a.count == 1);
    activate_deadlines(scheduler); // Dormant driver can restart without a new node.
    CHECK(schedule(*scheduler, first, far, &expiry::finish, &a)); context.poll();
    fail_deadlines(*scheduler);
    CHECK(a.count == 2 && a.code == status_code::unavailable);
    CHECK(!schedule(*scheduler, first, far, &expiry::finish, &a));
    deactivate_deadlines(*scheduler); context.run(); CHECK(context.run() == 0);
}
}
int main() {
    try { slots_and_index(); deadlines(); std::cout << "PASS slot generations, colliding stream index, precise shared deadlines and failure drain\n"; }
    catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
