// Randomized checks of the core building blocks against simple references.
#include "check.hpp"

#include "../src/core.hpp"

#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <vector>

namespace {

using rpc::detail::slab;
using rpc::detail::stream_state;
using rpc::detail::stream_table;
using rpc::detail::timing_wheel;
using rpc::detail::wheel_node;

void table_matches_reference() {
    std::mt19937 random{1};
    for (std::size_t const limit : {1U, 3U, 64U, 1000U}) {
        stream_table table{limit};
        std::vector<std::unique_ptr<stream_state>> pool;
        std::map<std::uint32_t, stream_state *> reference;
        std::uint32_t next = 1;
        for (int step = 0; step < 200000; ++step) {
            auto const action = random() % 8;
            if (action < 4 && reference.size() < limit) {
                // Mostly monotonic IDs, sometimes a jump that aliases a resident's home slot.
                next += random() % 16 == 0 ? static_cast<std::uint32_t>(random() % 4096) + 1 : 1;
                pool.emplace_back(new stream_state);
                pool.back()->id = next;
                CHECK(table.insert(*pool.back()));
                CHECK(!table.insert(*pool.back()));
                reference[next] = pool.back().get();
            } else if (action < 7 && !reference.empty()) {
                auto it = reference.begin();
                // Favour the oldest stream, as completions mostly do.
                if (random() % 2 != 0) std::advance(it, static_cast<long>(random() % reference.size()));
                table.erase(it->first);
                table.erase(it->first);
                reference.erase(it);
            } else {
                auto const id = static_cast<std::uint32_t>(random() % (next + 2));
                auto const found = reference.find(id);
                CHECK(table.find(id) == (found == reference.end() ? nullptr : found->second));
            }
            CHECK(table.size() == reference.size());
        }
        for (auto const &entry : reference) CHECK(table.find(entry.first) == entry.second);
        CHECK(table.size() < limit || !table.insert(*pool.front()));
    }
}

void wheel_matches_reference() {
    std::mt19937 random{2};
    timing_wheel wheel;
    std::vector<std::unique_ptr<wheel_node>> nodes;
    std::multimap<std::uint64_t, wheel_node *> reference;
    std::uint64_t now = 0;
    std::uint64_t first_open = 0; // Ticks before it are clamped up to it.
    auto expected_tick = [&](std::uint64_t tick) { return std::max(tick, first_open); };
    for (int step = 0; step < 100000; ++step) {
        auto const action = random() % 10;
        if (action < 5) {
            std::uint64_t tick = now + random() % 10000; // Beyond one revolution of 4096.
            if (random() % 20 == 0 && now > 5) tick = now - random() % 5; // Already due.
            nodes.emplace_back(new wheel_node);
            wheel.insert(*nodes.back(), tick);
            reference.emplace(expected_tick(tick), nodes.back().get());
        } else if (action < 7 && !reference.empty()) {
            auto it = reference.begin();
            std::advance(it, static_cast<long>(random() % reference.size()));
            wheel.erase(*it->second);
            CHECK(!it->second->scheduled());
            reference.erase(it);
        } else {
            now += random() % 50;
            std::set<wheel_node *> expired;
            wheel.advance(now, [&](wheel_node &node) {
                CHECK(!node.scheduled());
                expired.insert(&node);
            });
            first_open = now + 1;
            std::set<wheel_node *> due;
            while (!reference.empty() && reference.begin()->first <= now) {
                due.insert(reference.begin()->second);
                reference.erase(reference.begin());
            }
            CHECK(expired == due);
        }
        CHECK(wheel.empty() == reference.empty());
        if (!reference.empty()) CHECK(wheel.next_tick() <= reference.begin()->first);
    }
    for (auto const &entry : reference) wheel.erase(*entry.second);
    CHECK(wheel.empty());
    // Re-arming from inside expiry lands in a later pass.
    wheel_node first, second;
    wheel.insert(first, now + 3);
    bool rearmed = false;
    wheel.advance(now + 3, [&](wheel_node &node) {
        CHECK(&node == &first);
        wheel.insert(second, now + 3);
        rearmed = true;
    });
    CHECK(rearmed && second.scheduled());
    wheel.advance(now + 4, [&](wheel_node &node) { CHECK(&node == &second); });
    CHECK(!second.scheduled() && wheel.empty());
}

void slab_classes() {
    CHECK(slab::block_size(1) == 64 && slab::block_size(64) == 64 && slab::block_size(65) == 128);
    CHECK(slab::block_size(65536) == 65536 && slab::block_size(65537) == 65537);
    slab memory{4096};
    std::vector<std::pair<std::uint8_t *, std::size_t>> blocks;
    for (std::size_t size : {1U, 64U, 100U, 4000U, 70000U}) {
        for (int i = 0; i < 8; ++i) {
            blocks.emplace_back(memory.allocate(size), size);
            blocks.back().first[size - 1] = 1;
        }
    }
    for (auto const &block : blocks) memory.deallocate(block.first, block.second);
    CHECK(memory.cached_bytes() <= 4096 * slab::classes);
    auto *reused = memory.allocate(100);
    memory.deallocate(reused, 128);
}

} // namespace

int main() {
    try {
        table_matches_reference();
        std::cout << "PASS stream table\n";
        wheel_matches_reference();
        std::cout << "PASS timing wheel\n";
        slab_classes();
        std::cout << "PASS slab\n";
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
