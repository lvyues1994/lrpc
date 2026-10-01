#include <rpc/channel.hpp>

#include "client_core.hpp"

#include <net/resolver.hpp>
#include <net/run_async.hpp>
#include <net/timeout.hpp>

#include <algorithm>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>

namespace rpc {
namespace detail {

struct channel_core;

// One wait_ready caller, woken by a ready connection, close, or its deadline.
struct ready_waiter : stream_state {
    event wake;
    ready_waiter *prev = nullptr;
    ready_waiter *next = nullptr;
};

constexpr std::size_t fixed = static_cast<std::size_t>(-1);

struct subchannel {
    net::ip::tcp::endpoint endpoint; // Fixed, or the target's address in use.
    std::shared_ptr<client_core> client; // The current connection attempt.
    std::size_t target = fixed;
    std::vector<net::ip::tcp::endpoint> addresses{}; // The target's last answer.
    clock::time_point resolved{};
    std::size_t next = 0;     // Index of the address to try next.
    std::size_t failures = 0; // Consecutive failed connects since the answer.
    bool stale(clock::duration interval) const noexcept {
        return addresses.empty() || failures >= addresses.size() || now() - resolved >= interval;
    }
};

struct channel_core final : call_router {
    channel_core(shard_state &state, std::vector<net::ip::tcp::endpoint> const &endpoints,
                 channel_options const &config)
        : shard(state), options(config) {
        for (auto const &endpoint : endpoints)
            for (std::size_t i = 0; i < options.connections_per_endpoint; ++i) subs.push_back({endpoint, nullptr});
    }
    channel_core(shard_state &state, std::vector<channel_target> named, channel_options const &config)
        : shard(state), options(config), targets(std::move(named)) {
        for (std::size_t t = 0; t < targets.size(); ++t) {
            if (targets[t].host.empty() || targets[t].service.empty()) throw std::invalid_argument{"empty channel target"};
            for (std::size_t i = 0; i < options.connections_per_endpoint; ++i) {
                subchannel sub{};
                sub.target = t;
                sub.next = i; // Spreads the target's connections over its addresses.
                subs.push_back(std::move(sub));
            }
        }
    }

    client_core *pick(client_core const *avoid) noexcept override {
        client_core *best = nullptr;
        auto least = std::numeric_limits<std::size_t>::max();
        auto const count = subs.size();
        for (std::size_t i = 0; i < count; ++i) {
            auto *candidate = subs[(cursor + i) % count].client.get();
            if (candidate == nullptr || candidate == avoid || !candidate->ready() || !candidate->has_room()) continue;
            if (candidate->streams.size() < least) {
                best = candidate;
                least = candidate->streams.size();
            }
        }
        if (count != 0) cursor = (cursor + 1) % count; // Rotates ties.
        return best;
    }
    std::uint32_t max_attempts() const noexcept override { return std::max<std::uint32_t>(options.max_attempts, 1); }

    std::shared_ptr<client_core> make_client() {
        auto created = std::make_shared<client_core>(shard, options.client);
        for (auto const &name : methods) created->methods.push_back(method_slot{name, 0});
        return created;
    }
    std::size_t ready_count() const noexcept {
        std::size_t ready = 0;
        for (auto const &sub : subs)
            if (sub.client && sub.client->ready()) ++ready;
        return ready;
    }
    void link(ready_waiter &waiter) noexcept {
        waiter.next = waiters;
        if (waiters != nullptr) waiters->prev = &waiter;
        waiters = &waiter;
    }
    void unlink(ready_waiter &waiter) noexcept {
        if (waiter.prev != nullptr) waiter.prev->next = waiter.next;
        else if (waiters == &waiter) waiters = waiter.next;
        if (waiter.next != nullptr) waiter.next->prev = waiter.prev;
        waiter.prev = waiter.next = nullptr;
    }
    void wake_all() noexcept {
        for (auto *waiter = waiters; waiter != nullptr; waiter = waiter->next) waiter->wake.signal();
    }
    void close() noexcept {
        if (closed) return;
        closed = true;
        stop.request_stop();
        for (auto &sub : subs)
            if (sub.client) sub.client->close();
        wake_all();
    }

    shard_state &shard;
    channel_options const options;
    std::vector<channel_target> targets;
    std::vector<subchannel> subs;
    std::vector<std::string> methods;
    std::size_t cursor = 0;
    net::stop_source stop; // Interrupts reconnection backoff and connects.
    ready_waiter *waiters = nullptr;
    bool closed = false;
};

namespace {

void waiter_deadline(stream_state &node) noexcept { static_cast<ready_waiter &>(node).wake.signal(); }
stream_ops const waiter_ops{nullptr, nullptr, &waiter_deadline, nullptr};

using address_list = std::vector<net::ip::tcp::endpoint>;

auto system_lookup(net::ip::tcp::resolver &resolver, channel_target const &target)
    CO2_BEG(net::task<address_list>, (resolver, target), net::io_result<net::ip::tcp::resolver::results_type> answer;
            address_list addresses;) {
    CO2_AWAIT_SET(answer, resolver.resolve(target.host, target.service));
    if (!answer.ec) {
        addresses.reserve(answer.value.size());
        for (auto const &entry : answer.value) addresses.push_back(entry.endpoint);
    }
    CO2_RETURN(std::move(addresses));
}
CO2_END

// A lookup through the hook or getaddrinfo; an empty task fails it.
net::task<address_list> lookup(channel_core &core, std::unique_ptr<net::ip::tcp::resolver> &resolver,
                               channel_target const &target) noexcept {
    try {
        if (core.options.resolve) return core.options.resolve(target);
        if (!resolver) resolver.reset(new net::ip::tcp::resolver(core.shard.context));
        return system_lookup(*resolver, target);
    } catch (...) {
        return {};
    }
}

// Takes a fresh answer; a failed lookup keeps the last one. False when there
// is still nothing to connect to.
bool take_answer(subchannel &sub, address_list &answer) noexcept {
    if (!answer.empty()) {
        sub.addresses.swap(answer);
        sub.resolved = now();
        sub.failures = 0;
    }
    return !sub.addresses.empty();
}

auto maintain(std::shared_ptr<channel_core> core, std::size_t index)
    CO2_BEG(net::task<>, (core, index), std::shared_ptr<client_core> client; status_code connected;
            std::chrono::milliseconds backoff{0}; net::io_result<> slept;
            std::unique_ptr<net::ip::tcp::resolver> resolver; net::task<address_list> pending;
            address_list answer; bool usable = true;) {
    backoff = core->options.initial_backoff;
    while (!core->closed) {
        usable = true;
        if (core->subs[index].target != fixed && core->subs[index].stale(core->options.resolve_interval)) {
            answer.clear();
            pending = lookup(*core, resolver, core->targets[core->subs[index].target]);
            if (pending) {
                CO2_AWAIT_SET(answer, std::move(pending));
                if (core->closed) break;
            }
            usable = take_answer(core->subs[index], answer);
        }
        if (usable) {
            auto &sub = core->subs[index];
            if (sub.target != fixed) sub.endpoint = sub.addresses[sub.next % sub.addresses.size()];
            try {
                client = core->make_client();
            } catch (std::bad_alloc const &) {
                client.reset();
            }
        }
        if (usable && client) {
            core->subs[index].client = client;
            CO2_AWAIT_SET(connected, connect_client(client, core->subs[index].endpoint));
            if (connected == status_code::ok && !core->closed) {
                backoff = core->options.initial_backoff;
                core->subs[index].failures = 0;
                core->wake_all();
                CO2_AWAIT(client->closed_event.wait());
                continue;
            }
            client->close();
            ++core->subs[index].next;
            ++core->subs[index].failures;
        }
        if (core->closed) break;
        CO2_AWAIT_SET(slept, net::delay(backoff));
        backoff = std::min(backoff * 2, core->options.max_backoff);
    }
    CO2_RETURN();
}
CO2_END

auto wait_ready_task(std::shared_ptr<channel_core> core, clock::time_point deadline)
    CO2_BEG(net::task<status_code>, (core, deadline), ready_waiter waiter;) {
    if (core->ready_count() != 0) CO2_RETURN(status_code::ok);
    if (core->closed) CO2_RETURN(status_code::unavailable);
    waiter.ops = &waiter_ops;
    core->link(waiter);
    if (deadline != clock::time_point::max()) core->shard.schedule(waiter, deadline);
    while (core->ready_count() == 0 && !core->closed && (deadline == clock::time_point::max() || waiter.scheduled()))
        CO2_AWAIT(waiter.wake.wait());
    core->unlink(waiter);
    core->shard.unschedule(waiter);
    CO2_RETURN(core->ready_count() != 0 ? status_code::ok
               : core->closed           ? status_code::unavailable
                                        : status_code::deadline_exceeded);
}
CO2_END

} // namespace
} // namespace detail

channel::channel(shard &owner, std::vector<net::ip::tcp::endpoint> endpoints, channel_options options)
    : core_(std::make_shared<detail::channel_core>(owner.state(), endpoints, options)) {
    for (std::size_t index = 0; index != core_->subs.size(); ++index)
        net::run_async(core_->shard.executor, core_->stop.get_token(), nullptr)(detail::maintain(core_, index));
}

channel::channel(shard &owner, std::vector<channel_target> targets, channel_options options)
    : core_(std::make_shared<detail::channel_core>(owner.state(), std::move(targets), options)) {
    for (std::size_t index = 0; index != core_->subs.size(); ++index)
        net::run_async(core_->shard.executor, core_->stop.get_token(), nullptr)(detail::maintain(core_, index));
}

channel::~channel() { close(); }

net::task<status_code> channel::wait_ready(clock::time_point const deadline) {
    return detail::wait_ready_task(core_, deadline);
}

method_ref channel::bind(std::string const &name) {
    auto &methods = core_->methods;
    for (std::size_t index = 0; index != methods.size(); ++index)
        if (methods[index] == name) return method_ref{static_cast<std::uint32_t>(index + 1)};
    methods.push_back(name);
    for (auto &sub : core_->subs)
        if (sub.client) sub.client->methods.push_back(detail::method_slot{name, 0});
    return method_ref{static_cast<std::uint32_t>(methods.size())};
}

unary_call channel::call(method_ref method, request_body request, response_body response, call_spec const *spec,
                         response_trailer *trailer) noexcept {
    return unary_call{nullptr, core_.get(), method.index, request, response, spec, trailer};
}

open_operation channel::open(method_ref const method, method_kind const kind, call_spec const *spec) noexcept {
    return open_operation{nullptr, core_.get(), method.index, kind, spec};
}

std::size_t channel::ready_connections() const noexcept { return core_->ready_count(); }
void channel::close() noexcept { core_->close(); }

} // namespace rpc
