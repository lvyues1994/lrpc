#include <rpc/channel.hpp>
#include "connection.hpp"
#include "channel_private.hpp"
#include "stream_runtime.hpp"
#include <net/run.hpp>
#include <net/run_async.hpp>
#include <net/this_coro.hpp>
#include <net/timeout.hpp>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <unordered_map>

namespace rpc {
namespace detail {

struct channel_waiter;
struct channel_slot;
struct drain_pause {
    net::timer_wait_awaitable wait;
    net::io_env environment{};
    bool await_ready() noexcept { return false; }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> h, net::io_env const *env) noexcept {
        environment = *env; environment.stop_token = net::stop_token{};
        return wait.await_suspend(h, &environment);
    }
    void await_resume() noexcept { wait.await_resume(); }
};
struct channel_method {
    std::string name{};
    method_descriptor descriptor{};
};

struct channel_state : std::enable_shared_from_this<channel_state> {
    channel_state(net::io_context &context, channel_options config)
        : context(context), options(std::move(config)), deadlines(shard_deadlines(context)),
          deadline_capacity(*deadlines, options.max_waiting_calls), resolver_timer(context) {
        slots.reserve(options.max_connections);
        retired.reserve(options.max_retired_connections);
        methods.reserve(options.connection.max_registered_methods);
        identity = next_client_identity();
    }
    void start();
    void close() noexcept;
    void wake() noexcept;
    void remove(channel_waiter &waiter) noexcept;
    std::shared_ptr<client> pick() noexcept;
    method_handle bind(method_descriptor const &descriptor);
    std::vector<method_handle> bind(service_descriptor const &descriptor);
    channel_method const *lookup(method_handle handle) const noexcept {
        return handle.owner_ == identity && handle.index_ < methods.size() ? &methods[handle.index_] : nullptr;
    }
    metrics_snapshot snapshot() const noexcept;
    net::io_context &context;
    channel_options options;
    std::vector<std::unique_ptr<channel_slot>> slots{};
    std::vector<std::shared_ptr<client>> retired{};
    std::vector<channel_method> methods{};
    std::unordered_map<std::string, std::size_t> method_ids{};
    std::function<void(bool)> connectivity_observer{};
    std::uint64_t identity = 0;
    std::size_t cursor = 0;
    std::uint64_t random = 0x9e3779b97f4a7c15ULL;
    std::shared_ptr<deadline_scheduler> deadlines;
    deadline_reservation deadline_capacity;
    channel_waiter *head = nullptr, *tail = nullptr;
    net::stop_source stop{};
    net::steady_timer resolver_timer;
    metrics_snapshot counters{};
    bool started = false, closed = false, draining = false;
    std::size_t roots = 0;
    std::size_t logical_calls = 0;
    std::size_t replay_used = 0;
};

struct channel_slot final : client_events {
    channel_slot(channel_state &owner, net::ip::tcp::endpoint endpoint)
        : owner(owner), endpoint(endpoint), timer(owner.context) {}
    void on_connectivity_change() noexcept override { timer.cancel(); owner.wake(); }
    void on_capacity_change() noexcept override { owner.wake(); }
    channel_state &owner;
    net::ip::tcp::endpoint endpoint;
    std::shared_ptr<client> session{};
    net::steady_timer timer;
    connectivity state = connectivity::idle;
    clock::time_point connected_at{}, used_at{};
    clock::time_point next_connect_at{};
    std::chrono::milliseconds backoff{0};
    bool removed = false;
    bool dormant = false, running = false;
    bool demand = false;
};

enum class wait_phase { pending, ready, cancelled, expired };
struct wait_cancel {
    channel_waiter *waiter;
    void operator()() const noexcept;
};
struct channel_waiter {
    channel_state *owner = nullptr;
    channel_waiter *previous = nullptr, *next = nullptr;
    net::continuation continuation{};
    net::executor_ref executor{};
    std::atomic<wait_phase> phase{wait_phase::pending};
    inplace<net::stop_callback<wait_cancel>> cancellation{};
    deadline_node deadline{};
    clock::time_point entered{};
    std::size_t bytes = 0;
    bool linked = false;
    status_code result = status_code::ok;
    void publish(wait_phase value) noexcept {
        auto expected = wait_phase::pending;
        if (phase.compare_exchange_strong(expected, value)) executor.post(continuation);
    }
    static void expire(void *value, status_code code) noexcept {
        auto &self = *static_cast<channel_waiter *>(value);
        self.result = code; self.publish(wait_phase::expired);
    }
};
void wait_cancel::operator()() const noexcept { waiter->publish(wait_phase::cancelled); }

struct queued_wait {
    channel_waiter *waiter;
    clock::time_point until;
    bool await_ready() const noexcept { return false; }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
        auto &q = *waiter; auto &s = *q.owner;
        q.continuation.h = handle; q.executor = env->executor;
        q.phase.store(wait_phase::pending); q.entered = clock::now(); q.result = status_code::ok;
        if (s.closed || s.draining) { q.result = status_code::unavailable; q.publish(wait_phase::ready); return net::noop_coroutine(); }
        if (until == clock::time_point::max() || s.counters.waiting_calls >= s.options.max_waiting_calls ||
            q.bytes > s.options.max_waiting_bytes - s.counters.waiting_bytes) {
            q.result = status_code::resource_exhausted; ++s.counters.rejected;
            q.publish(wait_phase::ready); return net::noop_coroutine();
        }
        if (!schedule(*s.deadlines, q.deadline, until, &channel_waiter::expire, &q)) {
            q.result = status_code::unavailable; q.publish(wait_phase::ready); return net::noop_coroutine();
        }
        q.previous = s.tail; q.next = nullptr;
        if (s.tail) s.tail->next = &q; else s.head = &q;
        s.tail = &q; q.linked = true;
        ++s.counters.waiting_calls; s.counters.waiting_bytes += q.bytes; ++s.counters.queued;
        // Fully link and arm before stop_callback registration: it can fire synchronously.
        q.cancellation.emplace(env->stop_token, wait_cancel{&q});
        return net::noop_coroutine();
    }
    status_code await_resume() noexcept {
        auto &q = *waiter;
        q.cancellation.reset(); unschedule(*q.owner->deadlines, q.deadline);
        q.owner->remove(q); q.owner->wake();
        if (q.phase.load() == wait_phase::cancelled) return status_code::cancelled;
        return q.result;
    }
};

void channel_state::remove(channel_waiter &q) noexcept {
    if (!q.linked) return;
    if (q.previous) q.previous->next = q.next; else head = q.next;
    if (q.next) q.next->previous = q.previous; else tail = q.previous;
    --counters.waiting_calls; counters.waiting_bytes -= q.bytes;
    counters.queue_nanoseconds += static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now() - q.entered).count());
    q.linked = false; q.previous = q.next = nullptr;
}
void channel_state::wake() noexcept {
    if (connectivity_observer) {
        bool available = false;
        if (!closed && !draining) for (auto const &slot : slots)
            if (!slot->removed && slot->session && slot->session->ready()) { available = true; break; }
        connectivity_observer(available);
    }
    if (closed || draining) {
        for (auto *q = head; q; q = q->next) { q->result = status_code::unavailable; q->publish(wait_phase::ready); }
    } else if (head) {
        for (auto const &slot : slots) if (!slot->removed && slot->session && slot->session->has_capacity(0)) {
            head->publish(wait_phase::ready); break;
        }
    }
}

std::shared_ptr<client> channel_state::pick() noexcept {
    if (closed || draining || slots.empty()) return {};
    auto eligible = [](channel_slot const &slot) {
        return !slot.removed && slot.session && slot.session->ready() && slot.session->has_capacity(0);
    };
    channel_slot *chosen = nullptr;
    if (options.load_balance == balancing::power_of_two) {
        // Two choices among READY candidates; xorshift is shard-local, not a security RNG.
        random ^= random << 13; random ^= random >> 7; random ^= random << 17;
        for (std::size_t n = 0; n < slots.size(); ++n) {
            auto &candidate = *slots[(static_cast<std::size_t>(random) + n) % slots.size()];
            if (!eligible(candidate)) continue;
            if (!chosen) chosen = &candidate;
            else { if (candidate.session->stats().active_calls < chosen->session->stats().active_calls) chosen = &candidate; break; }
        }
    } else {
        for (std::size_t n = 0; n < slots.size(); ++n) {
            auto &candidate = *slots[cursor++ % slots.size()];
            if (eligible(candidate)) { chosen = &candidate; break; }
        }
    }
    if (!chosen) return {};
    chosen->used_at = clock::now();
    return chosen->session;
}

std::chrono::milliseconds next_backoff(channel_state &s, channel_slot &slot) noexcept {
    auto const value = slot.backoff.count() == 0 ? static_cast<double>(s.options.initial_backoff.count()) :
        std::min(static_cast<double>(s.options.max_backoff.count()), static_cast<double>(slot.backoff.count()) * s.options.backoff_multiplier);
    slot.backoff = std::chrono::milliseconds{static_cast<std::chrono::milliseconds::rep>(value)};
    s.random ^= s.random << 13; s.random ^= s.random >> 7; s.random ^= s.random << 17;
    auto const fraction = static_cast<double>(s.random % 10001U) / 5000.0 - 1.0;
    return std::chrono::milliseconds{std::max<std::chrono::milliseconds::rep>(1,
        static_cast<std::chrono::milliseconds::rep>(value * (1.0 + fraction * s.options.backoff_jitter)))};
}

auto manage_connection(std::shared_ptr<channel_state> s, channel_slot *slot)
    CO2_BEG(net::task<>, (s, slot), status_code connected; net::io_result<> waited; clock::time_point until; client_options config;) {
    while (!s->closed && !s->draining && !slot->removed) {
        s->retired.erase(std::remove_if(s->retired.begin(), s->retired.end(),
            [](std::shared_ptr<client> const &session) { return session->quiescent(); }), s->retired.end());
        if (!slot->session) {
            if (clock::now() < slot->next_connect_at) {
                slot->timer.expires_at(slot->next_connect_at);
                CO2_AWAIT_SET(waited, slot->timer.wait());
                continue; // Notifications never bypass the absolute backoff.
            }
            config = s->options.connection;
            config.events = slot;
            config.connection.handshake_timeout = s->options.connect_timeout;
            slot->session = std::shared_ptr<client>{make_client(s->context, config)};
            slot->state = connectivity::connecting;
            ++s->counters.reconnects;
            CO2_AWAIT_SET(connected, slot->session->connect(slot->endpoint));
            if (s->closed || s->draining || slot->removed) break;
            if (connected != status_code::ok) {
                slot->session->close(); slot->state = connectivity::backoff;
                slot->next_connect_at = clock::now() + next_backoff(*s, *slot);
            } else {
                slot->state = connectivity::ready; slot->backoff = std::chrono::milliseconds{0};
                slot->connected_at = slot->used_at = clock::now(); s->wake();
            }
        }
        if (s->closed || s->draining || slot->removed) break;
        if (slot->session && !slot->session->ready()) {
            if (slot->state == connectivity::ready && !slot->session->draining()) {
                slot->state = connectivity::backoff;
                slot->next_connect_at = clock::now() + next_backoff(*s, *slot);
            }
            if (slot->session->quiescent()) { slot->session.reset(); continue; }
            // GOAWAY: permit a bounded replacement while the old session drains.
            if (s->retired.size() < s->options.max_retired_connections) {
                slot->session->drain(); s->retired.push_back(std::move(slot->session)); continue;
            }
            slot->timer.expires_after(std::chrono::milliseconds{1});
            CO2_AWAIT_SET(waited, slot->timer.wait());
            continue;
        }
        until = clock::time_point::max();
        if (s->options.idle_timeout.count() != 0) until = slot->used_at + s->options.idle_timeout;
        if (s->options.max_connection_age.count() != 0) until = std::min(until, slot->connected_at + s->options.max_connection_age);
        if (clock::now() >= until) {
            if (slot->session->stats().active_calls == 0 ||
                (s->options.max_connection_age.count() != 0 && clock::now() >= slot->connected_at + s->options.max_connection_age)) {
                slot->session->drain(); slot->state = connectivity::draining;
                // Idle sessions stay closed until the next demand; age rotation reconnects.
                if (s->options.idle_timeout.count() != 0 && clock::now() >= slot->used_at + s->options.idle_timeout) { slot->dormant = true; break; }
                continue;
            }
            until = clock::now() + std::chrono::milliseconds{1};
        }
        slot->timer.expires_at(until);
        CO2_AWAIT_SET(waited, slot->timer.wait());
    }
    if (slot->session) {
        if (s->closed) slot->session->close(); else slot->session->drain();
        while (!slot->session->quiescent()) {
            slot->timer.expires_after(std::chrono::milliseconds{1});
            CO2_AWAIT((drain_pause{slot->timer.wait()}));
        }
        slot->session.reset();
    }
    slot->state = connectivity::closed;
    CO2_RETURN();
}
CO2_END

void launch_slot(std::shared_ptr<channel_state> const &s, channel_slot &slot) {
    slot.running = true;
    ++s->roots;
    try {
        net::run_async(s->context.get_executor(), s->stop.get_token(), nullptr,
            [s, &slot] {
                slot.running = false; --s->roots; s->resolver_timer.cancel();
                if (slot.demand && slot.dormant && !slot.removed && !s->closed && !s->draining) {
                    slot.demand = slot.dormant = false;
                    try { launch_slot(s, slot); } catch (...) { s->close(); }
                }
                s->wake();
            },
            [s, &slot](std::exception_ptr) { slot.running = false; --s->roots; s->close(); })
            ([s, &slot] { return manage_connection(s, &slot); });
    } catch (...) { slot.running = false; --s->roots; throw; }
}

auto resolve_channel(std::shared_ptr<channel_state> s)
    CO2_BEG(net::task<>, (s), resolution resolved; net::io_result<> waited;
            clock::time_point refresh_at{};) {
    while (!s->closed && !s->draining) {
        if (clock::now() >= refresh_at) {
            CO2_AWAIT_SET(resolved, s->options.resolve->resolve(s->context));
            refresh_at = clock::now() + s->options.resolver_refresh;
        }
        if (s->closed || s->draining) break;
        s->retired.erase(std::remove_if(s->retired.begin(), s->retired.end(),
            [](std::shared_ptr<client> const &session) { return session->quiescent(); }), s->retired.end());
        if (s->retired.empty()) s->slots.erase(std::remove_if(s->slots.begin(), s->slots.end(),
            [](std::unique_ptr<channel_slot> const &slot) { return slot->removed && !slot->running && !slot->session; }), s->slots.end());
        if (resolved.result.code == status_code::ok && !resolved.endpoints.empty() &&
            resolved.endpoints.size() <= s->options.max_connections / s->options.connections_per_backend) {
            for (auto &slot : s->slots) {
                if (std::find(resolved.endpoints.begin(), resolved.endpoints.end(), slot->endpoint) == resolved.endpoints.end()) {
                    slot->removed = true; slot->timer.cancel(); if (slot->session) slot->session->drain();
                }
            }
            if (s->retired.empty()) s->slots.erase(std::remove_if(s->slots.begin(), s->slots.end(),
                [](std::unique_ptr<channel_slot> const &slot) { return slot->removed && !slot->running && !slot->session; }), s->slots.end());
            for (auto const &endpoint : resolved.endpoints) {
                std::size_t existing = 0;
                for (auto const &slot : s->slots) if (slot->endpoint == endpoint && !slot->removed) ++existing;
                while (existing++ < s->options.connections_per_backend && s->slots.size() < s->options.max_connections) {
                    s->slots.push_back(std::make_unique<channel_slot>(*s, endpoint));
                    launch_slot(s, *s->slots.back());
                }
            }
        }
        s->wake();
        s->resolver_timer.expires_at(refresh_at);
        CO2_AWAIT_SET(waited, s->resolver_timer.wait());
    }
    while (!s->retired.empty()) {
        s->retired.erase(std::remove_if(s->retired.begin(), s->retired.end(),
            [](std::shared_ptr<client> const &session) { return session->quiescent(); }), s->retired.end());
        if (!s->retired.empty()) {
            s->resolver_timer.expires_after(std::chrono::milliseconds{1});
            CO2_AWAIT((drain_pause{s->resolver_timer.wait()}));
        }
    }
    CO2_RETURN();
}
CO2_END

void channel_state::start() {
    if (closed || draining) return;
    if (started) {
        for (auto &slot : slots) if (slot->dormant && !slot->removed) {
            if (slot->running) slot->demand = true;
            else { slot->dormant = false; launch_slot(shared_from_this(), *slot); }
        }
        return;
    }
    activate_deadlines(deadlines); started = true; ++roots;
    auto self = shared_from_this();
    try {
        net::run_async(context.get_executor(), stop.get_token(), nullptr,
            [self] { --self->roots; self->wake(); },
            [self](std::exception_ptr) { --self->roots; self->close(); })([self] { return resolve_channel(self); });
    } catch (...) { --roots; close(); throw; }
}
void channel_state::close() noexcept {
    if (closed) return;
    closed = true; stop.request_stop(); resolver_timer.cancel();
    for (auto &slot : slots) { slot->timer.cancel(); if (slot->session) slot->session->close(); }
    for (auto &session : retired) session->close();
    wake(); if (started) { started = false; deactivate_deadlines(*deadlines); }
}

method_handle channel_state::bind(method_descriptor const &descriptor) {
    if (!descriptor.name || descriptor.name[0] == '\0' || static_cast<unsigned>(descriptor.kind) > 3 ||
        !descriptor.request_codec || !descriptor.response_codec ||
        !descriptor.request_codec->size || !descriptor.request_codec->encode || !descriptor.request_codec->decode ||
        !descriptor.response_codec->size || !descriptor.response_codec->encode || !descriptor.response_codec->decode)
        throw std::invalid_argument{"invalid channel method"};
    std::string name{descriptor.name};
    if (name.size() > options.connection.connection.max_method_name_bytes) throw std::invalid_argument{"channel method name too long"};
    auto const found = method_ids.find(name);
    if (found != method_ids.end()) {
        auto const &old = methods[found->second].descriptor;
        if (old.request_codec != descriptor.request_codec || old.response_codec != descriptor.response_codec || old.semantics != descriptor.semantics || old.kind != descriptor.kind)
            throw std::invalid_argument{"conflicting channel method"};
        return {identity, found->second};
    }
    if (methods.size() == options.connection.max_registered_methods) throw std::invalid_argument{"channel method registry full"};
    auto const inserted = method_ids.emplace(name, methods.size());
    try { methods.push_back({std::move(name), descriptor}); }
    catch (...) { method_ids.erase(inserted.first); throw; }
    methods.back().descriptor.name = methods.back().name.c_str();
    return {identity, methods.size() - 1};
}
std::vector<method_handle> channel_state::bind(service_descriptor const &descriptor) {
    if (descriptor.size && !descriptor.methods) throw std::invalid_argument{"null channel methods"};
    std::vector<method_handle> result; result.reserve(descriptor.size);
    auto const previous = methods.size();
    try { for (std::size_t i = 0; i < descriptor.size; ++i) result.push_back(bind(descriptor.methods[i])); }
    catch (...) { while (methods.size() > previous) { method_ids.erase(methods.back().name); methods.pop_back(); } throw; }
    return result;
}
metrics_snapshot channel_state::snapshot() const noexcept {
    auto result = counters;
    result.replay_bytes_in_use = replay_used;
    auto add = [&](std::shared_ptr<client> const &session) {
        if (!session) return;
        auto const value = session->stats(); ++result.live_sessions;
        result.resources.connections += value.connections; result.resources.active_calls += value.active_calls;
        result.resources.request_bytes_in_use += value.request_bytes_in_use;
        result.resources.response_bytes_in_use += value.response_bytes_in_use;
        result.resources.control_bytes_in_use += value.control_bytes_in_use; result.resources.storage_bytes += value.storage_bytes;
    };
    for (auto const &slot : slots) add(slot->session);
    for (auto const &session : retired) add(session);
    return result;
}

status run_before(channel_state &s, call_info const &info, std::size_t &entered) noexcept {
    entered = 0;
    try { for (auto *hook : s.options.interceptors) {
        ++entered; auto result = hook->before(info);
        if (static_cast<unsigned>(result.code) > 16) return {status_code::internal};
        if (result.code != status_code::ok) return result;
    } }
    catch (...) {
        auto error = std::current_exception(); for (auto *hook : s.options.interceptors) hook->on_exception(error);
        return {status_code::internal, {}};
    }
    return {};
}
void run_after(channel_state &s, call_info const &info, call_result const &result, std::size_t entered) noexcept {
    while (entered != 0) {
        auto *hook = s.options.interceptors[--entered];
        try { hook->after(info, result); } catch (...) { hook->on_exception(std::current_exception()); }
    }
}

struct channel_buffers {
    wire::bytes_view request{};
    wire::mutable_bytes_view response{};
    encoded_request encoded{};
    decoded_response decoded{};
    bool typed = false;
};
struct logical_completion {
    channel_state *state = nullptr;
    call_info info{};
    call_result *result = nullptr;
    clock::time_point began{};
    std::size_t hooks = 0;
    void finish() noexcept {
        if (!state) return;
        ++state->counters.completed[static_cast<std::size_t>(result->code) <= 16 ? static_cast<std::size_t>(result->code) : 2];
        state->counters.call_nanoseconds += static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now() - began).count());
        run_after(*state, info, *result, hooks); state->wake(); state = nullptr;
    }
    ~logical_completion() { finish(); }
};
struct replay_reservation {
    channel_state *owner = nullptr;
    std::size_t bytes = 0;
    ~replay_reservation() { if (owner) owner->replay_used -= bytes; }
};
call_result finish_logical(logical_completion &completion, call_result &&result) noexcept {
    completion.finish(); return std::move(result);
}
struct guarded_attempt {
    net::task<call_result> task;
    bool await_ready() const noexcept { return task.await_ready(); }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> h, net::io_env const *env) noexcept { return task.await_suspend(h, env); }
    call_result await_resume() noexcept {
        try { return task.await_resume(); }
        catch (std::bad_alloc const &) { return {status_code::resource_exhausted}; }
        catch (...) { return {status_code::internal}; }
    }
};

auto channel_call_body(std::shared_ptr<channel_state> s, std::string method, method_handle handle,
                  channel_buffers buffers, call_options options)
    CO2_BEG(net::task<call_result>, (s, method, handle, buffers, options),
            net::io_env const *env = nullptr; call_result result; call_info info; status before;
            logical_completion completion; channel_waiter waiter; status_code waited;
            replay_reservation replay_budget;
            std::shared_ptr<client> session; std::vector<std::uint8_t> replay;
            std::vector<std::uint8_t> reply; std::vector<std::uint8_t> metadata;
            wire::bytes_view request; wire::mutable_bytes_view response; call_options attempt_options; wire::encode_result head_size;
            std::uint32_t attempt = 0; std::size_t attempt_hooks = 0; bool retrying = false; bool idempotent = false; bool ready = false;
            bool all_not_executed = true; bool terminal_remote = false; std::size_t replay_charge = 0; std::size_t reply_size = 0;
            std::unique_ptr<net::steady_timer> retry_timer; net::io_result<> delayed;
            std::chrono::milliseconds backoff; net::task<call_result> operation;) {
    CO2_AWAIT_SET(env, net::this_coro::environment);
    if (&env->executor.context() != &s->context) CO2_RETURN((call_result{status_code::failed_precondition}));
    if (s->closed || s->draining) { result.code = status_code::unavailable; result.not_executed = true; CO2_RETURN(std::move(result)); }
    options.deadline = resolve_deadline(options); options.timeout = std::chrono::microseconds{0};
    result.not_executed = true;
    if (handle) {
        auto *binding = s->lookup(handle);
        if (!binding || binding->descriptor.kind != method_kind::unary || !buffers.typed || buffers.encoded.operations != binding->descriptor.request_codec ||
            buffers.decoded.operations != binding->descriptor.response_codec) { result.code = status_code::invalid_argument; CO2_RETURN(finish_logical(completion, std::move(result))); }
        method = binding->name;
        idempotent = binding->descriptor.semantics == idempotency::idempotent || binding->descriptor.semantics == idempotency::no_side_effects;
    }
    if (method.empty() || method.size() > s->options.connection.connection.max_method_name_bytes ||
        (buffers.request.size && !buffers.request.data) || (buffers.response.size && !buffers.response.data) ||
        (options.response_metadata.size && !options.response_metadata.data) ||
        (buffers.typed && (!buffers.encoded.message || !buffers.encoded.operations || !buffers.encoded.operations->size ||
            !buffers.encoded.operations->encode || !buffers.decoded.message || !buffers.decoded.operations || !buffers.decoded.operations->decode))) {
        result.code = status_code::invalid_argument; CO2_RETURN(finish_logical(completion, std::move(result)));
    }
    if (buffers.response.size && options.response_metadata.size) {
        auto const a = reinterpret_cast<std::uintptr_t>(buffers.response.data);
        auto const b = reinterpret_cast<std::uintptr_t>(options.response_metadata.data);
        if (a <= b ? b - a < buffers.response.size : a - b < options.response_metadata.size) {
            result.code = status_code::invalid_argument; CO2_RETURN(finish_logical(completion, std::move(result)));
        }
    }
    info = {&method, options.deadline, options.metadata, 0};
    completion.state = s.get(); completion.info = info; completion.result = &result; completion.began = clock::now();
    ++s->counters.calls;
    info.call_id = s->counters.calls; info.began = completion.began; completion.info = info;
    before = run_before(*s, info, completion.hooks);
    if (before.code != status_code::ok) { result.code = before.code; result.message = std::move(before.message); CO2_RETURN(finish_logical(completion, std::move(result))); }
    if (options.retry.max_attempts == 0 || options.retry.max_attempts > 100 || options.retry.initial_backoff.count() < 0 ||
        options.retry.max_backoff < options.retry.initial_backoff || !std::isfinite(options.retry.multiplier) || options.retry.multiplier < 1.0) {
        result.code = status_code::invalid_argument; CO2_RETURN(finish_logical(completion, std::move(result)));
    }
    try {
        s->start(); request = buffers.request; response = buffers.response;
        if (buffers.typed && (!buffers.encoded.message || !buffers.encoded.operations || !buffers.decoded.message || !buffers.decoded.operations))
            throw std::invalid_argument{"null channel codec"};
        waiter.bytes = buffers.typed ? buffers.encoded.operations->size(buffers.encoded.message) : request.size;
        head_size = wire::request_head_size({0, {reinterpret_cast<std::uint8_t const *>(method.data()), method.size()}, options.metadata}, true);
        if (head_size.code != wire::error::none) {
            result.code = status_code::invalid_argument; CO2_RETURN(finish_logical(completion, std::move(result)));
        }
        // A request that cannot fit even on an empty local session must never
        // occupy a waiting permit. Full head grammar is validated by unary.
        if (waiter.bytes > s->options.connection.connection.receive.max_message_size ||
            head_size.written > s->options.connection.connection.receive.max_frame_size ||
            (!(s->options.connection.connection.receive.features & wire::streaming) &&
             waiter.bytes > s->options.connection.connection.receive.max_frame_size - head_size.written)) {
            result.code = status_code::resource_exhausted; CO2_RETURN(finish_logical(completion, std::move(result)));
        }
        if (options.retry.max_attempts > 1) {
            if (waiter.bytes > s->options.connection.connection.receive.max_message_size) throw std::invalid_argument{"replay exceeds message limit"};
            reply_size = buffers.typed ? s->options.connection.connection.receive.max_message_size : response.size;
            if (waiter.bytes > s->options.replay_bytes || reply_size > s->options.replay_bytes - waiter.bytes ||
                options.response_metadata.size > s->options.replay_bytes - waiter.bytes - reply_size) throw std::bad_alloc{};
            replay_charge = waiter.bytes + reply_size + options.response_metadata.size;
            if (replay_charge > s->options.replay_bytes - s->replay_used) throw std::bad_alloc{};
            s->replay_used += replay_charge; replay_budget.owner = s.get(); replay_budget.bytes = replay_charge;
            replay.resize(waiter.bytes);
            if (buffers.typed) {
                if (!buffers.encoded.operations->encode(buffers.encoded.message, {replay.data(), replay.size()})) throw std::invalid_argument{"replay encoding failed"};
            } else if (request.size) {
                if (!request.data) throw std::invalid_argument{"null channel request"};
                std::memcpy(replay.data(), request.data, request.size);
            }
            request = {replay.data(), replay.size()};
            reply.resize(reply_size);
            metadata.resize(options.response_metadata.size);
            response = {reply.data(), reply.size()};
            retry_timer = std::make_unique<net::steady_timer>(s->context);
        }
    } catch (std::bad_alloc const &) { result.code = status_code::resource_exhausted; CO2_RETURN(finish_logical(completion, std::move(result))); }
    catch (...) { result.code = status_code::invalid_argument; CO2_RETURN(finish_logical(completion, std::move(result))); }
    waiter.owner = s.get(); attempt_options = options;
    waiter.bytes += head_size.written + wire::header_size;
    if (options.retry.max_attempts > 1) attempt_options.response_metadata = {metadata.data(), metadata.size()};
    backoff = options.retry.initial_backoff;
    while (attempt < options.retry.max_attempts) {
        terminal_remote = false;
        if (env->stop_token.stop_requested()) { result = call_result{status_code::cancelled}; break; }
        if (clock::now() >= options.deadline) { result = call_result{status_code::deadline_exceeded}; break; }
        if (s->closed || s->draining) { result = call_result{status_code::unavailable}; break; }
        ready = false;
        for (auto const &slot : s->slots) if (!slot->removed && slot->session && slot->session->ready()) { ready = true; break; }
        session = s->head && !retrying ? std::shared_ptr<client>{} : s->pick();
        if (!session) {
            if (!(ready ? options.wait_for_capacity : options.wait_for_ready)) {
                result = call_result{ready ? status_code::resource_exhausted : status_code::unavailable}; ++s->counters.rejected; break;
            }
            CO2_AWAIT_SET(waited, (queued_wait{&waiter, options.deadline}));
            if (waited != status_code::ok) { result = call_result{waited}; break; }
            retrying = true; continue;
        }
        ++attempt; ++s->counters.attempts; info.attempt = attempt;
        info.began = clock::now();
        before = run_before(*s, info, attempt_hooks);
        if (before.code != status_code::ok) {
            result.code = before.code; result.message = std::move(before.message);
            run_after(*s, info, result, attempt_hooks); session.reset(); break;
        }
        try {
            operation = buffers.typed && options.retry.max_attempts == 1 ?
                session->call_encoded(method, buffers.encoded, buffers.decoded, attempt_options) :
                session->call(method, request, response, attempt_options);
        } catch (std::bad_alloc const &) { result.code = status_code::resource_exhausted; run_after(*s, info, result, attempt_hooks); session.reset(); break; }
        catch (...) { result.code = status_code::internal; run_after(*s, info, result, attempt_hooks); session.reset(); break; }
        CO2_AWAIT_SET(result, (guarded_attempt{std::move(operation)}));
        session.reset();
        terminal_remote = !result.capacity_rejected;
        all_not_executed = all_not_executed && result.not_executed;
        result.attempts = attempt; run_after(*s, info, result, attempt_hooks); s->wake();
        if (result.capacity_rejected && options.wait_for_capacity) {
            --attempt; --s->counters.attempts; terminal_remote = false;
            CO2_AWAIT_SET(waited, (queued_wait{&waiter, options.deadline}));
            if (waited != status_code::ok) { result = call_result{waited}; break; }
            retrying = true; continue;
        }
        if (result.code == status_code::ok || attempt == options.retry.max_attempts ||
            static_cast<unsigned>(result.code) > 16 ||
            (options.retry.retryable_codes & (1U << static_cast<unsigned>(result.code))) == 0 ||
            (!result.not_executed && !idempotent)) break;
        ++s->counters.retries; retrying = true;
        terminal_remote = false;
        retry_timer->expires_at(std::min(options.deadline, clock::now() + backoff));
        CO2_AWAIT_SET(delayed, retry_timer->wait());
        if (delayed.ec) { result = call_result{env->stop_token.stop_requested() ? status_code::cancelled : status_code::unavailable}; break; }
        backoff = std::chrono::milliseconds{static_cast<std::chrono::milliseconds::rep>(std::min(
            static_cast<double>(options.retry.max_backoff.count()), static_cast<double>(backoff.count()) * options.retry.multiplier))};
    }
    if (env->stop_token.stop_requested()) { result = call_result{status_code::cancelled}; terminal_remote = false; }
    else if (clock::now() >= options.deadline) { result = call_result{status_code::deadline_exceeded}; terminal_remote = false; }
    if (options.retry.max_attempts > 1 && attempt != 0 && terminal_remote) {
        if (result.code == status_code::ok) {
            try {
                if (buffers.typed && !buffers.decoded.operations->decode({reply.data(), result.response_size}, buffers.decoded.message)) result.code = status_code::data_loss;
                else if (!buffers.typed && result.response_size) std::memcpy(buffers.response.data, reply.data(), result.response_size);
            } catch (std::bad_alloc const &) { result.code = status_code::resource_exhausted; }
            catch (...) { result.code = status_code::data_loss; }
        }
        if (env->stop_token.stop_requested()) result = call_result{status_code::cancelled};
        else if (clock::now() >= options.deadline) result = call_result{status_code::deadline_exceeded};
        if (result.response_metadata.entries.size) {
            std::memcpy(options.response_metadata.data, result.response_metadata.entries.data, result.response_metadata.entries.size);
            result.response_metadata.entries.data = options.response_metadata.data;
        } else result.response_metadata = {};
    }
    result.not_executed = all_not_executed;
    result.attempts = attempt;
    CO2_RETURN(finish_logical(completion, std::move(result)));
}
CO2_END

struct relay_stop {
    net::stop_source *source;
    void operator()() const noexcept { source->request_stop(); }
};
struct logical_lifetime {
    channel_state *owner = nullptr;
    ~logical_lifetime() { if (owner) { --owner->logical_calls; owner->wake(); } }
};
auto channel_call(std::shared_ptr<channel_state> s, std::string method, method_handle handle,
                  channel_buffers buffers, call_options options)
    CO2_BEG(net::task<call_result>, (s, method, handle, buffers, options),
            logical_lifetime lifetime; net::io_env const *env = nullptr; net::stop_source stop;
            inplace<net::stop_callback<relay_stop>> parent, shutdown;
            call_result result;) {
    CO2_AWAIT_SET(env, net::this_coro::environment);
    if (&env->executor.context() != &s->context) CO2_RETURN((call_result{status_code::failed_precondition}));
    if (s->logical_calls >= s->options.max_logical_calls) { result.code = status_code::resource_exhausted; result.not_executed = true; CO2_RETURN(std::move(result)); }
    ++s->logical_calls; lifetime.owner = s.get();
    options.deadline = resolve_deadline(options); options.timeout = std::chrono::microseconds{0};
    parent.emplace(env->stop_token, relay_stop{&stop}); shutdown.emplace(s->stop.get_token(), relay_stop{&stop});
    CO2_AWAIT_SET(result, net::run(stop.get_token())(channel_call_body(s, std::move(method), handle, buffers, options)));
    CO2_RETURN(std::move(result));
}
CO2_END

auto warm_channel(std::shared_ptr<channel_state> s, call_options options)
    CO2_BEG(net::task<status_code>, (s, options), net::io_env const *env = nullptr;
            std::unique_ptr<net::steady_timer> timer; net::io_result<> waited;) {
    CO2_AWAIT_SET(env, net::this_coro::environment);
    if (&env->executor.context() != &s->context) CO2_RETURN(status_code::failed_precondition);
    options.deadline = resolve_deadline(options);
    if (options.deadline == clock::time_point::max()) options.deadline = clock::now() + s->options.connect_timeout;
    s->start(); timer = std::make_unique<net::steady_timer>(s->context);
    while (!s->closed && !s->draining) {
        if (env->stop_token.stop_requested()) CO2_RETURN(status_code::cancelled);
        if (clock::now() >= options.deadline) CO2_RETURN(status_code::deadline_exceeded);
        for (auto const &slot : s->slots) if (!slot->removed && slot->session && slot->session->ready()) CO2_RETURN(status_code::ok);
        timer->expires_at(std::min(options.deadline, clock::now() + std::chrono::milliseconds{1}));
        CO2_AWAIT_SET(waited, timer->wait());
        if (waited.ec) CO2_RETURN(io_status(waited.ec));
    }
    CO2_RETURN(status_code::unavailable);
}
CO2_END

auto shutdown_channel(std::shared_ptr<channel_state> s, std::chrono::milliseconds grace)
    CO2_BEG(net::task<>, (s, grace), std::unique_ptr<net::steady_timer> timer; clock::time_point until; net::io_result<> waited;) {
    timer = std::make_unique<net::steady_timer>(s->context);
    s->draining = true; s->wake(); s->resolver_timer.cancel();
    for (auto &slot : s->slots) { slot->timer.cancel(); if (slot->session) slot->session->drain(); }
    until = clock::now() + std::max(grace, std::chrono::milliseconds::zero());
    while (s->logical_calls != 0 && clock::now() < until) {
        timer->expires_at(std::min(until, clock::now() + std::chrono::milliseconds{1}));
        CO2_AWAIT_SET(waited, timer->wait()); if (waited.ec) break;
    }
    s->close();
    while (s->roots != 0 || s->logical_calls != 0 || s->snapshot().waiting_calls != 0) {
        timer->expires_after(std::chrono::milliseconds{1});
        CO2_AWAIT((drain_pause{timer->wait()}));
    }
    CO2_RETURN();
}
CO2_END

struct channel_stream_completion {
    std::shared_ptr<channel_state> owner;
    std::string method;
    call_info info;
    std::size_t logical_hooks = 0, attempt_hooks = 0;
    bool counted = false;
    void finish(call_result const &result) noexcept {
        info.method = &method;
        if (attempt_hooks) { info.attempt = 1; run_after(*owner, info, result, attempt_hooks); attempt_hooks = 0; }
        info.attempt = 0; run_after(*owner, info, result, logical_hooks); logical_hooks = 0;
        if (counted) {
            counted = false; ++owner->counters.completed[static_cast<unsigned>(result.code) <= 16 ? static_cast<unsigned>(result.code) : 2];
            owner->counters.call_nanoseconds += static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now() - info.began).count());
            --owner->logical_calls; owner->wake();
        }
    }
    ~channel_stream_completion() { if (counted) finish(call_result{status_code::cancelled}); }
};
struct session_stream final : byte_stream {
    session_stream(std::shared_ptr<client> session, std::shared_ptr<byte_stream> stream) : session(std::move(session)), stream(std::move(stream)) {}
    net::task<stream_read_result> read(byte_buffer &value) override { return stream->read(value); }
    net::task<status_code> write(byte_buffer value) override { return stream->write(std::move(value)); }
    net::task<status_code> write_encoded(encoded_request value) override { return stream->write_encoded(value); }
    net::task<status_code> writes_done() override { return stream->writes_done(); }
    net::task<call_result> finish() override { return stream->finish(); }
    void cancel() noexcept override { stream->cancel(); }
    std::shared_ptr<client> session;
    std::shared_ptr<byte_stream> stream;
};
struct open_stream_cleanup {
    std::shared_ptr<byte_stream> stream;
    ~open_stream_cleanup() { if (stream) stream->cancel(); }
};
stream_open_result failed_open(channel_stream_completion &completion, status_code code) {
    completion.finish(call_result{code}); return {code};
}
auto channel_open_stream(std::shared_ptr<channel_state> s, std::string method, method_kind kind, method_handle handle, call_options options)
    CO2_BEG(net::task<stream_open_result>, (s, method, kind, handle, options),
            net::io_env const *env; std::shared_ptr<channel_stream_completion> completion;
            channel_waiter waiter; status_code waited; status before; std::shared_ptr<client> session;
            stream_open_result opened; wire::encode_result head; bool ready = false, resumed = false; open_stream_cleanup cleanup;
            std::shared_ptr<session_stream> wrapper; std::function<void(call_result const &)> observer;) {
    CO2_AWAIT_SET(env, net::this_coro::environment);
    if (&env->executor.context() != &s->context) CO2_RETURN((stream_open_result{status_code::failed_precondition}));
    if (s->closed || s->draining) CO2_RETURN((stream_open_result{status_code::unavailable}));
    if (handle) { auto m = s->lookup(handle); if (!m) CO2_RETURN((stream_open_result{status_code::invalid_argument})); method = m->name; kind = m->descriptor.kind; }
    if (options.retry.max_attempts != 1 || static_cast<unsigned>(kind) > 3 || method.empty()) CO2_RETURN((stream_open_result{status_code::invalid_argument}));
    options.deadline = resolve_deadline(options); options.timeout = std::chrono::microseconds{0};
    if (s->logical_calls == s->options.max_logical_calls) CO2_RETURN((stream_open_result{status_code::resource_exhausted}));
    completion = std::make_shared<channel_stream_completion>(); completion->owner = s; completion->method = method;
    completion->info = {&completion->method, options.deadline, options.metadata, 0, ++s->counters.calls, clock::now()};
    ++s->logical_calls; completion->counted = true;
    before = run_before(*s, completion->info, completion->logical_hooks);
    if (before.code != status_code::ok) { completion->finish(call_result{before.code}); CO2_RETURN((stream_open_result{before.code})); }
    head = wire::request_head_size({0, {reinterpret_cast<std::uint8_t const *>(method.data()), method.size()}, options.metadata}, true);
    if (head.code != wire::error::none || head.written > s->options.connection.connection.receive.max_frame_size) CO2_RETURN(failed_open(*completion, status_code::invalid_argument));
    s->start(); waiter.owner = s.get(); waiter.bytes = head.written + wire::header_size;
    for (;;) {
        if (s->closed || s->draining) CO2_RETURN(failed_open(*completion, status_code::unavailable));
        if (env->stop_token.stop_requested()) CO2_RETURN(failed_open(*completion, status_code::cancelled));
        if (clock::now() >= options.deadline) CO2_RETURN(failed_open(*completion, status_code::deadline_exceeded));
        session = s->head && !resumed ? std::shared_ptr<client>{} : s->pick(); if (session) break;
        ready = false; for (auto const &slot : s->slots) if (!slot->removed && slot->session && slot->session->ready()) { ready = true; break; }
        if (!(ready ? options.wait_for_capacity : options.wait_for_ready)) CO2_RETURN(failed_open(*completion, ready ? status_code::resource_exhausted : status_code::unavailable));
        CO2_AWAIT_SET(waited, (queued_wait{&waiter, options.deadline}));
        if (waited != status_code::ok) CO2_RETURN(failed_open(*completion, waited));
        resumed = true;
    }
    completion->info.attempt = 1; ++s->counters.attempts;
    before = run_before(*s, completion->info, completion->attempt_hooks);
    if (before.code != status_code::ok) { completion->finish(call_result{before.code}); CO2_RETURN((stream_open_result{before.code})); }
    wrapper = std::make_shared<session_stream>(session, std::shared_ptr<byte_stream>{});
    observer = [record = completion](call_result const &result) { record->finish(result); };
    CO2_AWAIT_SET(opened, session->open_stream(method, kind, options));
    if (!opened.stream) { completion->finish(call_result{opened.code}); CO2_RETURN(std::move(opened)); }
    cleanup.stream = opened.stream;
    if (!opened.stream->observe_completion(std::move(observer))) {
        opened.stream->cancel(); completion->finish(call_result{status_code::unimplemented}); CO2_RETURN((stream_open_result{status_code::unimplemented}));
    }
    wrapper->stream = std::move(opened.stream); opened.stream = std::move(wrapper);
    cleanup.stream.reset(); completion.reset(); CO2_RETURN(std::move(opened));
}
CO2_END
struct channel_impl final : channel {
    explicit channel_impl(std::shared_ptr<channel_state> s) : state_(std::move(s)) {}
    ~channel_impl() override { state_->close(); }
    net::task<status_code> connect(net::ip::tcp::endpoint endpoint) override {
        if (state_->started) return failed_stream_operation(status_code::failed_precondition);
        if (!state_->started) state_->options.resolve = make_static_resolver({endpoint});
        return warm_channel(state_, {});
    }
    net::task<status_code> attach(std::unique_ptr<transport>) override {
        return unsupported_attach();
    }
    static auto unsupported_attach() CO2_BEG(net::task<status_code>, ()) { CO2_RETURN(status_code::unimplemented); } CO2_END
    net::task<call_result> call(std::string method, wire::bytes_view request, wire::mutable_bytes_view response, call_options options) override {
        return channel_call(state_, std::move(method), {}, {request, response}, options);
    }
    net::task<call_result> call_encoded(std::string method, encoded_request request, decoded_response response, call_options options) override {
        return channel_call(state_, std::move(method), {}, {{}, {}, request, response, true}, options);
    }
    net::task<call_result> call_encoded(method_handle method, encoded_request request, decoded_response response, call_options options) override {
        return channel_call(state_, {}, method, {{}, {}, request, response, true}, options);
    }
    method_handle bind(method_descriptor const &m) override { return state_->bind(m); }
    net::task<stream_open_result> open_stream(std::string method, method_kind kind, call_options options) override { return channel_open_stream(state_, std::move(method), kind, {}, options); }
    net::task<stream_open_result> open_stream(method_handle method, call_options options) override { return channel_open_stream(state_, {}, method_kind::unary, method, options); }
    std::vector<method_handle> bind(service_descriptor const &s) override { return state_->bind(s); }
    bool ready() const noexcept override { for (auto const &slot : state_->slots) if (!slot->removed && slot->session && slot->session->ready()) return true; return false; }
    resource_stats stats() const noexcept override { return state_->snapshot().resources; }
    metrics_snapshot metrics() const noexcept override { return state_->snapshot(); }
    bool quiescent() const noexcept override {
        if (state_->roots != 0 || state_->logical_calls != 0 || state_->counters.waiting_calls != 0) return false;
        for (auto const &slot : state_->slots) if (slot->session && !slot->session->quiescent()) return false;
        for (auto const &session : state_->retired) if (!session->quiescent()) return false;
        return true;
    }
    void close() noexcept override { state_->close(); }
    void drain() noexcept override {
        state_->draining = true; state_->wake(); state_->resolver_timer.cancel();
        for (auto &slot : state_->slots) { slot->timer.cancel(); if (slot->session) slot->session->drain(); }
        if (state_->started) { state_->started = false; deactivate_deadlines(*state_->deadlines); }
    }
    net::task<status_code> warmup(call_options options) override { return warm_channel(state_, options); }
    net::task<> shutdown(std::chrono::milliseconds grace) override { return shutdown_channel(state_, grace); }
private:
    friend std::size_t registration_checkpoint(channel &) noexcept;
    friend void rollback_registration(channel &, std::size_t) noexcept;
    friend void observe_connectivity(channel &, std::function<void(bool)>);
    std::shared_ptr<channel_state> state_;
};
std::size_t registration_checkpoint(channel &actor) noexcept { return static_cast<channel_impl &>(actor).state_->methods.size(); }
void rollback_registration(channel &actor, std::size_t checkpoint) noexcept {
    auto &s = *static_cast<channel_impl &>(actor).state_;
    while (s.methods.size() > checkpoint) { s.method_ids.erase(s.methods.back().name); s.methods.pop_back(); }
}
void observe_connectivity(channel &actor, std::function<void(bool)> callback) {
    static_cast<channel_impl &>(actor).state_->connectivity_observer = std::move(callback);
}
} // namespace detail

std::unique_ptr<channel> make_channel(net::io_context &owner, channel_options options) {
    if (options.connection.connection.receive.features & wire::streaming) detail::validate_stream_options(options.connection.connection);
    else detail::validate_options(options.connection.connection);
    if (!options.resolve || options.connections_per_backend == 0 || options.max_connections == 0 ||
        options.connections_per_backend > options.max_connections || options.max_logical_calls == 0 || options.connect_timeout.count() <= 0 ||
        options.initial_backoff.count() <= 0 || options.max_backoff < options.initial_backoff ||
        !std::isfinite(options.backoff_multiplier) || options.backoff_multiplier < 1.0 ||
        !std::isfinite(options.backoff_jitter) || options.backoff_jitter < 0.0 || options.backoff_jitter > 1.0 ||
        options.resolver_refresh.count() <= 0 || options.idle_timeout.count() < 0 || options.max_connection_age.count() < 0 ||
        ((options.max_waiting_calls == 0) != (options.max_waiting_bytes == 0)) ||
        std::find(options.interceptors.begin(), options.interceptors.end(), nullptr) != options.interceptors.end())
        throw std::invalid_argument{"invalid channel limits"};
    return std::make_unique<detail::channel_impl>(std::make_shared<detail::channel_state>(owner, std::move(options)));
}
} // namespace rpc
