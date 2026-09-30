#include "connection.hpp"
#include "stream_runtime.hpp"

#include <net/run_async.hpp>
#include <net/timeout.hpp>

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <unordered_map>

namespace rpc {
namespace detail {

struct server_connection;

struct server_call {
    std::shared_ptr<server_connection> owner{};
    slot_handle handle{};
    block_lease request{};
    block_lease response{};
    response_writer output{{}};
    net::stop_source stop{co2::nostopstate};
    deadline_node timeout{};
    method_binding const *method = nullptr;
    server_context call_context{};
    wire::bytes_view body{};
    net::io_env child_env{};
    net::task<status_code> child{};
    event available{};
    std::uint32_t id = 0;
    status_code immediate = status_code::internal;
    bool busy = false;
    bool worker_running = false;
    bool cancelled = false;
    bool expired = false;
    status_code forced_code = status_code::ok;
    server_call *previous = nullptr, *next = nullptr;
    std::size_t queued_bytes = 0;
    bool queued = false, executing = false, invoked = false;
};

struct server_state {
    server_state(net::io_context &ctx, std::vector<method_binding> bindings, server_options config, std::size_t response_size)
        : context(ctx), methods(std::move(bindings)), options(config),
          requests(config.connection.receive.max_frame_size, config.request_bytes),
          responses(response_size, config.response_bytes), controls(64, config.control_bytes),
          slots(config.max_active_calls + config.max_queued_calls), deadlines(shard_deadlines(ctx)), reserved_deadlines(*deadlines, slots.capacity()) {
        if (requests.charge() > config.request_bytes_per_connection ||
            responses.charge() > config.response_bytes_per_connection || controls.charge() > config.control_bytes_per_connection)
            throw std::invalid_argument{"per-connection budget cannot hold one pool block"};
        connections.reserve(config.max_connections);
    }
    void maybe_stop() noexcept;
    void remove_queued(server_call &call) noexcept;
    void promote() noexcept;
    net::io_context &context;
    std::vector<method_binding> methods;
    server_options options;
    block_pool requests;
    block_pool responses;
    block_pool controls;
    slot_pool<server_call> slots;
    std::shared_ptr<deadline_scheduler> deadlines;
    deadline_reservation reserved_deadlines;
    bool runtime_active = false;
    std::unique_ptr<net::tcp_acceptor> acceptor{};
    std::unordered_map<std::uint64_t, std::shared_ptr<server_connection>> connections{};
    std::size_t live_connections = 0;
    std::size_t active_calls = 0;
    std::size_t queued_calls = 0, queued_bytes = 0;
    server_call *queue_head = nullptr, *queue_tail = nullptr;
    std::uint64_t next_connection = 1;
    bool draining = false;
    bool closed = false;
};
void close_service(server_state &state) noexcept;

class session_slot {
public:
    explicit session_slot(server_state &service) noexcept : service_(service) { ++service_.live_connections; }
    ~session_slot() { --service_.live_connections; }
    session_slot(session_slot const &) = delete;
    session_slot &operator=(session_slot const &) = delete;
private:
    server_state &service_;
};


struct registered_method {
    method_binding const *binding = nullptr;
    bool defined = false;
};

struct server_connection final : connection_observer {
    server_connection(std::shared_ptr<server_state> service, std::uint64_t number)
        : service(std::move(service)), slot(*this->service), id(number),
          request_budget{this->service->options.request_bytes_per_connection, 0},
          response_budget{this->service->options.response_bytes_per_connection, 0},
          control_budget{this->service->options.control_bytes_per_connection, 0},
          conn(this->service->context, this->service->options.connection, this->service->controls, control_budget, *this),
          methods(static_cast<std::size_t>(this->service->options.connection.receive.max_method_ids) + 1),
          calls(this->service->options.connection.receive.max_concurrent_streams) {}

    void on_frame(wire::frame_view const &frame) override;
    void receive_request(wire::frame_view const &frame);
    void finish_call(server_call &call, status_code code) noexcept;
    bool prepare_request(block &) noexcept override { return true; }
    void on_idle() noexcept override {
        if (conn.state == phase::draining && calls.empty() && conn.tx.empty() && !conn.writing) conn.close();
    }
    void cancel(server_call &call) noexcept {
        call.cancelled = true;
        unschedule(*service->deadlines, call.timeout);
        call.stop.request_stop();
        if (call.queued) call.available.signal();
    }
    void on_close() noexcept override {
        for (std::size_t i = 0; i < service->slots.capacity(); ++i) {
            auto &call = service->slots.at(i);
            if (call.busy && call.owner.get() == this) cancel(call);
        }
        service->connections.erase(id);
        service->maybe_stop();
    }
    void drain() noexcept {
        if (conn.state == phase::handshaking) { conn.close(); return; }
        if (conn.state != phase::ready) return;
        conn.state = phase::draining;
        conn.control(wire::frame_type::goaway, 0, last_admitted);
        on_idle();
    }

    std::shared_ptr<server_state> service;
    // Declared before resources so its quota is returned after their destruction.
    session_slot slot;
    std::uint64_t id;
    std::weak_ptr<server_connection> self{};
    byte_budget request_budget;
    byte_budget response_budget;
    byte_budget control_budget;
    connection conn;
    std::vector<registered_method> methods;
    stream_index<server_call> calls;
    std::uint32_t last_request = 0;
    std::uint32_t last_admitted = 0;
};

void server_state::maybe_stop() noexcept {
    if (!runtime_active || slots.size() != 0 || !connections.empty() || (acceptor && !closed && !draining)) return;
    runtime_active = false;
    deactivate_deadlines(*deadlines);
    for (std::size_t i = 0; i < slots.capacity(); ++i) slots.at(i).available.signal();
}
void server_state::remove_queued(server_call &call) noexcept {
    if (!call.queued) return;
    if (call.previous) call.previous->next = call.next; else queue_head = call.next;
    if (call.next) call.next->previous = call.previous; else queue_tail = call.previous;
    --queued_calls; queued_bytes -= call.queued_bytes;
    call.previous = call.next = nullptr; call.queued = false;
}
void server_state::promote() noexcept {
    while (queue_head && active_calls < options.max_active_calls) {
        auto &call = *queue_head; remove_queued(call);
        if (!call.cancelled && !call.expired && clock::now() < call.call_context.deadline) {
            call.executing = true; ++active_calls;
            call.owner->last_admitted = std::max(call.owner->last_admitted, call.id);
        }
        call.available.signal();
    }
}

void server_connection::finish_call(server_call &call, status_code code) noexcept {
    unschedule(*service->deadlines, call.timeout);
    auto const invalid = call.output.overflowed() || call.call_context.response_metadata.overflowed() ||
        call.output.size() > call.method->max_response_bytes ||
        call.call_context.response_metadata.size() > call.method->max_response_head_bytes || static_cast<std::uint32_t>(code) > 16 ||
        (code == status_code::ok && call.call_context.response_metadata.has_message());
    if (invalid) code = status_code::internal;
    if (call.forced_code != status_code::ok) code = call.forced_code;
    if (call.expired || clock::now() >= call.call_context.deadline) code = status_code::deadline_exceeded;
    if (!call.cancelled && conn.state != phase::closed) {
        if (!encode_end(*call.response.get(), call.id, code, call.output.size(), conn.peer,
                        invalid ? 2 : call.call_context.response_metadata.size(), 16 + call.method->max_response_head_bytes)) conn.close();
        else {
            if (!call.invoked && code != status_code::ok &&
                (conn.peer.features & conn.options.receive.features & wire::explicit_rejection) != 0)
                call.response.get()->data[9] = wire::not_executed;
            conn.enqueue(std::move(call.response));
        }
    }
    calls.erase(call.id);
    service->remove_queued(call);
    if (call.executing) --service->active_calls;
    call.executing = call.invoked = false;
    call.request.reset(); call.response.reset(); call.call_context = {};
    call.stop = net::stop_source{co2::nostopstate};
    call.child_env = {}; call.body = {}; call.owner.reset(); call.busy = false;
    service->slots.release(call.handle);
    service->promote();
    on_idle();
    service->maybe_stop();
}

void prepare_handler(server_call &call) noexcept {
    call.immediate = status_code::internal;
    if (call.cancelled || call.owner->conn.state == phase::closed) { call.immediate = status_code::cancelled; return; }
    if (clock::now() >= call.call_context.deadline) { call.immediate = status_code::deadline_exceeded; return; }
    if (call.forced_code != status_code::ok) { call.immediate = call.forced_code; return; }
    try { call.invoked = true; call.child = call.method->handler->invoke(call.call_context, call.body, call.output); }
    catch (...) { call.immediate = status_code::internal; }
}

struct handler_ref {
    server_call *call;
    bool await_ready() const noexcept { return !call->child || call->child.await_ready(); }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> parent, net::io_env const *env) noexcept {
        call->child_env = *env;
        call->child_env.stop_token = call->call_context.stop_token;
        return call->child.await_suspend(parent, &call->child_env);
    }
    status_code await_resume() noexcept {
        if (!call->child) return call->immediate;
        try { return call->child.await_resume(); }
        catch (...) { return status_code::internal; }
    }
};

auto handler_worker(std::shared_ptr<server_state> service, server_call *call)
    CO2_BEG(net::task<>, (service, call), status_code code; std::shared_ptr<server_connection> owner;) {
    for (;;) {
        CO2_AWAIT(call->available.wait());
        if (!call->busy) { if (!service->runtime_active) break; continue; }
        owner = call->owner;
        prepare_handler(*call);
        CO2_AWAIT_SET(code, (handler_ref{call}));
        call->child = {}; // Handler/Arena must be destroyed before slot reuse.
        owner->finish_call(*call, code);
        owner.reset();
        if (!service->runtime_active) break;
    }
    CO2_RETURN();
}
CO2_END

void ensure_runtime(std::shared_ptr<server_state> const &service) {
    if (service->runtime_active) return;
    activate_deadlines(service->deadlines);
    service->runtime_active = true;
    try {
        for (std::size_t i = 0; i < service->slots.capacity(); ++i) {
            auto *call = &service->slots.at(i);
            if (call->worker_running) continue;
            call->worker_running = true;
            try {
                net::run_async(service->context.get_executor(), [service, call] { call->worker_running = false; },
                    [service, call](std::exception_ptr) { call->worker_running = false; close_service(*service); })
                    ([service, call] { return handler_worker(service, call); });
            } catch (...) { call->worker_running = false; throw; }
        }
    } catch (...) { service->closed = true; service->maybe_stop(); throw; }
}

void expire_server_call(void *value, status_code code) noexcept {
    auto &call = *static_cast<server_call *>(value);
    call.expired = code == status_code::deadline_exceeded;
    call.forced_code = code;
    call.stop.request_stop();
    if (call.queued) call.available.signal();
}

void server_connection::receive_request(wire::frame_view const &frame) {
    auto const received_at = clock::now();
    auto const &h = frame.header;
    if (h.stream_id <= last_request || h.aux >= methods.size()) { conn.close(); return; }
    last_request = h.stream_id; // Includes refusals, not just executed calls.
    auto const head = wire::decode_request_head(frame.head, (h.flags & wire::new_method) != 0).value;
    auto const deadline = head.timeout_us == 0 ? clock::time_point::max() : deadline_after(received_at, head.timeout_us);
    auto &entry = methods[h.aux];
    if ((h.flags & wire::new_method) != 0) {
        if (entry.defined || head.method_name.size > conn.options.max_method_name_bytes) { conn.close(); return; }
        entry.defined = true; // Definition survives an overload/unknown-method refusal.
        for (auto const &candidate : service->methods) {
            if (candidate.name.size() == head.method_name.size &&
                std::memcmp(candidate.name.data(), head.method_name.data, head.method_name.size) == 0) {
                entry.binding = &candidate; break;
            }
        }
    }
    auto reject = [&](status_code code) {
        auto const flags = (conn.peer.features & conn.options.receive.features & wire::explicit_rejection) != 0 ?
            wire::not_executed : std::uint8_t{0};
        conn.control(wire::frame_type::end, h.stream_id, static_cast<std::uint32_t>(code), flags);
    };
    if (conn.state != phase::ready || service->draining) { reject(status_code::unavailable); return; }
    if (!entry.defined) { reject(status_code::failed_precondition); return; }
    if (entry.binding == nullptr) { reject(status_code::unimplemented); return; }
    if (clock::now() >= deadline) { reject(status_code::deadline_exceeded); return; }
    auto const maximum = entry.binding->max_response_bytes;
    if (frame.body.size > conn.options.receive.max_message_size || maximum > conn.peer.max_message_size ||
        entry.binding->max_response_head_bytes > conn.peer.max_frame_size ||
        maximum > conn.peer.max_frame_size - entry.binding->max_response_head_bytes ||
        calls.size() >= conn.options.receive.max_concurrent_streams) {
        reject(status_code::resource_exhausted); return;
    }
    bool const queued = service->active_calls >= service->options.max_active_calls || service->queue_head != nullptr;
    if (queued && (deadline == clock::time_point::max() || service->queued_calls >= service->options.max_queued_calls ||
        h.length > service->options.max_queued_bytes - service->queued_bytes)) {
        reject(status_code::resource_exhausted); return;
    }
    auto response = service->responses.acquire(response_budget);
    auto request = service->requests.acquire(request_budget);
    if (response.get() == nullptr || request.get() == nullptr) { reject(status_code::resource_exhausted); return; }
    try {
        net::stop_source stop; // Old tokens may outlive a reused call slot.
        auto const handle = service->slots.acquire();
        auto *call = service->slots.get(handle);
        if (!call) { reject(status_code::resource_exhausted); return; }
        call->owner = self.lock(); call->handle = handle;
        call->request = std::move(request); call->response = std::move(response);
        call->method = entry.binding;
        call->output = response_writer{{call->response.get()->data + 16 + entry.binding->max_response_head_bytes, maximum}};
        call->stop = std::move(stop); call->call_context = {};
        call->call_context.stop_token = call->stop.get_token();
        call->call_context.response_metadata = metadata_writer{{call->response.get()->data + 16, entry.binding->max_response_head_bytes}};
        call->cancelled = false; call->expired = false; call->forced_code = status_code::ok; call->busy = true;
        std::memcpy(call->request.get()->data, frame.head.data, h.length);
        auto const saved = wire::decode_request_head({call->request.get()->data, h.head_length}, (h.flags & wire::new_method) != 0).value;
        call->id = h.stream_id;
        call->body = {call->request.get()->data + h.head_length, frame.body.size};
        call->call_context.metadata = saved.metadata;
        call->call_context.deadline = deadline;
        if (!schedule(*service->deadlines, call->timeout, deadline, &expire_server_call, call)) {
            call->request.reset(); call->response.reset(); call->call_context = {};
            call->stop = net::stop_source{co2::nostopstate}; call->owner.reset(); call->busy = false;
            service->slots.release(handle); reject(status_code::resource_exhausted); return;
        }
        auto const inserted = calls.insert(h.stream_id, *call);
        assert(inserted); static_cast<void>(inserted);
        call->executing = call->invoked = false; call->queued = false; call->previous = call->next = nullptr;
        if (queued) {
            call->queued = true; call->queued_bytes = h.length;
            call->previous = service->queue_tail;
            if (service->queue_tail) service->queue_tail->next = call; else service->queue_head = call;
            service->queue_tail = call; ++service->queued_calls; service->queued_bytes += h.length;
        } else { call->executing = true; ++service->active_calls; }
        if (!queued) last_admitted = h.stream_id;
        if (!queued) call->available.signal();
    } catch (std::bad_alloc const &) { reject(status_code::resource_exhausted); }
}

void server_connection::on_frame(wire::frame_view const &frame) {
    switch (frame.header.type) {
    case wire::frame_type::request: receive_request(frame); return;
    case wire::frame_type::cancel: {
        if (frame.header.stream_id > last_request) { conn.close(); return; }
        auto *call = calls.find(frame.header.stream_id);
        if (call != nullptr) cancel(*call);
        return;
    }
    case wire::frame_type::ping: conn.control(wire::frame_type::pong, 0, frame.header.aux); return;
    case wire::frame_type::pong: return;
    default: conn.close(); return;
    }
}

auto start_session(std::shared_ptr<server_connection> session)
    CO2_BEG(net::task<>, (session), net::io_result<> result;) {
    CO2_AWAIT_SET(result, net::timeout(handshake(session->conn, true), session->conn.options.handshake_timeout));
    if (result.ec || session->conn.state == phase::closed || session->service->draining) {
        session->conn.close(); CO2_RETURN();
    }
    session->conn.state = phase::ready;
    start_io(session->conn, session);
    CO2_RETURN();
}
CO2_END

bool attach_session(std::shared_ptr<server_state> const &service, std::unique_ptr<transport> stream) {
    if (!stream) return false;
    if (service->closed || service->draining || service->live_connections >= service->options.max_connections) {
        stream->close(); return false;
    }
    std::shared_ptr<server_connection> session;
    try {
        ensure_runtime(service);
        session = std::make_shared<server_connection>(service, service->next_connection++);
        session->self = session;
        session->conn.link = std::move(stream);
        session->conn.state = phase::handshaking;
        service->connections.emplace(session->id, session);
        net::run_async(service->context.get_executor(), [session] {}, [session](std::exception_ptr) {
            session->conn.close();
        })([session] { return start_session(session); });
        return true;
    } catch (...) {
        if (session) session->conn.close();
        if (stream) stream->close();
        service->maybe_stop();
        return false;
    }
}

auto accept_loop(std::shared_ptr<server_state> service)
    CO2_BEG(net::task<>, (service), net::io_result<net::tcp_socket> result;) {
    while (!service->closed && !service->draining) {
        CO2_AWAIT_SET(result, service->acceptor->accept());
        if (result.ec) break;
        if (result.value.set_option(net::socket_option::no_delay{true})) { result.value.close(); continue; }
        attach_session(service, make_tcp_transport(std::move(result.value)));
    }
    CO2_RETURN();
}
CO2_END

void close_service(server_state &state) noexcept {
    state.closed = true;
    if (state.acceptor) state.acceptor->close();
    while (!state.connections.empty()) { auto session = state.connections.begin()->second; session->conn.close(); }
    state.maybe_stop();
}

void drain_service(server_state &state) noexcept {
    state.draining = true;
    while (state.queue_head) {
        auto &call = *state.queue_head; state.remove_queued(call);
        call.forced_code = status_code::unavailable; call.available.signal();
    }
    if (state.acceptor) state.acceptor->close();
    for (auto it = state.connections.begin(); it != state.connections.end();) { auto session = (it++)->second; session->drain(); }
    state.maybe_stop();
}

auto shutdown_service(std::shared_ptr<server_state> state, std::chrono::milliseconds grace)
    CO2_BEG(net::task<>, (state, grace), std::unique_ptr<net::steady_timer> timer; clock::time_point until; net::io_result<> waited;) {
    timer = std::make_unique<net::steady_timer>(state->context);
    drain_service(*state);
    until = clock::now() + std::max(grace, std::chrono::milliseconds::zero());
    while (state->live_connections != 0 || state->slots.size() != 0) {
        if (clock::now() >= until) { close_service(*state); break; }
        timer->expires_at(std::min(until, clock::now() + std::chrono::milliseconds{1}));
        CO2_AWAIT_SET(waited, timer->wait());
        if (waited.ec) { close_service(*state); break; }
    }
    CO2_RETURN();
}
CO2_END

struct server_impl final : server {
    explicit server_impl(std::shared_ptr<server_state> state) : state_(std::move(state)) {}
    ~server_impl() override { close(); }
    net::ip::tcp::endpoint listen(net::ip::tcp::endpoint endpoint) override {
        if (state_->acceptor || state_->closed || state_->draining) throw std::logic_error{"server already started or stopped"};
        try {
        state_->acceptor = std::make_unique<net::tcp_acceptor>(state_->context, endpoint);
        std::error_code error;
        auto const bound = state_->acceptor->local_endpoint(error);
        if (error) throw std::system_error{error};
        auto state = state_;
        ensure_runtime(state);
        net::run_async(state_->context.get_executor(), [state] { if (!state->draining) close_service(*state); }, [state](std::exception_ptr) {
            close_service(*state);
        })([state] { return accept_loop(state); });
        return bound;
        } catch (...) { close_service(*state_); throw; }
    }
    bool attach(std::unique_ptr<transport> stream) override { return attach_session(state_, std::move(stream)); }
    void drain() noexcept override {
        drain_service(*state_);
    }
    resource_stats stats() const noexcept override {
        resource_stats result{};
        result.connections = state_->live_connections; result.active_calls = state_->active_calls;
        result.queued_calls = state_->queued_calls; result.queued_bytes = state_->queued_bytes;
        result.request_bytes_in_use = state_->requests.in_use();
        result.response_bytes_in_use = state_->responses.in_use();
        result.control_bytes_in_use = state_->controls.in_use();
        result.storage_bytes = state_->requests.allocated() + state_->responses.allocated() + state_->controls.allocated();
        for (auto const &method : state_->methods) if (method.handler) result.storage_bytes += method.handler->cached_storage_bytes();
        return result;
    }
    void close() noexcept override { close_service(*state_); }
    net::task<> shutdown(std::chrono::milliseconds grace) override { return shutdown_service(state_, grace); }
private:
    std::shared_ptr<server_state> state_;
};

} // namespace detail

std::unique_ptr<server> make_server(net::io_context &context, std::vector<method_binding> methods, server_options options) {
    if ((options.connection.receive.features & wire::streaming) ||
        std::any_of(methods.begin(), methods.end(), [](method_binding const &m) { return m.kind != method_kind::unary; }))
        return detail::make_stream_server(context, std::move(methods), options);
    detail::validate_options(options.connection);
    if (options.max_connections == 0 || options.max_active_calls == 0 ||
        options.max_queued_calls > std::numeric_limits<std::size_t>::max() - options.max_active_calls ||
        ((options.max_queued_calls == 0) != (options.max_queued_bytes == 0))) throw std::invalid_argument{"invalid server capacity"};
    std::size_t maximum = 18;
    std::unordered_map<std::string, bool> names;
    for (auto const &method : methods) {
        if (method.handler == nullptr || method.name.empty() || method.name.size() > options.connection.max_method_name_bytes ||
            method.max_response_head_bytes < 2 || method.max_response_head_bytes > 65535 ||
            method.max_response_bytes > std::numeric_limits<std::uint32_t>::max() - 16U - method.max_response_head_bytes ||
            (method.owned_handler && method.owned_handler.get() != method.handler) || !names.emplace(method.name, true).second)
            throw std::invalid_argument{"invalid or duplicate method binding"};
        maximum = std::max(maximum, 16 + method.max_response_head_bytes + method.max_response_bytes);
    }
    return std::make_unique<detail::server_impl>(
        std::make_shared<detail::server_state>(context, std::move(methods), options, maximum));
}

server_builder::server_builder(net::io_context &context, server_options options) : context_(context), options_(options) {}
server_builder &server_builder::add(method_binding method) {
    if (built_) throw std::logic_error{"server builder already consumed"};
    methods_.push_back(std::move(method)); return *this;
}
server_builder &server_builder::add(std::vector<method_binding> methods) {
    if (built_) throw std::logic_error{"server builder already consumed"};
    if (methods.size() > methods_.max_size() - methods_.size()) throw std::invalid_argument{"method registry overflow"};
    methods_.reserve(methods_.size() + methods.size());
    static_assert(std::is_nothrow_move_constructible<method_binding>::value, "batch registration must commit without throwing");
    for (auto &method : methods) methods_.push_back(std::move(method));
    return *this;
}
std::unique_ptr<server> server_builder::build() {
    if (built_) throw std::logic_error{"server builder already consumed"};
    auto result = make_server(context_, methods_, options_);
    methods_.clear(); built_ = true; return result;
}

} // namespace rpc
