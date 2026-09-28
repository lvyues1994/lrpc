#include "connection.hpp"

#include <net/run_async.hpp>
#include <net/timeout.hpp>

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <unordered_map>

namespace rpc {
namespace detail {

struct server_connection;

struct server_state {
    server_state(net::io_context &ctx, std::vector<method_binding> bindings, server_options config, std::size_t response_size)
        : context(ctx), methods(std::move(bindings)), options(config),
          requests(config.connection.receive.max_frame_size, config.request_bytes),
          responses(response_size, config.response_bytes), controls(64, config.control_bytes) {
        if (requests.charge() > config.request_bytes_per_connection ||
            responses.charge() > config.response_bytes_per_connection || controls.charge() > config.control_bytes_per_connection)
            throw std::invalid_argument{"per-connection budget cannot hold one pool block"};
        connections.reserve(config.max_connections);
    }
    net::io_context &context;
    std::vector<method_binding> methods;
    server_options options;
    block_pool requests;
    block_pool responses;
    block_pool controls;
    std::unique_ptr<net::tcp_acceptor> acceptor{};
    std::unordered_map<std::uint64_t, std::shared_ptr<server_connection>> connections{};
    std::size_t live_connections = 0;
    std::size_t active_calls = 0;
    std::uint64_t next_connection = 1;
    bool draining = false;
    bool closed = false;
};

class session_slot {
public:
    explicit session_slot(server_state &service) noexcept : service_(service) { ++service_.live_connections; }
    ~session_slot() { --service_.live_connections; }
    session_slot(session_slot const &) = delete;
    session_slot &operator=(session_slot const &) = delete;
private:
    server_state &service_;
};

struct server_call {
    server_call(net::io_context &context, block_lease request_storage, block_lease response_storage,
                method_binding const &binding)
        : request(std::move(request_storage)), response(std::move(response_storage)),
          output({response.get()->data + 16 + binding.max_response_head_bytes, binding.max_response_bytes}), timer(context), method(&binding) {
        call_context.stop_token = stop.get_token();
        call_context.response_metadata = metadata_writer{{response.get()->data + 16, binding.max_response_head_bytes}};
    }
    // Leases borrow the connection's budgets. Keep it alive independently of
    // task/callback member destruction order, including the final timer callback.
    std::shared_ptr<server_connection> owner{};
    block_lease request;
    block_lease response;
    response_writer output;
    net::stop_source stop{};
    net::steady_timer timer;
    method_binding const *method;
    server_context call_context{};
    wire::bytes_view body{};
    std::uint32_t id = 0;
    status_code code = status_code::internal;
    unsigned parts = 1; // Startup guard, then handler and optional deadline chain.
    bool handler_done = false;
    bool cancelled = false;
    bool expired = false;
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
          methods(static_cast<std::size_t>(this->service->options.connection.receive.max_method_ids) + 1) {
        calls.reserve(this->service->options.connection.receive.max_concurrent_streams);
    }

    void on_frame(wire::frame_view const &frame) override;
    void receive_request(wire::frame_view const &frame);
    void start_call(std::shared_ptr<server_call> call);
    void finish_part(std::shared_ptr<server_call> const &call) noexcept;
    bool prepare_request(block &) noexcept override { return true; }
    void on_idle() noexcept override {
        if (conn.state == phase::draining && calls.empty() && conn.tx.empty() && !conn.writing) conn.close();
    }
    void cancel(std::shared_ptr<server_call> const &call) noexcept {
        call->cancelled = true;
        call->timer.cancel();
        call->stop.request_stop();
    }
    void on_close() noexcept override {
        for (auto it = calls.begin(); it != calls.end();) { auto call = (it++)->second; cancel(call); }
        service->connections.erase(id);
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
    std::unordered_map<std::uint32_t, std::shared_ptr<server_call>> calls{};
    std::uint32_t last_request = 0;
    std::uint32_t last_admitted = 0;
};

auto invoke_handler(std::shared_ptr<server_connection> owner, std::shared_ptr<server_call> call)
    CO2_BEG(net::task<status_code>, (owner, call), status_code result;) {
    if (call->cancelled || owner->conn.state == phase::closed) CO2_RETURN(status_code::cancelled);
    if (clock::now() >= call->call_context.deadline) CO2_RETURN(status_code::deadline_exceeded);
    CO2_AWAIT_SET(result, call->method->handler->invoke(call->call_context, call->body, call->output));
    CO2_RETURN(result);
}
CO2_END

auto wait_deadline(std::shared_ptr<server_call> call)
    CO2_BEG((net::task<net::io_result<>>), (call), net::io_result<> result;) {
    if (call->handler_done || call->cancelled)
        CO2_RETURN((net::io_result<>{net::make_error_code(net::error::operation_aborted)}));
    CO2_AWAIT_SET(result, call->timer.wait());
    CO2_RETURN(result);
}
CO2_END

void server_connection::finish_part(std::shared_ptr<server_call> const &call) noexcept {
    if (--call->parts != 0) return;
    auto code = call->code;
    auto const invalid = call->output.overflowed() || call->call_context.response_metadata.overflowed() ||
        call->output.size() > call->method->max_response_bytes ||
        call->call_context.response_metadata.size() > call->method->max_response_head_bytes || static_cast<std::uint32_t>(code) > 16;
    if (invalid) code = status_code::internal;
    if (call->expired || clock::now() >= call->call_context.deadline) code = status_code::deadline_exceeded;
    if (!call->cancelled && conn.state != phase::closed) {
        if (!encode_end(*call->response.get(), call->id, code, call->output.size(), conn.peer,
                        invalid ? 2 : call->call_context.response_metadata.size(), 16 + call->method->max_response_head_bytes)) conn.close();
        else conn.enqueue(std::move(call->response));
    }
    calls.erase(call->id);
    --service->active_calls;
    on_idle();
}

void server_connection::start_call(std::shared_ptr<server_call> call) {
    auto owner = self.lock();
    auto complete_handler = [owner, call](status_code code) {
        call->code = code;
        call->handler_done = true;
        call->timer.cancel();
        owner->finish_part(call);
    };
    auto failed_handler = [complete_handler](std::exception_ptr) { complete_handler(status_code::internal); };
    try {
        if (call->call_context.deadline != clock::time_point::max()) {
            ++call->parts;
            try {
                net::run_async(conn.context.get_executor(), [owner, call](net::io_result<> result) {
                    if (!result.ec && !call->handler_done && !call->cancelled) {
                        call->expired = true;
                        call->stop.request_stop();
                    }
                    owner->finish_part(call);
                }, [owner, call](std::exception_ptr) {
                    call->expired = true; call->stop.request_stop(); owner->finish_part(call);
                })([call] { return wait_deadline(call); });
            } catch (...) { --call->parts; throw; }
        }
        ++call->parts;
        try {
            net::run_async(conn.context.get_executor(), call->stop.get_token(), nullptr,
                           complete_handler, failed_handler)([owner, call] { return invoke_handler(owner, call); });
        } catch (...) { --call->parts; throw; }
    } catch (...) {
        call->handler_done = true;
        call->code = status_code::internal;
        call->timer.cancel();
        call->stop.request_stop();
    }
    finish_part(call); // Release startup guard after both chains are published.
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
    auto reject = [&](status_code code) { conn.control(wire::frame_type::end, h.stream_id, static_cast<std::uint32_t>(code)); };
    if (conn.state != phase::ready || service->draining) { reject(status_code::unavailable); return; }
    if (!entry.defined) { reject(status_code::failed_precondition); return; }
    if (entry.binding == nullptr) { reject(status_code::unimplemented); return; }
    if (clock::now() >= deadline) { reject(status_code::deadline_exceeded); return; }
    auto const maximum = entry.binding->max_response_bytes;
    if (frame.body.size > conn.options.receive.max_message_size || maximum > conn.peer.max_message_size ||
        entry.binding->max_response_head_bytes > conn.peer.max_frame_size ||
        maximum > conn.peer.max_frame_size - entry.binding->max_response_head_bytes ||
        calls.size() >= conn.options.receive.max_concurrent_streams || service->active_calls >= service->options.max_active_calls) {
        reject(status_code::resource_exhausted); return;
    }
    auto response = service->responses.acquire(response_budget);
    auto request = service->requests.acquire(request_budget);
    if (response.get() == nullptr || request.get() == nullptr) { reject(status_code::resource_exhausted); return; }
    try {
        auto call = std::make_shared<server_call>(conn.context, std::move(request), std::move(response), *entry.binding);
        call->owner = self.lock();
        std::memcpy(call->request.get()->data, frame.head.data, h.length);
        auto const saved = wire::decode_request_head({call->request.get()->data, h.head_length}, (h.flags & wire::new_method) != 0).value;
        call->id = h.stream_id;
        call->body = {call->request.get()->data + h.head_length, frame.body.size};
        call->call_context.metadata = saved.metadata;
        call->call_context.deadline = deadline;
        call->timer.expires_at(call->call_context.deadline);
        calls.emplace(h.stream_id, call);
        ++service->active_calls;
        last_admitted = h.stream_id;
        start_call(std::move(call));
    } catch (std::bad_alloc const &) { reject(status_code::resource_exhausted); }
}

void server_connection::on_frame(wire::frame_view const &frame) {
    switch (frame.header.type) {
    case wire::frame_type::request: receive_request(frame); return;
    case wire::frame_type::cancel: {
        if (frame.header.stream_id > last_request) { conn.close(); return; }
        auto found = calls.find(frame.header.stream_id);
        if (found != calls.end()) cancel(found->second);
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
}

struct server_impl final : server {
    explicit server_impl(std::shared_ptr<server_state> state) : state_(std::move(state)) {}
    ~server_impl() override { close(); }
    net::ip::tcp::endpoint listen(net::ip::tcp::endpoint endpoint) override {
        if (state_->acceptor || state_->closed || state_->draining) throw std::logic_error{"server already started or stopped"};
        state_->acceptor = std::make_unique<net::tcp_acceptor>(state_->context, endpoint);
        std::error_code error;
        auto const bound = state_->acceptor->local_endpoint(error);
        if (error) throw std::system_error{error};
        auto state = state_;
        net::run_async(state_->context.get_executor(), [state] {}, [state](std::exception_ptr) {
            close_service(*state);
        })([state] { return accept_loop(state); });
        return bound;
    }
    bool attach(std::unique_ptr<transport> stream) override { return attach_session(state_, std::move(stream)); }
    void drain() noexcept override {
        state_->draining = true;
        if (state_->acceptor) state_->acceptor->close();
        for (auto it = state_->connections.begin(); it != state_->connections.end();) { auto session = (it++)->second; session->drain(); }
    }
    resource_stats stats() const noexcept override {
        resource_stats result{};
        result.connections = state_->live_connections; result.active_calls = state_->active_calls;
        result.request_bytes_in_use = state_->requests.in_use();
        result.response_bytes_in_use = state_->responses.in_use();
        result.control_bytes_in_use = state_->controls.in_use();
        result.storage_bytes = state_->requests.allocated() + state_->responses.allocated() + state_->controls.allocated();
        return result;
    }
    void close() noexcept override { close_service(*state_); }
private:
    std::shared_ptr<server_state> state_;
};

} // namespace detail

std::unique_ptr<server> make_server(net::io_context &context, std::vector<method_binding> methods, server_options options) {
    detail::validate_options(options.connection);
    if (options.max_connections == 0 || options.max_active_calls == 0) throw std::invalid_argument{"empty server capacity"};
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

} // namespace rpc
