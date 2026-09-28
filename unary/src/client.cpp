#include "connection.hpp"

#include <net/this_coro.hpp>
#include <net/timeout.hpp>

#include <algorithm>
#include <cstring>
#include <unordered_map>

namespace rpc {
namespace detail {

struct client_method {
    std::string const *name = nullptr;
    bool published = false;
};

struct client_state final : connection_observer {
    client_state(net::io_context &context, client_options config)
        : options(config), requests(static_cast<std::size_t>(config.connection.receive.max_frame_size) + 16, config.request_bytes),
          controls(64, config.control_bytes), request_budget{config.request_bytes, 0}, control_budget{config.control_bytes, 0},
          conn(context, config.connection, controls, control_budget, *this) {
        methods.reserve(config.connection.receive.max_method_ids);
        method_ids.reserve(config.connection.receive.max_method_ids);
        calls.reserve(config.connection.receive.max_concurrent_streams);
    }

    void finish(std::uint32_t id, status_code code, wire::bytes_view response = {}, wire::metadata_view metadata = {}) noexcept {
        auto found = calls.find(id);
        if (found == calls.end()) return;
        auto call = found->second;
        // Claim before invoking a user codec: it may synchronously close the
        // client. No pending-table iterator or second completion crosses it.
        call->done = true;
        calls.erase(found);
        auto const stopped = call->token.stop_requested() || clock::now() >= call->deadline;
        if (!stopped && call->response_metadata.data != nullptr) {
            if (metadata.entries.size > call->response_metadata.size) code = status_code::resource_exhausted;
            else {
                if (metadata.entries.size != 0)
                    std::memcpy(call->response_metadata.data, metadata.entries.data, metadata.entries.size);
                call->result.response_metadata = {{call->response_metadata.data, metadata.entries.size}, metadata.count};
            }
        }
        if (!stopped && code == status_code::ok) {
            if (call->decoded.operations != nullptr) {
                try {
                    if (!call->decoded.operations->decode(response, call->decoded.message)) code = status_code::data_loss;
                } catch (std::bad_alloc const &) { code = status_code::resource_exhausted; }
                catch (...) { code = status_code::data_loss; }
            } else if (response.size > call->response.size) code = status_code::resource_exhausted;
            else if (response.size != 0) std::memcpy(call->response.data, response.data, response.size);
        }
        if (call->token.stop_requested()) code = status_code::cancelled;
        else if (clock::now() >= call->deadline) code = status_code::deadline_exceeded;
        call->result.code = code;
        if (code == status_code::ok) call->result.response_size = response.size;
        call->wake.cancel();
    }

    void on_frame(wire::frame_view const &frame) override {
        auto const &h = frame.header;
        switch (h.type) {
        case wire::frame_type::end:
        case wire::frame_type::cancel:
            if (h.stream_id > last_submitted) { conn.close(); return; }
            finish(h.stream_id, h.type == wire::frame_type::cancel ? status_code::cancelled :
                (frame.body.size > options.connection.receive.max_message_size ? status_code::resource_exhausted :
                 static_cast<status_code>(h.aux)), frame.body,
                 h.type == wire::frame_type::end ? wire::decode_end_head(frame.head).value.metadata : wire::metadata_view{});
            on_idle();
            return;
        case wire::frame_type::ping: conn.control(wire::frame_type::pong, 0, h.aux); return;
        case wire::frame_type::pong: return;
        case wire::frame_type::goaway:
            if (received_goaway || (conn.state != phase::ready && conn.state != phase::draining) || h.aux > last_submitted) {
                conn.close(); return;
            }
            received_goaway = true;
            conn.state = phase::draining;
            for (auto it = calls.begin(); it != calls.end();) {
                auto const id = (it++)->first;
                if (id > h.aux) finish(id, status_code::unavailable);
            }
            on_idle();
            return;
        default: conn.close(); return;
        }
    }
    void on_close() noexcept override {
        if (connecting) connecting->close();
        while (!calls.empty()) finish(calls.begin()->first, status_code::unavailable);
    }
    void on_idle() noexcept override {
        if (conn.state == phase::draining && calls.empty() && conn.tx.empty() && !conn.writing) conn.close();
    }
    bool prepare_request(block &frame) noexcept override {
        auto call = frame.request.lock();
        if (!call || call->done) return false;
        auto const timeout = remaining_timeout(call->deadline);
        if (call->token.stop_requested() || (call->deadline != clock::time_point::max() && timeout == 0)) {
            finish(call->id, call->token.stop_requested() ? status_code::cancelled : status_code::deadline_exceeded);
            return false;
        }
        auto &method = methods[call->method_index];
        auto const fresh = !method.published;
        if (!fresh && call->method_bytes != 0) {
            std::memmove(frame.data + 24, frame.data + 24 + call->method_bytes, frame.size - 24 - call->method_bytes);
            frame.body_offset -= call->method_bytes;
        }
        // Timeout is fixed-width little endian. Preserve the already encoded
        // metadata; re-encoding views inside the same frame would alias output.
        for (unsigned i = 0; i < 8; ++i) frame.data[16 + i] = static_cast<std::uint8_t>(timeout >> (8U * i));
        auto const head_size = frame.body_offset - 16;
        wire::frame_header h{static_cast<std::uint32_t>(head_size + frame.body_size), call->id,
            wire::frame_type::request, static_cast<std::uint8_t>(wire::end_stream | (fresh ? wire::new_method : 0)),
            static_cast<std::uint16_t>(head_size), static_cast<std::uint32_t>(call->method_index + 1)};
        if (wire::encode_header(h, {frame.data, 16}, {conn.peer.max_frame_size, conn.peer.max_message_size}).code != wire::error::none) {
            finish(call->id, status_code::internal); return false;
        }
        frame.size = 16 + h.length;
        method.published = true;
        call->submitted = true;
        last_submitted = call->id;
        return true;
    }

    client_options options;
    block_pool requests;
    block_pool controls;
    byte_budget request_budget;
    byte_budget control_budget;
    connection conn;
    std::unique_ptr<net::tcp_socket> connecting{};
    std::unordered_map<std::string, std::size_t> method_ids{};
    std::vector<client_method> methods{};
    std::unordered_map<std::uint32_t, std::shared_ptr<client_call>> calls{};
    std::uint64_t next_id = 1;
    std::uint32_t last_submitted = 0;
    bool received_goaway = false;
};

struct admitted_call {
    status_code code = status_code::resource_exhausted;
    std::shared_ptr<client_call> value{};
};

struct call_buffers {
    wire::bytes_view request{};
    wire::mutable_bytes_view response{};
    encoded_request encoded{};
    decoded_response decoded{};
    bool typed = false;
};

class connect_cleanup {
public:
    connect_cleanup() noexcept = default;
    ~connect_cleanup() { if (owner_) owner_->conn.close(); }
    connect_cleanup(connect_cleanup &&) noexcept = default;
    connect_cleanup &operator=(connect_cleanup &&) = delete;
    connect_cleanup(connect_cleanup const &) = delete;
    connect_cleanup &operator=(connect_cleanup const &) = delete;
    void arm(std::shared_ptr<client_state> owner) noexcept { owner_ = std::move(owner); }
    void release() noexcept { owner_.reset(); }
private:
    std::shared_ptr<client_state> owner_{};
};

admitted_call admit(client_state &state, std::string const &name, call_buffers const &buffers,
                    clock::time_point deadline, net::stop_token token, call_options const &options) {
    auto &conn = state.conn;
    auto request = buffers.request;
    auto const response = buffers.response;
    auto const encoded_request_value = buffers.encoded;
    auto const decoded = buffers.decoded;
    if (conn.state != phase::ready) return {status_code::unavailable, {}};
    if (name.empty() || name.size() > conn.options.max_method_name_bytes ||
        (request.size != 0 && request.data == nullptr) || (response.size != 0 && response.data == nullptr) ||
        (options.response_metadata.size != 0 && options.response_metadata.data == nullptr))
        return {status_code::invalid_argument, {}};
    if (response.size != 0 && options.response_metadata.size != 0) {
        auto const a = reinterpret_cast<std::uintptr_t>(response.data);
        auto const b = reinterpret_cast<std::uintptr_t>(options.response_metadata.data);
        if (a <= b ? b - a < response.size : a - b < options.response_metadata.size)
            return {status_code::invalid_argument, {}};
    }
    if (buffers.typed) {
        if (encoded_request_value.message == nullptr || encoded_request_value.operations == nullptr ||
            decoded.message == nullptr || decoded.operations == nullptr ||
            encoded_request_value.operations->size == nullptr || encoded_request_value.operations->encode == nullptr ||
            decoded.operations->decode == nullptr) return {status_code::invalid_argument, {}};
        request.size = encoded_request_value.operations->size(encoded_request_value.message);
    }
    if (conn.state != phase::ready) return {status_code::unavailable, {}};
    if (token.stop_requested()) return {status_code::cancelled, {}};
    if (clock::now() >= deadline) return {status_code::deadline_exceeded, {}};
    if (state.next_id > wire::max_stream_id) { conn.state = phase::draining; state.on_idle(); return {status_code::unavailable, {}}; }
    if (state.calls.size() >= std::min(conn.options.receive.max_concurrent_streams, conn.peer.max_concurrent_streams)) return {};
    if (request.size > conn.peer.max_message_size || request.size > state.requests.block_size() - 16) return {};
    auto storage = state.requests.acquire(state.request_budget);
    if (storage.get() == nullptr) return {};
    auto &frame = *storage.get();
    auto const existing = state.method_ids.find(name);
    auto const fresh = existing == state.method_ids.end() || !state.methods[existing->second].published;
    wire::request_head head{};
    head.metadata = options.metadata;
    if (fresh) head.method_name = {reinterpret_cast<std::uint8_t const *>(name.data()), name.size()};
    auto const encoded = wire::encode_request_head(head, fresh, {frame.data + 16, state.requests.block_size() - 16});
    if (encoded.code == wire::error::invalid_argument) return {status_code::invalid_argument, {}};
    if (encoded.code != wire::error::none || encoded.written > conn.peer.max_frame_size ||
        request.size > conn.peer.max_frame_size - encoded.written ||
        request.size > state.requests.block_size() - 16 - encoded.written) return {};
    frame.body_offset = 16 + encoded.written;
    frame.body_size = request.size;
    frame.size = frame.body_offset + frame.body_size;
    if (encoded_request_value.operations != nullptr) {
        if (!encoded_request_value.operations->encode(encoded_request_value.message, {frame.data + frame.body_offset, request.size}))
            return {status_code::invalid_argument, {}};
    } else if (request.size != 0) std::memcpy(frame.data + frame.body_offset, request.data, request.size);
    if (conn.state != phase::ready) return {status_code::unavailable, {}};
    if (token.stop_requested()) return {status_code::cancelled, {}};
    if (clock::now() >= deadline) return {status_code::deadline_exceeded, {}};
    if (state.calls.size() >= std::min(conn.options.receive.max_concurrent_streams, conn.peer.max_concurrent_streams)) return {};
    if (state.next_id > wire::max_stream_id) return {status_code::unavailable, {}};
    bool inserted_method = false;
    auto const previous_methods = state.methods.size();
    try {
        auto found = state.method_ids.find(name);
        if (found == state.method_ids.end()) {
            if (state.methods.size() >= std::min(conn.peer.max_method_ids, conn.options.receive.max_method_ids)) return {};
            auto inserted = state.method_ids.emplace(name, state.methods.size());
            inserted_method = inserted.second;
            found = inserted.first;
            state.methods.push_back({&found->first, false});
        }
        auto call = std::make_shared<client_call>(conn.context);
        call->id = static_cast<std::uint32_t>(state.next_id);
        call->method_index = found->second;
        call->deadline = deadline;
        call->token = std::move(token);
        call->response = response;
        call->decoded = decoded;
        call->response_metadata = options.response_metadata;
        if (fresh) call->method_bytes = name.size() + wire::decode_varint({frame.data + 24, encoded.written - 8}).consumed;
        call->wake.expires_at(deadline);
        state.calls.emplace(call->id, call);
        ++state.next_id;
        frame.is_request = true;
        frame.request = call;
        conn.enqueue(std::move(storage));
        return {status_code::ok, std::move(call)};
    } catch (std::bad_alloc const &) {
        // A failed admission has published neither a frame nor a method ID.
        // Return its new slot so unrelated methods can still be admitted.
        if (inserted_method) {
            state.methods.resize(previous_methods);
            state.method_ids.erase(name);
        }
        return {};
    }
}

auto call_task(std::shared_ptr<client_state> state, std::string method, call_buffers buffers, call_options options)
    CO2_BEG(net::task<call_result>, (state, method, buffers, options),
            net::io_env const *env = nullptr; clock::time_point deadline; admitted_call admitted;
            net::io_result<> woke;) {
    CO2_AWAIT_SET(env, net::this_coro::environment);
    if (&env->executor.context() != &state->conn.context)
        CO2_RETURN((call_result{status_code::failed_precondition, 0}));
    deadline = resolve_deadline(options);
    if (env->stop_token.stop_requested()) CO2_RETURN((call_result{status_code::cancelled, 0}));
    if (clock::now() >= deadline) CO2_RETURN((call_result{status_code::deadline_exceeded, 0}));
    try { admitted = admit(*state, method, buffers, deadline, env->stop_token, options); }
    catch (std::bad_alloc const &) { CO2_RETURN((call_result{status_code::resource_exhausted, 0})); }
    catch (...) { CO2_RETURN((call_result{status_code::invalid_argument, 0})); }
    if (admitted.code != status_code::ok) CO2_RETURN((call_result{admitted.code, 0}));
    CO2_AWAIT_SET(woke, admitted.value->wake.wait());
    if (!admitted.value->done) {
        state->finish(admitted.value->id, env->stop_token.stop_requested() ? status_code::cancelled :
                      (clock::now() >= deadline ? status_code::deadline_exceeded : status_code::unavailable));
        if (admitted.value->submitted)
            state->conn.control(wire::frame_type::cancel, admitted.value->id,
                                static_cast<std::uint32_t>(admitted.value->result.code));
    }
    state->on_idle();
    CO2_RETURN(admitted.value->result);
}
CO2_END

auto finish_connect(std::shared_ptr<client_state> state, clock::time_point deadline)
    CO2_BEG(net::task<status_code>, (state, deadline), net::io_result<> result;) {
    CO2_AWAIT_SET(result, net::timeout(handshake(state->conn, false), deadline));
    if (result.ec || state->conn.state == phase::closed) {
        state->conn.close();
        CO2_RETURN(result.ec ? io_status(result.ec) : status_code::unavailable);
    }
    state->conn.state = phase::ready;
    try { start_io(state->conn, state); }
    catch (...) { state->conn.close(); CO2_RETURN(status_code::resource_exhausted); }
    CO2_RETURN(status_code::ok);
}
CO2_END

auto connect_task(std::shared_ptr<client_state> state, net::ip::tcp::endpoint endpoint)
    CO2_BEG(net::task<status_code>, (state, endpoint),
            net::io_env const *env = nullptr; clock::time_point deadline;
            net::io_result<> result; status_code status; connect_cleanup cleanup;) {
    CO2_AWAIT_SET(env, net::this_coro::environment);
    if (&env->executor.context() != &state->conn.context || state->conn.state != phase::idle)
        CO2_RETURN(status_code::failed_precondition);
    cleanup.arm(state);
    state->conn.state = phase::handshaking;
    deadline = resolve_deadline({clock::time_point::max(), state->options.connection.handshake_timeout});
    state->connecting = std::make_unique<net::tcp_socket>(state->conn.context);
    CO2_AWAIT_SET(result, net::timeout(state->connecting->connect(endpoint), deadline));
    if (result.ec || state->conn.state == phase::closed) {
        state->conn.close();
        CO2_RETURN(result.ec ? io_status(result.ec) : status_code::unavailable);
    }
    if (state->connecting->set_option(net::socket_option::no_delay{true})) {
        state->conn.close(); CO2_RETURN(status_code::unavailable);
    }
    state->conn.link = make_tcp_transport(std::move(*state->connecting));
    state->connecting.reset();
    CO2_AWAIT_SET(status, finish_connect(state, deadline));
    if (status == status_code::ok) cleanup.release();
    CO2_RETURN(status);
}
CO2_END

auto attach_task(std::shared_ptr<client_state> state, std::unique_ptr<transport> stream)
    CO2_BEG(net::task<status_code>, (state, stream), net::io_env const *env = nullptr; status_code result; connect_cleanup cleanup;) {
    CO2_AWAIT_SET(env, net::this_coro::environment);
    if (&env->executor.context() != &state->conn.context || state->conn.state != phase::idle || !stream)
        CO2_RETURN(status_code::failed_precondition);
    cleanup.arm(state);
    state->conn.state = phase::handshaking;
    state->conn.link = std::move(stream);
    CO2_AWAIT_SET(result, finish_connect(state, resolve_deadline({clock::time_point::max(), state->options.connection.handshake_timeout})));
    if (result == status_code::ok) cleanup.release();
    CO2_RETURN(result);
}
CO2_END

struct client_impl final : client {
    explicit client_impl(std::shared_ptr<client_state> state) : state_(std::move(state)) {}
    ~client_impl() override { close(); }
    net::task<status_code> connect(net::ip::tcp::endpoint endpoint) override { return connect_task(state_, endpoint); }
    net::task<status_code> attach(std::unique_ptr<transport> stream) override { return attach_task(state_, std::move(stream)); }
    net::task<call_result> call(std::string method, wire::bytes_view request, wire::mutable_bytes_view response,
                               call_options options) override {
        return call_task(state_, std::move(method), {request, response}, options);
    }
    net::task<call_result> call_encoded(std::string method, encoded_request request, decoded_response response,
                                       call_options options) override {
        return call_task(state_, std::move(method), {{}, {}, request, response, true}, options);
    }
    bool ready() const noexcept override { return state_->conn.state == phase::ready; }
    resource_stats stats() const noexcept override {
        resource_stats result{};
        result.connections = state_->conn.state == phase::ready || state_->conn.state == phase::draining ? 1 : 0;
        result.active_calls = state_->calls.size();
        result.request_bytes_in_use = state_->requests.in_use();
        result.control_bytes_in_use = state_->controls.in_use();
        result.storage_bytes = state_->requests.allocated() + state_->controls.allocated();
        return result;
    }
    void close() noexcept override { state_->conn.close(); }
private:
    std::shared_ptr<client_state> state_;
};

} // namespace detail

std::unique_ptr<client> make_client(net::io_context &context, client_options options) {
    detail::validate_options(options.connection);
    return std::make_unique<detail::client_impl>(std::make_shared<detail::client_state>(context, options));
}

} // namespace rpc
