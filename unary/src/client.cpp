#include "connection.hpp"
#include "stream_runtime.hpp"
#include <rpc/typed.hpp>

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
struct registered_client_method {
    std::string const *name = nullptr;
    codec_ops const *request = nullptr;
    codec_ops const *response = nullptr;
    std::size_t wire_index = std::numeric_limits<std::size_t>::max();
    method_kind kind = method_kind::unary;
};
std::uint64_t next_client_identity() {
    static std::atomic<std::uint64_t> next{1};
    auto const identity = next.fetch_add(1);
    if (identity == 0) std::terminate();
    return identity;
}

struct client_state final : connection_observer {
    client_state(net::io_context &context, client_options config)
        : options(config), requests(static_cast<std::size_t>(config.connection.receive.max_frame_size) + 16, config.request_bytes),
          controls(64, config.control_bytes), request_budget{config.request_bytes, 0}, control_budget{config.control_bytes, 0},
          conn(context, config.connection, controls, control_budget, *this),
          slots(config.connection.receive.max_concurrent_streams), calls(config.connection.receive.max_concurrent_streams),
          deadlines(shard_deadlines(context)), reserved_deadlines(*deadlines, slots.capacity()) {
        methods.reserve(config.connection.receive.max_method_ids);
        method_ids.reserve(config.connection.receive.max_method_ids);
        registered.reserve(config.max_registered_methods);
        registry_ids.reserve(config.max_registered_methods);
    }
    void activate() { activate_deadlines(deadlines); active = true; }
    method_handle bind(method_descriptor const &descriptor) {
        if (descriptor.name == nullptr || descriptor.name[0] == '\0' || static_cast<unsigned>(descriptor.kind) > 3 ||
            descriptor.request_codec == nullptr || descriptor.response_codec == nullptr ||
            descriptor.request_codec->size == nullptr || descriptor.request_codec->encode == nullptr ||
            descriptor.request_codec->decode == nullptr || descriptor.response_codec->size == nullptr ||
            descriptor.response_codec->encode == nullptr || descriptor.response_codec->decode == nullptr)
            throw std::invalid_argument{"invalid method descriptor"};
        std::string const name{descriptor.name};
        if (name.size() > conn.options.max_method_name_bytes) throw std::invalid_argument{"method name exceeds limit"};
        auto found = registry_ids.find(name);
        if (found != registry_ids.end()) {
            auto const &method = registered[found->second];
            if (method.request != descriptor.request_codec || method.response != descriptor.response_codec || method.kind != descriptor.kind)
                throw std::invalid_argument{"conflicting method codecs"};
            return {identity, found->second};
        }
        if (registered.size() == options.max_registered_methods) throw std::invalid_argument{"method registry full"};
        auto const inserted = registry_ids.emplace(name, registered.size());
        registered.push_back({&inserted.first->first, descriptor.request_codec, descriptor.response_codec,
                              std::numeric_limits<std::size_t>::max(), descriptor.kind});
        return {identity, registered.size() - 1};
    }
    std::vector<method_handle> bind(service_descriptor const &descriptor) {
        if (descriptor.size != 0 && descriptor.methods == nullptr) throw std::invalid_argument{"null service methods"};
        std::vector<method_handle> result;
        result.reserve(descriptor.size);
        auto const previous = registered.size();
        try { for (std::size_t i = 0; i < descriptor.size; ++i) result.push_back(bind(descriptor.methods[i])); }
        catch (...) {
            while (registered.size() > previous) {
                registry_ids.erase(*registered.back().name); registered.pop_back();
            }
            throw;
        }
        return result;
    }
    registered_client_method *lookup(method_handle handle) noexcept {
        return handle.owner_ == identity && handle.index_ < registered.size() ? &registered[handle.index_] : nullptr;
    }

    void finish(std::uint32_t id, status_code code, wire::bytes_view response = {}, wire::metadata_view metadata = {}) noexcept {
        auto *call = calls.find(id);
        if (call == nullptr) return;
        complete(*call, code, response, metadata);
    }
    void complete(client_call &value, status_code code, wire::bytes_view response = {}, wire::metadata_view metadata = {},
                  wire::bytes_view message = {}, bool rejected = false) noexcept {
        auto *call = &value;
        auto expected = completion_phase::pending;
        if (!call->phase.compare_exchange_strong(expected, completion_phase::publishing)) return;
        // Claim before invoking a user codec: it may synchronously close the
        // client. No pending-table iterator or second completion crosses it.
        calls.erase(call->id);
        unschedule(*deadlines, call->timeout);
        auto const stopped = call->token.stop_requested() || clock::now() >= call->deadline;
        auto const remote_code = code;
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
        if (!stopped && code == remote_code && code != status_code::ok && message.size != 0) {
            try { call->result.message.assign(reinterpret_cast<char const *>(message.data), message.size); }
            catch (...) { code = status_code::resource_exhausted; call->result.message.clear(); }
        }
        call->result.code = code;
        call->result.not_executed = rejected || !call->submitted;
        if (code == status_code::ok) call->result.response_size = response.size;
        call->phase.store(completion_phase::published);
        call->executor.post(call->continuation);
    }
    void consume(client_call &call) noexcept {
        // The notification continuation was removed from the executor queue
        // before resuming this consumer. Unregister/join the foreign callback
        // before making its storage available to another generation.
        call.cancellation.reset();
        if (call.phase.load() == completion_phase::cancelled) {
            calls.erase(call.id);
            unschedule(*deadlines, call.timeout);
            call.result.code = status_code::cancelled;
            call.result.not_executed = !call.submitted;
            if (call.submitted) conn.control(wire::frame_type::cancel, call.id, static_cast<std::uint32_t>(status_code::cancelled));
        }
        call.token = {};
        slots.release(call.handle);
        on_idle();
    }
    static void expire(void *value, status_code code) noexcept {
        auto &call = *static_cast<client_call *>(value);
        auto &state = *call.owner;
        state.complete(call, code);
        if (call.submitted && call.phase.load() == completion_phase::published)
            state.conn.control(wire::frame_type::cancel, call.id, static_cast<std::uint32_t>(call.result.code));
        state.on_idle();
    }

    void on_frame(wire::frame_view const &frame) override {
        auto const &h = frame.header;
        switch (h.type) {
        case wire::frame_type::end:
        case wire::frame_type::cancel: {
            if (h.stream_id > last_submitted) { conn.close(); return; }
            auto const head = h.type == wire::frame_type::end ? wire::decode_end_head(frame.head).value : wire::end_head_view{};
            auto *call = calls.find(h.stream_id);
            if (call) complete(*call, h.type == wire::frame_type::cancel ? status_code::cancelled :
                (frame.body.size > options.connection.receive.max_message_size ? status_code::resource_exhausted :
                 static_cast<status_code>(h.aux)), frame.body, head.metadata, head.message,
                 (h.flags & wire::not_executed) != 0);
            on_idle();
            return;
        }
        case wire::frame_type::ping: conn.control(wire::frame_type::pong, 0, h.aux); return;
        case wire::frame_type::pong: return;
        case wire::frame_type::goaway:
            if (received_goaway || (conn.state != phase::ready && conn.state != phase::draining) || h.aux > last_submitted) {
                conn.close(); return;
            }
            received_goaway = true;
            conn.state = phase::draining;
            if (options.events) options.events->on_connectivity_change();
            for (std::size_t i = 0; i < slots.capacity(); ++i) {
                auto &call = slots.at(i);
                if (call.id > h.aux && calls.find(call.id) == &call)
                    complete(call, status_code::unavailable, {}, {}, {}, true);
            }
            on_idle();
            return;
        default: conn.close(); return;
        }
    }
    void on_close() noexcept override {
        if (connecting) connecting->close();
        while (!calls.empty()) {
            auto *call = calls.first(); complete(*call, status_code::unavailable);
            // A foreign cancellation may already own the only notification.
            calls.erase(call->id); unschedule(*deadlines, call->timeout);
        }
        if (active) { active = false; deactivate_deadlines(*deadlines); }
        if (options.events) options.events->on_connectivity_change();
    }
    void on_idle() noexcept override {
        if (conn.state == phase::draining && calls.empty() && conn.tx.empty() && !conn.writing) conn.close();
        if (options.events) options.events->on_capacity_change();
    }
    bool prepare_request(block &frame) noexcept override {
        auto *call = slots.get(frame.request);
        if (!call || call->phase.load() != completion_phase::pending) return false;
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
    std::unordered_map<std::string, std::size_t> registry_ids{};
    std::vector<registered_client_method> registered{};
    std::uint64_t identity = next_client_identity();
    slot_pool<client_call> slots;
    stream_index<client_call> calls;
    std::shared_ptr<deadline_scheduler> deadlines;
    deadline_reservation reserved_deadlines;
    std::uint64_t next_id = 1;
    std::uint32_t last_submitted = 0;
    bool received_goaway = false;
    bool active = false;
};

void cancel_notice::operator()() const noexcept {
    auto expected = completion_phase::pending;
    if (call->phase.compare_exchange_strong(expected, completion_phase::cancelled))
        call->executor.post(call->continuation);
}

struct completion_wait {
    client_call *call;
    bool await_ready() const noexcept { return false; }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
        call->continuation.h = handle;
        call->executor = env->executor;
        call->cancellation.emplace(env->stop_token, cancel_notice{call});
        return net::noop_coroutine();
    }
    call_result await_resume() noexcept {
        auto &state = *call->owner;
        // consume chooses the cancellation result before it releases the slot.
        if (call->phase.load() == completion_phase::cancelled) call->result.code = status_code::cancelled;
        auto result = std::move(call->result);
        state.consume(*call);
        return result;
    }
};

struct admitted_call {
    status_code code = status_code::resource_exhausted;
    client_call *value = nullptr;
    bool temporary = false;
};

struct call_buffers {
    wire::bytes_view request{};
    wire::mutable_bytes_view response{};
    encoded_request encoded{};
    decoded_response decoded{};
    bool typed = false;
};

// Channel governance is consumed above this transport. Keep its retry/queue
// policy out of every direct client's coroutine frame and hot copy path.
struct direct_call_options {
    explicit direct_call_options(call_options const &options) noexcept
        : deadline(options.deadline), timeout(options.timeout), metadata(options.metadata), response_metadata(options.response_metadata) {}
    clock::time_point deadline;
    std::chrono::microseconds timeout;
    wire::metadata_list metadata;
    wire::mutable_bytes_view response_metadata;
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

admitted_call admit(client_state &state, std::string const &method_name, call_buffers const &buffers,
                    clock::time_point deadline, net::stop_token token, direct_call_options const &options, method_handle handle) {
    auto &conn = state.conn;
    auto *binding = handle ? state.lookup(handle) : nullptr;
    if (handle && binding == nullptr) return {status_code::invalid_argument, {}};
    auto const &name = binding ? *binding->name : method_name;
    auto request = buffers.request;
    auto const response = buffers.response;
    auto const encoded_request_value = buffers.encoded;
    auto const decoded = buffers.decoded;
    if (binding && (binding->kind != method_kind::unary || !buffers.typed || encoded_request_value.operations != binding->request || decoded.operations != binding->response))
        return {status_code::invalid_argument, {}};
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
        if (!encoded_request_value.operations->encode_bounded)
            request.size = encoded_request_value.operations->size(encoded_request_value.message);
    }
    if (conn.state != phase::ready) return {status_code::unavailable, {}};
    if (token.stop_requested()) return {status_code::cancelled, {}};
    if (clock::now() >= deadline) return {status_code::deadline_exceeded, {}};
    if (state.next_id > wire::max_stream_id) { conn.state = phase::draining; state.on_idle(); return {status_code::unavailable, {}}; }
    if (state.calls.size() >= std::min(conn.options.receive.max_concurrent_streams, conn.peer.max_concurrent_streams))
        return {status_code::resource_exhausted, {}, true};
    if (request.size > conn.peer.max_message_size || request.size > state.requests.block_size() - 16) return {};
    auto storage = state.requests.acquire(state.request_budget);
    if (storage.get() == nullptr) return {status_code::resource_exhausted, {}, true};
    auto &frame = *storage.get();
    auto method_index = binding ? binding->wire_index : std::numeric_limits<std::size_t>::max();
    if (method_index == std::numeric_limits<std::size_t>::max()) {
        auto const existing = state.method_ids.find(name);
        if (existing != state.method_ids.end()) method_index = existing->second;
    }
    auto const fresh = method_index == std::numeric_limits<std::size_t>::max() || !state.methods[method_index].published;
    wire::request_head head{};
    head.metadata = options.metadata;
    if (fresh) head.method_name = {reinterpret_cast<std::uint8_t const *>(name.data()), name.size()};
    auto const encoded = wire::encode_request_head(head, fresh, {frame.data + 16, state.requests.block_size() - 16});
    if (encoded.code == wire::error::invalid_argument) return {status_code::invalid_argument, {}};
    if (encoded.code != wire::error::none || encoded.written > conn.peer.max_frame_size ||
        request.size > conn.peer.max_frame_size - encoded.written ||
        request.size > state.requests.block_size() - 16 - encoded.written) return {};
    frame.body_offset = 16 + encoded.written;
    if (encoded_request_value.operations != nullptr) {
        if (encoded_request_value.operations->encode_bounded) {
            auto const capacity = std::min<std::size_t>(conn.peer.max_message_size,
                std::min<std::size_t>(conn.peer.max_frame_size - encoded.written,
                    state.requests.block_size() - frame.body_offset));
            auto const body = encoded_request_value.operations->encode_bounded(encoded_request_value.message,
                {frame.data + frame.body_offset, capacity});
            if (body.code == wire::error::output_too_small || body.written > capacity) return {};
            if (body.code != wire::error::none) return {status_code::invalid_argument, {}};
            request.size = body.written;
        } else if (!encoded_request_value.operations->encode(encoded_request_value.message,
            {frame.data + frame.body_offset, request.size})) return {status_code::invalid_argument, {}};
    } else if (request.size != 0) std::memcpy(frame.data + frame.body_offset, request.data, request.size);
    frame.body_size = request.size;
    frame.size = frame.body_offset + frame.body_size;
    if (conn.state != phase::ready) return {status_code::unavailable, {}};
    if (token.stop_requested()) return {status_code::cancelled, {}};
    if (clock::now() >= deadline) return {status_code::deadline_exceeded, {}};
    if (state.calls.size() >= std::min(conn.options.receive.max_concurrent_streams, conn.peer.max_concurrent_streams))
        return {status_code::resource_exhausted, {}, true};
    if (state.next_id > wire::max_stream_id) return {status_code::unavailable, {}};
    bool inserted_method = false;
    auto const previous_methods = state.methods.size();
    try {
        if (method_index == std::numeric_limits<std::size_t>::max()) {
            if (state.methods.size() >= std::min(conn.peer.max_method_ids, conn.options.receive.max_method_ids)) return {};
            auto inserted = state.method_ids.emplace(name, state.methods.size());
            inserted_method = inserted.second;
            method_index = inserted.first->second;
            state.methods.push_back({&inserted.first->first, false});
        }
        auto const handle = state.slots.acquire();
        auto *call = state.slots.get(handle);
        if (call == nullptr) {
            if (inserted_method) { state.methods.resize(previous_methods); state.method_ids.erase(name); }
            return {status_code::resource_exhausted, {}, true};
        }
        call->owner = &state;
        call->handle = handle;
        call->id = static_cast<std::uint32_t>(state.next_id);
        call->method_index = method_index;
        call->deadline = deadline;
        call->result = {};
        call->method_bytes = 0;
        call->submitted = false;
        call->phase.store(completion_phase::pending);
        call->token = std::move(token);
        call->response = response;
        call->decoded = decoded;
        call->response_metadata = options.response_metadata;
        if (fresh) call->method_bytes = name.size() + wire::decode_varint({frame.data + 24, encoded.written - 8}).consumed;
        if (!schedule(*state.deadlines, call->timeout, deadline, &client_state::expire, call)) {
            state.slots.release(handle);
            if (inserted_method) { state.methods.resize(previous_methods); state.method_ids.erase(name); }
            return {};
        }
        if (!state.calls.insert(call->id, *call)) {
            unschedule(*state.deadlines, call->timeout); state.slots.release(handle);
            if (inserted_method) { state.methods.resize(previous_methods); state.method_ids.erase(name); }
            return {};
        }
        ++state.next_id;
        if (binding) binding->wire_index = method_index;
        frame.is_request = true;
        frame.request = handle;
        conn.enqueue(std::move(storage));
        return {status_code::ok, call};
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

auto call_task(std::shared_ptr<client_state> state, std::string method, call_buffers buffers, direct_call_options options, method_handle handle = {})
    CO2_BEG(net::task<call_result>, (state, method, buffers, options, handle),
            net::io_env const *env = nullptr; clock::time_point deadline; admitted_call admitted;
            call_result result;) {
    CO2_AWAIT_SET(env, net::this_coro::environment);
    if (&env->executor.context() != &state->conn.context)
        CO2_RETURN((call_result{status_code::failed_precondition, 0}));
    deadline = resolve_deadline(options.deadline, options.timeout);
    if (env->stop_token.stop_requested()) CO2_RETURN((call_result{status_code::cancelled, 0}));
    if (clock::now() >= deadline) CO2_RETURN((call_result{status_code::deadline_exceeded, 0}));
    try { admitted = admit(*state, method, buffers, deadline, env->stop_token, options, handle); }
    catch (std::bad_alloc const &) { CO2_RETURN((call_result{status_code::resource_exhausted, 0})); }
    catch (...) { CO2_RETURN((call_result{status_code::invalid_argument, 0})); }
    if (admitted.code != status_code::ok) {
        result.code = admitted.code; result.not_executed = true;
        result.capacity_rejected = admitted.temporary;
        CO2_RETURN(std::move(result));
    }
    CO2_AWAIT_SET(result, (completion_wait{admitted.value}));
    CO2_RETURN(std::move(result));
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
    if (state->options.events) state->options.events->on_connectivity_change();
    try { state->activate(); start_io(state->conn, state); }
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
        return call_task(state_, std::move(method), {request, response}, direct_call_options{options});
    }
    net::task<call_result> call_encoded(std::string method, encoded_request request, decoded_response response,
                                       call_options options) override {
        return call_task(state_, std::move(method), {{}, {}, request, response, true}, direct_call_options{options});
    }
    method_handle bind(method_descriptor const &method) override { return state_->bind(method); }
    std::vector<method_handle> bind(service_descriptor const &service) override { return state_->bind(service); }
    net::task<call_result> call_encoded(method_handle method, encoded_request request, decoded_response response,
                                       call_options options) override {
        if (!method) return call_task(state_, {}, {{}, {}, request, response, true}, direct_call_options{options});
        return call_task(state_, {}, {{}, {}, request, response, true}, direct_call_options{options}, method);
    }
    bool ready() const noexcept override { return state_->conn.state == phase::ready; }
    bool quiescent() const noexcept override { return state_->conn.io_chains == 0 && state_->slots.size() == 0; }
    bool draining() const noexcept override { return state_->conn.state == phase::draining; }
    bool has_capacity(std::size_t) const noexcept override {
        return ready() && state_->slots.size() < std::min(state_->conn.options.receive.max_concurrent_streams, state_->conn.peer.max_concurrent_streams) &&
            state_->requests.charge() <= state_->request_budget.limit - state_->request_budget.used;
    }
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
    void drain() noexcept override {
        if (state_->conn.state == phase::ready) state_->conn.state = phase::draining;
        if (state_->options.events) state_->options.events->on_connectivity_change();
        state_->on_idle();
    }
private:
    std::shared_ptr<client_state> state_;
};

} // namespace detail

std::unique_ptr<client> make_client(net::io_context &context, client_options options) {
    if (options.connection.receive.features & wire::streaming) return detail::make_stream_client(context, options);
    detail::validate_options(options.connection);
    if (options.max_registered_methods == 0) throw std::invalid_argument{"empty method registry"};
    return std::make_unique<detail::client_impl>(std::make_shared<detail::client_state>(context, options));
}

} // namespace rpc
