#pragma once

#include <rpc/channel.hpp>
#include <rpc/client.hpp>
#include <rpc/codec.hpp>
#include <rpc/server.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace rpc {

// A method's wire name, message types and encoding.
template <class Request, class Response, class Policy = default_codec_policy> struct method {
    using request_type = Request;
    using response_type = Response;
    using policy_type = Policy;
    std::string name;
};

struct method_limits {
    std::size_t max_response_bytes = 64U * 1024U; // Encoded; charged per unary call, per message for streams.
    std::size_t max_trailer_bytes = 256;
};

namespace detail {

template <class Message, class Policy> codec_ops const &operations() {
    return Policy::template operations<Message>();
}

// One-pass codecs encode into an upper bound, others into their exact size.
template <class Message, class Policy> bool one_pass() {
    auto const &ops = operations<Message, Policy>();
    return ops.encode_bounded != nullptr && ops.upper_bound != nullptr;
}

template <class Message, class Policy> std::size_t encoding_bound(Message const &message) {
    auto const &ops = operations<Message, Policy>();
    return one_pass<Message, Policy>() ? ops.upper_bound(&message) : ops.size(&message);
}

// `out` is the exact size for other codecs, at most the bound for one-pass ones.
template <class Message, class Policy> std::size_t encode_message(void const *message, wire::mutable_bytes_view out) {
    try {
        auto const &ops = operations<Message, Policy>();
        if (one_pass<Message, Policy>()) {
            auto const result = ops.encode_bounded(message, out);
            return result.code == wire::error::none ? result.written : encode_failed;
        }
        return ops.encode(message, out) ? out.size : encode_failed;
    } catch (...) {
        return encode_failed;
    }
}

template <class Message, class Policy> bool decode_message(void *message, wire::bytes_view body) {
    try {
        return operations<Message, Policy>().decode(body, message);
    } catch (...) {
        return false;
    }
}

// A request the server cannot decode is the caller's fault, unless memory ran out.
template <class Message, class Policy> status_code decode_request(Message &message, wire::bytes_view body) noexcept {
    try {
        return operations<Message, Policy>().decode(body, &message) ? status_code::ok : status_code::invalid_argument;
    } catch (std::bad_alloc const &) {
        return status_code::resource_exhausted;
    } catch (...) {
        return status_code::invalid_argument;
    }
}

inline std::size_t refuse_encoding(void const *, wire::mutable_bytes_view) { return encode_failed; }

} // namespace detail

// Encodes the message straight into the request frame when the call starts.
template <class Message, class Policy = default_codec_policy> request_body encoded(Message const &message) noexcept {
    try {
        return {&message, detail::encoding_bound<Message, Policy>(message), &detail::encode_message<Message, Policy>,
                detail::one_pass<Message, Policy>()};
    } catch (...) {
        return {&message, 0, &detail::refuse_encoding, false}; // Fails the call with invalid_argument.
    }
}

// Decodes the response body in place into the message.
template <class Message, class Policy = default_codec_policy> response_body decoded(Message &message) noexcept {
    return {&message, &detail::decode_message<Message, Policy>};
}

// Where typed calls go: a client or a channel, borrowed.
class call_target {
public:
    call_target(client &target) noexcept : client_(&target) {}
    call_target(channel &target) noexcept : channel_(&target) {}

    method_ref bind(std::string const &name) const { return client_ ? client_->bind(name) : channel_->bind(name); }
    unary_call call(method_ref method, request_body request, response_body response, call_spec const *spec,
                    response_trailer *trailer) const noexcept {
        return client_ ? client_->call(method, request, response, spec, trailer)
                       : channel_->call(method, request, response, spec, trailer);
    }
    open_operation open(method_ref method, method_kind kind, call_spec const *spec) const noexcept {
        return client_ ? client_->open(method, kind, spec) : channel_->open(method, kind, spec);
    }

private:
    client *client_ = nullptr;
    channel *channel_ = nullptr;
};

// A method bound to one client or channel; calls allocate nothing and
// encode and decode in place.
template <class Request, class Response, class Policy = default_codec_policy> class bound_method {
public:
    bound_method(call_target target, method<Request, Response, Policy> const &operation)
        : target_(target), method_(target.bind(operation.name)) {}
    unary_call operator()(Request const &request, Response &response, call_spec const *spec = nullptr,
                          response_trailer *trailer = nullptr) const noexcept {
        return target_.call(method_, encoded<Request, Policy>(request), decoded<Response, Policy>(response), spec,
                            trailer);
    }

private:
    call_target target_;
    method_ref method_;
};

template <class Request, class Response, class Policy>
bound_method<Request, Response, Policy> bind(call_target target, method<Request, Response, Policy> const &operation) {
    return {target, operation};
}

// ---- streams ----

// A write of an encoded message, or the error that prevented encoding it.
class typed_write_operation {
public:
    typed_write_operation(write_operation operation, status_code failure) noexcept
        : operation_(operation), failure_(failure) {}
    bool await_ready() noexcept { return failure_ != status_code::ok || operation_.await_ready(); }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
        return operation_.await_suspend(handle, env);
    }
    status_code await_resume() noexcept { return failure_ != status_code::ok ? failure_ : operation_.await_resume(); }

private:
    write_operation operation_;
    status_code failure_;
};

// Decodes into the target; a message that fails to decode reads as internal.
template <class Message, class Policy> class typed_read_operation {
public:
    typed_read_operation(read_operation operation, Message &target) noexcept : operation_(operation), target_(&target) {}
    bool await_ready() noexcept { return operation_.await_ready(); }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
        return operation_.await_suspend(handle, env);
    }
    stream_read await_resume() noexcept {
        auto result = operation_.await_resume();
        if (result.code == status_code::ok && !result.ended &&
            !detail::decode_message<Message, Policy>(target_, result.message))
            result.code = status_code::internal;
        return result;
    }

private:
    read_operation operation_;
    Message *target_;
};

namespace detail {

// Encodes into a buffer reused across writes; it outlives each write.
class write_buffer {
public:
    template <class Message, class Policy> status_code encode(Message const &message, wire::bytes_view &out) noexcept {
        try {
            auto const bound = encoding_bound<Message, Policy>(message);
            if (bytes_.size() < bound) bytes_.resize(bound);
            auto const written = encode_message<Message, Policy>(&message, {bytes_.data(), bound});
            if (written == encode_failed) return status_code::invalid_argument;
            out = {bytes_.data(), written};
            return status_code::ok;
        } catch (std::bad_alloc const &) {
            return status_code::resource_exhausted;
        } catch (...) {
            return status_code::invalid_argument;
        }
    }

private:
    std::vector<std::uint8_t> bytes_;
};

} // namespace detail

template <class Request, class Response, class Policy = default_codec_policy> class typed_client_stream {
public:
    typed_client_stream() noexcept = default;
    explicit typed_client_stream(client_stream stream) noexcept : stream_(std::move(stream)) {}
    explicit operator bool() const noexcept { return static_cast<bool>(stream_); }

    typed_write_operation write(Request const &message) noexcept {
        wire::bytes_view bytes{};
        auto const code = buffer_.template encode<Request, Policy>(message, bytes);
        return {stream_.write(bytes), code};
    }
    write_operation writes_done() noexcept { return stream_.writes_done(); }
    typed_read_operation<Response, Policy> read(Response &message) noexcept { return {stream_.read(), message}; }
    finish_operation finish(response_trailer *trailer = nullptr) noexcept { return stream_.finish(trailer); }
    void cancel() noexcept { stream_.cancel(); }

private:
    client_stream stream_;
    detail::write_buffer buffer_;
};

template <class Request, class Response, class Policy> struct typed_open_result {
    status_code code = status_code::unknown;
    typed_client_stream<Request, Response, Policy> stream{};
};

template <class Request, class Response, class Policy> class typed_open_operation {
public:
    explicit typed_open_operation(open_operation operation) noexcept : operation_(std::move(operation)) {}
    bool await_ready() const noexcept { return operation_.await_ready(); }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
        return operation_.await_suspend(handle, env);
    }
    typed_open_result<Request, Response, Policy> await_resume() noexcept {
        auto opened = operation_.await_resume();
        return {opened.code, typed_client_stream<Request, Response, Policy>{std::move(opened.stream)}};
    }

private:
    open_operation operation_;
};

// A streaming method bound to one client or channel.
template <class Request, class Response, class Policy = default_codec_policy> class bound_stream {
public:
    bound_stream(call_target target, method<Request, Response, Policy> const &operation, method_kind kind)
        : target_(target), method_(target.bind(operation.name)), kind_(kind) {}
    typed_open_operation<Request, Response, Policy> operator()(call_spec const *spec = nullptr) const noexcept {
        return typed_open_operation<Request, Response, Policy>{target_.open(method_, kind_, spec)};
    }

private:
    call_target target_;
    method_ref method_;
    method_kind kind_;
};

template <class Request, class Response, class Policy> class typed_server_stream {
public:
    typed_server_stream() noexcept = default;
    explicit typed_server_stream(server_stream &stream) noexcept : stream_(&stream) {}

    typed_read_operation<Request, Policy> read(Request &message) noexcept { return {stream_->read(), message}; }
    typed_write_operation write(Response const &message) noexcept {
        wire::bytes_view bytes{};
        auto const code = buffer_.template encode<Response, Policy>(message, bytes);
        return {stream_->write(bytes), code};
    }
    bool set_trailer(wire::bytes_view message, wire::metadata_list metadata = {}) noexcept {
        return stream_->set_trailer(message, metadata);
    }

private:
    server_stream *stream_ = nullptr;
    detail::write_buffer buffer_;
};

// ---- servers ----

namespace detail {

// Per-call messages; codecs may specialize it (protobuf uses an Arena).
template <class Request, class Response, class = void> class message_storage {
public:
    Request &request() noexcept { return request_; }
    Response &response() noexcept { return response_; }

private:
    Request request_{};
    Response response_{};
};

template <class Message, class Policy> status_code write_response(response_writer &writer, Message const &message) noexcept {
    std::size_t bound = 0;
    try {
        bound = encoding_bound<Message, Policy>(message);
        if (one_pass<Message, Policy>() && bound > writer.max_size()) bound = writer.max_size();
    } catch (...) {
        return status_code::internal;
    }
    auto const out = writer.prepare(bound);
    if (out.data == nullptr && bound != 0) return status_code::internal; // Beyond max_response_bytes.
    auto const written = encode_message<Message, Policy>(&message, out);
    return written != encode_failed && writer.commit(written) ? status_code::ok : status_code::internal;
}

template <class Request, class Response, class Service, class Policy> struct typed_handler final : method_handler {
    using function_type = net::task<status_code> (Service::*)(server_context &, Request const &, Response &);
    typed_handler(Service &owner, function_type handler) : service(owner), function(handler) {
        if (function == nullptr) throw std::invalid_argument{"null RPC handler"};
    }
    net::task<status_code> invoke(server_context &context, wire::bytes_view request, response_writer &response) override;
    Service &service;
    function_type function;
};

template <class Request, class Response, class Service, class Policy>
auto run_typed(typed_handler<Request, Response, Service, Policy> *handler, server_context *context,
               wire::bytes_view bytes, response_writer *writer)
    CO2_BEG(net::task<status_code>, (handler, context, bytes, writer), message_storage<Request, Response> messages;
            status_code code = status_code::ok;) {
    code = decode_request<Request, Policy>(messages.request(), bytes);
    if (code != status_code::ok) CO2_RETURN(code);
    CO2_AWAIT_SET(code, (handler->service.*handler->function)(*context, messages.request(), messages.response()));
    if (code != status_code::ok) CO2_RETURN(code);
    CO2_RETURN((write_response<Response, Policy>(*writer, messages.response())));
}
CO2_END

template <class Request, class Response, class Service, class Policy>
net::task<status_code> typed_handler<Request, Response, Service, Policy>::invoke(server_context &context,
                                                                                 wire::bytes_view request,
                                                                                 response_writer &response) {
    return run_typed(this, &context, request, &response);
}

template <class Request, class Response, class Service, class Policy> struct typed_stream_handler final : stream_method_handler {
    using stream_type = typed_server_stream<Request, Response, Policy>;
    using function_type = net::task<status_code> (Service::*)(server_context &, stream_type &);
    typed_stream_handler(Service &owner, function_type handler) : service(owner), function(handler) {
        if (function == nullptr) throw std::invalid_argument{"null RPC handler"};
    }
    net::task<status_code> invoke(server_context &context, server_stream &stream) override;
    Service &service;
    function_type function;
};

template <class Request, class Response, class Service, class Policy>
auto run_typed_stream(typed_stream_handler<Request, Response, Service, Policy> *handler, server_context *context,
                      server_stream *stream)
    CO2_BEG(net::task<status_code>, (handler, context, stream), typed_server_stream<Request, Response, Policy> typed;
            status_code code = status_code::ok;) {
    typed = typed_server_stream<Request, Response, Policy>{*stream};
    CO2_AWAIT_SET(code, (handler->service.*handler->function)(*context, typed));
    CO2_RETURN(code);
}
CO2_END

template <class Request, class Response, class Service, class Policy>
net::task<status_code> typed_stream_handler<Request, Response, Service, Policy>::invoke(server_context &context,
                                                                                        server_stream &stream) {
    return run_typed_stream(this, &context, &stream);
}

} // namespace detail

// The binding owns its adapter; the service is borrowed until the server has drained.
template <class Request, class Response, class Policy, class Service>
method_binding bind_method(method<Request, Response, Policy> const &operation, Service &service,
                           net::task<status_code> (Service::*function)(server_context &, Request const &, Response &),
                           method_limits limits = {}) {
    auto handler = std::make_shared<detail::typed_handler<Request, Response, Service, Policy>>(service, function);
    method_binding binding{};
    binding.name = operation.name;
    binding.handler = handler.get();
    binding.max_response_bytes = limits.max_response_bytes;
    binding.max_trailer_bytes = limits.max_trailer_bytes;
    binding.owner = std::move(handler);
    return binding;
}

template <class Request, class Response, class Policy, class Service>
method_binding bind_stream_method(method<Request, Response, Policy> const &operation, method_kind kind, Service &service,
                                  net::task<status_code> (Service::*function)(
                                      server_context &, typed_server_stream<Request, Response, Policy> &),
                                  method_limits limits = {}) {
    if (kind == method_kind::unary) throw std::invalid_argument{"unary methods bind with bind_method"};
    auto handler = std::make_shared<detail::typed_stream_handler<Request, Response, Service, Policy>>(service, function);
    method_binding binding{};
    binding.name = operation.name;
    binding.max_response_bytes = limits.max_response_bytes;
    binding.max_trailer_bytes = limits.max_trailer_bytes;
    binding.kind = kind;
    binding.stream_handler = handler.get();
    binding.owner = std::move(handler);
    return binding;
}

} // namespace rpc
