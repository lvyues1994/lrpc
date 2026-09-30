#pragma once

#include <rpc/unary.hpp>

#include <utility>
#include <stdexcept>
#include <mutex>
#include <type_traits>

namespace rpc {

enum class idempotency { unknown, no_side_effects, idempotent };

struct method_descriptor {
    char const *name = nullptr; // Fully qualified wire name; static lifetime.
    method_kind kind = method_kind::unary;
    idempotency semantics = idempotency::unknown; // Descriptive; never enables retries.
    codec_ops const *request_codec = nullptr;
    codec_ops const *response_codec = nullptr;
    std::size_t ordinal = 0; // Service-local position, never a wire method ID.
};

struct service_descriptor {
    char const *name = nullptr;
    method_descriptor const *methods = nullptr;
    std::size_t size = 0; // Table position is not a connection's wire method ID.
};

struct method_limits {
    std::size_t max_response_bytes = std::numeric_limits<std::size_t>::max();
    std::size_t max_response_head_bytes = 2;
    std::size_t message_cache_entries = 0; // Opt-in, bounded protobuf Arena cache; default is per-call storage.
};

template <class Request, class Response, class Policy = default_codec_policy> struct method {
    std::string name{};
    idempotency semantics = idempotency::unknown;
};

template <class Request, class Response, class Policy>
net::task<call_result> call(client &channel, method<Request, Response, Policy> const &operation,
                             Request const &request, Response &response, call_options options = {}) {
    // A semantic contract needs a registered descriptor. Runtime facades only
    // permit registration before start(); use a prebound stub there.
    if (operation.semantics != idempotency::unknown) {
        method_descriptor descriptor{operation.name.c_str(), method_kind::unary, operation.semantics,
            &Policy::template operations<Request>(), &Policy::template operations<Response>()};
        return channel.call_encoded(channel.bind(descriptor), {&request, descriptor.request_codec},
                                    {&response, descriptor.response_codec}, options);
    }
    return channel.call_encoded(operation.name, {&request, &Policy::template operations<Request>()},
                                {&response, &Policy::template operations<Response>()}, options);
}

template <class Request, class Response, class Policy> class bound_method;
template <class Request, class Response, class Policy>
bound_method<Request, Response, Policy> bind(client &, method<Request, Response, Policy> const &);

// A typed, client-bound value. Only bind can create it; arbitrary erased handles
// cannot be relabelled with unrelated message types or encoding policies.
template <class Request, class Response, class Policy = default_codec_policy> class bound_method {
public:
    net::task<call_result> operator()(Request const &request, Response &response, call_options options = {}) const {
        return channel_->call_encoded(handle_, {&request, &Policy::template operations<Request>()},
            {&response, &Policy::template operations<Response>()}, options);
    }
private:
    friend bound_method bind<Request, Response, Policy>(client &, method<Request, Response, Policy> const &);
    bound_method(client &channel, method_handle handle) noexcept : channel_(&channel), handle_(handle) {}
    client *channel_;
    method_handle handle_;
};

template <class Request, class Response, class Policy>
bound_method<Request, Response, Policy> bind(client &channel, method<Request, Response, Policy> const &operation) {
    method_descriptor descriptor{operation.name.c_str(), method_kind::unary, operation.semantics,
        &Policy::template operations<Request>(), &Policy::template operations<Response>()};
    return {channel, channel.bind(descriptor)};
}

template <class Request, class Response>
net::task<call_result> call(client &channel, method_handle operation,
                             Request const &request, Response &response, call_options options = {}) {
    return channel.call_encoded(operation, {&request, &codec_for<Request>()}, {&response, &codec_for<Response>()}, options);
}

namespace detail {

// Protobuf specializes this resource owner with a per-invocation Arena.
template <class Request, class Response, class = void> class message_storage {
public:
    static constexpr bool reusable = false;
    Request &request() noexcept { return request_; }
    Response &response() noexcept { return response_; }
    void reset() { throw std::logic_error{"this codec has no reusable message storage"}; }
    std::size_t retained_bytes() const noexcept { return sizeof(*this); }
private:
    Request request_{};
    Response response_{};
};

inline status_code normalize_status(server_context &, status_code result) noexcept { return result; }
inline status_code normalize_status(server_context &context, status const &result) noexcept { return set_status(context, result); }
inline status_code decode_input(codec_ops const &ops, wire::bytes_view bytes, void *message) {
    try { return ops.decode(bytes, message) ? status_code::ok : status_code::invalid_argument; }
    catch (std::bad_alloc const &) { return status_code::resource_exhausted; }
}

template <class Request, class Response, class Service, class Result, class Policy = default_codec_policy>
struct typed_method_handler final : method_handler {
    using function_type = net::task<Result> (Service::*)(server_context &, Request const &, Response &);
    typed_method_handler(Service &service, function_type function, std::size_t count) : service(service), function(function) {
        if (count > 65536 || (count && !message_storage<Request, Response>::reusable)) throw std::invalid_argument{"invalid message cache"};
        cache.reserve(count); free.reserve(count);
        for (std::size_t i = 0; i < count; ++i) { cache.push_back(std::make_unique<message_storage<Request, Response>>()); free.push_back(i); }
    }
    net::task<status_code> invoke(server_context &context, wire::bytes_view bytes, response_writer &writer) override;
    Service &service;
    function_type function;
    std::vector<std::unique_ptr<message_storage<Request, Response>>> cache;
    std::vector<std::size_t> free;
    mutable std::mutex cache_mutex;
    bool acquire(std::size_t &index) {
        std::lock_guard<std::mutex> lock(cache_mutex);
        if (free.empty()) return false;
        index = free.back();
        if (!cache[index]) cache[index] = std::make_unique<message_storage<Request, Response>>();
        free.pop_back(); return true;
    }
    std::size_t cached_storage_bytes() const noexcept override {
        std::lock_guard<std::mutex> lock(cache_mutex);
        // Active Arena growth belongs to the handler frame's memory usage;
        // report fixed cache payload and its two index/ownership arrays here.
        return cache.capacity() * sizeof(cache[0]) + free.capacity() * sizeof(free[0]) + cache.size() * sizeof(message_storage<Request, Response>);
    }
    void release(std::size_t index) noexcept {
        try { cache[index]->reset(); } catch (...) { cache[index].reset(); }
        std::lock_guard<std::mutex> lock(cache_mutex);
        free.push_back(index);
    }
};

template <class Request, class Response, class Service, class Result, class Policy>
auto invoke_typed(typed_method_handler<Request, Response, Service, Result, Policy> *handler, server_context *context,
                  wire::bytes_view bytes, response_writer *writer)
    CO2_BEG(net::task<status_code>, (handler, context, bytes, writer),
            message_storage<Request, Response> messages; Result result; status_code code; wire::encode_result encoded;) {
    code = decode_input(Policy::template operations<Request>(), bytes, &messages.request());
    if (code != status_code::ok) CO2_RETURN(code);
    if (context->stop_token.stop_requested()) CO2_RETURN(status_code::cancelled);
    if (clock::now() >= context->deadline) CO2_RETURN(status_code::deadline_exceeded);
    CO2_AWAIT_SET(result, (handler->service.*handler->function)(*context, messages.request(), messages.response()));
    code = normalize_status(*context, result);
    if (code != status_code::ok) CO2_RETURN(code);
    try { encoded = encode_into(Policy::template operations<Response>(), &messages.response(), writer->buffer()); }
    catch (std::bad_alloc const &) { CO2_RETURN(status_code::resource_exhausted); }
    if (encoded.code != wire::error::none || encoded.written > writer->buffer().size) {
        writer->commit(writer->buffer().size + 1);
        CO2_RETURN(status_code::internal);
    }
    writer->commit(encoded.written);
    CO2_RETURN(status_code::ok);
}
CO2_END

template <class Handler> struct message_cache_lease {
    Handler *handler = nullptr; std::size_t index = 0;
    ~message_cache_lease() { if (handler) handler->release(index); }
};
template <class Request, class Response, class Service, class Result, class Policy>
auto invoke_cached(typed_method_handler<Request, Response, Service, Result, Policy> *handler, server_context *context,
                   wire::bytes_view bytes, response_writer *writer)
    CO2_BEG(net::task<status_code>, (handler, context, bytes, writer),
            message_cache_lease<typed_method_handler<Request, Response, Service, Result, Policy>> lease;
            message_storage<Request, Response> *messages = nullptr; Result result; status_code code; wire::encode_result encoded;) {
    if (!handler->acquire(lease.index)) CO2_RETURN(status_code::resource_exhausted);
    lease.handler = handler; messages = handler->cache[lease.index].get();
    code = decode_input(Policy::template operations<Request>(), bytes, &messages->request());
    if (code != status_code::ok) CO2_RETURN(code);
    if (context->stop_token.stop_requested()) CO2_RETURN(status_code::cancelled);
    if (clock::now() >= context->deadline) CO2_RETURN(status_code::deadline_exceeded);
    CO2_AWAIT_SET(result, (handler->service.*handler->function)(*context, messages->request(), messages->response()));
    code = normalize_status(*context, result); if (code != status_code::ok) CO2_RETURN(code);
    try { encoded = encode_into(Policy::template operations<Response>(), &messages->response(), writer->buffer()); }
    catch (std::bad_alloc const &) { CO2_RETURN(status_code::resource_exhausted); }
    if (encoded.code != wire::error::none || encoded.written > writer->buffer().size) {
        writer->commit(writer->buffer().size + 1); CO2_RETURN(status_code::internal);
    }
    writer->commit(encoded.written); CO2_RETURN(status_code::ok);
}
CO2_END
template <class Request, class Response, class Service, class Result, class Policy>
net::task<status_code> typed_method_handler<Request, Response, Service, Result, Policy>::invoke(
    server_context &context, wire::bytes_view bytes, response_writer &writer) {
    return cache.empty() ? invoke_typed(this, &context, bytes, &writer) : invoke_cached(this, &context, bytes, &writer);
}

} // namespace detail

// The binding owns its adapter; the service is borrowed until all server work
// has drained. Moving/copying bindings cannot invalidate adapters in flight.
template <class Request, class Response, class Service, class Result, class Policy>
method_binding bind_method(method<Request, Response, Policy> const &operation, Service &service,
    net::task<Result> (Service::*function)(server_context &, Request const &, Response &), method_limits limits) {
    static_assert(std::is_same<Result, status_code>::value || std::is_same<Result, status>::value,
        "RPC handlers must return task<status_code> or task<status>");
    auto handler = std::make_shared<detail::typed_method_handler<Request, Response, Service, Result, Policy>>(service, function, limits.message_cache_entries);
    return {operation.name, limits.max_response_bytes, handler.get(), limits.max_response_head_bytes, std::move(handler)};
}

template <class Request, class Response, class Policy, class Service, class Result>
server_builder &server_builder::add(method<Request, Response, Policy> const &operation, Service &service,
    net::task<Result> (Service::*function)(server_context &, Request const &, Response &), method_limits limits) {
    return add(bind_method(operation, service, function, limits));
}

} // namespace rpc
