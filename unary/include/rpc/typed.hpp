#pragma once

#include <rpc/unary.hpp>

#include <utility>
#include <stdexcept>
#include <mutex>

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

template <class Request, class Response> struct method {
    std::string name{};
};

template <class Request, class Response>
net::task<call_result> call(client &channel, method<Request, Response> const &operation,
                             Request const &request, Response &response, call_options options = {}) {
    return channel.call_encoded(operation.name, {&request, &codec_for<Request>()},
                                {&response, &codec_for<Response>()}, options);
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

template <class Request, class Response, class Service, class Result>
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

template <class Request, class Response, class Service, class Result>
auto invoke_typed(typed_method_handler<Request, Response, Service, Result> *handler, server_context *context,
                  wire::bytes_view bytes, response_writer *writer)
    CO2_BEG(net::task<status_code>, (handler, context, bytes, writer),
            message_storage<Request, Response> messages; Result result; status_code code; std::size_t size = 0;) {
    if (!codec<Request>::decode(bytes, messages.request())) CO2_RETURN(status_code::invalid_argument);
    if (context->stop_token.stop_requested()) CO2_RETURN(status_code::cancelled);
    if (clock::now() >= context->deadline) CO2_RETURN(status_code::deadline_exceeded);
    CO2_AWAIT_SET(result, (handler->service.*handler->function)(*context, messages.request(), messages.response()));
    code = normalize_status(*context, result);
    if (code != status_code::ok) CO2_RETURN(code);
    size = codec<Response>::size(messages.response());
    if (size > writer->buffer().size ||
        !codec<Response>::encode(messages.response(), {writer->buffer().data, size})) {
        writer->commit(writer->buffer().size + 1);
        CO2_RETURN(status_code::internal);
    }
    writer->commit(size);
    CO2_RETURN(status_code::ok);
}
CO2_END

template <class Handler> struct message_cache_lease {
    Handler *handler = nullptr; std::size_t index = 0;
    ~message_cache_lease() { if (handler) handler->release(index); }
};
template <class Request, class Response, class Service, class Result>
auto invoke_cached(typed_method_handler<Request, Response, Service, Result> *handler, server_context *context,
                   wire::bytes_view bytes, response_writer *writer)
    CO2_BEG(net::task<status_code>, (handler, context, bytes, writer),
            message_cache_lease<typed_method_handler<Request, Response, Service, Result>> lease;
            message_storage<Request, Response> *messages = nullptr; Result result; status_code code; std::size_t size = 0;) {
    if (!handler->acquire(lease.index)) CO2_RETURN(status_code::resource_exhausted);
    lease.handler = handler; messages = handler->cache[lease.index].get();
    if (!codec<Request>::decode(bytes, messages->request())) CO2_RETURN(status_code::invalid_argument);
    if (context->stop_token.stop_requested()) CO2_RETURN(status_code::cancelled);
    if (clock::now() >= context->deadline) CO2_RETURN(status_code::deadline_exceeded);
    CO2_AWAIT_SET(result, (handler->service.*handler->function)(*context, messages->request(), messages->response()));
    code = normalize_status(*context, result); if (code != status_code::ok) CO2_RETURN(code);
    size = codec<Response>::size(messages->response());
    if (size > writer->buffer().size || !codec<Response>::encode(messages->response(), {writer->buffer().data, size})) {
        writer->commit(writer->buffer().size + 1); CO2_RETURN(status_code::internal);
    }
    writer->commit(size); CO2_RETURN(status_code::ok);
}
CO2_END
template <class Request, class Response, class Service, class Result>
net::task<status_code> typed_method_handler<Request, Response, Service, Result>::invoke(
    server_context &context, wire::bytes_view bytes, response_writer &writer) {
    return cache.empty() ? invoke_typed(this, &context, bytes, &writer) : invoke_cached(this, &context, bytes, &writer);
}

} // namespace detail

// The binding owns its adapter; the service is borrowed until all server work
// has drained. Moving/copying bindings cannot invalidate adapters in flight.
template <class Request, class Response, class Service, class Result>
method_binding bind_method(method<Request, Response> const &operation, Service &service,
    net::task<Result> (Service::*function)(server_context &, Request const &, Response &), method_limits limits) {
    auto handler = std::make_shared<detail::typed_method_handler<Request, Response, Service, Result>>(service, function, limits.message_cache_entries);
    return {operation.name, limits.max_response_bytes, handler.get(), limits.max_response_head_bytes, std::move(handler)};
}

} // namespace rpc
