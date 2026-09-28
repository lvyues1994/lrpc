#pragma once

#include <rpc/unary.hpp>

#include <utility>

namespace rpc {

enum class method_kind { unary };
enum class idempotency { unknown, no_side_effects, idempotent };

struct method_descriptor {
    char const *name = nullptr; // Fully qualified wire name; static lifetime.
    method_kind kind = method_kind::unary;
    idempotency semantics = idempotency::unknown; // Descriptive; never enables retries.
    codec_ops const *request_codec = nullptr;
    codec_ops const *response_codec = nullptr;
};

struct service_descriptor {
    char const *name = nullptr;
    method_descriptor const *methods = nullptr;
    std::size_t size = 0; // Table position is not a connection's wire method ID.
};

struct method_limits {
    std::size_t max_response_bytes = std::numeric_limits<std::size_t>::max();
    std::size_t max_response_head_bytes = 2;
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

namespace detail {

// Protobuf specializes this resource owner with a per-invocation Arena.
template <class Request, class Response, class = void> class message_storage {
public:
    Request &request() noexcept { return request_; }
    Response &response() noexcept { return response_; }
private:
    Request request_{};
    Response response_{};
};

template <class Request, class Response, class Service>
struct typed_method_handler final : method_handler {
    using function_type = net::task<status_code> (Service::*)(server_context &, Request const &, Response &);
    typed_method_handler(Service &service, function_type function) : service(service), function(function) {}
    net::task<status_code> invoke(server_context &context, wire::bytes_view bytes, response_writer &writer) override;
    Service &service;
    function_type function;
};

template <class Request, class Response, class Service>
auto invoke_typed(typed_method_handler<Request, Response, Service> *handler, server_context *context,
                  wire::bytes_view bytes, response_writer *writer)
    CO2_BEG(net::task<status_code>, (handler, context, bytes, writer),
            message_storage<Request, Response> messages; status_code result; std::size_t size = 0;) {
    if (!codec<Request>::decode(bytes, messages.request())) CO2_RETURN(status_code::invalid_argument);
    if (context->stop_token.stop_requested()) CO2_RETURN(status_code::cancelled);
    if (clock::now() >= context->deadline) CO2_RETURN(status_code::deadline_exceeded);
    CO2_AWAIT_SET(result, (handler->service.*handler->function)(*context, messages.request(), messages.response()));
    if (result != status_code::ok) CO2_RETURN(result);
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

template <class Request, class Response, class Service>
net::task<status_code> typed_method_handler<Request, Response, Service>::invoke(
    server_context &context, wire::bytes_view bytes, response_writer &writer) {
    return invoke_typed(this, &context, bytes, &writer);
}

} // namespace detail

// The binding owns its adapter; the service is borrowed until all server work
// has drained. Moving/copying bindings cannot invalidate adapters in flight.
template <class Request, class Response, class Service>
method_binding bind_method(method<Request, Response> const &operation, Service &service,
    net::task<status_code> (Service::*function)(server_context &, Request const &, Response &), method_limits limits) {
    auto handler = std::make_shared<detail::typed_method_handler<Request, Response, Service>>(service, function);
    return {operation.name, limits.max_response_bytes, handler.get(), limits.max_response_head_bytes, std::move(handler)};
}

} // namespace rpc
