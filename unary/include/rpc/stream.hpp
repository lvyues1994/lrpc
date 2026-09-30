#pragma once
#include <rpc/typed.hpp>
#include <rpc/byte_buffer.hpp>
#include <rpc/compression.hpp>
#include <functional>

namespace rpc {
struct stream_read_result : status {
    stream_read_result(status_code code = status_code::ok, bool ended = false) : status{code, {}}, ended(ended) {}
    bool ended = false; // Empty message is a successful read with ended=false.
};
// Fixed to its opening session and owner executor. One read and one write may
// run concurrently; a second operation in the same direction is rejected.
// No message retry. The opening deadline and stop token cover the entire RPC.
// Opening call_options.metadata/response_metadata remain borrowed until the
// terminal result (finish), even though open_stream itself has returned.
struct byte_stream {
    virtual ~byte_stream() = default;
    virtual net::task<stream_read_result> read(byte_buffer &message) = 0;
    virtual net::task<status_code> write(byte_buffer message) = 0;
    virtual net::task<status_code> write_encoded(encoded_request message) = 0;
    virtual net::task<status_code> writes_done() = 0;
    virtual net::task<call_result> finish() = 0;
    virtual void cancel() noexcept = 0;
    // One owner-thread observer, used by channel tracing/accounting. The
    // terminal callback runs once, including deadline and connection failure.
    virtual bool observe_completion(std::function<void(call_result const &)>) { return false; }
};
struct stream_open_result : status {
    stream_open_result(status_code code = status_code::ok, std::shared_ptr<byte_stream> stream = {})
        : status{code, {}}, stream(std::move(stream)) {}
    std::shared_ptr<byte_stream> stream{};
};
namespace detail {
template <class Message> void prepare_stream_read(Message &) noexcept {}
inline void prepare_stream_read(byte_buffer &message) noexcept { message.clear(); }
net::task<stream_read_result> read_typed_stream(std::shared_ptr<byte_stream> stream, decoded_response message);
net::task<stream_read_result> failed_stream_read(status_code code);
net::task<status_code> failed_stream_operation(status_code code);
net::task<call_result> failed_stream_finish(status_code code);
}
template <class Read, class Write> class typed_stream {
public:
    typed_stream() noexcept = default;
    explicit typed_stream(std::shared_ptr<byte_stream> stream, status_code code = status_code::ok, bool cancel_on_drop = true)
        : stream_(std::move(stream)), code_(code), cancel_on_drop_(cancel_on_drop) {}
    ~typed_stream() { if (cancel_on_drop_) cancel(); }
    typed_stream(typed_stream &&other) noexcept
        : stream_(std::move(other.stream_)), code_(other.code_), cancel_on_drop_(std::exchange(other.cancel_on_drop_, false)) {}
    typed_stream &operator=(typed_stream &&other) noexcept {
        if (this != &other) {
            if (cancel_on_drop_) cancel();
            stream_ = std::move(other.stream_); code_ = other.code_;
            cancel_on_drop_ = std::exchange(other.cancel_on_drop_, false);
        }
        return *this;
    }
    typed_stream(typed_stream const &) = delete;
    typed_stream &operator=(typed_stream const &) = delete;
    explicit operator bool() const noexcept { return stream_ != nullptr && code_ == status_code::ok; }
    status_code code() const noexcept { return error(); }
    // For byte_buffer, releases the previous result before starting the next
    // read; other copies/slices may still retain that message's flow credit.
    net::task<stream_read_result> read(Read &message) {
        detail::prepare_stream_read(message);
        return stream_ ? detail::read_typed_stream(stream_, {&message, &codec_for<Read>()}) : detail::failed_stream_read(error());
    }
    net::task<status_code> write(Write const &message) {
        return stream_ ? stream_->write_encoded({&message, &codec_for<Write>()}) : detail::failed_stream_operation(error());
    }
    net::task<status_code> writes_done() { return stream_ ? stream_->writes_done() : detail::failed_stream_operation(error()); }
    net::task<call_result> finish() { return stream_ ? stream_->finish() : detail::failed_stream_finish(error()); }
    void cancel() noexcept { if (stream_) stream_->cancel(); }
private:
    status_code error() const noexcept { return !stream_ && code_ == status_code::ok ? status_code::failed_precondition : code_; }
    std::shared_ptr<byte_stream> stream_{};
    status_code code_ = status_code::failed_precondition;
    bool cancel_on_drop_ = false;
};
template <class Request, class Response> using stream_call = typed_stream<Response, Request>;
template <class Request, class Response> using server_stream = typed_stream<Request, Response>;
struct stream_method_handler {
    virtual ~stream_method_handler() = default;
    virtual net::task<status_code> invoke(server_context &, std::shared_ptr<byte_stream> stream) = 0;
};
namespace detail {
template <class Request, class Response> struct typed_open {
    using call_type = stream_call<Request, Response>;
    using task_type = net::task<call_type>;
    static auto run(client *c, method_handle method, call_options options)
        CO2_BEG(task_type, (c, method, options), stream_open_result opened;) {
        CO2_AWAIT_SET(opened, c->open_stream(method, options));
        CO2_RETURN((call_type{std::move(opened.stream), opened.code}));
    }
    CO2_END
};
template <class Request, class Response, class Service> struct typed_stream_handler final : stream_method_handler {
    using stream_type = server_stream<Request, Response>;
    using function_type = net::task<status_code> (Service::*)(server_context &, server_stream<Request, Response> &);
    typed_stream_handler(Service &service, function_type function) : service(service), function(function) {}
    net::task<status_code> invoke(server_context &context, std::shared_ptr<byte_stream> stream) override { return run(this, &context, std::move(stream)); }
    static auto run(typed_stream_handler *handler, server_context *context, std::shared_ptr<byte_stream> stream)
        CO2_BEG(net::task<status_code>, (handler, context, stream),
                stream_type typed; status_code result;) {
        typed = stream_type{std::move(stream), status_code::ok, false};
        CO2_AWAIT_SET(result, (handler->service.*handler->function)(*context, typed));
        CO2_RETURN(result);
    }
    CO2_END
    Service &service;
    function_type function;
};
}
template <class Request, class Response>
net::task<stream_call<Request, Response>> open_stream(client &c, method_handle method, call_options options = {}) {
    return detail::typed_open<Request, Response>::run(&c, method, options);
}
template <class Request, class Response, class Service>
method_binding bind_stream_method(method<Request, Response> const &operation, method_kind kind, Service &service,
    net::task<status_code> (Service::*function)(server_context &, server_stream<Request, Response> &), method_limits limits) {
    auto adapter = std::make_shared<detail::typed_stream_handler<Request, Response, Service>>(service, function);
    return {operation.name, limits.max_response_bytes, nullptr, limits.max_response_head_bytes, {}, kind, adapter.get(), std::move(adapter)};
}
} // namespace rpc
