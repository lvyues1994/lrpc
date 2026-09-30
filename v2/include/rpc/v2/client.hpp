#pragma once

#include <rpc/v2/call.hpp>
#include <rpc/v2/detail/stream_state.hpp>
#include <rpc/v2/options.hpp>
#include <rpc/v2/shard.hpp>
#include <rpc/v2/stream.hpp>
#include <rpc/v2/transport.hpp>

#include <net/io_awaitable_promise_base.hpp>
#include <net/task.hpp>

#include <memory>
#include <string>

namespace rpc {
namespace v2 {

struct client_options {
    connection_options connection{};
};

namespace detail {
struct client_core;
struct call_router;
struct stop_hook;
struct call_access;
} // namespace detail

class channel;

// The complete state of one unary call. It is an IoAwaitable that lives in
// the awaiting coroutine's frame, so issuing a call allocates nothing. Await
// it on the client's shard; request, response and spec are borrowed until it
// completes, and so is the trailer if one is given. The caller may resume
// inline from the connection's reader. A stop request may come from any
// thread.
class unary_call : detail::stream_state {
public:
    unary_call(unary_call &&other) noexcept; // Only before it is awaited.
    unary_call(unary_call const &) = delete;
    unary_call &operator=(unary_call const &) = delete;
    unary_call &operator=(unary_call &&) = delete;

    bool await_ready() const noexcept { return false; }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept;
    call_result await_resume() noexcept;

private:
    friend class client;
    friend class channel;
    friend struct detail::call_access;
    unary_call(detail::client_core *core, detail::call_router *router, std::uint32_t method,
               wire::bytes_view request, wire::mutable_bytes_view response, call_spec const *spec,
               response_trailer *trailer) noexcept;

    detail::client_core *core_; // Of the current attempt; a router picks it.
    detail::call_router *router_;
    wire::bytes_view request_;
    wire::mutable_bytes_view response_;
    call_spec const *spec_;
    response_trailer *trailer_;
    net::continuation continuation_{};
    net::io_env const *env_ = nullptr;
    detail::stop_hook *hook_ = nullptr;
    clock::time_point deadline_ = clock::time_point::min(); // Fixed by the first attempt.
    std::uint32_t method_;
    std::uint32_t size_ = 0;
    std::uint8_t phase_ = 0;
    std::uint8_t attempts_ = 0;
    bool not_executed_ = false;
    status_code code_ = status_code::unknown;
};

static_assert(sizeof(net::detail::env_awaiter<unary_call>) <= CO2_AWAIT_STORAGE_SIZE,
              "a unary call must fit the awaiting frame's inline awaiter slot");

// One connection to one server, bound to a shard. All members, including the
// destructor, run on the shard's thread. The client must outlive its calls.
class client {
public:
    explicit client(shard &owner, client_options options = {});
    ~client();
    client(client const &) = delete;
    client &operator=(client const &) = delete;

    net::task<status_code> connect(net::ip::tcp::endpoint endpoint);
    net::task<status_code> attach(std::unique_ptr<transport> link);
    method_ref bind(std::string const &name);
    unary_call call(method_ref method, wire::bytes_view request, wire::mutable_bytes_view response,
                    call_spec const *spec = nullptr, response_trailer *trailer = nullptr) noexcept {
        return unary_call{core_.get(), nullptr, method.index, request, response, spec, trailer};
    }
    // Opens a stream; the spec's deadline and the awaiting coroutine's stop
    // token cover the whole stream.
    open_operation open(method_ref method, method_kind kind, call_spec const *spec = nullptr) noexcept;
    bool ready() const noexcept;
    std::size_t pending() const noexcept;
    void close() noexcept;

private:
    std::shared_ptr<detail::client_core> core_;
};

} // namespace v2
} // namespace rpc
