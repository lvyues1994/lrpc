#pragma once

#include <rpc/call.hpp>

#include <net/continuation.hpp>
#include <net/coroutine.hpp>
#include <net/io_env.hpp>

#include <cstdint>
#include <utility>

namespace rpc {

struct stream_read {
    status_code code = status_code::ok;
    bool ended = false;         // The peer sends no more messages.
    wire::bytes_view message{}; // Valid until the next read on the stream, or its destruction.
};

class client;
class channel;

namespace detail {
struct stream_core;
struct client_stream_core;
struct server_stream_call;
struct client_core;
struct call_router;
struct stream_access;
} // namespace detail

// Stream operations are IoAwaitables for the stream's shard. At most one read
// and one write may be outstanding on a stream at a time.
class read_operation {
public:
    bool await_ready() noexcept;
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept;
    stream_read await_resume() noexcept;

private:
    friend struct detail::stream_access;
    explicit read_operation(detail::stream_core *core) noexcept : core_(core) {}
    detail::stream_core *core_;
    stream_read result_{};
    bool done_ = false;
};

// Completes once the whole message is framed for sending, after waiting for
// flow-control credit and for the stream's turns on the connection; the
// bytes are borrowed until then.
class write_operation {
public:
    bool await_ready() noexcept;
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept;
    status_code await_resume() noexcept;

private:
    friend struct detail::stream_access;
    write_operation(detail::stream_core *core, wire::bytes_view message, bool half) noexcept
        : core_(core), message_(message), half_(half) {}
    detail::stream_core *core_;
    wire::bytes_view message_;
    bool half_;
    bool done_ = false;
    status_code result_ = status_code::ok;
};

class finish_operation {
public:
    bool await_ready() noexcept;
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept;
    call_result await_resume() noexcept;

private:
    friend struct detail::stream_access;
    finish_operation(detail::client_stream_core *core, response_trailer *trailer) noexcept
        : core_(core), trailer_(trailer) {}
    detail::client_stream_core *core_;
    response_trailer *trailer_;
};

// The client's end of a stream. Destroying it before the server has ended
// the stream cancels it.
class client_stream {
public:
    client_stream() noexcept = default;
    client_stream(client_stream &&other) noexcept : core_(std::exchange(other.core_, nullptr)) {}
    client_stream &operator=(client_stream &&other) noexcept {
        if (this != &other) {
            reset();
            core_ = std::exchange(other.core_, nullptr);
        }
        return *this;
    }
    client_stream(client_stream const &) = delete;
    client_stream &operator=(client_stream const &) = delete;
    ~client_stream() { reset(); }

    explicit operator bool() const noexcept { return core_ != nullptr; }
    write_operation write(wire::bytes_view message) noexcept;
    write_operation writes_done() noexcept;
    read_operation read() noexcept;
    // The status the server ended the stream with, or the local failure.
    finish_operation finish(response_trailer *trailer = nullptr) noexcept;
    void cancel() noexcept;

private:
    friend struct detail::stream_access;
    explicit client_stream(detail::client_stream_core *core) noexcept : core_(core) {}
    void reset() noexcept;
    detail::client_stream_core *core_ = nullptr;
};

struct open_result {
    status_code code = status_code::unknown;
    client_stream stream{};
};

// Opening queues the REQUEST and completes without waiting for the server.
class open_operation {
public:
    bool await_ready() const noexcept { return false; }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept;
    open_result await_resume() noexcept { return std::move(result_); }

private:
    friend class client;
    friend class channel;
    open_operation(detail::client_core *core, detail::call_router *router, std::uint32_t method, method_kind kind,
                   call_spec const *spec) noexcept
        : core_(core), router_(router), spec_(spec), method_(method), kind_(kind) {}
    detail::client_core *core_;
    detail::call_router *router_;
    call_spec const *spec_;
    std::uint32_t method_;
    method_kind kind_;
    open_result result_{};
};

// The server's end of a stream, valid until the handler completes.
class server_stream {
public:
    read_operation read() noexcept;
    write_operation write(wire::bytes_view message) noexcept; // Bounded by max_response_bytes.
    // As response_writer::set_trailer.
    bool set_trailer(wire::bytes_view message, wire::metadata_list metadata = {}) noexcept;

private:
    friend struct detail::stream_access;
    detail::server_stream_call *call_ = nullptr;
};

} // namespace rpc
