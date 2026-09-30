#pragma once

#include "engine.hpp"

#include <rpc/v2/stream.hpp>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace rpc {
namespace v2 {
namespace detail {

// One suspended stream operation.
struct stream_waiter {
    net::continuation continuation{};
    net::executor_ref executor{};
    bool armed = false;
    void arm(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
        continuation.h = handle;
        executor = env->executor;
        armed = true;
    }
    void wake() noexcept {
        if (!armed) return;
        armed = false;
        executor.post(continuation);
    }
};

struct inbound_message {
    std::uint8_t *block = nullptr;
    std::size_t capacity = 0;
    std::size_t size = 0;
    std::uint32_t cost = 0; // Flow-control credit it holds.
    bool held = false;
};

// Message flow shared by both ends of a stream: fragments with a descriptor
// on the first, reassembly, per-message window accounting with credit
// returned once the reader lets go of a message, and half-close. How a
// stream ends belongs to the role.
struct stream_core : stream_state {
    stream_core(shard_state &state, method_kind stream_kind, bool client) noexcept
        : shard(state), kind(stream_kind), client_side(client) {}
    virtual ~stream_core();
    stream_core(stream_core const &) = delete;
    stream_core &operator=(stream_core const &) = delete;

    virtual void fail(status_code code) noexcept = 0; // A stream error found while receiving.

    bool flow_controlled() const noexcept { return kind != method_kind::unary; }
    bool sends_one() const noexcept {
        return kind == method_kind::unary ||
               kind == (client_side ? method_kind::server_streaming : method_kind::client_streaming);
    }
    bool receives_one() const noexcept {
        return kind == method_kind::unary ||
               kind == (client_side ? method_kind::client_streaming : method_kind::server_streaming);
    }
    void open(connection &link, std::size_t outbound_limit) noexcept;

    bool push(inbound_message const &message) noexcept; // False when full.
    bool on_message(wire::frame_view const &frame) noexcept;
    bool decompress() noexcept;
    bool on_window(wire::frame_view const &frame) noexcept;
    status_code send(wire::bytes_view message) noexcept;
    status_code send_half() noexcept;
    bool take(stream_read &out) noexcept; // False: nothing to report yet.
    void release_current() noexcept;
    void drop_messages() noexcept;
    void wake_all() noexcept {
        reader.wake();
        writer.wake();
        finisher.wake();
    }

    shard_state &shard;
    connection *conn = nullptr; // Until the stream leaves its connection.
    method_kind const kind;
    bool const client_side;
    std::uint32_t peer_window = 0;
    std::uint32_t send_credit = 0;
    std::uint32_t receive_credit = 0;
    std::uint32_t writer_need = 0;
    std::uint32_t sent = 0;
    std::uint32_t received = 0;
    std::size_t max_inbound = 0;
    std::size_t max_outbound = 0;
    std::size_t max_buffered = 0;
    bool local_half = false;
    bool remote_half = false;
    bool ended = false;
    status_code result = status_code::ok;
    inbound_message assembly{};
    std::size_t assembly_total = 0;   // Encoded bytes expected.
    std::size_t assembly_decoded = 0; // Declared decoded size.
    std::uint8_t assembly_algorithm = 0;
    bool assembling = false;
    std::vector<inbound_message> inbox; // Ring; grows by doubling up to max_buffered.
    std::size_t inbox_head = 0;
    std::size_t inbox_count = 0;
    inbound_message current{}; // Lent to the reader until its next read.
    stream_waiter reader;
    stream_waiter writer;
    stream_waiter finisher;
};

struct stream_access {
    static read_operation read(stream_core *core) noexcept { return read_operation{core}; }
    static write_operation write(stream_core *core, wire::bytes_view message, bool half) noexcept {
        return write_operation{core, message, half};
    }
    static finish_operation finish(client_stream_core *core, response_trailer *trailer) noexcept {
        return finish_operation{core, trailer};
    }
    static client_stream make_client_stream(client_stream_core *core) noexcept { return client_stream{core}; }
    static void bind(server_stream &stream, server_stream_call *call) noexcept { stream.call_ = call; }
    static server_stream_call *call(server_stream const &stream) noexcept { return stream.call_; }
};

} // namespace detail
} // namespace v2
} // namespace rpc
