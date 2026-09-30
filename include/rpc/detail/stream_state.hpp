#pragma once

#include <rpc/status.hpp>
#include <rpc/wire.hpp>

#include <cstdint>

namespace rpc {
namespace detail {

// Intrusive node of the shard's deadline wheel.
struct wheel_node {
    wheel_node *prev = nullptr;
    wheel_node *next = nullptr;
    std::uint64_t tick = 0;
    bool scheduled() const noexcept { return next != nullptr; }
};

struct stream_state;

// One static table per kind of stream: the reader dispatches without type tags.
// Each callback runs on the shard's thread and must unregister the stream.
struct stream_ops {
    bool (*on_frame)(stream_state &, wire::frame_view const &) noexcept; // False: protocol error.
    // Connection closed or going away; not_executed when the peer proved it
    // never started the stream.
    void (*on_abort)(stream_state &, status_code, bool not_executed) noexcept;
    void (*on_deadline)(stream_state &) noexcept;
    void (*on_cancel)(stream_state &) noexcept; // Local stop request.
};

// Common prefix of everything registered in a connection's stream table.
struct stream_state : wheel_node {
    stream_ops const *ops = nullptr;
    std::uint32_t id = 0;
};

} // namespace detail
} // namespace rpc
