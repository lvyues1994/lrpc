#pragma once

#include <rpc/options.hpp>
#include <rpc/status.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace rpc {

// Borrowed by a call until it completes; typically a long-lived object.
struct call_spec {
    clock::time_point deadline = clock::time_point::max();
    std::chrono::microseconds timeout{0}; // Zero: no additional relative limit.
    wire::metadata_list metadata{};
};

struct call_result {
    status_code code = status_code::unknown;
    std::size_t size = 0; // Response bytes written into the caller's buffer.
    // The server provably did not run the call: it failed before sending,
    // or the server said so. Such a call is safe to retry.
    bool not_executed = false;
};

constexpr std::size_t encode_failed = static_cast<std::size_t>(-1);

// A request body: bytes, or a message encoded straight into the frame. The
// source is borrowed until the call completes; an encoder must not throw.
struct request_body {
    request_body() noexcept = default;
    request_body(wire::bytes_view bytes) noexcept : source(bytes.data), size(bytes.size) {}
    request_body(void const *data, std::size_t length) noexcept : source(data), size(length) {}
    // The encoder returns how many bytes it wrote, or encode_failed. An exact
    // encoder is given exactly `size` bytes. A bounded one is given at most
    // `size`, less when the frame has less room; failing then means the
    // request is too large (resource_exhausted).
    request_body(void const *message, std::size_t size,
                 std::size_t (*encoder)(void const *message, wire::mutable_bytes_view out), bool bounded) noexcept
        : source(message), size(size), encode(encoder), bounded(bounded) {}

    void const *source = nullptr;
    std::size_t size = 0;
    std::size_t (*encode)(void const *, wire::mutable_bytes_view) = nullptr;
    bool bounded = false;
};

// Where a response body goes: a buffer it is copied into, or a decoder that
// reads it in place on the shard's thread. The decoder must not throw; false
// fails the call with internal.
struct response_body {
    response_body() noexcept = default;
    response_body(wire::mutable_bytes_view bytes) noexcept : target(bytes.data), capacity(bytes.size) {}
    response_body(void *data, std::size_t length) noexcept : target(data), capacity(length) {}
    response_body(void *message, bool (*decoder)(void *message, wire::bytes_view body)) noexcept
        : target(message), decode(decoder) {}

    void *target = nullptr;
    std::size_t capacity = 0;
    bool (*decode)(void *, wire::bytes_view) = nullptr;
};

// Receives the status message and metadata of a response. The views point
// into storage; truncated means they did not fit and were dropped.
struct response_trailer {
    wire::mutable_bytes_view storage{};
    wire::bytes_view message{};
    wire::metadata_view metadata{};
    bool truncated = false;
};

// A method bound to one client or channel; bind() is the only (cold) string lookup.
struct method_ref {
    std::uint32_t index = 0; // One-based.
    explicit operator bool() const noexcept { return index != 0; }
};

enum class method_kind : std::uint8_t { unary, client_streaming, server_streaming, bidirectional };

} // namespace rpc
