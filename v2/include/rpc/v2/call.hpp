#pragma once

#include <rpc/v2/options.hpp>
#include <rpc/v2/status.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace rpc {
namespace v2 {

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

} // namespace v2
} // namespace rpc
