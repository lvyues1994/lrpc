#pragma once

#include <rpc/wire.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace rpc {
namespace v2 {

using clock = std::chrono::steady_clock;

struct connection_options {
    // Advertised in SETTINGS. A unary message travels in exactly one frame, so
    // the frame limit leaves room for the head (deadline, method, metadata).
    // Streams are flow controlled per stream, a whole message at a time.
    wire::settings receive{(1U << 20) + (16U << 10), 1U << 20, 1024, 1U << 20, 4096, 0,
                           wire::explicit_rejection | wire::streaming};
    // Fixed read window. Frames that do not fit are read into a dedicated
    // buffer, so this bounds idle per-connection memory, not the frame size.
    std::size_t receive_buffer_bytes = 64U * 1024U;
    std::size_t max_method_name_bytes = 256;
    // Complete messages a stream may hold for its reader. The window bounds
    // bytes; this bounds count, as each message costs at least one byte.
    std::size_t max_buffered_messages = 1024;
    // Above the high watermark the reader stops taking new frames until the
    // writer drains below the low one: TCP pushes the pressure back to the peer.
    std::size_t tx_high_watermark = 8U << 20;
    std::size_t tx_low_watermark = 2U << 20;
    std::uint32_t frames_per_turn = 64;
    std::chrono::milliseconds handshake_timeout{5000};
    // Stream messages only; both ends must enable wire::message_compression
    // and share an algorithm in receive.compression (bit 0 zstd, bit 1 LZ4).
    // A message is sent compressed only when that makes it smaller.
    std::uint8_t preferred_compression = 0; // 0 none, 1 zstd, 2 LZ4.
    std::size_t compression_threshold = 1024;
};

} // namespace v2
} // namespace rpc
