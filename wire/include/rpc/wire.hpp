#pragma once

#include <cstddef>
#include <cstdint>

namespace rpc {
namespace wire {

constexpr std::size_t header_size = 16;
constexpr std::size_t preface_size = 8;
constexpr std::size_t message_descriptor_size = 9;
constexpr std::uint32_t max_stream_id = 0x7fffffffU;
constexpr std::uint8_t end_stream = 0x01;
constexpr std::uint8_t compressed = 0x02;
constexpr std::uint8_t more = 0x04;
constexpr std::uint8_t new_method = 0x08;
constexpr std::uint8_t not_executed = 0x10;
constexpr std::uint32_t explicit_rejection = 0x01;
constexpr std::uint32_t streaming = 0x02;
constexpr std::uint32_t message_compression = 0x04;

// Borrowed storage. A nonzero size requires that many accessible bytes.
// Decoded views remain valid only while the input is alive and unchanged.
struct bytes_view {
    std::uint8_t const *data = nullptr;
    std::size_t size = 0;
};

struct mutable_bytes_view {
    std::uint8_t *data = nullptr;
    std::size_t size = 0;
};

enum class error {
    none,
    need_more,
    output_too_small,
    invalid_argument,
    invalid_preface,
    invalid_type,
    invalid_flags,
    invalid_stream_id,
    invalid_length,
    frame_too_large,
    message_too_large,
    invalid_aux,
    invalid_varint,
    invalid_head,
    invalid_settings,
    unsupported_feature,
};

// On failure, consumed/written is zero and value/output must be discarded.
// Encoding requires all source views, metadata descriptors and parameter
// objects to be disjoint from output. In-place/overlapping encoding is invalid.
template <class T>
struct decode_result {
    T value{};
    error code = error::none;
    std::size_t consumed = 0;
};

struct encode_result {
    error code = error::none;
    std::size_t written = 0;
};

enum class frame_type : std::uint8_t {
    settings = 1,
    request = 2,
    message = 3,
    end = 4,
    cancel = 5,
    window_update = 6,
    ping = 7,
    pong = 8,
    goaway = 9,
};

struct frame_header {
    std::uint32_t length = 0;
    std::uint32_t stream_id = 0;
    frame_type type = frame_type::settings;
    std::uint8_t flags = 0;
    std::uint16_t head_length = 0;
    std::uint32_t aux = 0;
};

struct limits {
    std::uint32_t max_frame_size = 4U * 1024U * 1024U;
    std::uint32_t max_message_size = 64U * 1024U * 1024U;
    std::uint32_t features = 0; // Negotiated profile; zero retains the v1 unary grammar.
};

struct frame_view {
    frame_header header{};
    bytes_view head{};
    bytes_view body{};
};
// First MESSAGE fragment only. Lengths describe the entire message. Algorithm
// 0 is identity, 1 is a single zstd frame, 2 is an LZ4 block.
struct message_descriptor {
    std::uint32_t encoded_size = 0;
    std::uint32_t decoded_size = 0;
    std::uint8_t algorithm = 0;
};

struct settings {
    std::uint32_t max_frame_size = 4U * 1024U * 1024U;
    std::uint32_t max_message_size = 64U * 1024U * 1024U;
    std::uint32_t max_concurrent_streams = 1024;
    std::uint32_t initial_stream_window = 1024U * 1024U;
    std::uint32_t max_method_ids = 4096;
    std::uint32_t compression = 0;
    std::uint32_t features = 0; // SETTINGS key 7; missing means the original unary profile.
};

struct metadata_entry {
    bytes_view key{};
    bytes_view value{};
};

struct metadata_list {
    metadata_entry const *data = nullptr;
    std::size_t size = 0;
};

// Entries excludes the count prefix; use decode_metadata_entry to iterate.
struct metadata_view {
    bytes_view entries{};
    std::size_t count = 0;
};

struct request_head {
    std::uint64_t timeout_us = 0;
    bytes_view method_name{};
    metadata_list metadata{};
};

struct request_head_view {
    std::uint64_t timeout_us = 0;
    bytes_view method_name{};
    metadata_view metadata{};
};

struct end_head {
    bytes_view message{};
    metadata_list metadata{};
};

struct end_head_view {
    bytes_view message{};
    metadata_view metadata{};
};

// Only the first object is consumed, permitting concatenated input.
decode_result<std::uint64_t> decode_varint(bytes_view input) noexcept;
encode_result encode_varint(std::uint64_t value, mutable_bytes_view output) noexcept;
encode_result encode_preface(mutable_bytes_view output) noexcept;
decode_result<bool> decode_preface(bytes_view input) noexcept;

// The default remains the original unary grammar. Extended MESSAGE/flow-control
// grammar requires negotiated feature bits. Stateful ordering/reassembly and
// algorithm negotiation belong above wire.
decode_result<frame_header> decode_header(bytes_view input, limits bounds = {}) noexcept;
encode_result encode_header(frame_header const &header, mutable_bytes_view output,
                            limits bounds = {}) noexcept;
// Validates complete head grammar too; no allocation, I/O or body copies.
decode_result<frame_view> decode_frame(bytes_view input, limits bounds = {}) noexcept;
decode_result<message_descriptor> decode_message_descriptor(bytes_view input, limits bounds = {}) noexcept;
encode_result encode_message_descriptor(message_descriptor const &, mutable_bytes_view output, limits bounds = {}) noexcept;

// SETTINGS: omitted fields use defaults; unknown keys (including duplicates)
// are ignored; repeated known keys are rejected. Known values are uint32_t,
// including zero. Connection setup separately checks configuration feasibility.
// These receive one complete head. Truncation is invalid_head/invalid_settings,
// not need_more: callers must never extend a head into body or the next frame.
decode_result<settings> decode_settings(bytes_view input) noexcept;
encode_result encode_settings(settings const &value, mutable_bytes_view output) noexcept;
decode_result<request_head_view> decode_request_head(bytes_view input, bool has_new_method) noexcept;
encode_result encode_request_head(request_head const &value, bool has_new_method,
                                  mutable_bytes_view output) noexcept;
encode_result request_head_size(request_head const &value, bool has_new_method) noexcept;
decode_result<end_head_view> decode_end_head(bytes_view input) noexcept;
encode_result encode_end_head(end_head const &value, mutable_bytes_view output) noexcept;
encode_result encode_metadata(metadata_list value, mutable_bytes_view output) noexcept;
decode_result<metadata_entry> decode_metadata_entry(bytes_view input) noexcept;

} // namespace wire
} // namespace rpc
