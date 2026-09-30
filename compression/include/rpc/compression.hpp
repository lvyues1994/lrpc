#pragma once
#include <rpc/wire.hpp>
#include <memory>
namespace rpc {
enum class compression_algorithm : std::uint8_t { none = 0, zstd = 1, lz4 = 2 };
std::uint32_t compression_algorithms() noexcept; // SETTINGS bitmap.
struct message_compressor {
    // Mutable workspace: use on one shard, with no concurrent/reentrant calls.
    virtual ~message_compressor() = default;
    virtual compression_algorithm algorithm() const noexcept = 0;
    virtual std::size_t workspace_bytes() const noexcept = 0;
    virtual std::size_t bound(std::size_t size) const noexcept = 0; // Zero if unsupported size.
    virtual wire::encode_result compress(wire::bytes_view input, wire::mutable_bytes_view output) noexcept = 0;
    // One zstd frame (no concatenated/skippable frames) or one LZ4 block.
    // Output size is the independently validated declared decoded length.
    virtual wire::encode_result decompress_exact(wire::bytes_view input, wire::mutable_bytes_view output) noexcept = 0;
};
// Cold allocation of fixed aligned workspace; no per-message library allocation.
// Returns nullptr when the build does not provide the selected algorithm.
std::unique_ptr<message_compressor> make_message_compressor(compression_algorithm algorithm);
} // namespace rpc
