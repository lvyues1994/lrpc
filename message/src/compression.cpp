#include <rpc/compression.hpp>
#include <vector>
#include <limits>
#include <stdexcept>
#if defined(LRPC_HAS_COMPRESSION)
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#include <lz4.h>
#endif

namespace rpc {
#if defined(LRPC_HAS_COMPRESSION)
namespace {
bool valid(wire::bytes_view input, wire::mutable_bytes_view output) noexcept {
    if ((input.size && !input.data) || (output.size && !output.data)) return false;
    if (!input.size || !output.size) return true;
    auto a = reinterpret_cast<std::uintptr_t>(input.data), b = reinterpret_cast<std::uintptr_t>(output.data);
    return a <= b ? b - a >= input.size : a - b >= output.size;
}
struct compressor final : message_compressor {
    explicit compressor(compression_algorithm algorithm) : selected(algorithm) {
        auto const encode_size = selected == compression_algorithm::zstd ? ZSTD_estimateCCtxSize(1) : static_cast<std::size_t>(LZ4_sizeofState());
        auto const decode_size = selected == compression_algorithm::zstd ? ZSTD_estimateDCtxSize() : std::size_t{0};
        if (ZSTD_isError(encode_size) || ZSTD_isError(decode_size)) throw std::runtime_error{"compression workspace estimate"};
        encode_space.resize((encode_size + 7) / 8); decode_space.resize((decode_size + 7) / 8);
        if (selected == compression_algorithm::zstd) {
            encoder = ZSTD_initStaticCCtx(encode_space.data(), encode_space.size() * 8);
            decoder = ZSTD_initStaticDCtx(decode_space.data(), decode_space.size() * 8);
            if (!encoder || !decoder) throw std::runtime_error{"compression workspace initialization"};
        }
    }
    compression_algorithm algorithm() const noexcept override { return selected; }
    std::size_t workspace_bytes() const noexcept override { return (encode_space.capacity() + decode_space.capacity()) * 8; }
    std::size_t bound(std::size_t size) const noexcept override {
        if (selected == compression_algorithm::zstd) { auto value = ZSTD_compressBound(size); return ZSTD_isError(value) ? 0 : value; }
        if (size > static_cast<std::size_t>(LZ4_MAX_INPUT_SIZE)) return 0;
        return static_cast<std::size_t>(LZ4_compressBound(static_cast<int>(size)));
    }
    wire::encode_result compress(wire::bytes_view input, wire::mutable_bytes_view output) noexcept override {
        if (!valid(input, output)) return {wire::error::invalid_argument, 0};
        if (!bound(input.size)) return {wire::error::message_too_large, 0};
        static std::uint8_t const empty = 0;
        auto const *source = input.size ? input.data : &empty;
        if (selected == compression_algorithm::zstd) {
            auto value = ZSTD_compressCCtx(encoder, output.data, output.size, source, input.size, 1);
            return ZSTD_isError(value) ? wire::encode_result{wire::error::output_too_small, 0} : wire::encode_result{wire::error::none, value};
        }
        if (output.size > static_cast<std::size_t>(std::numeric_limits<int>::max())) return {wire::error::invalid_argument, 0};
        auto value = LZ4_compress_fast_extState(encode_space.data(), reinterpret_cast<char const *>(source),
            reinterpret_cast<char *>(output.data), static_cast<int>(input.size), static_cast<int>(output.size), 1);
        return value <= 0 ? wire::encode_result{wire::error::output_too_small, 0} : wire::encode_result{wire::error::none, static_cast<std::size_t>(value)};
    }
    wire::encode_result decompress_exact(wire::bytes_view input, wire::mutable_bytes_view output) noexcept override {
        if (!valid(input, output)) return {wire::error::invalid_argument, 0};
        std::uint8_t empty = 0;
        auto *destination = output.size ? output.data : &empty;
        if (selected == compression_algorithm::zstd) {
            if (input.size < 4 || input.data[0] != 0x28 || input.data[1] != 0xb5 || input.data[2] != 0x2f || input.data[3] != 0xfd)
                return {wire::error::invalid_length, 0};
            auto frame_size = ZSTD_findFrameCompressedSize(input.data, input.size);
            auto content_size = ZSTD_getFrameContentSize(input.data, input.size);
            if (ZSTD_isError(frame_size) || frame_size != input.size || content_size == ZSTD_CONTENTSIZE_ERROR ||
                (content_size != ZSTD_CONTENTSIZE_UNKNOWN && content_size != output.size)) return {wire::error::invalid_length, 0};
            auto value = ZSTD_decompressDCtx(decoder, destination, output.size, input.data, input.size);
            return ZSTD_isError(value) || value != output.size ? wire::encode_result{wire::error::invalid_length, 0} : wire::encode_result{wire::error::none, value};
        }
        if (input.size > static_cast<std::size_t>(std::numeric_limits<int>::max()) || output.size > static_cast<std::size_t>(std::numeric_limits<int>::max()))
            return {wire::error::invalid_argument, 0};
        auto value = LZ4_decompress_safe(reinterpret_cast<char const *>(input.data), reinterpret_cast<char *>(destination),
            static_cast<int>(input.size), static_cast<int>(output.size));
        return value < 0 || static_cast<std::size_t>(value) != output.size ? wire::encode_result{wire::error::invalid_length, 0} : wire::encode_result{wire::error::none, static_cast<std::size_t>(value)};
    }
    compression_algorithm selected;
    std::vector<std::uint64_t> encode_space{}, decode_space{};
    ZSTD_CCtx *encoder = nullptr;
    ZSTD_DCtx *decoder = nullptr;
};
}
#endif
std::uint32_t compression_algorithms() noexcept {
#if defined(LRPC_HAS_COMPRESSION)
    return 3;
#else
    return 0;
#endif
}
std::unique_ptr<message_compressor> make_message_compressor(compression_algorithm algorithm) {
#if defined(LRPC_HAS_COMPRESSION)
    if (algorithm == compression_algorithm::zstd || algorithm == compression_algorithm::lz4) return std::make_unique<compressor>(algorithm);
#else
    static_cast<void>(algorithm);
#endif
    return {};
}
} // namespace rpc
