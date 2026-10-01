#include <rpc/wire.hpp>

#include <array>
#include <cstdlib>

extern "C" int LLVMFuzzerTestOneInput(std::uint8_t const *data, std::size_t size) {
    namespace w = rpc::wire;
    w::bytes_view const input{data, size};
    auto const integer = w::decode_varint(input);
    if (integer.code == w::error::none) {
        std::array<std::uint8_t, 10> encoded{};
        auto const written = w::encode_varint(integer.value, {encoded.data(), encoded.size()});
        if (written.code != w::error::none || written.written != integer.consumed) std::abort();
        for (std::size_t i = 0; i < written.written; ++i)
            if (encoded[i] != data[i]) std::abort();
    }
    auto const frame = w::decode_frame(input);
    if (frame.code == w::error::none) {
        if (frame.consumed > size || frame.consumed != w::header_size + frame.value.header.length)
            std::abort();
        std::array<std::uint8_t, 16> encoded{};
        auto const written = w::encode_header(frame.value.header, {encoded.data(), encoded.size()});
        if (written.code != w::error::none) std::abort();
        for (std::size_t i = 0; i < written.written; ++i)
            if (encoded[i] != data[i]) std::abort();
    } else if (frame.consumed != 0) std::abort();
    w::limits bounds{4U * 1024U * 1024U, 64U * 1024U * 1024U,
        w::streaming | w::message_compression | w::explicit_rejection | w::method_codecs};
    auto const extended = w::decode_frame(input, bounds);
    if (extended.code == w::error::none) {
        std::array<std::uint8_t, 16> encoded{};
        if (extended.consumed > size || w::encode_header(extended.value.header, {encoded.data(), encoded.size()}, bounds).code != w::error::none) std::abort();
    } else if (extended.consumed != 0) std::abort();
    w::decode_message_descriptor(input, bounds);
    w::decode_preface(input);
    w::decode_settings(input);
    w::decode_request_head(input, false);
    w::decode_request_head(input, true);
    auto const labelled = w::decode_request_head(input, true, true);
    if (labelled.code == w::error::none && !w::valid_codec(labelled.value.codec)) std::abort();
    w::decode_end_head(input);
    w::decode_metadata_entry(input);
    return 0;
}
