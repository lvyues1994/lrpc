#include <rpc/wire.hpp>

#include <algorithm>
#include <cstring>
#include <limits>

namespace rpc {
namespace wire {
namespace {

constexpr std::uint8_t preface[preface_size] = {'N', 'R', 'P', 'C', 0, 1, 13, 10};
constexpr std::size_t max_head_size = 65535;

bool valid(bytes_view bytes) noexcept { return bytes.size == 0 || bytes.data != nullptr; }
bool valid(mutable_bytes_view bytes) noexcept { return bytes.size == 0 || bytes.data != nullptr; }

template <class T>
decode_result<T> failed(error code) noexcept { return {T{}, code, 0}; }

// Byte-wise access is endian independent and works on unaligned buffers.
template <class T>
T load_le(std::uint8_t const *data) noexcept {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i)
        value |= std::uint64_t{data[i]} << (8U * i);
    return static_cast<T>(value);
}

template <class T>
void store_le(T value, std::uint8_t *data) noexcept {
    for (std::size_t i = 0; i < sizeof(T); ++i)
        data[i] = static_cast<std::uint8_t>(value >> (8U * i));
}

// A bounded cursor. Failed reads do not advance and poison subsequent reads.
class reader {
public:
    explicit reader(bytes_view input) noexcept : input_(input) {
        if (!valid(input)) code_ = error::invalid_argument;
    }

    bytes_view take(std::size_t count) noexcept {
        if (code_ != error::none) return {};
        if (count > remaining()) { code_ = error::invalid_head; return {}; }
        auto const *data = input_.data == nullptr ? nullptr : input_.data + offset_;
        offset_ += count;
        return {data, count};
    }

    std::uint64_t varint() noexcept {
        if (code_ != error::none) return 0;
        auto const rest = tail();
        auto const result = decode_varint(rest);
        if (result.code != error::none) {
            code_ = result.code == error::need_more ? error::invalid_head : result.code;
            return 0;
        }
        offset_ += result.consumed;
        return result.value;
    }

    bytes_view field() noexcept {
        auto const size = varint();
        if (size > remaining()) { code_ = error::invalid_head; return {}; }
        return take(static_cast<std::size_t>(size));
    }

    bytes_view tail() const noexcept {
        auto const *data = input_.data == nullptr ? nullptr : input_.data + offset_;
        return {data, remaining()};
    }
    std::size_t remaining() const noexcept { return input_.size - offset_; }
    std::size_t consumed() const noexcept { return offset_; }
    error code() const noexcept { return code_; }
    void reject() noexcept { code_ = error::invalid_head; }

private:
    bytes_view input_{};
    std::size_t offset_ = 0;
    error code_ = error::none;
};

// Owns a write cursor, never the output storage. Head size is bounded even if
// the caller supplies a larger buffer. Partial output is discarded on failure.
class writer {
public:
    writer() noexcept : output_{nullptr, max_head_size}, counting_(true) {}
    explicit writer(mutable_bytes_view output) noexcept : output_(output) {
        if (!valid(output)) code_ = error::invalid_argument;
    }

    void put(bytes_view bytes) noexcept {
        if (code_ != error::none) return;
        if (!valid(bytes)) { code_ = error::invalid_argument; return; }
        if (bytes.size > max_head_size - offset_) { code_ = error::invalid_head; return; }
        if (bytes.size > output_.size - offset_) { code_ = error::output_too_small; return; }
        if (!counting_ && bytes.size != 0) std::memcpy(output_.data + offset_, bytes.data, bytes.size);
        offset_ += bytes.size;
    }

    void varint(std::uint64_t value) noexcept {
        std::uint8_t bytes[10]{};
        auto const result = encode_varint(value, {bytes, sizeof(bytes)});
        put({bytes, result.written});
    }
    void field(bytes_view value) noexcept { varint(value.size); put(value); }
    void reject(error code) noexcept { if (code_ == error::none) code_ = code; }
    encode_result result() const noexcept { return {code_, code_ == error::none ? offset_ : 0}; }

private:
    mutable_bytes_view output_{};
    std::size_t offset_ = 0;
    error code_ = error::none;
    bool counting_ = false;
};

metadata_view read_metadata(reader &input) noexcept {
    auto const count = input.varint();
    // Even an empty key/value pair takes two bytes. Bound work before iterating.
    if (count > input.remaining() / 2U) { input.reject(); return {}; }
    auto const start = input.tail();
    for (std::uint64_t i = 0; i < count && input.code() == error::none; ++i) {
        input.field();
        input.field();
    }
    return {{start.data, start.size - input.remaining()}, static_cast<std::size_t>(count)};
}

void write_metadata(writer &output, metadata_list values) noexcept {
    if (values.size != 0 && values.data == nullptr) {
        output.reject(error::invalid_argument);
        return;
    }
    if (values.size > max_head_size / 2U) { output.reject(error::invalid_head); return; }
    output.varint(values.size);
    for (std::size_t i = 0; i < values.size && output.result().code == error::none; ++i) {
        output.field(values.data[i].key);
        output.field(values.data[i].value);
    }
}

error validate_header(frame_header const &h, limits bounds) noexcept {
    auto const type = static_cast<std::uint8_t>(h.type);
    if (type < 1 || type > 9) return error::invalid_type;
    if ((h.flags & 0xe0U) != 0) return error::invalid_flags;
    if ((h.flags & not_executed) != 0 &&
        ((bounds.features & explicit_rejection) == 0 || h.type != frame_type::end))
        return error::invalid_flags;
    if (h.head_length > h.length) return error::invalid_length;
    if (h.length > bounds.max_frame_size) return error::frame_too_large;
    auto const connection_frame = h.type == frame_type::settings || h.type == frame_type::ping ||
                                  h.type == frame_type::pong || h.type == frame_type::goaway;
    if (connection_frame ? h.stream_id != 0 : (h.stream_id == 0 || h.stream_id > max_stream_id))
        return error::invalid_stream_id;
    if (((h.flags & more) != 0 || h.type == frame_type::message || h.type == frame_type::window_update) &&
        (bounds.features & streaming) == 0) return error::unsupported_feature;
    if ((h.flags & compressed) != 0 && (bounds.features & message_compression) == 0) return error::unsupported_feature;
    if (h.type == frame_type::request) {
        if ((h.flags & end_stream) == 0) {
            if ((bounds.features & streaming) == 0) return error::unsupported_feature;
            if (h.length != h.head_length) return error::invalid_length;
        }
        if ((h.flags & ~(end_stream | new_method)) != 0) return error::invalid_flags;
        if (h.aux == 0) return error::invalid_aux;
        if (h.head_length < 9) return error::invalid_head;
    } else if (h.type == frame_type::message) {
        if ((h.flags & ~(end_stream | compressed | more)) != 0) return error::invalid_flags;
        if (h.aux != 0) return error::invalid_aux;
        if ((h.flags & end_stream) != 0) {
            if (h.flags != end_stream || h.length != 0) return error::invalid_flags;
        } else {
            if (h.head_length != 0 && h.head_length != message_descriptor_size) return error::invalid_head;
            if (h.head_length == 0 && (h.flags & compressed) != 0) return error::invalid_flags;
            if (h.head_length == 0 && h.length == 0) return error::invalid_length;
        }
    } else if (h.flags != 0 && !(h.type == frame_type::end && h.flags == not_executed)) return error::invalid_flags;
    switch (h.type) {
    case frame_type::settings:
        if (h.aux != 0) return error::invalid_aux;
        if (h.length != h.head_length) return error::invalid_length;
        break;
    case frame_type::end:
        if (h.aux > 16) return error::invalid_aux;
        if (h.head_length < 2) return error::invalid_head;
        if (h.aux != 0 && h.length != h.head_length) return error::invalid_length;
        if (h.flags == not_executed && h.aux == 0) return error::invalid_aux;
        break;
    case frame_type::cancel:
    case frame_type::ping:
    case frame_type::pong:
        if (h.length != 0) return error::invalid_length;
        break;
    case frame_type::window_update:
        if (h.aux == 0) return error::invalid_aux;
        if (h.length != 0) return error::invalid_length;
        break;
    case frame_type::goaway:
        if (h.aux > max_stream_id) return error::invalid_aux;
        if (h.length != h.head_length) return error::invalid_length;
        break;
    default: break;
    }
    if ((h.type == frame_type::request || h.type == frame_type::end) &&
        h.length - h.head_length > bounds.max_message_size) return error::message_too_large;
    return error::none;
}

bool valid_utf8(bytes_view text) noexcept {
    std::size_t offset = 0;
    while (offset < text.size) {
        auto const first = text.data[offset++];
        if (first < 0x80U) continue;
        unsigned count = 0;
        std::uint32_t point = 0;
        std::uint32_t minimum = 0;
        if (first >= 0xc2U && first <= 0xdfU) { count = 1; point = first & 0x1fU; minimum = 0x80; }
        else if (first >= 0xe0U && first <= 0xefU) { count = 2; point = first & 0x0fU; minimum = 0x800; }
        else if (first >= 0xf0U && first <= 0xf4U) { count = 3; point = first & 0x07U; minimum = 0x10000; }
        else return false;
        if (count > text.size - offset) return false;
        for (unsigned i = 0; i < count; ++i) {
            auto const next = text.data[offset++];
            if ((next & 0xc0U) != 0x80U) return false;
            point = (point << 6U) | (next & 0x3fU);
        }
        if (point < minimum || point > 0x10ffffU || (point >= 0xd800U && point <= 0xdfffU)) return false;
    }
    return true;
}

error validate_head(frame_view const &frame, limits bounds) noexcept {
    switch (frame.header.type) {
    case frame_type::settings: return decode_settings(frame.head).code;
    case frame_type::request:
        return decode_request_head(frame.head, (frame.header.flags & new_method) != 0,
                                   (bounds.features & method_codecs) != 0).code;
    case frame_type::end: {
        auto const result = decode_end_head(frame.head);
        if (result.code != error::none) return result.code;
        return frame.header.aux == 0 && result.value.message.size != 0 ? error::invalid_head : error::none;
    }
    case frame_type::message:
        if (frame.head.size != 0) {
            auto descriptor = decode_message_descriptor(frame.head, bounds);
            if (descriptor.code != error::none) return descriptor.code;
            if (((frame.header.flags & compressed) != 0) != (descriptor.value.algorithm != 0)) return error::invalid_flags;
            if (frame.body.size > descriptor.value.encoded_size ||
                ((frame.header.flags & more) != 0) != (frame.body.size < descriptor.value.encoded_size)) return error::invalid_length;
        }
        return error::none;
    case frame_type::goaway: return valid_utf8(frame.head) ? error::none : error::invalid_head;
    default: return error::none;
    }
}

} // namespace

decode_result<std::uint64_t> decode_varint(bytes_view input) noexcept {
    if (!valid(input)) return failed<std::uint64_t>(error::invalid_argument);
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < 10; ++i) {
        if (i == input.size) return failed<std::uint64_t>(error::need_more);
        auto const byte = input.data[i];
        if (i == 9 && byte > 1) return failed<std::uint64_t>(error::invalid_varint);
        value |= std::uint64_t{static_cast<std::uint8_t>(byte & 0x7fU)} << (7U * i);
        if ((byte & 0x80U) == 0) {
            if (i != 0 && byte == 0) return failed<std::uint64_t>(error::invalid_varint);
            return {value, error::none, i + 1};
        }
    }
    return failed<std::uint64_t>(error::invalid_varint);
}

encode_result encode_varint(std::uint64_t value, mutable_bytes_view output) noexcept {
    if (!valid(output)) return {error::invalid_argument, 0};
    std::size_t used = 0;
    do {
        if (used == output.size) return {error::output_too_small, 0};
        auto byte = static_cast<std::uint8_t>(value & 0x7fU);
        value >>= 7U;
        if (value != 0) byte = static_cast<std::uint8_t>(byte | 0x80U);
        output.data[used++] = byte;
    } while (value != 0);
    return {error::none, used};
}

encode_result encode_preface(mutable_bytes_view output) noexcept {
    if (!valid(output)) return {error::invalid_argument, 0};
    if (output.size < preface_size) return {error::output_too_small, 0};
    std::memcpy(output.data, preface, preface_size);
    return {error::none, preface_size};
}

decode_result<bool> decode_preface(bytes_view input) noexcept {
    if (!valid(input)) return failed<bool>(error::invalid_argument);
    auto const count = std::min(input.size, preface_size);
    if (count != 0 && std::memcmp(input.data, preface, count) != 0)
        return failed<bool>(error::invalid_preface);
    if (input.size < preface_size) return failed<bool>(error::need_more);
    return {true, error::none, preface_size};
}

decode_result<frame_header> decode_header(bytes_view input, limits bounds) noexcept {
    if (!valid(input)) return failed<frame_header>(error::invalid_argument);
    if (input.size < header_size) return failed<frame_header>(error::need_more);
    frame_header h{};
    h.length = load_le<std::uint32_t>(input.data);
    h.stream_id = load_le<std::uint32_t>(input.data + 4);
    h.type = static_cast<frame_type>(input.data[8]);
    h.flags = input.data[9];
    h.head_length = load_le<std::uint16_t>(input.data + 10);
    h.aux = load_le<std::uint32_t>(input.data + 12);
    auto const code = validate_header(h, bounds);
    if (code != error::none) return failed<frame_header>(code);
    return {h, error::none, header_size};
}

encode_result encode_header(frame_header const &h, mutable_bytes_view output, limits bounds) noexcept {
    if (!valid(output)) return {error::invalid_argument, 0};
    auto const code = validate_header(h, bounds);
    if (code != error::none) return {code, 0};
    if (output.size < header_size) return {error::output_too_small, 0};
    store_le(h.length, output.data);
    store_le(h.stream_id, output.data + 4);
    output.data[8] = static_cast<std::uint8_t>(h.type);
    output.data[9] = h.flags;
    store_le(h.head_length, output.data + 10);
    store_le(h.aux, output.data + 12);
    return {error::none, header_size};
}

decode_result<frame_view> decode_frame(bytes_view input, limits bounds) noexcept {
    auto const result = decode_header(input, bounds);
    if (result.code != error::none) return failed<frame_view>(result.code);
    auto const h = result.value;
    // Subtraction avoids 16 + length overflowing size_t on 32-bit hosts.
    if (h.length > input.size - header_size) return failed<frame_view>(error::need_more);
    frame_view frame{h, {input.data + header_size, h.head_length},
                     {input.data + header_size + h.head_length, h.length - h.head_length}};
    auto const code = validate_head(frame, bounds);
    if (code != error::none) return failed<frame_view>(code);
    return {frame, error::none, header_size + static_cast<std::size_t>(h.length)};
}
decode_result<message_descriptor> decode_message_descriptor(bytes_view input, limits bounds) noexcept {
    if (!valid(input)) return failed<message_descriptor>(error::invalid_argument);
    if (input.size != message_descriptor_size) return failed<message_descriptor>(error::invalid_head);
    message_descriptor value{load_le<std::uint32_t>(input.data), load_le<std::uint32_t>(input.data + 4), input.data[8]};
    if (value.algorithm > 2 || (value.algorithm == 0 && value.encoded_size != value.decoded_size) ||
        (value.algorithm != 0 && value.encoded_size == 0)) return failed<message_descriptor>(error::invalid_head);
    if (value.algorithm != 0 && (bounds.features & message_compression) == 0) return failed<message_descriptor>(error::unsupported_feature);
    if (value.encoded_size > bounds.max_message_size || value.decoded_size > bounds.max_message_size) return failed<message_descriptor>(error::message_too_large);
    return {value, error::none, message_descriptor_size};
}
encode_result encode_message_descriptor(message_descriptor const &value, mutable_bytes_view output, limits bounds) noexcept {
    std::uint8_t temporary[message_descriptor_size]{};
    store_le(value.encoded_size, temporary); store_le(value.decoded_size, temporary + 4); temporary[8] = value.algorithm;
    auto checked = decode_message_descriptor({temporary, message_descriptor_size}, bounds);
    if (checked.code != error::none) return {checked.code, 0};
    if (!valid(output)) return {error::invalid_argument, 0};
    if (output.size < message_descriptor_size) return {error::output_too_small, 0};
    std::memcpy(output.data, temporary, message_descriptor_size); return {error::none, message_descriptor_size};
}

decode_result<settings> decode_settings(bytes_view input) noexcept {
    if (!valid(input)) return failed<settings>(error::invalid_argument);
    if (input.size > max_head_size) return failed<settings>(error::invalid_settings);
    reader r{input};
    settings value{};
    std::uint8_t seen = 0;
    while (r.remaining() != 0 && r.code() == error::none) {
        auto const key = r.varint();
        auto const number = r.varint();
        if (r.code() != error::none) break;
        if (key < 1 || key > 7) continue;
        auto const bit = static_cast<std::uint8_t>(1U << (key - 1));
        if ((seen & bit) != 0 || number > std::numeric_limits<std::uint32_t>::max())
            return failed<settings>(error::invalid_settings);
        seen = static_cast<std::uint8_t>(seen | bit);
        auto const narrowed = static_cast<std::uint32_t>(number);
        switch (key) {
        case 1: value.max_frame_size = narrowed; break;
        case 2: value.max_message_size = narrowed; break;
        case 3: value.max_concurrent_streams = narrowed; break;
        case 4: value.initial_stream_window = narrowed; break;
        case 5: value.max_method_ids = narrowed; break;
        case 6: value.compression = narrowed; break;
        case 7: value.features = narrowed; break;
        default: break;
        }
    }
    if (r.code() != error::none) return failed<settings>(error::invalid_settings);
    return {value, error::none, input.size};
}

encode_result encode_settings(settings const &value, mutable_bytes_view output) noexcept {
    writer w{output};
    std::uint32_t const fields[] = {value.max_frame_size, value.max_message_size,
        value.max_concurrent_streams, value.initial_stream_window, value.max_method_ids, value.compression};
    for (std::size_t i = 0; i < 6; ++i) { w.varint(i + 1); w.varint(fields[i]); }
    if (value.features != 0) { w.varint(7); w.varint(value.features); }
    return w.result();
}

bool valid_codec(bytes_view const label) noexcept {
    if (!valid(label) || label.size > max_codec_size) return false;
    for (std::size_t i = 0; i < label.size; ++i)
        if (label.data[i] < 0x21U || label.data[i] > 0x7eU) return false;
    return true;
}

decode_result<request_head_view> decode_request_head(bytes_view input, bool has_new_method, bool has_codec) noexcept {
    if (!valid(input)) return failed<request_head_view>(error::invalid_argument);
    if (input.size < 9 || input.size > max_head_size) return failed<request_head_view>(error::invalid_head);
    reader r{input};
    request_head_view value{};
    value.timeout_us = load_le<std::uint64_t>(r.take(8).data);
    if (has_new_method) {
        value.method_name = r.field();
        if (value.method_name.size == 0) r.reject();
        if (has_codec) {
            value.codec = r.field();
            if (r.code() == error::none && !valid_codec(value.codec)) r.reject();
        }
    }
    value.metadata = read_metadata(r);
    if (r.code() != error::none) return failed<request_head_view>(r.code());
    if (r.remaining() != 0) return failed<request_head_view>(error::invalid_head);
    return {value, error::none, input.size};
}

namespace {
// The fields after the timeout; a label needs NEW_METHOD under method_codecs.
void write_request_fields(writer &w, request_head const &value, bool has_new_method, bool has_codec) noexcept {
    if (has_new_method == (value.method_name.size == 0) ||
        (value.codec.size != 0 && !(has_new_method && has_codec)) || !valid_codec(value.codec)) {
        w.reject(error::invalid_head);
        return;
    }
    if (has_new_method) w.field(value.method_name);
    if (has_new_method && has_codec) w.field(value.codec);
    write_metadata(w, value.metadata);
}
} // namespace

encode_result encode_request_head(request_head const &value, bool has_new_method, mutable_bytes_view output,
                                  bool has_codec) noexcept {
    writer w{output};
    std::uint8_t timeout[8]{};
    store_le(value.timeout_us, timeout);
    w.put({timeout, sizeof(timeout)});
    write_request_fields(w, value, has_new_method, has_codec);
    return w.result();
}
encode_result request_head_size(request_head const &value, bool has_new_method, bool has_codec) noexcept {
    writer w;
    std::uint8_t timeout[8]{}; w.put({timeout, sizeof(timeout)});
    write_request_fields(w, value, has_new_method, has_codec);
    return w.result();
}

decode_result<end_head_view> decode_end_head(bytes_view input) noexcept {
    if (!valid(input)) return failed<end_head_view>(error::invalid_argument);
    if (input.size > max_head_size) return failed<end_head_view>(error::invalid_head);
    reader r{input};
    end_head_view value{};
    value.message = r.field();
    value.metadata = read_metadata(r);
    if (r.code() != error::none) return failed<end_head_view>(r.code());
    if (r.remaining() != 0) return failed<end_head_view>(error::invalid_head);
    return {value, error::none, input.size};
}

encode_result encode_end_head(end_head const &value, mutable_bytes_view output) noexcept {
    writer w{output};
    w.field(value.message);
    write_metadata(w, value.metadata);
    return w.result();
}
encode_result encode_metadata(metadata_list value, mutable_bytes_view output) noexcept {
    writer w{output};
    write_metadata(w, value);
    return w.result();
}

decode_result<metadata_entry> decode_metadata_entry(bytes_view input) noexcept {
    if (!valid(input)) return failed<metadata_entry>(error::invalid_argument);
    if (input.size > max_head_size) return failed<metadata_entry>(error::invalid_head);
    reader r{input};
    metadata_entry value{};
    value.key = r.field();
    value.value = r.field();
    if (r.code() != error::none) return failed<metadata_entry>(r.code());
    return {value, error::none, r.consumed()};
}

} // namespace wire
} // namespace rpc
