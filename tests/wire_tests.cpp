#include "check.hpp"
#include <rpc/wire.hpp>

#include <algorithm>
#include <array>
#include <cstring>
#include <iostream>
#include <limits>
#include <random>
#include <vector>

namespace w = rpc::wire;
namespace {

template <std::size_t N>
w::bytes_view bytes(std::array<std::uint8_t, N> const &value) { return {value.data(), value.size()}; }

template <std::size_t N>
w::mutable_bytes_view writable(std::array<std::uint8_t, N> &value) { return {value.data(), value.size()}; }

template <std::size_t N>
w::bytes_view literal(char const (&value)[N]) {
    return {reinterpret_cast<std::uint8_t const *>(value), N - 1};
}

bool same(w::bytes_view left, w::bytes_view right) {
    return left.size == right.size && (left.size == 0 || std::memcmp(left.data, right.data, left.size) == 0);
}

std::array<std::uint8_t, 28> const request_golden{{
    12, 0, 0, 0, 1, 0, 0, 0, 2, 1, 9, 0, 7, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 'A', 'B', 'C'}};

void preface_contract() {
    std::array<std::uint8_t, 9> encoded{};
    std::array<std::uint8_t, 8> const golden{{'N', 'R', 'P', 'C', 0, 1, 13, 10}};
    auto result = w::encode_preface(writable(encoded));
    CHECK(result.code == w::error::none && result.written == 8);
    CHECK(same({encoded.data(), 8}, bytes(golden)));
    for (std::size_t size = 0; size < 8; ++size) {
        CHECK(w::decode_preface({encoded.data(), size}).code == w::error::need_more);
        CHECK(w::encode_preface({encoded.data(), size}).code == w::error::output_too_small);
    }
    auto decoded = w::decode_preface(bytes(encoded));
    CHECK(decoded.code == w::error::none && decoded.consumed == 8);
    encoded[5] = 2;
    CHECK(w::decode_preface(bytes(encoded)).code == w::error::invalid_preface);
}

void varint_contract() {
    struct example { std::uint64_t value; std::vector<std::uint8_t> encoded; };
    std::vector<example> const examples{
        {0, {0}}, {127, {127}}, {128, {128, 1}}, {16383, {255, 127}}, {16384, {128, 128, 1}},
        {std::numeric_limits<std::uint64_t>::max(), {255, 255, 255, 255, 255, 255, 255, 255, 255, 1}}};
    std::array<std::uint8_t, 12> buffer{};
    for (auto const &example : examples) {
        auto const encoded = w::encode_varint(example.value, writable(buffer));
        CHECK(encoded.code == w::error::none);
        CHECK(same({buffer.data(), encoded.written}, {example.encoded.data(), example.encoded.size()}));
        auto const decoded = w::decode_varint(bytes(buffer));
        CHECK(decoded.code == w::error::none && decoded.value == example.value);
        CHECK(decoded.consumed == encoded.written);
        for (std::size_t size = 0; size < encoded.written; ++size) {
            CHECK(w::decode_varint({buffer.data(), size}).code == w::error::need_more);
            CHECK(w::encode_varint(example.value, {buffer.data(), size}).code == w::error::output_too_small);
        }
    }
    std::vector<std::vector<std::uint8_t>> const malformed{
        {128, 0}, {129, 0}, {128, 128, 0},
        {255, 255, 255, 255, 255, 255, 255, 255, 255, 2},
        {128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 1},
        {128, 128, 128, 128, 128, 128, 128, 128, 128, 0}};
    for (auto const &data : malformed) {
        auto const result = w::decode_varint({data.data(), data.size()});
        CHECK(result.code == w::error::invalid_varint && result.consumed == 0);
    }
}

void frame_golden_and_prefixes() {
    auto const decoded = w::decode_frame(bytes(request_golden));
    CHECK(decoded.code == w::error::none && decoded.consumed == request_golden.size());
    CHECK(decoded.value.header.stream_id == 1 && decoded.value.header.aux == 7);
    CHECK(decoded.value.head.data == request_golden.data() + 16);
    CHECK(same(decoded.value.body, literal("ABC")));
    std::array<std::uint8_t, 16> encoded{};
    CHECK(w::encode_header(decoded.value.header, writable(encoded)).code == w::error::none);
    CHECK(same(bytes(encoded), {request_golden.data(), 16}));
    for (std::size_t size = 0; size < request_golden.size(); ++size) {
        auto const truncated = w::decode_frame({request_golden.data(), size});
        CHECK(truncated.code == w::error::need_more && truncated.consumed == 0);
    }
    // A misaligned frame followed by another frame: consume exactly the first.
    std::vector<std::uint8_t> joined(1, 0xff);
    joined.insert(joined.end(), request_golden.begin(), request_golden.end());
    joined.insert(joined.end(), request_golden.begin(), request_golden.end());
    auto const first = w::decode_frame({joined.data() + 1, joined.size() - 1});
    CHECK(first.code == w::error::none && first.consumed == request_golden.size());
    auto const second = w::decode_frame({joined.data() + 1 + first.consumed, request_golden.size()});
    CHECK(second.code == w::error::none && same(second.value.body, literal("ABC")));
}

void header_rejections() {
    auto const base = w::decode_header(bytes(request_golden)).value;
    std::array<std::uint8_t, 16> out{};
    auto h = base;
    h.head_length = 13;
    CHECK(w::encode_header(h, writable(out)).code == w::error::invalid_length);
    CHECK(w::decode_frame(bytes(request_golden), {11, 3}).code == w::error::frame_too_large);
    CHECK(w::decode_frame(bytes(request_golden), {12, 2}).code == w::error::message_too_large);
    CHECK(w::decode_frame(bytes(request_golden), {12, 3}).code == w::error::none);
    h = base; h.stream_id = 0;
    CHECK(w::encode_header(h, writable(out)).code == w::error::invalid_stream_id);
    h.stream_id = 0x80000000U;
    CHECK(w::encode_header(h, writable(out)).code == w::error::invalid_stream_id);
    h = base; h.aux = 0;
    CHECK(w::encode_header(h, writable(out)).code == w::error::invalid_aux);
    h = base; h.head_length = 8;
    CHECK(w::encode_header(h, writable(out)).code == w::error::invalid_head);
    h = base; h.type = static_cast<w::frame_type>(255);
    CHECK(w::encode_header(h, writable(out)).code == w::error::invalid_type);
    for (unsigned flags = 0; flags < 256; ++flags) {
        h = base; h.flags = static_cast<std::uint8_t>(flags);
        auto const result = w::encode_header(h, writable(out));
        CHECK((result.code == w::error::none) == (flags == 1 || flags == 9));
    }
    h = base; h.length = std::numeric_limits<std::uint32_t>::max();
    CHECK(w::encode_header(h, writable(out), {h.length, h.length}).code == w::error::none);
    CHECK(w::decode_frame(bytes(out), {h.length, h.length}).code == w::error::need_more);
    auto damaged = request_golden; damaged[10] = 8;
    CHECK(w::decode_frame(bytes(damaged)).code == w::error::invalid_head);
}

void control_frames() {
    std::array<std::uint8_t, 16> buffer{};
    for (auto type : {w::frame_type::settings, w::frame_type::ping, w::frame_type::pong, w::frame_type::goaway}) {
        w::frame_header h{}; h.type = type;
        CHECK(w::encode_header(h, writable(buffer)).code == w::error::none);
        CHECK(w::decode_frame(bytes(buffer)).code == w::error::none);
        h.stream_id = 1;
        CHECK(w::encode_header(h, writable(buffer)).code == w::error::invalid_stream_id);
        h.stream_id = 0; h.flags = w::new_method;
        CHECK(w::encode_header(h, writable(buffer)).code == w::error::invalid_flags);
    }
    w::frame_header h{}; h.type = w::frame_type::cancel; h.stream_id = 2; h.aux = 0xffffffffU;
    CHECK(w::encode_header(h, writable(buffer)).code == w::error::none);
    CHECK(w::decode_frame(bytes(buffer)).value.header.aux == 0xffffffffU);
    h.length = 1;
    CHECK(w::encode_header(h, writable(buffer)).code == w::error::invalid_length);
    h.length = 0;
    for (auto type : {w::frame_type::message, w::frame_type::window_update}) {
        h.type = type;
        CHECK(w::encode_header(h, writable(buffer)).code == w::error::unsupported_feature);
    }
    h = {}; h.aux = 1;
    CHECK(w::encode_header(h, writable(buffer)).code == w::error::invalid_aux);
    h.aux = 0; h.length = 1;
    CHECK(w::encode_header(h, writable(buffer)).code == w::error::invalid_length);
    h = {}; h.type = w::frame_type::goaway; h.aux = 0x80000000U;
    CHECK(w::encode_header(h, writable(buffer)).code == w::error::invalid_aux);
}

void settings_contract() {
    auto const defaults = w::decode_settings({});
    CHECK(defaults.code == w::error::none && defaults.value.max_frame_size == 4194304);
    std::array<std::uint8_t, 64> buffer{};
    auto const written = w::encode_settings({}, writable(buffer));
    std::array<std::uint8_t, 22> const golden{{
        1, 128, 128, 128, 2, 2, 128, 128, 128, 32, 3, 128, 8,
        4, 128, 128, 64, 5, 128, 32, 6, 0}};
    CHECK(written.code == w::error::none && same({buffer.data(), written.written}, bytes(golden)));
    auto const decoded = w::decode_settings(bytes(golden));
    CHECK(decoded.code == w::error::none && decoded.value.max_method_ids == 4096);
    CHECK(decoded.value.initial_stream_window == 1048576 && decoded.value.compression == 0);
    std::array<std::uint8_t, 8> const unordered{{6, 0, 99, 1, 3, 0, 2, 0}};
    auto const zero = w::decode_settings(bytes(unordered));
    CHECK(zero.code == w::error::none && zero.value.max_concurrent_streams == 0);
    CHECK(zero.value.max_message_size == 0 && zero.value.max_frame_size == 4194304);
    std::vector<std::vector<std::uint8_t>> const invalid{
        {1}, {1, 128}, {1, 1, 1, 1}, {1, 128, 128, 128, 128, 16}, {1, 128, 0}};
    for (auto const &data : invalid)
        CHECK(w::decode_settings({data.data(), data.size()}).code == w::error::invalid_settings);
    for (std::size_t size = 0; size < golden.size(); ++size)
        CHECK(w::encode_settings({}, {buffer.data(), size}).code == w::error::output_too_small);
}

void goaway_utf8() {
    std::vector<std::vector<std::uint8_t>> const valid{
        {}, {'o', 'k'}, {0xc2, 0x80}, {0xe0, 0xa0, 0x80}, {0xed, 0x9f, 0xbf},
        {0xf0, 0x90, 0x80, 0x80}, {0xf4, 0x8f, 0xbf, 0xbf}};
    std::vector<std::vector<std::uint8_t>> const invalid{
        {0xff}, {0x80}, {0xc0, 0x80}, {0xc2}, {0xc2, 0x7f}, {0xe0, 0x9f, 0xbf},
        {0xed, 0xa0, 0x80}, {0xf0, 0x8f, 0xbf, 0xbf}, {0xf4, 0x90, 0x80, 0x80},
        {0xf5, 0x80, 0x80, 0x80}, {0xf0, 0x90, 0x80}};
    auto check = [](std::vector<std::uint8_t> const &text, w::error expected) {
        std::array<std::uint8_t, 32> buffer{};
        w::frame_header h{}; h.type = w::frame_type::goaway;
        h.length = static_cast<std::uint32_t>(text.size());
        h.head_length = static_cast<std::uint16_t>(text.size());
        CHECK(w::encode_header(h, writable(buffer)).code == w::error::none);
        std::copy(text.begin(), text.end(), buffer.begin() + 16);
        CHECK(w::decode_frame({buffer.data(), 16 + text.size()}).code == expected);
    };
    for (auto const &text : valid) check(text, w::error::none);
    for (auto const &text : invalid) check(text, w::error::invalid_head);
}

void request_head_contract() {
    w::metadata_entry const entries[] = {{literal("k"), literal("v")}};
    w::request_head const request{0x0102030405060708ULL, literal("svc"), {entries, 1}};
    std::array<std::uint8_t, 64> buffer{};
    auto const encoded = w::encode_request_head(request, true, writable(buffer));
    std::array<std::uint8_t, 17> const golden{{8, 7, 6, 5, 4, 3, 2, 1, 3, 's', 'v', 'c', 1, 1, 'k', 1, 'v'}};
    CHECK(encoded.code == w::error::none && same({buffer.data(), encoded.written}, bytes(golden)));
    auto const decoded = w::decode_request_head(bytes(golden), true);
    CHECK(decoded.code == w::error::none && decoded.value.timeout_us == request.timeout_us);
    CHECK(same(decoded.value.method_name, request.method_name));
    CHECK(decoded.value.metadata.count == 1);
    auto const entry = w::decode_metadata_entry(decoded.value.metadata.entries);
    CHECK(entry.code == w::error::none && entry.consumed == 4);
    CHECK(same(entry.value.key, literal("k")) && same(entry.value.value, literal("v")));
    for (std::size_t size = 0; size < golden.size(); ++size) {
        CHECK(w::decode_request_head({golden.data(), size}, true).code != w::error::none);
        CHECK(w::encode_request_head(request, true, {buffer.data(), size}).code == w::error::output_too_small);
    }
    CHECK(w::encode_request_head(request, false, writable(buffer)).code == w::error::invalid_head);
    CHECK(w::encode_request_head({}, true, writable(buffer)).code == w::error::invalid_head);
    auto extended = golden;
    extended[12] = 127; // Claims 127 metadata entries in four remaining bytes.
    CHECK(w::decode_request_head(bytes(extended), true).code == w::error::invalid_head);
    std::array<std::uint8_t, 10> tail{};
    CHECK(w::decode_request_head(bytes(tail), false).code == w::error::invalid_head);
    std::array<std::uint8_t, 11> empty_name{};
    CHECK(w::decode_request_head(bytes(empty_name), true).code == w::error::invalid_head);
    // Timeout changes never move the metadata or change head size.
    auto updated = request; updated.timeout_us = 1;
    auto const rewritten = w::encode_request_head(updated, true, writable(buffer));
    CHECK(rewritten.written == golden.size());
    CHECK(same({buffer.data() + 8, rewritten.written - 8}, {golden.data() + 8, golden.size() - 8}));
}

void end_head_contract() {
    std::array<std::uint8_t, 18> const internal{{
        2, 0, 0, 0, 1, 0, 0, 0, 4, 0, 2, 0, 13, 0, 0, 0, 0, 0}};
    auto const decoded = w::decode_frame(bytes(internal));
    CHECK(decoded.code == w::error::none && decoded.value.body.size == 0);
    std::array<std::uint8_t, 32> buffer{};
    auto result = w::encode_end_head({}, writable(buffer));
    CHECK(result.code == w::error::none && result.written == 2 && buffer[0] == 0 && buffer[1] == 0);
    result = w::encode_end_head({literal("bad"), {}}, writable(buffer));
    std::array<std::uint8_t, 5> const golden{{3, 'b', 'a', 'd', 0}};
    CHECK(result.code == w::error::none && same({buffer.data(), result.written}, bytes(golden)));
    auto const head = w::decode_end_head(bytes(golden));
    CHECK(head.code == w::error::none && same(head.value.message, literal("bad")));
    for (std::size_t size = 0; size < golden.size(); ++size)
        CHECK(w::decode_end_head({golden.data(), size}).code != w::error::none);
    w::frame_header header{5, 1, w::frame_type::end, 0, 5, 0};
    CHECK(w::encode_header(header, writable(buffer)).code == w::error::none);
    std::copy(golden.begin(), golden.end(), buffer.begin() + 16);
    CHECK(w::decode_frame({buffer.data(), 21}).code == w::error::invalid_head); // OK with message.
    header.aux = 13; header.length = 6;
    CHECK(w::encode_header(header, writable(buffer)).code == w::error::invalid_length); // Error with body.
    header.length = 5; header.aux = 17;
    CHECK(w::encode_header(header, writable(buffer)).code == w::error::invalid_aux);
}

void hostile_head_lengths() {
    std::array<std::uint8_t, 10> const huge{{255, 255, 255, 255, 255, 255, 255, 255, 255, 1}};
    std::array<std::uint8_t, 18> request{};
    std::copy(huge.begin(), huge.end(), request.begin() + 8);
    CHECK(w::decode_request_head(bytes(request), false).code == w::error::invalid_head); // Count.
    CHECK(w::decode_request_head(bytes(request), true).code == w::error::invalid_head); // Name length.
    CHECK(w::decode_end_head(bytes(huge)).code == w::error::invalid_head);
    CHECK(w::decode_metadata_entry(bytes(huge)).code == w::error::invalid_head);
    std::vector<std::uint8_t> message(65531, 'x');
    std::vector<std::uint8_t> output(65536);
    auto const maximum = w::encode_end_head({{message.data(), message.size()}, {}}, {output.data(), output.size()});
    CHECK(maximum.code == w::error::none && maximum.written == 65535);
    CHECK(w::decode_end_head({output.data(), maximum.written}).code == w::error::none);
    CHECK(w::decode_end_head({output.data(), output.size()}).code == w::error::invalid_head);
    message.push_back('x');
    CHECK(w::encode_end_head({{message.data(), message.size()}, {}}, {output.data(), output.size()}).code == w::error::invalid_head);
    CHECK(w::encode_request_head({0, {}, {nullptr, 32768}}, false, {output.data(), output.size()}).code != w::error::none);
    std::array<std::uint8_t, 9> const empty_request{};
    w::frame_header h{9, 1, w::frame_type::request, w::end_stream, 9, 1};
    CHECK(w::encode_header(h, {output.data(), output.size()}, {9, 0}).code == w::error::none);
    std::copy(empty_request.begin(), empty_request.end(), output.begin() + 16);
    CHECK(w::decode_frame({output.data(), 25}, {9, 0}).code == w::error::none);
}

void invalid_storage_and_guards() {
    CHECK(w::decode_varint({nullptr, 1}).code == w::error::invalid_argument);
    CHECK(w::decode_frame({nullptr, 16}).code == w::error::invalid_argument);
    CHECK(w::decode_settings({nullptr, 1}).code == w::error::invalid_argument);
    CHECK(w::decode_request_head({nullptr, 9}, false).code == w::error::invalid_argument);
    CHECK(w::decode_end_head({nullptr, 1}).code == w::error::invalid_argument);
    CHECK(w::encode_varint(1, {nullptr, 1}).code == w::error::invalid_argument);
    CHECK(w::encode_request_head({}, false, {nullptr, 9}).code == w::error::invalid_argument);
    auto const header = w::decode_header(bytes(request_golden)).value;
    for (std::size_t size = 0; size <= 16; ++size) {
        std::array<std::uint8_t, 18> guard{}; guard.fill(0xa5);
        auto const result = w::encode_header(header, {guard.data() + 1, size});
        CHECK((result.code == w::error::none) == (size == 16));
        CHECK(guard.front() == 0xa5 && guard[size + 1] == 0xa5);
    }
}

void randomized_roundtrips() {
    std::mt19937_64 random{0x6c727063};
    std::array<std::uint8_t, 16> buffer{};
    for (int i = 0; i < 20000; ++i) {
        auto const number = random();
        auto const encoded = w::encode_varint(number, writable(buffer));
        auto const decoded = w::decode_varint({buffer.data(), encoded.written});
        CHECK(decoded.code == w::error::none && decoded.value == number);
        w::frame_header h{};
        h.type = w::frame_type::request; h.flags = w::end_stream;
        h.head_length = static_cast<std::uint16_t>(9U + random() % 65000U);
        h.length = static_cast<std::uint32_t>(h.head_length + random() % 10000U);
        h.stream_id = static_cast<std::uint32_t>(1U + random() % w::max_stream_id);
        h.aux = static_cast<std::uint32_t>(1U + random() % 4096U);
        CHECK(w::encode_header(h, writable(buffer)).code == w::error::none);
        auto const restored = w::decode_header(bytes(buffer));
        CHECK(restored.code == w::error::none && restored.value.length == h.length);
        CHECK(restored.value.stream_id == h.stream_id && restored.value.aux == h.aux);
        CHECK(restored.value.head_length == h.head_length && restored.value.flags == h.flags);
    }
}
void extended_messages() {
    std::array<std::uint8_t, 64> data{};
    w::limits bounds{64, 1024, w::streaming | w::message_compression | w::explicit_rejection};
    w::frame_header h{12, 1, w::frame_type::message, w::more, 9, 0};
    CHECK(w::encode_header(h, writable(data), bounds).code == w::error::none);
    CHECK(w::encode_message_descriptor({8, 8, 0}, {data.data() + 16, 9}, bounds).code == w::error::none);
    CHECK(w::decode_frame({data.data(), 28}, bounds).code == w::error::none);
    h = {1, 1, w::frame_type::message, w::compressed, 0, 0};
    CHECK(w::encode_header(h, writable(data), bounds).code == w::error::invalid_flags);
    h = {12, 1, w::frame_type::message, w::more, 9, 0};
    CHECK(w::encode_header(h, writable(data), bounds).code == w::error::none);
    CHECK(w::decode_frame({data.data(), 28}).code == w::error::unsupported_feature);
    data[9] = 0; CHECK(w::decode_frame({data.data(), 28}, bounds).code == w::error::invalid_length);
    data[9] = w::compressed | w::more;
    CHECK(w::decode_frame({data.data(), 28}, bounds).code == w::error::invalid_flags);
    CHECK(w::encode_message_descriptor({8, 32, 1}, {data.data() + 16, 9}, bounds).code == w::error::none);
    CHECK(w::decode_frame({data.data(), 28}, bounds).code == w::error::none);
    CHECK(w::encode_message_descriptor({8, 1025, 1}, {data.data() + 16, 9}, bounds).code == w::error::message_too_large);
    CHECK(w::encode_message_descriptor({8, 7, 0}, {data.data() + 16, 9}, bounds).code == w::error::invalid_head);
    h = {0, 1, w::frame_type::message, w::end_stream, 0, 0};
    CHECK(w::encode_header(h, writable(data), bounds).code == w::error::none);
    CHECK(w::decode_frame({data.data(), 16}, bounds).code == w::error::none);
    h.flags |= w::more; CHECK(w::encode_header(h, writable(data), bounds).code == w::error::invalid_flags);
    h = {2, 1, w::frame_type::end, w::more, 2, 0};
    CHECK(w::encode_header(h, writable(data), bounds).code == w::error::invalid_flags);
    h = {0, 1, w::frame_type::window_update, 0, 0, 8};
    CHECK(w::encode_header(h, writable(data), bounds).code == w::error::none);
    h.aux = 0; CHECK(w::encode_header(h, writable(data), bounds).code == w::error::invalid_aux);
    h = {10, 1, w::frame_type::request, w::new_method, 10, 1};
    CHECK(w::encode_header(h, writable(data), bounds).code == w::error::none);
    ++h.length; CHECK(w::encode_header(h, writable(data), bounds).code == w::error::invalid_length);
}

} // namespace

int main() {
    struct test_case { char const *name; void (*run)(); };
    test_case const cases[] = {
        {"preface", preface_contract}, {"varint", varint_contract},
        {"frame golden/prefixes", frame_golden_and_prefixes}, {"header rejection", header_rejections},
        {"control frames", control_frames}, {"settings", settings_contract}, {"GOAWAY UTF-8", goaway_utf8},
        {"request head", request_head_contract}, {"end head", end_head_contract},
        {"hostile lengths", hostile_head_lengths}, {"storage guards", invalid_storage_and_guards},
        {"randomized roundtrips", randomized_roundtrips}, {"negotiated message grammar", extended_messages}};
    try {
        for (auto const &test : cases) { test.run(); std::cout << "PASS " << test.name << '\n'; }
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
