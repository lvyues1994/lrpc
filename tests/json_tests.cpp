#include <rpc/json.hpp>
#include "check.hpp"
#include <cmath>
#include <iostream>

struct Item { std::int64_t id = 9; std::string text{"default"}; };
struct Message { Item item; std::vector<int> numbers; bool active = false; double real = 0; std::uint64_t large = 0; float single = 0; };
struct Required { int value = 3; };
struct Limited { std::string text; std::vector<int> values; };
struct MemoryLimited { std::string text; };
struct Recursive { std::vector<Recursive> children; };
struct Invalid { int a = 0, b = 0; };
RPC_JSON_FIELDS(Item, id, text);
RPC_JSON_FIELDS(Message, item, numbers, active, real, large, single);
RPC_JSON_FIELDS(Limited, text, values);
RPC_JSON_FIELDS(Recursive, children);
RPC_JSON_FIELDS(MemoryLimited, text);
namespace rpc {
template <> struct json_traits<Required> {
    static auto fields() { return std::make_tuple(json_field("v", &Required::value, true)); }
};
template <> struct json_traits<Invalid> {
    static auto fields() { return std::make_tuple(json_field("a", &Invalid::a), json_field_descriptor<Invalid, int>{nullptr, &Invalid::b, false, 1}); }
};
template <> struct json_options<MemoryLimited> {
    static json_limits limits() noexcept { json_limits l; l.max_decoded_bytes = 8; l.max_string_bytes = 1024; l.max_scratch_bytes = 512; return l; }
};
template <> struct json_options<Limited> {
    static json_limits limits() noexcept { json_limits l; l.max_depth = 4; l.max_string_bytes = 32; l.max_array_elements = 2; l.max_object_fields = 4; l.max_decoded_bytes = 512; l.max_scratch_bytes = 512; return l; }
};
}
namespace {
template <class T> bool decode(std::string const &s, T &out) {
    return rpc::json_codec<T>::decode({reinterpret_cast<std::uint8_t const *>(s.data()), s.size()}, out);
}
template <class T> std::string encode(T const &value) {
    std::string out(rpc::json_codec<T>::upper_bound(value), '\0');
    auto const result = rpc::json_codec<T>::encode_bounded(value, {reinterpret_cast<std::uint8_t *>(&out[0]), out.size()});
    CHECK(result.code == rpc::wire::error::none); out.resize(result.written); return out;
}
void values() {
    Message m; m.item.id = std::numeric_limits<std::int64_t>::min(); m.item.text = std::string{"a\0b", 3} + "中文\n\"\\";
    m.numbers = {-4, 0, 7}; m.active = true; m.real = std::nextafter(1.0, 2.0); m.large = std::numeric_limits<std::uint64_t>::max();
    Message out; auto const bytes = encode(m); CHECK(decode(bytes, out));
    CHECK(out.item.id == m.item.id && out.item.text == m.item.text && out.numbers == m.numbers && out.active && out.real == m.real && out.large == m.large);
    CHECK(rpc::json_codec<Message>::size(m) == bytes.size());
    std::vector<std::uint8_t> exact(bytes.size()); CHECK(rpc::json_codec<Message>::encode(m, {exact.data(), exact.size()}));
    CHECK(!rpc::json_codec<Message>::encode(m, {exact.data(), exact.size() - 1}));
    auto small = rpc::json_codec<Message>::encode_bounded(m, {exact.data(), 1}); CHECK(small.code == rpc::wire::error::output_too_small && small.written == 0);
    CHECK(decode("{}", out) && out.item.id == 9 && out.item.text == "default" && out.numbers.empty() && !out.active);
    CHECK(decode(R"({"single":9007199791611905})", out) && out.single == 9007200328482816.0F);
    CHECK(decode(R"({"item":{"id":9223372036854775807}})", out) && out.item.id == std::numeric_limits<std::int64_t>::max());
    m.item.text.assign(8192, 'x'); CHECK(decode(encode(m), out) && out.item.text == m.item.text);
    CHECK(decode(R"({"unknown":{"array":[null,true,{"x":"ok"}]}})", out));
    CHECK(decode("{\"item\":{\"text\":\"copy\"}}", out));
    std::string borrowed = R"({"item":{"text":"owned"}})"; CHECK(decode(borrowed, out)); borrowed.assign(borrowed.size(), 'x'); CHECK(out.item.text == "owned");
}
void rejected() {
    Message m;
    for (auto const *s : {"", "[]", "null", "{}{}", "{} x", "{", "{\"active\":1}", "{\"item\":null}", "{\"numbers\":[1.0]}",
        "{\"large\":-1}", "{\"large\":18446744073709551616}", "{\"item\":{\"id\":9223372036854775808}}", "{\"real\":NaN}", "{\"real\":1e400}",
        "{\"active\":true,\"active\":false}", "{\"active\":true,\"\\u0061ctive\":false}", "{\"z\":1,\"z\":2}", "{\"z\":{\"a\":1,\"a\":2}}",
        "{\"numbers\":[1,]}", "{\"numbers\":[2147483648]}", "{/*comment*/}", "{\"real\":Infinity}", "{\"single\":1e100}"}) CHECK(!decode(s, m));
    CHECK(!decode(std::string{"{}\0{}", 5}, m)); CHECK(!decode(std::string{"{}\0", 3}, m));
    CHECK(!decode(std::string{"{\"item\":{\"text\":\""} + char(0xC0) + char(0xAF) + "\"}}", m));
    CHECK(!decode(R"({"item":{"text":"\uD800"}})", m));
    Required required; CHECK(!decode("{}", required)); CHECK(decode("{\"v\":7}", required) && required.value == 7);
    bool invalid = false; try { static_cast<void>(rpc::json_codec_policy::operations<Invalid>()); } catch (std::invalid_argument const &) { invalid = true; } CHECK(invalid);
    m.real = std::numeric_limits<double>::infinity(); std::uint8_t bytes[1024];
    CHECK(rpc::json_codec<Message>::encode_bounded(m, {bytes, sizeof(bytes)}).code == rpc::wire::error::invalid_argument);
    m.real = 0; m.item.text.assign(1, char(0xFF)); CHECK(rpc::json_codec<Message>::encode_bounded(m, {bytes, sizeof(bytes)}).code == rpc::wire::error::invalid_argument);
}
void limits() {
    Limited l; CHECK(!decode(R"({"values":[1,2,3]})", l));
    CHECK(!decode("{\"text\":\"" + std::string(33, 'a') + "\"}", l));
    CHECK(!decode(R"({"a":0,"b":0,"c":0,"d":0,"e":0})", l));
    CHECK(!decode(R"({"a":[[[[]]]]})", l));
    MemoryLimited memory; bool exhausted = false; try { decode(R"({"text":"123456789"})", memory); } catch (std::bad_alloc const &) { exhausted = true; } CHECK(exhausted);
    // Large escaped string needs Reader scratch growth beyond the inline budget.
    exhausted = false; try { decode("{\"text\":\"" + std::string(700, 'a') + "\"}", memory); } catch (std::bad_alloc const &) { exhausted = true; } CHECK(exhausted);
    Recursive root; auto *node = &root; for (int i = 0; i < 40; ++i) { node->children.emplace_back(); node = &node->children.back(); }
    bool rejected = false; try { rpc::json_codec<Recursive>::upper_bound(root); } catch (std::invalid_argument const &) { rejected = true; } CHECK(rejected);
    std::uint8_t bytes[4096]; CHECK(rpc::json_codec<Recursive>::encode_bounded(root, {bytes, sizeof(bytes)}).code == rpc::wire::error::invalid_argument);
}
}
int main() {
    try { values(); rejected(); limits(); std::cout << "PASS JSON mapping, strict input, limits\n"; }
    catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
