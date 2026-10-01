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

enum class Color { red, green, blue, unnamed };
enum Size { small_size, large_size };
enum class Twice { first, second };
struct Shapes {
    Color color = Color::green;
    Size size = small_size;
    std::vector<Color> palette;
    std::unique_ptr<int> count;
    std::unique_ptr<Item> item;
    std::vector<std::unique_ptr<int>> holes;
    std::map<std::string, int> scores;
    std::unordered_map<std::string, std::vector<Item>> groups;
    std::map<std::string, std::unique_ptr<Color>> maybe;
};
struct Wide {
    int f1 = 0, f2 = 0, f3 = 0, f4 = 0, f5 = 0, f6 = 0, f7 = 0, f8 = 0, f9 = 0, f10 = 0;
    int f11 = 0, f12 = 0, f13 = 0, f14 = 0, f15 = 0, f16 = 0, f17 = 0, f18 = 0, f19 = 0, f20 = 0;
};
struct Present { std::unique_ptr<int> value; };
struct Duplicated { Twice twice = Twice::first; };
RPC_JSON_ENUM(Color, red, green, blue);
RPC_JSON_ENUM(Size, small_size, large_size);
RPC_JSON_FIELDS(Shapes, color, size, palette, count, item, holes, scores, groups, maybe);
RPC_JSON_FIELDS(Wide, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20);
RPC_JSON_FIELDS(Duplicated, twice);
namespace rpc {
template <> struct json_traits<Present> {
    static auto fields() { return std::make_tuple(json_field("value", &Present::value, true)); }
};
template <> struct json_enum_traits<Twice> {
    static std::vector<json_enumerator_descriptor<Twice>> values() {
        return {json_enumerator("same", Twice::first), json_enumerator("same", Twice::second)};
    }
};
}
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
void enums() {
    Shapes s; s.color = Color::blue; s.size = large_size; s.palette = {Color::red, Color::green};
    auto const bytes = encode(s);
    CHECK(bytes.find(R"("color":"blue")") != std::string::npos && bytes.find(R"("size":"large_size")") != std::string::npos);
    Shapes out; CHECK(decode(bytes, out));
    CHECK(out.color == Color::blue && out.size == large_size && out.palette == s.palette);
    for (auto const *bad : {R"({"color":"purple"})", R"({"color":2})", R"({"color":null})", R"({"palette":["red",1]})",
                            R"({"color":"Blue"})"})
        CHECK(!decode(bad, out));
    s.color = Color::unnamed; std::uint8_t buffer[1024];
    CHECK(rpc::json_codec<Shapes>::encode_bounded(s, {buffer, sizeof(buffer)}).code == rpc::wire::error::invalid_argument);
    bool rejected = false; try { rpc::json_codec<Shapes>::upper_bound(s); } catch (std::invalid_argument const &) { rejected = true; } CHECK(rejected);
    Duplicated d; rejected = false;
    try { decode(R"({"twice":"same"})", d); } catch (std::invalid_argument const &) { rejected = true; } CHECK(rejected);
}
void nullable() {
    Shapes s; auto bytes = encode(s);
    CHECK(bytes.find("count") == std::string::npos && bytes.find("\"item\"") == std::string::npos); // Empty fields are left out.
    s.count.reset(new int(7)); s.item.reset(new Item{}); s.item->id = 5;
    s.holes.emplace_back(new int(1)); s.holes.emplace_back(); s.holes.emplace_back(new int(3));
    bytes = encode(s);
    CHECK(bytes.find(R"("holes":[1,null,3])") != std::string::npos);
    Shapes out; CHECK(decode(bytes, out));
    CHECK(out.count && *out.count == 7 && out.item && out.item->id == 5 && out.item->text == "default");
    CHECK(out.holes.size() == 3 && *out.holes[0] == 1 && !out.holes[1] && *out.holes[2] == 3);
    CHECK(decode(R"({"count":null,"item":null})", out) && !out.count && !out.item);
    CHECK(decode(R"({"item":{}})", out) && out.item && out.item->id == 9 && !out.count);
    for (auto const *bad : {R"({"count":"7"})", R"({"item":7})", R"({"count":{}})", R"({"holes":[[]]})"}) CHECK(!decode(bad, out));
    Present p; CHECK(!decode("{}", p)); CHECK(decode(R"({"value":null})", p) && !p.value);
    CHECK(decode(R"({"value":4})", p) && p.value && *p.value == 4);
}
void maps() {
    Shapes s; s.scores = {{"a", 1}, {"b\"c", -2}, {"", 0}};
    s.groups["x"] = {Item{}, Item{}}; s.groups["y"] = {};
    s.maybe["on"].reset(new Color(Color::red)); s.maybe["off"];
    auto const bytes = encode(s);
    CHECK(bytes.find(R"("scores":{"":0,"a":1,"b\"c":-2})") != std::string::npos);
    CHECK(bytes.find(R"("maybe":{"off":null,"on":"red"})") != std::string::npos);
    Shapes out; CHECK(decode(bytes, out));
    CHECK(out.scores == s.scores && out.groups.size() == 2 && out.groups["x"].size() == 2 && out.groups["y"].empty());
    CHECK(out.maybe.size() == 2 && !out.maybe["off"] && *out.maybe["on"] == Color::red);
    CHECK(decode(R"({"scores":{}})", out) && out.scores.empty());
    for (auto const *bad : {R"({"scores":{"a":1,"a":2}})", R"({"scores":{"a":1,"\u0061":2}})", R"({"scores":{"a":"1"}})",
                            R"({"scores":[1]})", R"({"scores":null})", R"({"groups":{"x":{}}})"})
        CHECK(!decode(bad, out));
}
void wide() {
    Wide w; w.f1 = 1; w.f20 = 20;
    Wide out; CHECK(decode(encode(w), out) && out.f1 == 1 && out.f20 == 20);
    CHECK(decode(R"({"f17":17})", out) && out.f17 == 17 && out.f1 == 0);
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
    try {
        values(); rejected(); limits(); std::cout << "PASS JSON mapping, strict input, limits\n";
        enums(); nullable(); maps(); wide(); std::cout << "PASS JSON enums, nullable fields, maps, 20 fields\n";
    }
    catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
