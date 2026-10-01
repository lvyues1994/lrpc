// std::optional fields, available to C++17 users of the C++14 library.
#include <rpc/json.hpp>
#include "check.hpp"
#include <iostream>

#if !defined(RPC_JSON_OPTIONAL)
#error "std::optional support expected in C++17"
#endif

struct Inner { int id = 1; };
struct Outer {
    std::optional<int> count;
    std::optional<std::string> name;
    std::optional<Inner> inner;
    std::vector<std::optional<double>> samples;
    std::map<std::string, std::optional<Inner>> lookup;
};
RPC_JSON_FIELDS(Inner, id);
RPC_JSON_FIELDS(Outer, count, name, inner, samples, lookup);

namespace {
bool decode(std::string const &s, Outer &out) {
    return rpc::json_codec<Outer>::decode({reinterpret_cast<std::uint8_t const *>(s.data()), s.size()}, out);
}
std::string encode(Outer const &value) {
    std::string out(rpc::json_codec<Outer>::upper_bound(value), '\0');
    auto const result = rpc::json_codec<Outer>::encode_bounded(value, {reinterpret_cast<std::uint8_t *>(&out[0]), out.size()});
    CHECK(result.code == rpc::wire::error::none);
    out.resize(result.written);
    return out;
}
} // namespace

int main() {
    try {
        Outer empty;
        CHECK(encode(empty) == R"({"samples":[],"lookup":{}})");
        Outer full;
        full.count = 0;
        full.name = "";
        full.inner = Inner{5};
        full.samples = {1.5, std::nullopt};
        full.lookup["x"] = Inner{};
        full.lookup["y"];
        auto const bytes = encode(full);
        CHECK(bytes == R"({"count":0,"name":"","inner":{"id":5},"samples":[1.5,null],"lookup":{"x":{"id":1},"y":null}})");
        Outer out;
        CHECK(decode(bytes, out));
        CHECK(out.count == 0 && out.name == std::string{} && out.inner && out.inner->id == 5);
        CHECK(out.samples.size() == 2 && out.samples[0] == 1.5 && !out.samples[1]);
        CHECK(out.lookup.size() == 2 && out.lookup["x"]->id == 1 && !out.lookup["y"]);
        CHECK(decode(R"({"count":null,"inner":{}})", out) && !out.count && !out.name && out.inner && out.inner->id == 1);
        CHECK(!decode(R"({"count":"1"})", out) && !decode(R"({"inner":[]})", out));
        std::cout << "PASS JSON std::optional fields\n";
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
