#pragma once

#include <rpc/typed.hpp>
#include <cmath>
#include <cstdint>
#include <limits>
#include <vector>
#include <cstring>
#include <tuple>
#include <type_traits>

namespace rpc {

// Limits apply to one complete JSON message. decoded_bytes bounds logical
// retained values, not std::string/vector capacity or allocator bookkeeping.
struct json_limits {
    std::size_t max_depth = 32;
    std::size_t max_string_bytes = 64U * 1024U;
    std::size_t max_array_elements = 16384;
    std::size_t max_object_fields = 256;
    std::size_t max_decoded_bytes = 1024U * 1024U;
    std::size_t max_scratch_bytes = 256U * 1024U;
};

template <class Message> struct json_traits;
template <class Message> struct json_options {
    static json_limits limits() noexcept { return {}; }
};

template <class Owner, class Member> struct json_field_descriptor {
    char const *name;
    Member Owner::*member;
    bool required;
    std::size_t name_size;
};
template <class Owner, class Member>
json_field_descriptor<Owner, Member> json_field(char const *name, Member Owner::*member, bool required = false) {
    if (!name || !member) throw std::invalid_argument{"invalid JSON field"};
    return {name, member, required, std::strlen(name)};
}

namespace json_detail {
template <class...> using void_t = void;
template <class T, class = void> struct mapped : std::false_type {};
template <class T> struct mapped<T, void_t<decltype(json_traits<T>::fields())>> : std::true_type {};

enum class atom_kind { null, boolean, signed_integer, unsigned_integer, number, string };
struct atom {
    atom_kind kind = atom_kind::null;
    bool boolean = false;
    std::int64_t signed_integer = 0;
    std::uint64_t unsigned_integer = 0;
    double number = 0;
    char const *text = nullptr;
    std::size_t size = 0;
};

// The backend is hidden in json.cpp; public traits expose no RapidJSON types.
struct output {
    virtual ~output() = default;
    virtual bool value(atom const &) = 0;
    virtual bool start_object() = 0;
    virtual bool key(char const *, std::size_t) = 0;
    virtual bool end_object() = 0;
    virtual bool start_array() = 0;
    virtual bool end_array() = 0;
};

struct write_context {
    output &sink;
    json_limits limits;
    std::size_t depth = 0;
};
class depth_guard {
public:
    explicit depth_guard(write_context &context) noexcept : context_(context) { ++context_.depth; }
    ~depth_guard() { --context_.depth; }
    bool valid() const noexcept { return context_.depth <= context_.limits.max_depth; }
private:
    write_context &context_;
};
struct read_context {
    json_limits limits;
    std::size_t decoded_bytes = 0;
    void charge(std::size_t bytes) {
        if (bytes > limits.max_decoded_bytes - decoded_bytes) throw std::bad_alloc{};
        decoded_bytes += bytes;
    }
};

enum class node_kind { scalar, object, array };
struct node_ops;
struct node { void *target = nullptr; node_ops const *operations = nullptr; };
struct node_ops {
    node_kind kind;
    bool (*value)(read_context &, void *, atom const &);
    node (*field)(void *, char const *, std::size_t, std::size_t &);
    node (*element)(read_context &, void *);
    std::uint64_t (*required)();
    void (*reset)(void *);
};
using write_function = bool (*)(write_context &, void const *);
wire::encode_result write_message(void const *, write_function, wire::mutable_bytes_view, json_limits, bool count = false);
bool read_message(wire::bytes_view, node, json_limits);

inline std::size_t add_bound(std::size_t a, std::size_t b) noexcept {
    auto const maximum = std::numeric_limits<std::size_t>::max();
    return b > maximum - a ? maximum : a + b;
}
inline std::size_t string_bound(std::size_t size) noexcept {
    auto const maximum = std::numeric_limits<std::size_t>::max();
    return size > (maximum - 2) / 6 ? maximum : 2 + 6 * size;
}

template <class T, class = void> struct value_codec {
    static_assert(sizeof(T) == 0, "JSON field needs a supported value type or json_traits<T>");
};
template <class T> node_ops const &operations();
template <class T> node make_node(T &value) { return {&value, &operations<T>()}; }

template <class T> struct value_codec<T, typename std::enable_if<std::is_integral<T>::value &&
        std::is_signed<T>::value && !std::is_same<T, bool>::value>::type> {
    static constexpr node_kind kind = node_kind::scalar;
    static bool read(read_context &, T &out, atom const &value) {
        if (value.kind == atom_kind::signed_integer) {
            if (value.signed_integer < std::numeric_limits<T>::min() || value.signed_integer > std::numeric_limits<T>::max()) return false;
            out = static_cast<T>(value.signed_integer); return true;
        }
        if (value.kind != atom_kind::unsigned_integer || value.unsigned_integer > static_cast<std::uint64_t>(std::numeric_limits<T>::max())) return false;
        out = static_cast<T>(value.unsigned_integer); return true;
    }
    static bool write(write_context &context, T value) { atom a; a.kind = atom_kind::signed_integer; a.signed_integer = value; return context.sink.value(a); }
    static void reset(T &value) noexcept { value = T{}; }
    static std::size_t bound(T, json_limits const &, std::size_t) noexcept { return 21; }
};
template <class T> struct value_codec<T, typename std::enable_if<std::is_integral<T>::value &&
        std::is_unsigned<T>::value && !std::is_same<T, bool>::value>::type> {
    static constexpr node_kind kind = node_kind::scalar;
    static bool read(read_context &, T &out, atom const &value) {
        if (value.kind == atom_kind::signed_integer) {
            if (value.signed_integer < 0 || static_cast<std::uint64_t>(value.signed_integer) > std::numeric_limits<T>::max()) return false;
            out = static_cast<T>(value.signed_integer); return true;
        }
        if (value.kind != atom_kind::unsigned_integer || value.unsigned_integer > std::numeric_limits<T>::max()) return false;
        out = static_cast<T>(value.unsigned_integer); return true;
    }
    static bool write(write_context &context, T value) { atom a; a.kind = atom_kind::unsigned_integer; a.unsigned_integer = value; return context.sink.value(a); }
    static void reset(T &value) noexcept { value = T{}; }
    static std::size_t bound(T, json_limits const &, std::size_t) noexcept { return 20; }
};
template <> struct value_codec<bool> {
    static constexpr node_kind kind = node_kind::scalar;
    static bool read(read_context &, bool &out, atom const &value) {
        if (value.kind != atom_kind::boolean) return false;
        out = value.boolean; return true;
    }
    static bool write(write_context &context, bool value) { atom a; a.kind = atom_kind::boolean; a.boolean = value; return context.sink.value(a); }
    static void reset(bool &value) noexcept { value = false; }
    static std::size_t bound(bool, json_limits const &, std::size_t) noexcept { return 5; }
};
template <class T> struct value_codec<T, typename std::enable_if<std::is_floating_point<T>::value>::type> {
    static_assert(sizeof(T) <= sizeof(double), "JSON supports float and double, not long double");
    static constexpr node_kind kind = node_kind::scalar;
    static bool read(read_context &, T &out, atom const &value) {
        double number = 0;
        if (value.kind == atom_kind::number) number = value.number;
        else if (value.kind == atom_kind::signed_integer) { out = static_cast<T>(value.signed_integer); return true; }
        else if (value.kind == atom_kind::unsigned_integer) { out = static_cast<T>(value.unsigned_integer); return true; }
        else return false;
        if (!std::isfinite(number) || number > std::numeric_limits<T>::max() || number < -std::numeric_limits<T>::max()) return false;
        out = static_cast<T>(number); return true;
    }
    static bool write(write_context &context, T value) {
        if (!std::isfinite(value)) return false;
        atom a; a.kind = atom_kind::number; a.number = value; return context.sink.value(a);
    }
    static void reset(T &value) noexcept { value = T{}; }
    static std::size_t bound(T, json_limits const &, std::size_t) noexcept { return 32; }
};
template <> struct value_codec<std::string> {
    static constexpr node_kind kind = node_kind::scalar;
    static bool read(read_context &context, std::string &out, atom const &value) {
        if (value.kind != atom_kind::string || value.size > context.limits.max_string_bytes) return false;
        context.charge(value.size); out.assign(value.text, value.size); return true;
    }
    static bool write(write_context &context, std::string const &value) {
        if (value.size() > context.limits.max_string_bytes) return false;
        atom a; a.kind = atom_kind::string; a.text = value.c_str(); a.size = value.size(); return context.sink.value(a);
    }
    static void reset(std::string &value) noexcept { value.clear(); }
    static std::size_t bound(std::string const &value, json_limits const &limits, std::size_t) {
        if (value.size() > limits.max_string_bytes) throw std::invalid_argument{"JSON string limit"};
        return string_bound(value.size());
    }
};

template <class Tuple, class Function, std::size_t... I>
void each(Tuple const &fields, Function function, std::index_sequence<I...>) {
    int const unused[] = {0, (function(std::get<I>(fields), I), 0)...};
    static_cast<void>(unused);
}
template <class T> auto const &fields() {
    static auto const value = json_traits<T>::fields();
    return value;
}
template <class T> struct value_codec<T, typename std::enable_if<mapped<T>::value>::type> {
    using tuple_type = typename std::decay<decltype(json_traits<T>::fields())>::type;
    static constexpr std::size_t count = std::tuple_size<tuple_type>::value;
    static_assert(count <= 64, "JSON supports at most 64 mapped fields per object");
    using indices = std::make_index_sequence<count>;
    static constexpr node_kind kind = node_kind::object;
    static bool read(read_context &, T &, atom const &) { return false; }
    static node field(T &value, char const *name, std::size_t size, std::size_t &index) {
        node result;
        each(fields<T>(), [&](auto const &f, std::size_t i) {
            if (f.name_size == size && std::memcmp(f.name, name, size) == 0) {
                index = i; result = make_node(value.*f.member);
            }
        }, indices{});
        return result;
    }
    static std::uint64_t required() {
        std::uint64_t result = 0;
        each(fields<T>(), [&](auto const &f, std::size_t i) { if (f.required) result |= std::uint64_t{1} << i; }, indices{});
        return result;
    }
    static void validate() {
        each(fields<T>(), [&](auto const &f, std::size_t) {
            if (!f.name || !f.member || f.name_size != std::strlen(f.name))
                throw std::invalid_argument{"invalid JSON field mapping"};
        }, indices{});
        each(fields<T>(), [&](auto const &f, std::size_t i) {
            each(fields<T>(), [&](auto const &g, std::size_t j) {
                if (i < j && f.name_size == g.name_size && std::memcmp(f.name, g.name, f.name_size) == 0)
                    throw std::invalid_argument{"duplicate JSON field mapping"};
            }, indices{});
        }, indices{});
    }
    static void reset(T &value) {
        value = T{};
    }
    static bool write(write_context &context, T const &value) {
        static_cast<void>(operations<T>());
        depth_guard guard{context};
        if (!guard.valid() || count > context.limits.max_object_fields || !context.sink.start_object()) return false;
        bool okay = true;
        each(fields<T>(), [&](auto const &f, std::size_t) {
            using member_type = typename std::decay<decltype(value.*f.member)>::type;
            okay = okay && f.name_size <= context.limits.max_string_bytes && context.sink.key(f.name, f.name_size) &&
                value_codec<member_type>::write(context, value.*f.member);
        }, indices{});
        return okay && context.sink.end_object();
    }
    static std::size_t bound(T const &value, json_limits const &limits, std::size_t depth) {
        static_cast<void>(operations<T>());
        if (depth >= limits.max_depth || count > limits.max_object_fields) throw std::invalid_argument{"JSON object limit"};
        std::size_t size = 2;
        each(fields<T>(), [&](auto const &f, std::size_t) {
            using member_type = typename std::decay<decltype(value.*f.member)>::type;
            if (f.name_size > limits.max_string_bytes) throw std::invalid_argument{"JSON key limit"};
            size = add_bound(size, add_bound(string_bound(f.name_size), add_bound(2, value_codec<member_type>::bound(value.*f.member, limits, depth + 1))));
        }, indices{});
        return size;
    }
};
template <class T, class Allocator> struct value_codec<std::vector<T, Allocator>> {
    static_assert(!std::is_same<T, bool>::value, "use a JSON boolean field instead of vector<bool>");
    using vector_type = std::vector<T, Allocator>;
    static constexpr node_kind kind = node_kind::array;
    static bool read(read_context &, vector_type &, atom const &) { return false; }
    static node element(read_context &context, vector_type &value) {
        if (value.size() >= context.limits.max_array_elements) return {};
        context.charge(sizeof(T)); value.emplace_back(); return make_node(value.back());
    }
    static void reset(vector_type &value) { value.clear(); }
    static bool write(write_context &context, vector_type const &value) {
        depth_guard guard{context};
        if (!guard.valid() || value.size() > context.limits.max_array_elements || !context.sink.start_array()) return false;
        for (auto const &item : value) if (!value_codec<T>::write(context, item)) return false;
        return context.sink.end_array();
    }
    static std::size_t bound(vector_type const &value, json_limits const &limits, std::size_t depth) {
        if (depth >= limits.max_depth || value.size() > limits.max_array_elements) throw std::invalid_argument{"JSON array limit"};
        std::size_t size = 2;
        for (auto const &item : value) size = add_bound(size, add_bound(1, value_codec<T>::bound(item, limits, depth + 1)));
        return size;
    }
};

template <class T, class = void> struct container_access {
    static node field(void *, char const *, std::size_t, std::size_t &) { return {}; }
    static node element(read_context &, void *) { return {}; }
    static std::uint64_t required() { return 0; }
    static void validate() {}
};
template <class T> struct container_access<T, typename std::enable_if<mapped<T>::value>::type> {
    static node field(void *target, char const *name, std::size_t size, std::size_t &index) {
        return value_codec<T>::field(*static_cast<T *>(target), name, size, index);
    }
    static node element(read_context &, void *) { return {}; }
    static std::uint64_t required() { return value_codec<T>::required(); }
    static void validate() { value_codec<T>::validate(); }
};
template <class T, class Allocator> struct container_access<std::vector<T, Allocator>> {
    static node field(void *, char const *, std::size_t, std::size_t &) { return {}; }
    static node element(read_context &context, void *target) {
        return value_codec<std::vector<T, Allocator>>::element(context, *static_cast<std::vector<T, Allocator> *>(target));
    }
    static std::uint64_t required() { return 0; }
    static void validate() {}
};
template <class T> node_ops const &operations() {
    static node_ops const value = [] {
        container_access<T>::validate();
        return node_ops{value_codec<T>::kind,
            [](read_context &context, void *target, atom const &a) { return value_codec<T>::read(context, *static_cast<T *>(target), a); },
            &container_access<T>::field, &container_access<T>::element, &container_access<T>::required,
            [](void *target) { value_codec<T>::reset(*static_cast<T *>(target)); }};
    }();
    return value;
}
} // namespace json_detail

// Separate from codec<T>: registering a JSON mapping does not change another
// encoding of that same C++ type. Methods choose their encoding policy.
template <class Message> struct json_codec {
    static_assert(json_detail::mapped<Message>::value, "JSON RPC messages require json_traits<Message>");
    static wire::encode_result encode_bounded(Message const &value, wire::mutable_bytes_view output) {
        static_cast<void>(json_detail::operations<Message>());
        return json_detail::write_message(&value, &write, output, json_options<Message>::limits());
    }
    static std::size_t size(Message const &value) {
        static_cast<void>(json_detail::operations<Message>());
        auto const encoded = json_detail::write_message(&value, &write, {}, json_options<Message>::limits(), true);
        if (encoded.code != wire::error::none) throw std::invalid_argument{"invalid JSON message"};
        return encoded.written;
    }
    static bool encode(Message const &value, wire::mutable_bytes_view output) {
        auto const encoded = encode_bounded(value, output);
        return encoded.code == wire::error::none && encoded.written == output.size;
    }
    static bool decode(wire::bytes_view input, Message &value) {
        return json_detail::read_message(input, json_detail::make_node(value), json_options<Message>::limits());
    }
    static std::size_t upper_bound(Message const &value) {
        auto const limits = json_options<Message>::limits();
        if (!limits.max_depth || limits.max_depth > 64 || limits.max_scratch_bytes < 512)
            throw std::invalid_argument{"invalid JSON limits"};
        return json_detail::value_codec<Message>::bound(value, limits, 0);
    }
private:
    static bool write(json_detail::write_context &context, void const *value) {
        return json_detail::value_codec<Message>::write(context, *static_cast<Message const *>(value));
    }
};

struct json_codec_policy {
    template <class Message> static codec_ops const &operations() {
        static codec_ops const value{
            [](void const *p) { return json_codec<Message>::size(*static_cast<Message const *>(p)); },
            [](void const *p, wire::mutable_bytes_view out) { return json_codec<Message>::encode(*static_cast<Message const *>(p), out); },
            [](wire::bytes_view in, void *p) { return json_codec<Message>::decode(in, *static_cast<Message *>(p)); },
            [](void const *p, wire::mutable_bytes_view out) { return json_codec<Message>::encode_bounded(*static_cast<Message const *>(p), out); },
            [](void const *p) { return json_codec<Message>::upper_bound(*static_cast<Message const *>(p)); }};
        static_cast<void>(json_detail::operations<Message>());
        return value;
    }
};
template <class Request, class Response> using json_method = method<Request, Response, json_codec_policy>;
template <class Request, class Response> using json_bound_method = bound_method<Request, Response, json_codec_policy>;

} // namespace rpc

// Convenience mapping, used at namespace scope outside namespace rpc.
#define RPC_DETAIL_JSON_FIELD(Type, Field) ::rpc::json_field(#Field, &Type::Field)
#define RPC_DETAIL_JSON_JOIN_IMPL(A, B) A##B
#define RPC_DETAIL_JSON_JOIN(A, B) RPC_DETAIL_JSON_JOIN_IMPL(A, B)
#define RPC_DETAIL_JSON_COUNT_IMPL(_1,_2,_3,_4,_5,_6,_7,_8,_9,_10,_11,_12,_13,_14,_15,_16,N,...) N
#define RPC_DETAIL_JSON_COUNT(...) RPC_DETAIL_JSON_COUNT_IMPL(__VA_ARGS__,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0)
#define RPC_DETAIL_JSON_FIELDS_1(T,A) RPC_DETAIL_JSON_FIELD(T,A)
#define RPC_DETAIL_JSON_FIELDS_2(T,A,...) RPC_DETAIL_JSON_FIELD(T,A), RPC_DETAIL_JSON_FIELDS_1(T,__VA_ARGS__)
#define RPC_DETAIL_JSON_FIELDS_3(T,A,...) RPC_DETAIL_JSON_FIELD(T,A), RPC_DETAIL_JSON_FIELDS_2(T,__VA_ARGS__)
#define RPC_DETAIL_JSON_FIELDS_4(T,A,...) RPC_DETAIL_JSON_FIELD(T,A), RPC_DETAIL_JSON_FIELDS_3(T,__VA_ARGS__)
#define RPC_DETAIL_JSON_FIELDS_5(T,A,...) RPC_DETAIL_JSON_FIELD(T,A), RPC_DETAIL_JSON_FIELDS_4(T,__VA_ARGS__)
#define RPC_DETAIL_JSON_FIELDS_6(T,A,...) RPC_DETAIL_JSON_FIELD(T,A), RPC_DETAIL_JSON_FIELDS_5(T,__VA_ARGS__)
#define RPC_DETAIL_JSON_FIELDS_7(T,A,...) RPC_DETAIL_JSON_FIELD(T,A), RPC_DETAIL_JSON_FIELDS_6(T,__VA_ARGS__)
#define RPC_DETAIL_JSON_FIELDS_8(T,A,...) RPC_DETAIL_JSON_FIELD(T,A), RPC_DETAIL_JSON_FIELDS_7(T,__VA_ARGS__)
#define RPC_DETAIL_JSON_FIELDS_9(T,A,...) RPC_DETAIL_JSON_FIELD(T,A), RPC_DETAIL_JSON_FIELDS_8(T,__VA_ARGS__)
#define RPC_DETAIL_JSON_FIELDS_10(T,A,...) RPC_DETAIL_JSON_FIELD(T,A), RPC_DETAIL_JSON_FIELDS_9(T,__VA_ARGS__)
#define RPC_DETAIL_JSON_FIELDS_11(T,A,...) RPC_DETAIL_JSON_FIELD(T,A), RPC_DETAIL_JSON_FIELDS_10(T,__VA_ARGS__)
#define RPC_DETAIL_JSON_FIELDS_12(T,A,...) RPC_DETAIL_JSON_FIELD(T,A), RPC_DETAIL_JSON_FIELDS_11(T,__VA_ARGS__)
#define RPC_DETAIL_JSON_FIELDS_13(T,A,...) RPC_DETAIL_JSON_FIELD(T,A), RPC_DETAIL_JSON_FIELDS_12(T,__VA_ARGS__)
#define RPC_DETAIL_JSON_FIELDS_14(T,A,...) RPC_DETAIL_JSON_FIELD(T,A), RPC_DETAIL_JSON_FIELDS_13(T,__VA_ARGS__)
#define RPC_DETAIL_JSON_FIELDS_15(T,A,...) RPC_DETAIL_JSON_FIELD(T,A), RPC_DETAIL_JSON_FIELDS_14(T,__VA_ARGS__)
#define RPC_DETAIL_JSON_FIELDS_16(T,A,...) RPC_DETAIL_JSON_FIELD(T,A), RPC_DETAIL_JSON_FIELDS_15(T,__VA_ARGS__)
#define RPC_JSON_FIELDS(Type, ...) \
    template <> struct rpc::json_traits<Type> { \
        static auto fields() { \
            return std::make_tuple(RPC_DETAIL_JSON_JOIN(RPC_DETAIL_JSON_FIELDS_, RPC_DETAIL_JSON_COUNT(__VA_ARGS__))(Type, __VA_ARGS__)); \
        } \
    }
