#pragma once

#include <rpc/typed.hpp>
#include <cmath>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <vector>
#include <cstring>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>
#if defined(__has_include)
#if __has_include(<optional>) && __cplusplus >= 201703L
#include <optional>
#define RPC_JSON_OPTIONAL 1
#endif
#endif

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

// An enum travels as the name of its value. Specializations provide
//   static <container of json_enumerator_descriptor<Enum>> values();
// Names must be unique; a value with several names writes the first.
template <class Enum> struct json_enum_traits;
template <class Enum> struct json_enumerator_descriptor {
    char const *name;
    Enum value;
    std::size_t name_size;
};
template <class Enum> json_enumerator_descriptor<Enum> json_enumerator(char const *name, Enum value) {
    static_assert(std::is_enum<Enum>::value, "JSON enumerators name the values of an enum");
    if (!name) throw std::invalid_argument{"invalid JSON enumerator"};
    return {name, value, std::strlen(name)};
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
    void (*reset)(read_context &, void *);
    // Objects with arbitrary keys (maps): the slot for a new key, or nothing
    // for a repeated one. Null for every other kind of node.
    node (*entry)(read_context &, void *, char const *, std::size_t);
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
    static_assert(sizeof(T) == 0, "JSON field needs a supported value type, json_traits<T> or json_enum_traits<T>");
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
    static void reset(read_context &, T &value) noexcept { value = T{}; }
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
    static void reset(read_context &, T &value) noexcept { value = T{}; }
    static std::size_t bound(T, json_limits const &, std::size_t) noexcept { return 20; }
};
template <> struct value_codec<bool> {
    static constexpr node_kind kind = node_kind::scalar;
    static bool read(read_context &, bool &out, atom const &value) {
        if (value.kind != atom_kind::boolean) return false;
        out = value.boolean; return true;
    }
    static bool write(write_context &context, bool value) { atom a; a.kind = atom_kind::boolean; a.boolean = value; return context.sink.value(a); }
    static void reset(read_context &, bool &value) noexcept { value = false; }
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
    static void reset(read_context &, T &value) noexcept { value = T{}; }
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
    static void reset(read_context &, std::string &value) noexcept { value.clear(); }
    static std::size_t bound(std::string const &value, json_limits const &limits, std::size_t) {
        if (value.size() > limits.max_string_bytes) throw std::invalid_argument{"JSON string limit"};
        return string_bound(value.size());
    }
};

template <class T, class = void> struct named_enum : std::false_type {};
template <class T> struct named_enum<T, void_t<decltype(json_enum_traits<T>::values())>> : std::true_type {};
// Validated on first use, like nested mappings.
template <class E> std::vector<json_enumerator_descriptor<E>> const &enumerators() {
    static auto const table = [] {
        std::vector<json_enumerator_descriptor<E>> result;
        for (auto const &e : json_enum_traits<E>::values()) {
            if (!e.name || e.name_size != std::strlen(e.name)) throw std::invalid_argument{"invalid JSON enumerator"};
            for (auto const &seen : result)
                if (seen.name_size == e.name_size && std::memcmp(seen.name, e.name, e.name_size) == 0)
                    throw std::invalid_argument{"duplicate JSON enumerator"};
            result.push_back(e);
        }
        return result;
    }();
    return table;
}
template <class E> struct value_codec<E, typename std::enable_if<named_enum<E>::value>::type> {
    static constexpr node_kind kind = node_kind::scalar;
    static json_enumerator_descriptor<E> const *find(E value) {
        for (auto const &e : enumerators<E>()) if (e.value == value) return &e;
        return nullptr;
    }
    static bool read(read_context &, E &out, atom const &value) {
        if (value.kind != atom_kind::string) return false;
        for (auto const &e : enumerators<E>())
            if (e.name_size == value.size && std::memcmp(e.name, value.text, value.size) == 0) { out = e.value; return true; }
        return false;
    }
    static bool write(write_context &context, E value) {
        auto const *e = find(value);
        if (!e || e->name_size > context.limits.max_string_bytes) return false;
        atom a; a.kind = atom_kind::string; a.text = e->name; a.size = e->name_size; return context.sink.value(a);
    }
    static void reset(read_context &, E &value) noexcept { value = E{}; }
    static std::size_t bound(E value, json_limits const &, std::size_t) {
        auto const *e = find(value);
        if (!e) throw std::invalid_argument{"JSON enum value without a name"};
        return string_bound(e->name_size);
    }
};

// Values that may be absent: null or a missing field reads as empty, and an
// empty field is left out of its object (null inside arrays and maps).
template <class T> struct nullable_traits {};
template <class T> struct nullable_traits<std::unique_ptr<T>> {
    using value_type = T;
    static bool engaged(std::unique_ptr<T> const &value) noexcept { return value != nullptr; }
    static T const &get(std::unique_ptr<T> const &value) noexcept { return *value; }
    static T &get(std::unique_ptr<T> &value) noexcept { return *value; }
    static T &engage(read_context &context, std::unique_ptr<T> &value) {
        if (!value) {
            context.charge(sizeof(T));
            value.reset(new T{});
        }
        return *value;
    }
    static void clear(std::unique_ptr<T> &value) noexcept { value.reset(); }
};
#if defined(RPC_JSON_OPTIONAL)
template <class T> struct nullable_traits<std::optional<T>> {
    using value_type = T;
    static bool engaged(std::optional<T> const &value) noexcept { return value.has_value(); }
    static T const &get(std::optional<T> const &value) noexcept { return *value; }
    static T &get(std::optional<T> &value) noexcept { return *value; }
    static T &engage(read_context &, std::optional<T> &value) {
        if (!value) value.emplace();
        return *value;
    }
    static void clear(std::optional<T> &value) noexcept { value.reset(); }
};
#endif
template <class T, class = void> struct nullable : std::false_type {};
template <class T> struct nullable<T, void_t<typename nullable_traits<T>::value_type>> : std::true_type {};

template <class N> struct value_codec<N, typename std::enable_if<nullable<N>::value>::type> {
    using traits = nullable_traits<N>;
    using inner = value_codec<typename traits::value_type>;
    static constexpr node_kind kind = inner::kind;
    static bool read(read_context &context, N &out, atom const &value) {
        if (value.kind == atom_kind::null) {
            traits::clear(out);
            return true;
        }
        return inner::read(context, traits::engage(context, out), value);
    }
    static bool write(write_context &context, N const &value) {
        if (!traits::engaged(value)) return context.sink.value(atom{});
        return inner::write(context, traits::get(value));
    }
    // An object or array is arriving: it fills a fresh value.
    static void reset(read_context &context, N &value) { inner::reset(context, traits::engage(context, value)); }
    static std::size_t bound(N const &value, json_limits const &limits, std::size_t depth) {
        return traits::engaged(value) ? inner::bound(traits::get(value), limits, depth) : 4;
    }
};

template <class T, class = void> struct presence {
    static bool present(T const &) noexcept { return true; }
};
template <class N> struct presence<N, typename std::enable_if<nullable<N>::value>::type> {
    static bool present(N const &value) noexcept { return nullable_traits<N>::engaged(value); }
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
    static void reset(read_context &, T &value) {
        value = T{};
    }
    static bool write(write_context &context, T const &value) {
        static_cast<void>(operations<T>());
        depth_guard guard{context};
        if (!guard.valid() || count > context.limits.max_object_fields || !context.sink.start_object()) return false;
        bool okay = true;
        each(fields<T>(), [&](auto const &f, std::size_t) {
            using member_type = typename std::decay<decltype(value.*f.member)>::type;
            if (!okay || !presence<member_type>::present(value.*f.member)) return;
            okay = f.name_size <= context.limits.max_string_bytes && context.sink.key(f.name, f.name_size) &&
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
            if (!presence<member_type>::present(value.*f.member)) return;
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
    static void reset(read_context &, vector_type &value) { value.clear(); }
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

// An object with arbitrary keys; entries count against max_object_fields.
template <class T> struct string_map : std::false_type {};
template <class T, class Compare, class Allocator>
struct string_map<std::map<std::string, T, Compare, Allocator>> : std::true_type {};
template <class T, class Hash, class Equal, class Allocator>
struct string_map<std::unordered_map<std::string, T, Hash, Equal, Allocator>> : std::true_type {};
template <class M> struct value_codec<M, typename std::enable_if<string_map<M>::value>::type> {
    using mapped_type = typename M::mapped_type;
    static constexpr node_kind kind = node_kind::object;
    static bool read(read_context &, M &, atom const &) { return false; }
    static node entry(read_context &context, M &value, char const *name, std::size_t size) {
        context.charge(add_bound(size, sizeof(typename M::value_type)));
        auto const inserted = value.emplace(std::piecewise_construct, std::forward_as_tuple(name, size), std::forward_as_tuple());
        return inserted.second ? make_node(inserted.first->second) : node{};
    }
    static void reset(read_context &, M &value) { value.clear(); }
    static bool write(write_context &context, M const &value) {
        depth_guard guard{context};
        if (!guard.valid() || value.size() > context.limits.max_object_fields || !context.sink.start_object()) return false;
        for (auto const &item : value)
            if (item.first.size() > context.limits.max_string_bytes || !context.sink.key(item.first.data(), item.first.size()) ||
                !value_codec<mapped_type>::write(context, item.second)) return false;
        return context.sink.end_object();
    }
    static std::size_t bound(M const &value, json_limits const &limits, std::size_t depth) {
        if (depth >= limits.max_depth || value.size() > limits.max_object_fields) throw std::invalid_argument{"JSON object limit"};
        std::size_t size = 2;
        for (auto const &item : value) {
            if (item.first.size() > limits.max_string_bytes) throw std::invalid_argument{"JSON key limit"};
            size = add_bound(size, add_bound(string_bound(item.first.size()),
                                             add_bound(2, value_codec<mapped_type>::bound(item.second, limits, depth + 1))));
        }
        return size;
    }
};

template <class T, class = void> struct container_access {
    static constexpr bool keyed = false;
    static node field(void *, char const *, std::size_t, std::size_t &) { return {}; }
    static node element(read_context &, void *) { return {}; }
    static node entry(read_context &, void *, char const *, std::size_t) { return {}; }
    static std::uint64_t required() { return 0; }
    static void validate() {}
};
template <class T> struct container_access<T, typename std::enable_if<mapped<T>::value>::type> {
    static constexpr bool keyed = false;
    static node field(void *target, char const *name, std::size_t size, std::size_t &index) {
        return value_codec<T>::field(*static_cast<T *>(target), name, size, index);
    }
    static node element(read_context &, void *) { return {}; }
    static node entry(read_context &, void *, char const *, std::size_t) { return {}; }
    static std::uint64_t required() { return value_codec<T>::required(); }
    static void validate() { value_codec<T>::validate(); }
};
template <class T, class Allocator> struct container_access<std::vector<T, Allocator>> {
    static constexpr bool keyed = false;
    static node field(void *, char const *, std::size_t, std::size_t &) { return {}; }
    static node element(read_context &context, void *target) {
        return value_codec<std::vector<T, Allocator>>::element(context, *static_cast<std::vector<T, Allocator> *>(target));
    }
    static node entry(read_context &, void *, char const *, std::size_t) { return {}; }
    static std::uint64_t required() { return 0; }
    static void validate() {}
};
template <class M> struct container_access<M, typename std::enable_if<string_map<M>::value>::type> {
    static constexpr bool keyed = true;
    static node field(void *, char const *, std::size_t, std::size_t &) { return {}; }
    static node element(read_context &, void *) { return {}; }
    static node entry(read_context &context, void *target, char const *name, std::size_t size) {
        return value_codec<M>::entry(context, *static_cast<M *>(target), name, size);
    }
    static std::uint64_t required() { return 0; }
    static void validate() {}
};
// An object or array reaches a nullable only after reset has engaged it.
template <class N> struct container_access<N, typename std::enable_if<nullable<N>::value>::type> {
    using traits = nullable_traits<N>;
    using inner = container_access<typename traits::value_type>;
    static constexpr bool keyed = inner::keyed;
    static void *held(void *target) noexcept { return &traits::get(*static_cast<N *>(target)); }
    static node field(void *target, char const *name, std::size_t size, std::size_t &index) {
        return inner::field(held(target), name, size, index);
    }
    static node element(read_context &context, void *target) { return inner::element(context, held(target)); }
    static node entry(read_context &context, void *target, char const *name, std::size_t size) {
        return inner::entry(context, held(target), name, size);
    }
    static std::uint64_t required() { return inner::required(); }
    static void validate() { inner::validate(); }
};
template <class T> node_ops const &operations() {
    static node_ops const value = [] {
        container_access<T>::validate();
        return node_ops{value_codec<T>::kind,
            [](read_context &context, void *target, atom const &a) { return value_codec<T>::read(context, *static_cast<T *>(target), a); },
            &container_access<T>::field, &container_access<T>::element, &container_access<T>::required,
            [](read_context &context, void *target) { value_codec<T>::reset(context, *static_cast<T *>(target)); },
            container_access<T>::keyed ? &container_access<T>::entry : nullptr};
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
    template <class Message> static char const *label() noexcept { return "json"; }
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

// Convenience mappings, used at namespace scope outside namespace rpc.
#define RPC_DETAIL_JSON_FIELD(Type, Field) ::rpc::json_field(#Field, &Type::Field)
#define RPC_DETAIL_JSON_ENUMERATOR(Type, Value) ::rpc::json_enumerator(#Value, Type::Value)
#define RPC_DETAIL_JSON_JOIN_IMPL(A, B) A##B
#define RPC_DETAIL_JSON_JOIN(A, B) RPC_DETAIL_JSON_JOIN_IMPL(A, B)
#define RPC_DETAIL_JSON_COUNT_IMPL(_1,_2,_3,_4,_5,_6,_7,_8,_9,_10,_11,_12,_13,_14,_15,_16,_17,_18,_19,_20,_21,_22,_23,_24,_25,_26,_27,_28,_29,_30,_31,_32,_33,_34,_35,_36,_37,_38,_39,_40,_41,_42,_43,_44,_45,_46,_47,_48,_49,_50,_51,_52,_53,_54,_55,_56,_57,_58,_59,_60,_61,_62,_63,_64,N,...) N
#define RPC_DETAIL_JSON_COUNT(...) RPC_DETAIL_JSON_COUNT_IMPL(__VA_ARGS__,64,63,62,61,60,59,58,57,56,55,54,53,52,51,50,49,48,47,46,45,44,43,42,41,40,39,38,37,36,35,34,33,32,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0)
#define RPC_DETAIL_JSON_EACH_1(M,T,A) M(T,A)
#define RPC_DETAIL_JSON_EACH_2(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_1(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_3(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_2(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_4(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_3(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_5(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_4(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_6(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_5(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_7(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_6(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_8(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_7(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_9(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_8(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_10(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_9(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_11(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_10(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_12(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_11(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_13(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_12(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_14(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_13(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_15(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_14(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_16(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_15(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_17(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_16(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_18(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_17(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_19(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_18(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_20(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_19(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_21(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_20(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_22(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_21(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_23(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_22(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_24(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_23(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_25(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_24(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_26(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_25(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_27(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_26(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_28(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_27(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_29(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_28(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_30(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_29(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_31(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_30(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_32(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_31(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_33(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_32(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_34(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_33(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_35(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_34(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_36(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_35(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_37(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_36(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_38(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_37(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_39(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_38(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_40(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_39(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_41(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_40(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_42(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_41(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_43(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_42(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_44(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_43(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_45(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_44(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_46(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_45(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_47(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_46(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_48(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_47(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_49(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_48(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_50(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_49(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_51(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_50(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_52(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_51(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_53(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_52(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_54(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_53(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_55(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_54(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_56(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_55(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_57(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_56(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_58(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_57(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_59(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_58(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_60(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_59(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_61(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_60(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_62(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_61(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_63(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_62(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH_64(M,T,A,...) M(T,A), RPC_DETAIL_JSON_EACH_63(M,T,__VA_ARGS__)
#define RPC_DETAIL_JSON_EACH(M, T, ...) RPC_DETAIL_JSON_JOIN(RPC_DETAIL_JSON_EACH_, RPC_DETAIL_JSON_COUNT(__VA_ARGS__))(M, T, __VA_ARGS__)
// 1 to 64 fields, mapped under their member names.
#define RPC_JSON_FIELDS(Type, ...) \
    template <> struct rpc::json_traits<Type> { \
        static auto fields() { return std::make_tuple(RPC_DETAIL_JSON_EACH(RPC_DETAIL_JSON_FIELD, Type, __VA_ARGS__)); } \
    }
// 1 to 64 enumerators, carried as their names.
#define RPC_JSON_ENUM(Type, ...) \
    template <> struct rpc::json_enum_traits<Type> { \
        static std::vector<::rpc::json_enumerator_descriptor<Type>> values() { \
            return {RPC_DETAIL_JSON_EACH(RPC_DETAIL_JSON_ENUMERATOR, Type, __VA_ARGS__)}; \
        } \
    }
