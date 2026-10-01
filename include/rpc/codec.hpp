#pragma once

#include <rpc/wire.hpp>

#include <cstddef>

namespace rpc {

// Specializations provide size, encode and decode. encode receives exactly the
// size returned by size(); the message must not change between these calls.
// decode may modify its destination on failure, and must copy what it keeps:
// its input is borrowed only for that call. false means invalid data;
// exceptions are translated at the RPC boundary. Codecs run synchronously on
// the shard's thread and must not wait on it.
template <class Message, class = void> struct codec;

struct codec_ops {
    std::size_t (*size)(void const *);
    bool (*encode)(void const *, wire::mutable_bytes_view);
    bool (*decode)(wire::bytes_view, void *);
    // Optional one-pass encoding into a cheap, conservative upper bound;
    // upper_bound must not serialize. On failure written is zero.
    wire::encode_result (*encode_bounded)(void const *, wire::mutable_bytes_view) = nullptr;
    std::size_t (*upper_bound)(void const *) = nullptr;
};

inline wire::encode_result encode_into(codec_ops const &operations, void const *message,
                                        wire::mutable_bytes_view output) {
    if (operations.encode_bounded) return operations.encode_bounded(message, output);
    auto const size = operations.size(message);
    if (size > output.size) return {wire::error::output_too_small, 0};
    if (!operations.encode(message, {output.data, size})) return {wire::error::invalid_argument, 0};
    return {wire::error::none, size};
}

template <class Message> codec_ops const &codec_for() {
    static codec_ops const operations{
        [](void const *value) { return codec<Message>::size(*static_cast<Message const *>(value)); },
        [](void const *value, wire::mutable_bytes_view bytes) {
            return codec<Message>::encode(*static_cast<Message const *>(value), bytes);
        },
        [](wire::bytes_view bytes, void *value) {
            return codec<Message>::decode(bytes, *static_cast<Message *>(value));
        }};
    return operations;
}

namespace detail {
template <class...> using codec_void_t = void;
template <class Message, class = void> struct codec_label_of {
    static char const *get() noexcept { return ""; }
};
template <class Message> struct codec_label_of<Message, codec_void_t<decltype(codec<Message>::label())>> {
    static char const *get() noexcept { return codec<Message>::label(); }
};
template <class Message, class Policy, class = void> struct policy_label {
    static char const *get() noexcept { return ""; }
};
template <class Message, class Policy>
struct policy_label<Message, Policy, codec_void_t<decltype(Policy::template label<Message>())>> {
    static char const *get() noexcept { return Policy::template label<Message>(); }
};
} // namespace detail

// A policy's label() names its encoding on the wire (see method_binding::codec).
struct default_codec_policy {
    template <class Message> static codec_ops const &operations() { return codec_for<Message>(); }
    // A codec<Message> specialization may provide static char const *label().
    template <class Message> static char const *label() noexcept { return detail::codec_label_of<Message>::get(); }
};

// The label Policy gives Message, or "" for a policy without labels.
template <class Message, class Policy = default_codec_policy> char const *codec_label() noexcept {
    return detail::policy_label<Message, Policy>::get();
}

} // namespace rpc
