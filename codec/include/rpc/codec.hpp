#pragma once

#include <rpc/wire.hpp>

namespace rpc {

// Specializations provide size, encode and decode. encode receives exactly the
// size returned by size(); the message must not change between these calls.
// decode may modify its destination on failure. Exceptions are translated at
// the unary boundary; false means invalid input/output, not an exception.
// decode input is borrowed only for that function invocation; the destination
// must own any retained contents, never a view into the receive buffer.
// These are synchronous callbacks: do not recursively run/poll the owning
// io_context or wait for work on it. Synchronous client.close() is supported.
template <class Message, class = void> struct codec;
class byte_buffer;
struct owned_codec_ops {
    // encode must produce size(message) bytes. The caller validates retained
    // capacity against its budget before taking shared ownership. decode may
    // retain owned storage; borrowed decode above must still copy retained data.
    bool (*encode)(void const *, byte_buffer &);
    bool (*decode)(byte_buffer const &, void *);
};
namespace detail {
template <class Message> struct owned_codec_for { static owned_codec_ops const *get() noexcept { return nullptr; } };
}

struct codec_ops {
    std::size_t (*size)(void const *);
    bool (*encode)(void const *, wire::mutable_bytes_view);
    bool (*decode)(wire::bytes_view, void *);
    owned_codec_ops const *owned = nullptr;
    // Optional one-pass encoding into already budgeted capacity. On failure
    // written is zero and the output is discarded. upper_bound is cheap and
    // conservative; it must not serialize or cache the message.
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
        }, detail::owned_codec_for<Message>::get()};
    return operations;
}

struct default_codec_policy {
    template <class Message> static codec_ops const &operations() { return codec_for<Message>(); }
};

struct encoded_request {
    void const *message = nullptr;
    codec_ops const *operations = nullptr;
};

struct decoded_response {
    void *message = nullptr;
    codec_ops const *operations = nullptr;
};

} // namespace rpc
