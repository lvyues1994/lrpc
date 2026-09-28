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

struct codec_ops {
    std::size_t (*size)(void const *);
    bool (*encode)(void const *, wire::mutable_bytes_view);
    bool (*decode)(wire::bytes_view, void *);
};

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

struct encoded_request {
    void const *message = nullptr;
    codec_ops const *operations = nullptr;
};

struct decoded_response {
    void *message = nullptr;
    codec_ops const *operations = nullptr;
};

} // namespace rpc
