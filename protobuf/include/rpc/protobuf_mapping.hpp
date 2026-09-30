#pragma once

#include <rpc/protobuf.hpp>

namespace rpc {

// Explicit mapping to a generated .proto type. Specializations provide:
//   using message_type = ...;
//   bool to_protobuf(T const &, message_type &);
//   bool from_protobuf(message_type const &, T &);
//   size_t upper_bound(T const &); // cheap conservative wire byte bound
// Field numbers, presence, validation and defaults remain the mapping's choice.
// No implicit relationship is inferred from JSON field names or order.
// Conversions are synchronous and may throw; false means invalid data. Decoded
// values must own all retained data: the temporary Arena dies before return.
// from_protobuf may modify its destination on failure; unknown fields are lost.
template <class Message> struct protobuf_mapping;

template <class Message> struct mapped_protobuf_codec {
    using mapping = protobuf_mapping<Message>;
    using wire_type = typename mapping::message_type;
    static_assert(std::is_base_of<google::protobuf::MessageLite, wire_type>::value,
                  "protobuf mappings require a generated protobuf message");
    static_assert(std::is_same<decltype(mapping::to_protobuf(std::declval<Message const &>(), std::declval<wire_type &>())), bool>::value &&
                  std::is_same<decltype(mapping::from_protobuf(std::declval<wire_type const &>(), std::declval<Message &>())), bool>::value,
                  "protobuf mapping conversions must return bool");

    static std::size_t size(Message const &value) {
        call_arena arena;
        auto &message = *google::protobuf::Arena::CreateMessage<wire_type>(&arena.get());
        if (!mapping::to_protobuf(value, message) || !message.IsInitialized())
            throw std::invalid_argument{"invalid protobuf mapping"};
        return message.ByteSizeLong();
    }
    static wire::encode_result encode_bounded(Message const &value, wire::mutable_bytes_view output) {
        call_arena arena;
        auto &message = *google::protobuf::Arena::CreateMessage<wire_type>(&arena.get());
        if (!mapping::to_protobuf(value, message) || !message.IsInitialized()) return {wire::error::invalid_argument, 0};
        auto const bytes = message.ByteSizeLong();
        if (bytes > output.size) return {wire::error::output_too_small, 0};
        if (!codec<wire_type>::encode(message, {output.data, bytes})) return {wire::error::invalid_argument, 0};
        return {wire::error::none, bytes};
    }
    static bool encode(Message const &value, wire::mutable_bytes_view output) {
        auto const result = encode_bounded(value, output);
        return result.code == wire::error::none && result.written == output.size;
    }
    static bool decode(wire::bytes_view input, Message &value) {
        call_arena arena;
        auto &message = *google::protobuf::Arena::CreateMessage<wire_type>(&arena.get());
        return codec<wire_type>::decode(input, message) && mapping::from_protobuf(message, value);
    }
    static codec_ops const &operations() {
        static codec_ops const ops{
            [](void const *p) { return size(*static_cast<Message const *>(p)); },
            [](void const *p, wire::mutable_bytes_view out) { return encode(*static_cast<Message const *>(p), out); },
            [](wire::bytes_view in, void *p) { return decode(in, *static_cast<Message *>(p)); },
            [](void const *p, wire::mutable_bytes_view out) { return encode_bounded(*static_cast<Message const *>(p), out); },
            [](void const *p) { return mapping::upper_bound(*static_cast<Message const *>(p)); }};
        return ops;
    }
};

struct mapped_protobuf_codec_policy {
    template <class Message> static codec_ops const &operations() { return mapped_protobuf_codec<Message>::operations(); }
};

} // namespace rpc
