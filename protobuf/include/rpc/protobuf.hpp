#pragma once

#include <rpc/typed.hpp>
#include <google/protobuf/arena.h>
#include <google/protobuf/message_lite.h>

#include <array>
#include <limits>
#include <type_traits>

namespace rpc {

template <class Message>
struct codec<Message, typename std::enable_if<std::is_base_of<google::protobuf::MessageLite, Message>::value>::type> {
    static std::size_t size(Message const &value) { return value.ByteSizeLong(); }
    static bool encode(Message const &value, wire::mutable_bytes_view output) {
        if (output.size > static_cast<std::size_t>(std::numeric_limits<int>::max()) ||
            (output.size != 0 && output.data == nullptr) || !value.IsInitialized() ||
            value.GetCachedSize() < 0 || static_cast<std::size_t>(value.GetCachedSize()) != output.size) return false;
        if (output.size == 0) return true;
        return value.SerializeWithCachedSizesToArray(output.data) == output.data + output.size;
    }
    static bool decode(wire::bytes_view input, Message &value) {
        if (input.size > static_cast<std::size_t>(std::numeric_limits<int>::max()) ||
            (input.size != 0 && input.data == nullptr)) return false;
        // Avoid depending on whether a particular protobuf release accepts a
        // null pointer for a zero-length array.
        static std::uint8_t const empty = 0;
        return value.ParseFromArray(input.size == 0 ? &empty : input.data, static_cast<int>(input.size));
    }
};

// Owns messages, their Arena and its initial block in destruction order.
// Its stable address is required; it lives in a nonmoving handler task frame.
class call_arena {
public:
    call_arena() : arena_(initial_.data(), initial_.size()) {}
    call_arena(call_arena const &) = delete;
    call_arena &operator=(call_arena const &) = delete;
    call_arena(call_arena &&) = delete;
    call_arena &operator=(call_arena &&) = delete;
    google::protobuf::Arena &get() noexcept { return arena_; }
private:
    alignas(std::max_align_t) std::array<char, 2048> initial_{};
    google::protobuf::Arena arena_;
};

namespace detail {

template <class Request, class Response>
class message_storage<Request, Response, typename std::enable_if<
    std::is_base_of<google::protobuf::MessageLite, Request>::value &&
    std::is_base_of<google::protobuf::MessageLite, Response>::value>::type> {
public:
    static constexpr bool reusable = true;
    message_storage() : request_(google::protobuf::Arena::CreateMessage<Request>(&arena_.get())),
                        response_(google::protobuf::Arena::CreateMessage<Response>(&arena_.get())) {}
    Request &request() noexcept { return *request_; }
    Response &response() noexcept { return *response_; }
    void reset() {
        arena_.get().Reset();
        request_ = google::protobuf::Arena::CreateMessage<Request>(&arena_.get());
        response_ = google::protobuf::Arena::CreateMessage<Response>(&arena_.get());
    }
private:
    call_arena arena_{};
    Request *request_;
    Response *response_;
};

} // namespace detail
} // namespace rpc
