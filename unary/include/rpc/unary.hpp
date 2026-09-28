#pragma once

#include <rpc/wire.hpp>
#include <rpc/codec.hpp>
#include <net/any_stream.hpp>
#include <net/io_context.hpp>
#include <net/task.hpp>
#include <net/tcp.hpp>

#include <chrono>
#include <limits>
#include <memory>
#include <string>
#include <vector>

namespace rpc {

using clock = std::chrono::steady_clock;

enum class status_code : std::uint8_t {
    ok = 0, cancelled = 1, unknown = 2, invalid_argument = 3, deadline_exceeded = 4,
    not_found = 5, already_exists = 6, permission_denied = 7, resource_exhausted = 8,
    failed_precondition = 9, aborted = 10, out_of_range = 11, unimplemented = 12,
    internal = 13, unavailable = 14, data_loss = 15, unauthenticated = 16,
};

struct call_options {
    clock::time_point deadline = clock::time_point::max();
    std::chrono::microseconds timeout{0}; // Zero: no additional relative deadline.
    wire::metadata_list metadata{}; // Borrowed, immutable until call completion.
    // Optional storage for encoded metadata entries (without count prefix).
    // Default {nullptr, 0} discards it. A supplied undersized buffer yields
    // resource_exhausted. Storage must survive the call and any returned views.
    // It must be disjoint from the reply object's/raw response buffer's storage.
    wire::mutable_bytes_view response_metadata{};
};

struct call_result {
    status_code code = status_code::unknown;
    std::size_t response_size = 0;
    wire::metadata_view response_metadata{}; // Points into call_options storage.
};

// Immediately encodes/copies metadata into a pre-reserved END head. Sources
// must be disjoint from this storage. Any failed assign is sticky and causes
// the server to send an empty END(internal), even if the handler returns ok.
class metadata_writer {
public:
    metadata_writer() noexcept = default;
    explicit metadata_writer(wire::mutable_bytes_view storage) noexcept;
    bool assign(wire::metadata_list entries) noexcept;
    std::size_t size() const noexcept;
    bool overflowed() const noexcept;
private:
    wire::mutable_bytes_view storage_{};
    std::size_t size_ = 2;
    bool overflowed_ = false;
};

struct server_context {
    clock::time_point deadline = clock::time_point::max();
    net::stop_token stop_token{};
    wire::metadata_view metadata{};
    metadata_writer response_metadata{};
};

// A borrowed writer over pre-reserved response storage. A rejected assign or
// commit permanently marks this response oversized, yielding END(internal).
class response_writer {
public:
    explicit response_writer(wire::mutable_bytes_view storage) noexcept;
    wire::mutable_bytes_view buffer() const noexcept;
    bool assign(wire::bytes_view bytes) noexcept;
    bool commit(std::size_t size) noexcept;
    std::size_t size() const noexcept;
    bool overflowed() const noexcept;
private:
    wire::mutable_bytes_view storage_{};
    std::size_t size_ = 0;
    bool overflowed_ = false;
};

struct method_handler {
    virtual ~method_handler() = default;
    virtual net::task<status_code> invoke(server_context &context, wire::bytes_view request,
                                          response_writer &response) = 0;
};

struct method_binding {
    std::string name{};
    // Must be set explicitly; zero declares an empty response.
    std::size_t max_response_bytes = std::numeric_limits<std::size_t>::max();
    method_handler *handler = nullptr; // Borrowed until all server work has drained.
    // Complete encoded END head, including its two empty-field prefixes.
    std::size_t max_response_head_bytes = 2;
    // Optional ownership for generated adapters. The service itself is borrowed.
    std::shared_ptr<method_handler> owned_handler{};
};

struct connection_options {
    wire::settings receive{64U * 1024U, 64U * 1024U, 64, 0, 256, 0};
    std::size_t max_method_name_bytes = 256;
    std::chrono::milliseconds handshake_timeout{5000};
    // Fixed read window, independent of the accepted wire frame size.
    // Zero keeps max_frame_size + 16; explicit values must be between that
    // size and 16 MiB + 16. The window is allocated once per connection.
    std::size_t receive_buffer_bytes = 0;
};

struct client_options {
    connection_options connection{};
    std::size_t request_bytes = 8U * 1024U * 1024U;
    std::size_t control_bytes = 16U * 1024U;
};

struct server_options {
    connection_options connection{};
    std::size_t max_connections = 64;
    std::size_t max_active_calls = 256;
    std::size_t request_bytes = 32U * 1024U * 1024U;
    std::size_t response_bytes = 16U * 1024U * 1024U;
    std::size_t control_bytes = 128U * 1024U;
    std::size_t request_bytes_per_connection = 8U * 1024U * 1024U;
    std::size_t response_bytes_per_connection = 8U * 1024U * 1024U;
    std::size_t control_bytes_per_connection = 16U * 1024U;
};

struct resource_stats {
    // Server: all live sessions, including closed ones retained by pending work.
    // Client: 1 while ready/draining, otherwise 0.
    std::size_t connections = 0;
    std::size_t active_calls = 0;
    std::size_t request_bytes_in_use = 0;
    std::size_t response_bytes_in_use = 0;
    std::size_t control_bytes_in_use = 0;
    std::size_t storage_bytes = 0; // Pools' allocated payload + block descriptors, including idle blocks.
};

// Owns the byte stream. close() cancels pending I/O but does not permit buffer
// destruction until completion. Also serves as the deterministic test seam.
struct transport {
    virtual ~transport() = default;
    virtual net::any_stream &stream() noexcept = 0;
    virtual void close() noexcept = 0;
};
std::unique_ptr<transport> make_tcp_transport(net::tcp_socket socket);

// All APIs (including destruction) run on the owning shard, or before/after
// ctx.run() on that same thread. Use a parent's net stop_token for cancellation;
// requesting that token from another thread is supported. Other cross-thread
// operations are not. ctx must outlive all work; close(), then drain ctx.run().
struct client {
    virtual ~client() = default;
    virtual net::task<status_code> connect(net::ip::tcp::endpoint endpoint) = 0;
    // Alternative to connect for an already-connected owned transport.
    virtual net::task<status_code> attach(std::unique_ptr<transport> stream) = 0;
    // Request/reply storage is borrowed until completion. No admission queue,
    // implicit connection establishment, reconnect or automatic retry.
    virtual net::task<call_result> call(std::string method, wire::bytes_view request,
                                       wire::mutable_bytes_view response, call_options options = {}) = 0;
    // Same admission/deadline/cancellation path. Messages and operations are
    // borrowed until completion. Failed response decoding may modify the reply.
    virtual net::task<call_result> call_encoded(std::string method, encoded_request request,
                                               decoded_response response, call_options options = {}) = 0;
    virtual bool ready() const noexcept = 0;
    virtual resource_stats stats() const noexcept = 0;
    virtual void close() noexcept = 0;
};

struct server {
    virtual ~server() = default;
    // Configuration/listen errors throw before accepting work.
    virtual net::ip::tcp::endpoint listen(net::ip::tcp::endpoint endpoint) = 0;
    virtual bool attach(std::unique_ptr<transport> stream) = 0;
    // Stop admitting calls, send GOAWAY, finish admitted calls and responses.
    // close() is the immediate cooperative cancellation path.
    virtual void drain() noexcept = 0;
    virtual resource_stats stats() const noexcept = 0;
    virtual void close() noexcept = 0;
};

std::unique_ptr<client> make_client(net::io_context &context, client_options options = {});
std::unique_ptr<server> make_server(net::io_context &context, std::vector<method_binding> methods,
                                   server_options options = {});

} // namespace rpc
