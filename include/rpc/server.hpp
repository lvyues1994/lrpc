#pragma once

#include <rpc/options.hpp>
#include <rpc/shard.hpp>
#include <rpc/status.hpp>
#include <rpc/stream.hpp>
#include <rpc/transport.hpp>

#include <net/task.hpp>
#include <net/tcp.hpp>

#include <memory>
#include <string>
#include <vector>

namespace rpc {

namespace detail {
struct response_state;
struct server_access;
} // namespace detail

// Valid until the handler completes. Do not keep copies of the stop token
// beyond that: an unrequested stop state is reused by later calls.
struct server_context {
    clock::time_point deadline = clock::time_point::max();
    net::stop_token stop_token{};
    wire::metadata_view metadata{};

    // As response_writer::set_trailer, for handlers that only see the context.
    bool set_trailer(wire::bytes_view message, wire::metadata_list metadata = {}) noexcept;

private:
    friend struct detail::server_access;
    detail::response_state *response_ = nullptr;
};

// Response storage owned by the call, bounded by the method's declared
// maximum. Use it on the shard's thread.
class response_writer {
public:
    bool assign(wire::bytes_view bytes) noexcept;
    wire::mutable_bytes_view prepare(std::size_t size) noexcept; // Empty on failure.
    bool commit(std::size_t size) noexcept;
    std::size_t size() const noexcept;
    std::size_t max_size() const noexcept; // The method's max_response_bytes.
    // Metadata sent with any status, and a message sent only with an error.
    // Copied at once and bounded by the method's max_trailer_bytes; a later
    // call replaces an earlier one.
    bool set_trailer(wire::bytes_view message, wire::metadata_list metadata = {}) noexcept;

private:
    friend struct detail::server_access;
    detail::response_state *call_ = nullptr;
};

struct method_handler {
    virtual ~method_handler() = default;
    // The request bytes stay valid until the returned task completes.
    virtual net::task<status_code> invoke(server_context &context, wire::bytes_view request,
                                          response_writer &response) = 0;
};

// For every kind but unary. The END is sent when the returned task
// completes: client_streaming must have written exactly one message, and
// server_streaming must have read one message and the end of the stream.
struct stream_method_handler {
    virtual ~stream_method_handler() = default;
    virtual net::task<status_code> invoke(server_context &context, server_stream &stream) = 0;
};

struct method_binding {
    std::string name{};
    method_handler *handler = nullptr; // Borrowed until the server has drained; unary only.
    // For unary calls both are charged to the response ledger for every
    // admitted call, so they bound concurrency as well as the reply. For
    // streams the first bounds each message written.
    std::size_t max_response_bytes = 0;
    std::size_t max_trailer_bytes = 0; // Encoded END head; zero allows no trailer.
    method_kind kind = method_kind::unary;
    stream_method_handler *stream_handler = nullptr; // Borrowed; every other kind.
    std::shared_ptr<void> owner{}; // Kept alive with the server, e.g. a typed adapter.
};

struct server_options {
    connection_options connection{};
    std::size_t max_connections = 1024;
    std::size_t max_active_calls = 1024;
    // Admission ledger: copied request bytes, and each method's declared
    // maximum for its response. Pure counters; memory is taken on use.
    std::size_t max_request_bytes = 64U << 20;
    std::size_t max_response_bytes = 64U << 20;
};

struct server_stats {
    std::size_t connections = 0;
    std::size_t active_calls = 0;
    std::size_t request_bytes = 0;
    std::size_t response_bytes = 0;
};

namespace detail {
struct server_core;
}

// Serves unary calls on one shard. All members, including the destructor,
// run on the shard's thread.
class server {
public:
    server(shard &owner, std::vector<method_binding> methods, server_options options = {});
    ~server();
    server(server const &) = delete;
    server &operator=(server const &) = delete;

    net::ip::tcp::endpoint listen(net::ip::tcp::endpoint endpoint);
    bool attach(std::unique_ptr<transport> link);
    void drain() noexcept; // GOAWAY, finish admitted calls, then close each connection.
    void close() noexcept; // Cancel everything.
    server_stats stats() const noexcept;

private:
    std::shared_ptr<detail::server_core> core_;
};

} // namespace rpc
