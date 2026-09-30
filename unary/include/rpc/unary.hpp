#pragma once

#include <rpc/wire.hpp>
#include <rpc/codec.hpp>
#include <rpc/status.hpp>
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

struct method_descriptor;
struct service_descriptor;
struct byte_stream;
struct stream_open_result;
struct stream_method_handler;
enum class method_kind { unary, client_streaming, server_streaming, bidirectional };
namespace detail { struct client_state; struct channel_state; struct runtime_channel_state; struct stream_peer; }

// Cold-bound identity, distinct from a connection's wire method ID. A handle
// can only be used with the client that created it.
class method_handle {
public:
    method_handle() noexcept = default;
    explicit operator bool() const noexcept { return owner_ != 0; }
private:
    friend struct detail::client_state;
    friend struct detail::channel_state;
    friend struct detail::runtime_channel_state;
    friend struct detail::stream_peer;
    method_handle(std::uint64_t owner, std::size_t index) noexcept : owner_(owner), index_(index) {}
    std::uint64_t owner_ = 0;
    std::size_t index_ = 0;
};

using clock = std::chrono::steady_clock;

struct retry_policy {
    std::uint32_t max_attempts = 1;
    std::chrono::milliseconds initial_backoff{10};
    std::chrono::milliseconds max_backoff{1000};
    double multiplier = 2.0;
    std::uint32_t retryable_codes = 1U << static_cast<unsigned>(status_code::unavailable);
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
    bool wait_for_ready = false; // channel only; requires a finite deadline and configured queue capacity.
    bool wait_for_capacity = false; // Independent of waiting for a connection.
    retry_policy retry{}; // channel only; one attempt by default.
};

struct call_result : status {
    call_result(status_code code = status_code::unknown, std::size_t size = 0, wire::metadata_view metadata = {})
        : status{code, {}}, response_size(size), response_metadata(metadata) {}
    std::size_t response_size = 0;
    wire::metadata_view response_metadata{}; // Points into call_options storage.
    bool not_executed = false; // Transport submission was avoided, or peer explicitly proved rejection.
    std::uint32_t attempts = 1;
    bool capacity_rejected = false; // Temporary LOCAL shortage, never inferred from a peer status.
};

// Immediately encodes/copies metadata into a pre-reserved END head. Sources
// must be disjoint from this storage. Any failed assign is sticky and causes
// the server to send an empty END(internal), even if the handler returns ok.
class metadata_writer {
public:
    metadata_writer() noexcept = default;
    explicit metadata_writer(wire::mutable_bytes_view storage) noexcept;
    bool assign(wire::metadata_list entries) noexcept;
    bool assign_message(wire::bytes_view message) noexcept;
    bool has_message() const noexcept;
    std::size_t size() const noexcept;
    bool overflowed() const noexcept;
private:
    wire::mutable_bytes_view storage_{};
    std::size_t size_ = 2;
    std::size_t message_end_ = 1;
    bool overflowed_ = false;
};

struct server_context {
    clock::time_point deadline = clock::time_point::max();
    net::stop_token stop_token{};
    wire::metadata_view metadata{};
    metadata_writer response_metadata{};
};
// Bridges an owning status to the existing status_code handler API. Text is
// immediately copied into the same bounded END head as response metadata.
status_code set_status(server_context &context, status const &result) noexcept;

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
    virtual std::size_t cached_storage_bytes() const noexcept { return 0; }
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
    method_kind kind = method_kind::unary;
    stream_method_handler *stream_handler = nullptr;
    std::shared_ptr<stream_method_handler> owned_stream_handler{};
};

struct connection_options {
    wire::settings receive{64U * 1024U, 64U * 1024U, 64, 0, 256, 0, wire::explicit_rejection};
    std::size_t max_method_name_bytes = 256;
    std::chrono::milliseconds handshake_timeout{5000};
    // Fixed read window, independent of the accepted wire frame size.
    // Zero keeps max_frame_size + 16; explicit values must be between that
    // size and 16 MiB + 16. The window is allocated once per connection.
    std::size_t receive_buffer_bytes = 0;
    std::chrono::milliseconds keepalive_interval{0}; // Disabled by default; probe idle connections.
    std::chrono::milliseconds keepalive_timeout{5000};
    std::size_t max_buffered_messages = 64; // Per stream; bounds empty messages and receipts.
    std::size_t compression_threshold = 1024;
    std::uint8_t preferred_compression = 0; // 0 disables outgoing compression; 1 zstd, 2 LZ4.
    bool use_receive_source = false; // TCP only; consumes backend buffers into owned storage.
    std::size_t auxiliary_bytes = 8U * 1024U * 1024U; // RX window, source pool, compression workspace and bounded receipts.
};

struct client_events {
    virtual ~client_events() = default;
    virtual void on_connectivity_change() noexcept = 0;
    virtual void on_capacity_change() noexcept {}
};

struct client_options {
    connection_options connection{};
    std::size_t request_bytes = 8U * 1024U * 1024U;
    std::size_t control_bytes = 16U * 1024U;
    std::size_t max_registered_methods = 4096;
    client_events *events = nullptr; // Borrowed until this client's I/O is quiescent.
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
    std::size_t max_queued_calls = 0; // Opt-in, deadline-bounded FIFO before user code.
    std::size_t max_queued_bytes = 0; // Encoded head + body; pools still charge full blocks.
    std::size_t auxiliary_bytes = 64U * 1024U * 1024U; // Streaming sessions' shared auxiliary budget.
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
    std::size_t queued_calls = 0;
    std::size_t queued_bytes = 0;
};

// Owns the byte stream. close() cancels pending I/O but does not permit buffer
// destruction until completion. Also serves as the deterministic test seam.
struct transport {
    virtual ~transport() = default;
    virtual net::any_stream &stream() noexcept = 0;
    virtual net::socket_base *socket() noexcept { return nullptr; }
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
    // Descriptor binding is a cold operation and may throw. Service binding is
    // atomic: failure leaves the registry unchanged. Names have static lifetime.
    virtual method_handle bind(method_descriptor const &method) = 0;
    virtual std::vector<method_handle> bind(service_descriptor const &service) = 0;
    virtual net::task<call_result> call_encoded(method_handle method, encoded_request request,
                                               decoded_response response, call_options options = {}) = 0;
    virtual net::task<stream_open_result> open_stream(std::string method, method_kind kind, call_options options = {});
    virtual net::task<stream_open_result> open_stream(method_handle method, call_options options = {});
    virtual bool ready() const noexcept = 0;
    virtual resource_stats stats() const noexcept = 0;
    virtual void close() noexcept = 0;
    virtual void drain() noexcept { close(); }
    virtual bool quiescent() const noexcept { return !ready() && stats().active_calls == 0; }
    virtual bool has_capacity(std::size_t) const noexcept { return ready(); }
    virtual bool draining() const noexcept { return false; }
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
    virtual net::task<> shutdown(std::chrono::milliseconds grace) = 0;
};

std::unique_ptr<client> make_client(net::io_context &context, client_options options = {});
std::unique_ptr<server> make_server(net::io_context &context, std::vector<method_binding> methods,
                                   server_options options = {});

// Cold registration; build freezes a registry and performs the same validation
// as make_server. A successful build consumes the builder; failure is retryable.
class server_builder {
public:
    explicit server_builder(net::io_context &context, server_options options = {});
    server_builder(server_builder const &) = delete;
    server_builder &operator=(server_builder const &) = delete;
    server_builder(server_builder &&) = default;
    server_builder &operator=(server_builder &&) = delete;
    server_builder &add(method_binding method);
    server_builder &add(std::vector<method_binding> methods);
    std::unique_ptr<server> build();
private:
    net::io_context &context_;
    server_options options_;
    std::vector<method_binding> methods_{};
    bool built_ = false;
};

} // namespace rpc
