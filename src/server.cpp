#include <rpc/server.hpp>

#include "engine.hpp"
#include "stream_core.hpp"

#include <net/error.hpp>
#include <net/memory_resource.hpp>
#include <net/run_async.hpp>

#include <algorithm>
#include <array>
#include <cstring>
#include <new>
#include <stdexcept>
#include <system_error>

namespace rpc {
namespace detail {

namespace {
constexpr std::size_t max_tracked_streams = 1U << 16;
constexpr std::uint64_t max_timeout_us = 100ULL * 365 * 24 * 3600 * 1000000; // A century.
constexpr std::size_t end_head_bytes = 2;                                   // Empty message and metadata.
} // namespace

struct method_entry {
    std::string name;
    method_handler *handler;
    stream_method_handler *stream_handler;
    method_kind kind;
    std::size_t max_response;
    std::size_t max_trailer;
    std::shared_ptr<void> owner;
    std::size_t max_head() const noexcept { return std::max(max_trailer, end_head_bytes); }
    std::size_t charge() const noexcept { return max_response + max_trailer; }
};

struct server_core;
struct session;

// Response storage and END trailer of one call, streaming or not.
struct response_state {
    server_core *core = nullptr;
    method_entry const *method = nullptr;
    std::uint8_t *response_block = nullptr;
    std::size_t response_capacity = 0;
    std::size_t response_size = 0;
    std::uint8_t *trailer_block = nullptr; // Encoded END head, max_trailer bytes.
    std::size_t trailer_size = 0;
    std::size_t trailer_metadata = 0; // Offset of the metadata block, after the message.

    // The wire carries a status message only with an error: ok keeps just the metadata.
    std::size_t skipped(bool const ok) const noexcept { return ok && trailer_size != 0 ? trailer_metadata - 1 : 0; }
    std::size_t end_head(bool const ok) const noexcept {
        return trailer_size != 0 ? trailer_size - skipped(ok) : end_head_bytes;
    }
    void write_end_head(std::uint8_t *out, bool const ok) const noexcept {
        if (trailer_size == 0) {
            out[0] = out[1] = 0;
        } else if (skipped(ok) == 0) {
            std::memcpy(out, trailer_block, trailer_size);
        } else {
            out[0] = 0; // Empty message.
            std::memcpy(out + 1, trailer_block + trailer_metadata, trailer_size - trailer_metadata);
        }
    }
    void release_response(slab &memory) noexcept {
        memory.deallocate(response_block, response_capacity);
        if (trailer_block != nullptr) memory.deallocate(trailer_block, method->max_trailer);
        response_block = trailer_block = nullptr;
        response_capacity = response_size = trailer_size = 0;
    }
};

struct server_call final : stream_state, response_state {
    explicit server_call(server_core &owner) noexcept;
    static net::coroutine_handle<> on_done(void *self) noexcept;

    session *owner = nullptr;
    wire::bytes_view request{};
    std::uint8_t *request_block = nullptr;
    std::size_t request_capacity = 0;
    std::size_t request_charge = 0;
    server_context context{};
    response_writer writer{};
    // Reused while no stop was requested and the handler took no token.
    net::stop_source stop{co2::nostopstate};
    net::io_env env{};
    net::task<status_code> task{};
    net::detail::completion_frame done;
    status_code forced = status_code::ok;
    bool remote_cancelled = false;
    bool detached = false; // The connection closed; nothing is sent.
    server_call *next_free = nullptr;
};

// A stream, including a unary call that arrived as one, as streaming-profile
// clients may send them.
struct server_stream_call final : stream_core, response_state {
    server_stream_call(server_core &owner, session &from, method_entry const &entry); // Throws std::bad_alloc.
    ~server_stream_call() override;
    static net::coroutine_handle<> on_done(void *self) noexcept;

    void fail(status_code const code) noexcept override { stop_with(code); }
    // Ends reads and writes and stops the handler; the END waits for it.
    void stop_with(status_code const code) noexcept {
        if (forced == status_code::ok) forced = code;
        if (!ended) {
            ended = true;
            result = code;
            abandon_write();
            drop_messages();
            wake_all();
        }
        stop.request_stop(); // Last: the handler may complete synchronously.
    }

    session *owner;
    std::uint8_t *head_block = nullptr; // Request head, for the metadata view.
    std::size_t head_capacity = 0;
    std::size_t head_charge = 0;
    inbound_message request{}; // Extended unary only.
    server_context context{};
    server_stream handle{};
    response_writer writer{};
    net::stop_source stop{co2::nostopstate}; // From server_core::take_stop.
    net::io_env env{};
    net::task<status_code> task{};
    net::detail::completion_frame done;
    status_code forced = status_code::ok;
    bool remote_cancelled = false;
    bool detached = false;
};

struct server_access {
    static void bind(response_writer &writer, response_state *call) noexcept { writer.call_ = call; }
    static void bind(server_context &context, response_state *call) noexcept { context.response_ = call; }
    static void set_stop(server_context &context, net::stop_token token) noexcept {
        context.stop_ = std::move(token);
        context.shared_ = false;
    }
    // The handler took a token that may outlive the call.
    static bool stop_shared(server_context const &context) noexcept { return context.shared_; }
};

struct wire_method {
    bool defined = false;
    method_entry const *entry = nullptr; // Null: defined but not served.
};

struct server_core : std::enable_shared_from_this<server_core> {
    server_core(shard_state &state, std::vector<method_binding> bindings, server_options const &config);
    ~server_core();
    server_core(server_core const &) = delete;
    server_core &operator=(server_core const &) = delete;

    method_entry const *find(wire::bytes_view name) const noexcept;
    bool attach(std::unique_ptr<transport> link) noexcept;
    void drain() noexcept;
    void close() noexcept;
    server_call &acquire_call();
    void release_call(server_call &call) noexcept;
    net::stop_source take_stop(); // Throws std::bad_alloc.
    void keep_stop(net::stop_source &stop) noexcept;
    void link(session &value) noexcept;
    void unlink(session &value) noexcept;

    shard_state &shard;
    server_options const options;
    std::vector<method_entry> methods;
    net::memory_resource *frame_allocator;
    std::unique_ptr<net::tcp_acceptor> acceptor;
    session *sessions = nullptr;
    server_stats stats{};
    server_call *free_calls = nullptr;
    // As a unary call keeps its own, a finished stream that was never asked
    // to stop, and whose handler took no token, leaves its stop state here.
    std::vector<net::stop_source> spare_stops;
    bool draining = false;
    bool closed = false;
};

struct session final : connection_handler {
    session(std::shared_ptr<server_core> owner, std::unique_ptr<transport> link);

    bool on_frame(wire::frame_view const &frame) noexcept override;
    void on_ready() noexcept override {}
    void on_closed() noexcept override;
    void on_finished() noexcept override;

    bool on_request(wire::frame_view const &frame) noexcept;
    void reject(std::uint32_t stream, status_code code) noexcept;
    void start_stream(method_entry const &entry, wire::frame_view const &frame, wire::metadata_view metadata,
                      clock::time_point deadline) noexcept;
    void invoke(server_call &call) noexcept;
    void invoke(server_stream_call &call) noexcept;
    void complete(server_call &call, status_code code) noexcept;
    void complete(server_stream_call &call, status_code code) noexcept;
    void send_end(std::uint32_t stream, response_state const &state, status_code code, bool with_body) noexcept;
    void goaway() noexcept;
    void release_if_done() noexcept;

    std::shared_ptr<server_core> core;
    connection conn;
    stream_table streams;
    std::vector<wire_method> ids; // Wire method ID -> method; grows on NEW_METHOD.
    std::uint32_t last_request = 0;
    std::uint32_t last_admitted = 0;
    std::size_t active = 0; // Calls not yet completed, including detached ones.
    bool goaway_sent = false;
    bool io_finished = false;
    std::shared_ptr<session> self; // Until I/O has exited and every call completed.
    session *prev = nullptr;
    session *next = nullptr;
};

namespace {

server_call &call_of(stream_state &stream) noexcept { return static_cast<server_call &>(stream); }

bool call_frame(stream_state &, wire::frame_view const &) noexcept { return false; }

void call_abort(stream_state &stream, status_code const code, bool) noexcept {
    auto &call = call_of(stream);
    call.detached = true;
    call.owner->streams.erase(call.id);
    if (call.forced == status_code::ok) call.forced = code;
    call.stop.request_stop(); // Last: the handler may complete synchronously.
}

void call_deadline(stream_state &stream) noexcept {
    auto &call = call_of(stream);
    if (call.forced == status_code::ok) call.forced = status_code::deadline_exceeded;
    call.stop.request_stop();
}

void call_cancel(stream_state &stream) noexcept {
    auto &call = call_of(stream);
    call.remote_cancelled = true;
    call.stop.request_stop();
}

stream_ops const server_ops{&call_frame, &call_abort, &call_deadline, &call_cancel};

server_stream_call &stream_of(stream_state &node) noexcept { return static_cast<server_stream_call &>(node); }

bool stream_frame(stream_state &node, wire::frame_view const &frame) noexcept {
    auto &call = stream_of(node);
    switch (frame.header.type) {
    case wire::frame_type::message: return call.on_message(frame);
    case wire::frame_type::window_update: return call.on_window(frame);
    default: return false;
    }
}

void stream_abort(stream_state &node, status_code const code, bool) noexcept {
    auto &call = stream_of(node);
    call.detached = true;
    call.owner->streams.erase(call.id);
    call.conn = nullptr;
    call.stop_with(code);
}

void stream_deadline(stream_state &node) noexcept { stream_of(node).stop_with(status_code::deadline_exceeded); }

void stream_cancel(stream_state &node) noexcept {
    auto &call = stream_of(node);
    call.remote_cancelled = true;
    call.stop_with(status_code::cancelled);
}

stream_ops const server_stream_ops{&stream_frame, &stream_abort, &stream_deadline, &stream_cancel};

// Such a unary call is one MESSAGE and a half-close, and expects one MESSAGE
// before the END.
auto unary_over_stream(server_stream_call &call)
    CO2_BEG(net::task<status_code>, (call), stream_read read; status_code code = status_code::ok;) {
    CO2_AWAIT_SET(read, stream_access::read(&call));
    if (read.code != status_code::ok || read.ended)
        CO2_RETURN(read.code != status_code::ok ? read.code : status_code::invalid_argument);
    call.request = std::exchange(call.current, inbound_message{}); // Outlives the next read.
    CO2_AWAIT_SET(read, stream_access::read(&call));
    if (read.code != status_code::ok || !read.ended)
        CO2_RETURN(read.code != status_code::ok ? read.code : status_code::invalid_argument);
    CO2_AWAIT_SET(code, call.method->handler->invoke(call.context, {call.request.block, call.request.size}, call.writer));
    if (code != status_code::ok) CO2_RETURN(code);
    CO2_AWAIT_SET(code, stream_access::write(&call, {call.response_block, call.response_size}, false));
    CO2_RETURN(code);
}
CO2_END

struct allocator_scope {
    explicit allocator_scope(net::memory_resource *resource) noexcept : saved(net::get_cached_frame_allocator()) {
        net::set_cached_frame_allocator(resource);
    }
    ~allocator_scope() { net::set_cached_frame_allocator(saved); }
    net::memory_resource *saved;
};

bool set_trailer(response_state &call, wire::bytes_view const message, wire::metadata_list const metadata) noexcept {
    if (call.method == nullptr || call.method->max_trailer == 0) return false;
    auto const limit = call.method->max_trailer;
    if (call.trailer_block == nullptr) {
        try {
            call.trailer_block = call.core->shard.memory.allocate(limit);
        } catch (std::bad_alloc const &) {
            return false;
        }
    }
    auto const encoded = wire::encode_end_head({message, metadata}, {call.trailer_block, limit});
    std::array<std::uint8_t, 10> length{};
    auto const prefix = wire::encode_varint(message.size, {length.data(), length.size()});
    call.trailer_size = encoded.code == wire::error::none ? encoded.written : 0;
    call.trailer_metadata = prefix.written + message.size;
    return call.trailer_size != 0;
}

} // namespace

server_call::server_call(server_core &owner) noexcept : done(&server_call::on_done, this) {
    core = &owner;
    server_access::bind(writer, this);
    server_access::bind(context, this);
}

net::coroutine_handle<> server_call::on_done(void *const self) noexcept {
    auto &call = *static_cast<server_call *>(self);
    auto code = status_code::internal;
    try {
        code = call.task.await_resume();
    } catch (...) {
    }
    call.task = net::task<status_code>{};
    call.owner->complete(call, code);
    return net::noop_coroutine();
}

server_stream_call::server_stream_call(server_core &owner, session &from, method_entry const &entry)
    : stream_core(owner.shard, entry.kind, false), owner(&from), done(&server_stream_call::on_done, this) {
    core = &owner;
    method = &entry;
    stream_access::bind(handle, this);
    server_access::bind(writer, this);
    server_access::bind(context, this);
}

server_stream_call::~server_stream_call() {
    auto &memory = core->shard.memory;
    memory.deallocate(head_block, head_capacity);
    memory.deallocate(request.block, request.capacity);
    release_response(memory);
}

net::coroutine_handle<> server_stream_call::on_done(void *const self) noexcept {
    auto &call = *static_cast<server_stream_call *>(self);
    auto code = status_code::internal;
    try {
        code = call.task.await_resume();
    } catch (...) {
    }
    call.task = net::task<status_code>{};
    call.owner->complete(call, code);
    return net::noop_coroutine();
}

// ---- server_core ----

server_core::server_core(shard_state &state, std::vector<method_binding> bindings, server_options const &config)
    : shard(state), options(config), frame_allocator(state.context.get_frame_allocator()) {
    methods.reserve(bindings.size());
    for (auto &binding : bindings) {
        bool const unary = binding.kind == method_kind::unary;
        if (binding.name.empty() || binding.max_trailer_bytes > 0xffffU || binding.kind > method_kind::bidirectional ||
            (unary ? binding.handler == nullptr : binding.stream_handler == nullptr))
            throw std::invalid_argument{"invalid method binding"};
        for (auto const &existing : methods)
            if (existing.name == binding.name) throw std::invalid_argument{"duplicate method " + binding.name};
        methods.push_back(method_entry{std::move(binding.name), binding.handler, binding.stream_handler, binding.kind,
                                       binding.max_response_bytes, binding.max_trailer_bytes,
                                       std::move(binding.owner)});
    }
    shard.endpoint_opened();
}

server_core::~server_core() {
    while (free_calls != nullptr) {
        auto *call = free_calls;
        free_calls = call->next_free;
        delete call;
    }
}

method_entry const *server_core::find(wire::bytes_view const name) const noexcept {
    for (auto const &entry : methods)
        if (entry.name.size() == name.size && std::memcmp(entry.name.data(), name.data, name.size) == 0) return &entry;
    return nullptr;
}

server_call &server_core::acquire_call() {
    if (auto *call = free_calls) {
        free_calls = call->next_free;
        call->next_free = nullptr;
        return *call;
    }
    return *new server_call(*this);
}

void server_core::release_call(server_call &call) noexcept {
    shard.memory.deallocate(call.request_block, call.request_capacity);
    call.release_response(shard.memory);
    call.request_block = nullptr;
    call.request_capacity = call.request_charge = 0;
    call.request = {};
    call.context.deadline = clock::time_point::max();
    call.context.metadata = {};
    if (call.stop.stop_requested() || server_access::stop_shared(call.context)) {
        call.stop = net::stop_source{co2::nostopstate};
        server_access::set_stop(call.context, {});
        call.env = net::io_env{};
    }
    call.owner = nullptr;
    call.method = nullptr;
    call.next_free = free_calls;
    free_calls = &call;
}

net::stop_source server_core::take_stop() {
    if (spare_stops.empty()) return net::stop_source{};
    auto stop = std::move(spare_stops.back());
    spare_stops.pop_back();
    return stop;
}

void server_core::keep_stop(net::stop_source &stop) noexcept {
    if (!stop.stop_possible() || stop.stop_requested()) return;
    try {
        spare_stops.push_back(std::move(stop));
    } catch (std::bad_alloc const &) {
    }
}

void server_core::link(session &value) noexcept {
    value.next = sessions;
    if (sessions != nullptr) sessions->prev = &value;
    sessions = &value;
}

void server_core::unlink(session &value) noexcept {
    if (value.prev != nullptr) value.prev->next = value.next;
    else sessions = value.next;
    if (value.next != nullptr) value.next->prev = value.prev;
    value.prev = value.next = nullptr;
}

bool server_core::attach(std::unique_ptr<transport> link_) noexcept {
    if (closed || draining || !link_ || stats.connections >= options.max_connections) return false;
    std::shared_ptr<session> created;
    try {
        created = std::make_shared<session>(shared_from_this(), std::move(link_));
    } catch (...) {
        return false;
    }
    created->self = created;
    link(*created);
    ++stats.connections;
    try {
        created->conn.start(created);
    } catch (...) {
        if (!created->conn.io_running()) {
            unlink(*created);
            --stats.connections;
            created->self.reset();
        }
        return false;
    }
    return true;
}

void server_core::drain() noexcept {
    if (closed || draining) return;
    draining = true;
    if (acceptor) acceptor->close();
    for (auto *value = sessions; value != nullptr;) {
        auto *following = value->next;
        value->goaway();
        value = following;
    }
}

void server_core::close() noexcept {
    if (closed) return;
    closed = draining = true;
    if (acceptor) acceptor->close();
    for (auto *value = sessions; value != nullptr;) {
        auto *following = value->next;
        value->conn.close();
        value = following;
    }
    shard.endpoint_closed();
}

// ---- session ----

session::session(std::shared_ptr<server_core> owner, std::unique_ptr<transport> link)
    : core(std::move(owner)), conn(core->shard, core->options.connection, std::move(link), true, *this),
      streams(std::min<std::size_t>(core->options.connection.receive.max_concurrent_streams, max_tracked_streams)) {}

bool session::on_frame(wire::frame_view const &frame) noexcept {
    auto const &header = frame.header;
    switch (header.type) {
    case wire::frame_type::request: return on_request(frame);
    case wire::frame_type::message:
    case wire::frame_type::window_update:
        if (auto *stream = streams.find(header.stream_id)) return stream->ops->on_frame(*stream, frame);
        return header.stream_id <= last_request; // Late frames of a finished stream.
    case wire::frame_type::cancel:
        if (header.stream_id > last_request) return false;
        if (auto *stream = streams.find(header.stream_id)) stream->ops->on_cancel(*stream);
        return true;
    case wire::frame_type::goaway: return true;
    default: return false;
    }
}

void session::reject(std::uint32_t const stream, status_code const code) noexcept {
    auto const flags = (conn.features() & wire::explicit_rejection) != 0 ? wire::not_executed : std::uint8_t{0};
    conn.send_control(wire::frame_type::end, stream, static_cast<std::uint32_t>(code), flags);
}

bool session::on_request(wire::frame_view const &frame) noexcept {
    auto const &header = frame.header;
    if (header.stream_id <= last_request || header.aux == 0 || header.aux > conn.options.receive.max_method_ids)
        return false;
    last_request = header.stream_id;
    bool const defines = (header.flags & wire::new_method) != 0;
    bool const streaming = (header.flags & wire::end_stream) == 0;
    auto const head = wire::decode_request_head(frame.head, defines);
    if (head.code != wire::error::none) return false;
    if (header.aux >= ids.size()) {
        if (!defines) {
            reject(header.stream_id, status_code::failed_precondition);
            return true;
        }
        try {
            ids.resize(header.aux + 1);
        } catch (std::bad_alloc const &) {
            return false;
        }
    }
    auto &slot = ids[header.aux];
    if (defines) {
        if (slot.defined || head.value.method_name.size > conn.options.max_method_name_bytes) return false;
        slot.defined = true; // Survives a refusal below.
        slot.entry = core->find(head.value.method_name);
    }
    if (goaway_sent || core->draining) return reject(header.stream_id, status_code::unavailable), true;
    if (!slot.defined) return reject(header.stream_id, status_code::failed_precondition), true;
    if (slot.entry == nullptr) return reject(header.stream_id, status_code::unimplemented), true;
    auto const &entry = *slot.entry;
    if (!streaming && entry.kind != method_kind::unary) return reject(header.stream_id, status_code::invalid_argument), true;

    auto const &peer = conn.peer();
    auto const &limits = core->options;
    auto &stats = core->stats;
    if (streams.size() >= streams.limit() || stats.active_calls >= limits.max_active_calls ||
        entry.max_response > peer.max_message_size || entry.max_head() > peer.max_frame_size ||
        header.length > limits.max_request_bytes - stats.request_bytes ||
        (!streaming && (frame.body.size > conn.options.receive.max_message_size ||
                        entry.max_response > peer.max_frame_size - entry.max_head() ||
                        entry.charge() > limits.max_response_bytes - stats.response_bytes)))
        return reject(header.stream_id, status_code::resource_exhausted), true;

    auto deadline = clock::time_point::max();
    if (head.value.timeout_us != 0)
        deadline = now() + std::chrono::microseconds{
                               static_cast<std::int64_t>(std::min(head.value.timeout_us, max_timeout_us))};
    if (streaming) {
        start_stream(entry, frame, head.value.metadata, deadline);
        return true;
    }

    server_call *call = nullptr;
    try {
        call = &core->acquire_call();
        call->owner = this;
        if (header.length != 0) {
            call->request_block = core->shard.memory.allocate(header.length);
            call->request_capacity = slab::block_size(header.length);
        }
        if (!call->stop.stop_possible()) {
            call->stop = net::stop_source{};
            server_access::set_stop(call->context, call->stop.get_token());
            call->env = net::io_env{net::executor_ref{core->shard.executor}, call->stop.get_token(),
                                    core->frame_allocator};
        }
    } catch (std::bad_alloc const &) {
        if (call != nullptr) core->release_call(*call);
        return reject(header.stream_id, status_code::resource_exhausted), true;
    }
    // Head and body are contiguous in the frame; the copy outlives the receive window.
    if (header.length != 0) std::memcpy(call->request_block, frame.head.data, header.length);
    auto metadata = head.value.metadata;
    metadata.entries.data = metadata.entries.size != 0
                                ? call->request_block + (metadata.entries.data - frame.head.data)
                                : nullptr;
    call->id = header.stream_id;
    call->ops = &server_ops;
    call->method = &entry;
    call->request = {call->request_block + header.head_length, frame.body.size};
    call->request_charge = header.length;
    call->forced = status_code::ok;
    call->remote_cancelled = call->detached = false;
    call->context.deadline = deadline;
    call->context.metadata = metadata;
    streams.insert(*call);
    ++active;
    ++stats.active_calls;
    stats.request_bytes += header.length;
    stats.response_bytes += entry.charge();
    last_admitted = header.stream_id;
    if (deadline != clock::time_point::max()) core->shard.schedule(*call, deadline);
    invoke(*call);
    return true;
}

void session::start_stream(method_entry const &entry, wire::frame_view const &frame, wire::metadata_view metadata,
                           clock::time_point const deadline) noexcept {
    auto const id = frame.header.stream_id;
    if (conn.options.receive.initial_stream_window == 0) return reject(id, status_code::unimplemented);
    auto &memory = core->shard.memory;
    server_stream_call *call = nullptr;
    try {
        call = make_in<server_stream_call>(memory, *core, *this, entry);
        call->stop = core->take_stop();
        if (frame.head.size != 0) {
            call->head_block = memory.allocate(frame.head.size);
            call->head_capacity = slab::block_size(frame.head.size);
        }
    } catch (std::bad_alloc const &) {
        destroy_in(memory, call);
        return reject(id, status_code::resource_exhausted);
    }
    if (frame.head.size != 0) std::memcpy(call->head_block, frame.head.data, frame.head.size);
    metadata.entries.data =
        metadata.entries.size != 0 ? call->head_block + (metadata.entries.data - frame.head.data) : nullptr;
    call->id = id;
    call->ops = &server_stream_ops;
    call->open(conn, entry.max_response);
    call->head_charge = frame.head.size;
    call->context.deadline = deadline;
    server_access::set_stop(call->context, call->stop.get_token());
    call->context.metadata = metadata;
    call->env = net::io_env{net::executor_ref{core->shard.executor}, call->stop.get_token(), core->frame_allocator};
    streams.insert(*call);
    ++active;
    ++core->stats.active_calls;
    core->stats.request_bytes += call->head_charge;
    last_admitted = id;
    if (deadline != clock::time_point::max()) core->shard.schedule(*call, deadline);
    invoke(*call);
}

void session::invoke(server_call &call) noexcept {
    bool failed = false;
    {
        allocator_scope scope{core->frame_allocator};
        try {
            call.task = call.method->handler->invoke(call.context, call.request, call.writer);
        } catch (...) {
            failed = true;
        }
    }
    if (failed || !call.task) {
        complete(call, status_code::internal);
        return;
    }
    // Runs until the handler first suspends; it may complete, and release
    // the call, before returning.
    net::safe_resume(call.task.await_suspend(call.done.handle(), &call.env));
}

void session::invoke(server_stream_call &call) noexcept {
    bool failed = false;
    {
        allocator_scope scope{core->frame_allocator};
        try {
            call.task = call.kind == method_kind::unary ? unary_over_stream(call)
                                                        : call.method->stream_handler->invoke(call.context, call.handle);
        } catch (...) {
            failed = true;
        }
    }
    if (failed || !call.task) {
        complete(call, status_code::internal);
        return;
    }
    net::safe_resume(call.task.await_suspend(call.done.handle(), &call.env));
}

void session::send_end(std::uint32_t const stream, response_state const &state, status_code const code,
                       bool const with_body) noexcept {
    bool const ok = code == status_code::ok;
    std::size_t const body = ok && with_body ? state.response_size : 0;
    std::size_t const head = state.end_head(ok);
    std::size_t const payload = head + body;
    std::uint8_t *frame = nullptr;
    try {
        frame = conn.reserve(wire::header_size + payload);
    } catch (std::bad_alloc const &) {
        conn.close();
        return;
    }
    wire::frame_header header{};
    header.length = static_cast<std::uint32_t>(payload);
    header.stream_id = stream;
    header.type = wire::frame_type::end;
    header.head_length = static_cast<std::uint16_t>(head);
    header.aux = static_cast<std::uint32_t>(code);
    if (wire::encode_header(header, {frame, wire::header_size}, conn.outgoing()).code != wire::error::none) {
        conn.close();
        return;
    }
    state.write_end_head(frame + wire::header_size, ok);
    if (body != 0) std::memcpy(frame + wire::header_size + head, state.response_block, body);
    conn.commit(wire::header_size + payload);
}

namespace {
status_code checked(status_code const code) noexcept {
    return static_cast<unsigned>(code) > static_cast<unsigned>(status_code::unauthenticated) ? status_code::internal
                                                                                             : code;
}
} // namespace

void session::complete(server_call &call, status_code code) noexcept {
    if (call.forced != status_code::ok) code = call.forced;
    code = checked(code);
    if (!call.remote_cancelled && !call.detached && conn.state() != phase::closed) send_end(call.id, call, code, true);
    if (!call.detached) streams.erase(call.id);
    core->shard.unschedule(call);
    auto &stats = core->stats;
    --stats.active_calls;
    stats.request_bytes -= call.request_charge;
    stats.response_bytes -= call.method->charge();
    core->release_call(call);
    --active;
    if (goaway_sent && active == 0) conn.close_when_flushed();
    release_if_done(); // May destroy this session.
}

void session::complete(server_stream_call &call, status_code code) noexcept {
    if (call.forced != status_code::ok) code = call.forced;
    code = checked(code);
    // An ok end must honour the kind's single-message sides.
    if (code == status_code::ok && ((call.sends_one() && call.sent != 1) ||
                                    (call.receives_one() && (call.received != 1 || !call.remote_half))))
        code = status_code::invalid_argument;
    if (!call.remote_cancelled && !call.detached && conn.state() != phase::closed) send_end(call.id, call, code, false);
    if (!call.detached) streams.erase(call.id);
    core->shard.unschedule(call);
    --core->stats.active_calls;
    core->stats.request_bytes -= call.head_charge;
    call.ended = true; // Held messages are freed without returning credit.
    call.abandon_write();
    call.conn = nullptr;
    if (!server_access::stop_shared(call.context)) core->keep_stop(call.stop);
    destroy_in(core->shard.memory, &call);
    --active;
    if (goaway_sent && active == 0) conn.close_when_flushed();
    release_if_done(); // May destroy this session.
}

void session::goaway() noexcept {
    if (goaway_sent || conn.state() == phase::closed) return;
    goaway_sent = true;
    conn.send_control(wire::frame_type::goaway, 0, last_admitted, 0);
    if (active == 0) conn.close_when_flushed();
}

void session::on_closed() noexcept {
    while (auto *stream = streams.any()) stream->ops->on_abort(*stream, status_code::unavailable, false);
}

void session::on_finished() noexcept {
    io_finished = true;
    release_if_done();
}

void session::release_if_done() noexcept {
    if (!io_finished || active != 0 || !self) return;
    core->unlink(*this);
    --core->stats.connections;
    auto last = std::move(self);
}

namespace {

auto accept_loop(std::shared_ptr<server_core> core)
    CO2_BEG(net::task<>, (core), net::io_result<net::tcp_socket> accepted;) {
    while (!core->draining) {
        CO2_AWAIT_SET(accepted, core->acceptor->accept());
        if (core->draining) break;
        if (accepted.ec) {
            if (accepted.ec == net::cond::canceled) break;
            CO2_AWAIT(yield_awaiter{}); // For example EMFILE: let completions free descriptors.
            continue;
        }
        if (accepted.value.set_option(net::socket_option::no_delay{true})) continue;
        core->attach(make_tcp_transport(std::move(accepted.value)));
    }
    CO2_RETURN();
}
CO2_END

} // namespace
} // namespace detail

// ---- response_writer ----

wire::mutable_bytes_view response_writer::prepare(std::size_t const size) noexcept {
    auto *const call = call_;
    if (call == nullptr || call->method == nullptr || size > call->method->max_response) return {};
    if (size > call->response_capacity) {
        std::uint8_t *block = nullptr;
        try {
            block = call->core->shard.memory.allocate(size);
        } catch (std::bad_alloc const &) {
            return {};
        }
        call->core->shard.memory.deallocate(call->response_block, call->response_capacity);
        call->response_block = block;
        call->response_capacity = detail::slab::block_size(size);
    }
    call->response_size = 0;
    return {call->response_block, size};
}

bool response_writer::commit(std::size_t const size) noexcept {
    auto *const call = call_;
    if (call == nullptr || call->method == nullptr || size > call->response_capacity ||
        size > call->method->max_response)
        return false;
    call->response_size = size;
    return true;
}

bool response_writer::assign(wire::bytes_view const bytes) noexcept {
    if (bytes.size == 0) return commit(0);
    auto const out = prepare(bytes.size);
    if (out.data == nullptr) return false;
    std::memcpy(out.data, bytes.data, bytes.size);
    return commit(bytes.size);
}

std::size_t response_writer::size() const noexcept { return call_ != nullptr ? call_->response_size : 0; }

std::size_t response_writer::max_size() const noexcept {
    return call_ != nullptr && call_->method != nullptr ? call_->method->max_response : 0;
}

bool server_context::set_trailer(wire::bytes_view const message, wire::metadata_list const metadata) noexcept {
    return response_ != nullptr && detail::set_trailer(*response_, message, metadata);
}

bool response_writer::set_trailer(wire::bytes_view const message, wire::metadata_list const metadata) noexcept {
    return call_ != nullptr && detail::set_trailer(*call_, message, metadata);
}

// ---- server_stream ----

read_operation server_stream::read() noexcept { return detail::stream_access::read(call_); }

write_operation server_stream::write(wire::bytes_view const message) noexcept {
    return detail::stream_access::write(call_, message, false);
}

bool server_stream::set_trailer(wire::bytes_view const message, wire::metadata_list const metadata) noexcept {
    return call_ != nullptr && detail::set_trailer(*call_, message, metadata);
}

// ---- server ----

server::server(shard &owner, std::vector<method_binding> methods, server_options options)
    : core_(std::make_shared<detail::server_core>(owner.state(), std::move(methods), options)) {}

server::~server() { close(); }

net::ip::tcp::endpoint server::listen(net::ip::tcp::endpoint endpoint) {
    auto &core = *core_;
    if (core.acceptor || core.draining) throw std::logic_error{"server already listening or stopped"};
    core.acceptor.reset(new net::tcp_acceptor(core.shard.context, endpoint));
    std::error_code error;
    auto const bound = core.acceptor->local_endpoint(error);
    if (error) throw std::system_error{error};
    net::run_async(core.shard.executor)(detail::accept_loop(core_));
    return bound;
}

bool server::attach(std::unique_ptr<transport> link) { return core_->attach(std::move(link)); }
void server::drain() noexcept { core_->drain(); }
void server::close() noexcept { core_->close(); }
server_stats server::stats() const noexcept { return core_->stats; }

} // namespace rpc
