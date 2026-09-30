#include <rpc/v2/server.hpp>

#include "engine.hpp"

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
namespace v2 {
namespace detail {

namespace {
constexpr std::size_t max_tracked_streams = 1U << 16;
constexpr std::uint64_t max_timeout_us = 100ULL * 365 * 24 * 3600 * 1000000; // A century.
constexpr std::size_t end_head_bytes = 2;                                   // Empty message and metadata.
} // namespace

struct method_entry {
    std::string name;
    method_handler *handler;
    std::size_t max_response;
    std::size_t max_trailer;
    std::size_t max_head() const noexcept { return std::max(max_trailer, end_head_bytes); }
    std::size_t charge() const noexcept { return max_response + max_trailer; }
};

struct server_core;
struct session;

struct server_call final : stream_state {
    explicit server_call(server_core &owner) noexcept;
    static net::coroutine_handle<> on_done(void *self) noexcept;

    server_core *core;
    session *owner = nullptr;
    method_entry const *method = nullptr;
    wire::bytes_view request{};
    std::uint8_t *request_block = nullptr;
    std::size_t request_capacity = 0;
    std::size_t request_charge = 0;
    std::uint8_t *response_block = nullptr;
    std::size_t response_capacity = 0;
    std::size_t response_size = 0;
    std::uint8_t *trailer_block = nullptr; // Encoded END head, max_trailer bytes.
    std::size_t trailer_size = 0;
    std::size_t trailer_metadata = 0; // Offset of the metadata block, after the message.
    server_context context{};
    response_writer writer{};
    // Reused while no stop was requested; the context documents that its
    // token does not outlive the handler.
    net::stop_source stop{co2::nostopstate};
    net::io_env env{};
    net::task<status_code> task{};
    net::detail::completion_frame done;
    status_code forced = status_code::ok;
    bool remote_cancelled = false;
    bool detached = false; // The connection closed; nothing is sent.
    server_call *next_free = nullptr;
};

struct server_access {
    static void bind(response_writer &writer, server_call *call) noexcept { writer.call_ = call; }
    static server_call *call(response_writer const &writer) noexcept { return writer.call_; }
};

struct method_slot {
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
    void invoke(server_call &call) noexcept;
    void complete(server_call &call, status_code code) noexcept;
    void send_end(server_call &call, status_code code) noexcept;
    void goaway() noexcept;
    void release_if_done() noexcept;

    std::shared_ptr<server_core> core;
    connection conn;
    stream_table streams;
    std::vector<method_slot> ids; // Wire method ID -> method; grows on NEW_METHOD.
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

void call_frame(stream_state &, wire::frame_view const &) noexcept {}

void call_abort(stream_state &stream, status_code const code) noexcept {
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

} // namespace

server_call::server_call(server_core &owner) noexcept : core(&owner), done(&server_call::on_done, this) {
    server_access::bind(writer, this);
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

// ---- server_core ----

server_core::server_core(shard_state &state, std::vector<method_binding> bindings, server_options const &config)
    : shard(state), options(config), frame_allocator(state.context.get_frame_allocator()) {
    methods.reserve(bindings.size());
    for (auto &binding : bindings) {
        if (binding.name.empty() || binding.handler == nullptr || binding.max_trailer_bytes > 0xffffU)
            throw std::invalid_argument{"invalid method binding"};
        for (auto const &existing : methods)
            if (existing.name == binding.name) throw std::invalid_argument{"duplicate method " + binding.name};
        methods.push_back(method_entry{std::move(binding.name), binding.handler, binding.max_response_bytes,
                                       binding.max_trailer_bytes});
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
    shard.memory.deallocate(call.response_block, call.response_capacity);
    if (call.trailer_block != nullptr) shard.memory.deallocate(call.trailer_block, call.method->max_trailer);
    call.request_block = call.response_block = call.trailer_block = nullptr;
    call.request_capacity = call.response_capacity = call.response_size = call.request_charge = 0;
    call.trailer_size = 0;
    call.request = {};
    call.context.deadline = clock::time_point::max();
    call.context.metadata = {};
    if (call.stop.stop_requested()) {
        call.stop = net::stop_source{co2::nostopstate};
        call.context.stop_token = {};
        call.env = net::io_env{};
    }
    call.owner = nullptr;
    call.method = nullptr;
    call.next_free = free_calls;
    free_calls = &call;
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
    auto const &peer = conn.peer();
    auto const &limits = core->options;
    auto &stats = core->stats;
    if (frame.body.size > conn.options.receive.max_message_size || entry.max_response > peer.max_message_size ||
        entry.max_head() > peer.max_frame_size || entry.max_response > peer.max_frame_size - entry.max_head() ||
        streams.size() >= streams.limit() || stats.active_calls >= limits.max_active_calls ||
        header.length > limits.max_request_bytes - stats.request_bytes ||
        entry.charge() > limits.max_response_bytes - stats.response_bytes)
        return reject(header.stream_id, status_code::resource_exhausted), true;

    auto deadline = clock::time_point::max();
    if (head.value.timeout_us != 0)
        deadline = now() + std::chrono::microseconds{
                               static_cast<std::int64_t>(std::min(head.value.timeout_us, max_timeout_us))};

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
            call->context.stop_token = call->stop.get_token();
            call->env = net::io_env{net::executor_ref{core->shard.executor}, call->context.stop_token,
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

void session::invoke(server_call &call) noexcept {
    bool failed = false;
    {
        struct allocator_scope {
            net::memory_resource *saved;
            ~allocator_scope() { net::set_cached_frame_allocator(saved); }
        } scope{net::get_cached_frame_allocator()};
        net::set_cached_frame_allocator(core->frame_allocator);
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

void session::send_end(server_call &call, status_code const code) noexcept {
    bool const ok = code == status_code::ok;
    std::size_t const body = ok ? call.response_size : 0;
    // The wire carries a status message only with an error: ok keeps just the metadata.
    std::size_t const skipped = ok && call.trailer_size != 0 ? call.trailer_metadata - 1 : 0;
    std::size_t const head = call.trailer_size != 0 ? call.trailer_size - skipped : end_head_bytes;
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
    header.stream_id = call.id;
    header.type = wire::frame_type::end;
    header.head_length = static_cast<std::uint16_t>(head);
    header.aux = static_cast<std::uint32_t>(code);
    if (wire::encode_header(header, {frame, wire::header_size}, conn.outgoing()).code != wire::error::none) {
        conn.close();
        return;
    }
    if (call.trailer_size == 0) {
        frame[16] = frame[17] = 0;
    } else if (skipped == 0) {
        std::memcpy(frame + wire::header_size, call.trailer_block, head);
    } else {
        frame[16] = 0; // Empty message.
        std::memcpy(frame + wire::header_size + 1, call.trailer_block + call.trailer_metadata, head - 1);
    }
    if (body != 0) std::memcpy(frame + wire::header_size + head, call.response_block, body);
    conn.commit(wire::header_size + payload);
}

void session::complete(server_call &call, status_code code) noexcept {
    if (call.forced != status_code::ok) code = call.forced;
    if (static_cast<unsigned>(code) > static_cast<unsigned>(status_code::unauthenticated)) code = status_code::internal;
    if (!call.remote_cancelled && !call.detached && conn.state() != phase::closed) send_end(call, code);
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

void session::goaway() noexcept {
    if (goaway_sent || conn.state() == phase::closed) return;
    goaway_sent = true;
    conn.send_control(wire::frame_type::goaway, 0, last_admitted, 0);
    if (active == 0) conn.close_when_flushed();
}

void session::on_closed() noexcept {
    while (auto *stream = streams.any()) stream->ops->on_abort(*stream, status_code::unavailable);
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

bool response_writer::set_trailer(wire::bytes_view const message, wire::metadata_list const metadata) noexcept {
    auto *const call = call_;
    if (call == nullptr || call->method == nullptr || call->method->max_trailer == 0) return false;
    auto const limit = call->method->max_trailer;
    if (call->trailer_block == nullptr) {
        try {
            call->trailer_block = call->core->shard.memory.allocate(limit);
        } catch (std::bad_alloc const &) {
            return false;
        }
    }
    auto const encoded = wire::encode_end_head({message, metadata}, {call->trailer_block, limit});
    std::array<std::uint8_t, 10> length{};
    auto const prefix = wire::encode_varint(message.size, {length.data(), length.size()});
    call->trailer_size = encoded.code == wire::error::none ? encoded.written : 0;
    call->trailer_metadata = prefix.written + message.size;
    return call->trailer_size != 0;
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

} // namespace v2
} // namespace rpc
