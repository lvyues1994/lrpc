#include "bench.hpp"

#include <net/run_async.hpp>
#include <net/stream.hpp>
#include <net/signal_set.hpp>

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <csignal>
#include <iostream>
#include <unistd.h>

namespace bench {
namespace {

auto echo(rpc::wire::bytes_view request, rpc::response_writer &response)
    CO2_BEG(net::task<rpc::status_code>, (request, response)) {
    CO2_RETURN(response.assign(request) ? rpc::status_code::ok : rpc::status_code::internal);
}
CO2_END

struct echo_handler final : rpc::method_handler {
    net::task<rpc::status_code> invoke(rpc::server_context &, rpc::wire::bytes_view request,
                                      rpc::response_writer &response) override { return echo(request, response); }
};

struct rpc_channel final : channel {
    rpc_channel(net::io_context &context, options const &config) {
        rpc::server_options so{};
        so.connection.receive = {8192, 8192, static_cast<std::uint32_t>(config.rpc_streams), 0, 16, 0};
        so.connection.receive_buffer_bytes = config.receive_buffer_bytes;
        so.max_connections = 1; so.max_active_calls = config.rpc_streams;
        so.request_bytes = 8 * 1024 * 1024; so.response_bytes = 8 * 1024 * 1024;
        rpc::client_options co{}; co.connection = so.connection;
        co.connection.receive.max_concurrent_streams = static_cast<std::uint32_t>(config.inflight);
        if (config.role != "client") server = rpc::make_server(context, {{"bench/Echo", config.bytes, &handler}}, so);
        if (config.role != "server") client = rpc::make_client(context, co);
        endpoint = {net::ip::address_v4::loopback(), config.port};
        measured_deadline = std::chrono::microseconds{static_cast<std::int64_t>(config.deadline_us)};
        deadline.timeout = config.deadline_us == 0 ? measured_deadline : std::max(measured_deadline, std::chrono::microseconds{1000000});
    }
    net::task<rpc::status_code> connect() override {
        if (server) endpoint = server->listen({net::ip::address_v4::loopback(), 0});
        return client->connect(endpoint);
    }
    net::task<rpc::call_result> call(slot &storage) override {
        return client->call("bench/Echo", {storage.request.data(), storage.request.size()},
                            {storage.reply.data(), storage.reply.size()}, deadline);
    }
    void close() noexcept override { if (client) client->close(); if (server) server->close(); }
    void begin_measurement() noexcept override { deadline.timeout = measured_deadline; }
    echo_handler handler{};
    std::unique_ptr<rpc::server> server{};
    std::unique_ptr<rpc::client> client{};
    net::ip::tcp::endpoint endpoint{};
    rpc::call_options deadline{};
    std::chrono::microseconds measured_deadline{};
};

// Fixed-capacity FIFO over driver-owned slots; no per-enqueue allocation.
class slot_queue {
public:
    explicit slot_queue(std::size_t capacity) : entries_(capacity, nullptr) {}
    void push(slot &value) {
        if (size_ == entries_.size()) throw std::logic_error{"raw queue overflow"};
        entries_[(head_ + size_++) % entries_.size()] = &value;
    }
    slot &front() const { return *entries_[head_]; }
    slot &pop() { auto &value = front(); head_ = (head_ + 1) % entries_.size(); --size_; return value; }
    bool empty() const noexcept { return size_ == 0; }
private:
    std::vector<slot *> entries_;
    std::size_t head_ = 0;
    std::size_t size_ = 0;
};

struct raw_channel final : channel {
    raw_channel(net::io_context &ctx, options const &config)
        : context(ctx), client(ctx),
          outgoing(config.inflight), incoming(config.inflight), active(config.inflight, nullptr),
          direct(config.inflight == 1 && config.rate == 0), endpoint{net::ip::address_v4::loopback(), config.port} {
        if (config.role != "client") acceptor = std::make_unique<net::tcp_acceptor>(ctx,
            net::ip::tcp::endpoint{net::ip::address_v4::loopback(), 0});
    }
    net::task<rpc::status_code> connect() override;
    net::task<rpc::call_result> call(slot &storage) override;
    void begin_measurement() noexcept override {}
    void close() noexcept override {
        if (closed) return;
        closed = true; if (acceptor) acceptor->close(); client.close(); server.close(); writer_wake.signal();
        // Driver slots remain alive until all I/O chains drain. On failure the
        // driver stops issuing calls; no signaled slot's buffers are reused.
        for (auto *entry : active) if (entry) entry->ready.signal();
    }
    void enqueue(slot &storage) {
        storage.sent = false; storage.received = false;
        auto found = std::find(active.begin(), active.end(), nullptr);
        if (found == active.end()) throw std::logic_error{"raw active overflow"};
        *found = &storage;
        outgoing.push(storage); incoming.push(storage); writer_wake.signal();
    }
    void release(slot &storage) {
        auto found = std::find(active.begin(), active.end(), &storage);
        if (found != active.end()) *found = nullptr;
    }
    template <class Factory> void spawn(Factory factory) {
        net::run_async(context.get_executor(), [] {}, [this](std::exception_ptr) { close(); })(factory);
    }
    net::io_context &context;
    std::unique_ptr<net::tcp_acceptor> acceptor{};
    net::tcp_socket client;
    net::tcp_socket server{};
    slot_queue outgoing;
    slot_queue incoming;
    std::vector<slot *> active;
    event writer_wake{};
    bool direct;
    bool closed = false;
    net::ip::tcp::endpoint endpoint;
};

auto raw_echo(raw_channel &self)
    CO2_BEG(net::task<>, (self), net::io_result<net::tcp_socket> accepted;
            net::io_result<std::size_t> result; std::array<std::uint8_t, 65536> buffer;) {
    CO2_AWAIT_SET(accepted, self.acceptor->accept());
    if (accepted.ec) { self.close(); CO2_RETURN(); }
    self.server = std::move(accepted.value);
    if (self.server.set_option(net::socket_option::no_delay{true})) { self.close(); CO2_RETURN(); }
    while (!self.closed) {
        CO2_AWAIT_SET(result, self.server.read_some(net::buffer(buffer)));
        if (result.ec || result.value == 0) break;
        CO2_AWAIT_SET(result, net::write(self.server, net::const_buffer{buffer.data(), result.value}));
        if (result.ec) break;
    }
    self.close(); CO2_RETURN();
}
CO2_END

auto raw_writer(raw_channel &self)
    CO2_BEG(net::task<>, (self), std::array<slot *, 16> batch{}; std::array<net::const_buffer, 16> buffers;
            std::size_t count = 0; net::io_result<std::size_t> result;) {
    while (!self.closed) {
        if (self.outgoing.empty()) { CO2_AWAIT(self.writer_wake.wait()); continue; }
        count = 0;
        while (!self.outgoing.empty() && count < batch.size()) {
            batch[count] = &self.outgoing.pop();
            buffers[count] = net::buffer(batch[count]->request);
            ++count;
        }
        CO2_AWAIT_SET(result, net::write(self.client, net::const_buffer_span{buffers.data(), count}));
        if (result.ec) { self.close(); break; }
        for (std::size_t i = 0; i < count; ++i) {
            batch[i]->sent = true;
            if (batch[i]->received) batch[i]->ready.signal();
        }
    }
    CO2_RETURN();
}
CO2_END

auto raw_reader(raw_channel &self)
    CO2_BEG(net::task<>, (self), std::array<std::uint8_t, 65536> buffer; net::io_result<std::size_t> result;
            std::size_t offset = 0;) {
    while (!self.closed) {
        CO2_AWAIT_SET(result, self.client.read_some(net::buffer(buffer)));
        if (result.ec || result.value == 0) { self.close(); break; }
        std::size_t consumed = 0;
        while (consumed < result.value) {
            if (self.incoming.empty()) throw std::logic_error{"unexpected raw reply"};
            auto &entry = self.incoming.front();
            auto const count = std::min(result.value - consumed, entry.reply.size() - offset);
            std::memcpy(entry.reply.data() + offset, buffer.data() + consumed, count);
            consumed += count; offset += count;
            if (offset == entry.reply.size()) {
                self.incoming.pop(); offset = 0; entry.received = true;
                if (entry.sent) entry.ready.signal();
            }
        }
    }
    CO2_RETURN();
}
CO2_END

auto raw_connect(raw_channel &self)
    CO2_BEG(net::task<rpc::status_code>, (self), net::io_result<> connected; std::error_code error;
            net::ip::tcp::endpoint endpoint;) {
    endpoint = self.endpoint;
    if (self.acceptor) {
        self.spawn([owner = &self] { return raw_echo(*owner); });
        endpoint = self.acceptor->local_endpoint(error);
    }
    if (error) throw std::system_error{error};
    CO2_AWAIT_SET(connected, self.client.connect(endpoint));
    if (connected.ec) CO2_RETURN(rpc::status_code::unavailable);
    if (self.client.set_option(net::socket_option::no_delay{true})) CO2_RETURN(rpc::status_code::unavailable);
    if (!self.direct) {
        self.spawn([owner = &self] { return raw_reader(*owner); });
        self.spawn([owner = &self] { return raw_writer(*owner); });
    }
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

auto raw_call(raw_channel &self, slot &storage)
    CO2_BEG(net::task<rpc::call_result>, (self, storage), net::io_result<std::size_t> result;) {
    if (self.closed) CO2_RETURN((rpc::call_result{rpc::status_code::unavailable, 0}));
    if (self.direct) {
        CO2_AWAIT_SET(result, net::write(self.client, net::buffer(storage.request)));
        if (result.ec) CO2_RETURN((rpc::call_result{rpc::status_code::unavailable, 0}));
        CO2_AWAIT_SET(result, net::read(self.client, net::buffer(storage.reply)));
        CO2_RETURN((rpc::call_result{result.ec ? rpc::status_code::unavailable : rpc::status_code::ok, result.value}));
    }
    self.enqueue(storage);
    CO2_AWAIT(storage.ready.wait());
    self.release(storage);
    CO2_RETURN((rpc::call_result{self.closed ? rpc::status_code::unavailable : rpc::status_code::ok,
                                self.closed ? 0 : storage.reply.size()}));
}
CO2_END

net::task<rpc::status_code> raw_channel::connect() { return raw_connect(*this); }
net::task<rpc::call_result> raw_channel::call(slot &storage) { return raw_call(*this, storage); }

auto wait_signal(net::signal_set &signals)
    CO2_BEG(net::task<>, (signals), net::io_result<int> result;) {
    CO2_AWAIT_SET(result, signals.wait());
    CO2_RETURN();
}
CO2_END

} // namespace

std::unique_ptr<channel> make_channel(net::io_context &context, options const &config) {
    if (config.transport == "rpc") return std::make_unique<rpc_channel>(context, config);
    return std::make_unique<raw_channel>(context, config);
}

void serve(options const &config) {
    net::recycling_memory_resource frames;
    net::io_context context{config.backend, net::single_thread_hint};
    context.set_frame_allocator(config.recycle_frames ? static_cast<net::memory_resource *>(&frames) : net::new_delete_resource());
    net::signal_set signals{context, SIGINT, SIGTERM};
    net::stop_source shutdown;
    std::unique_ptr<rpc_channel> rpc_server;
    std::unique_ptr<raw_channel> raw_server;
    std::exception_ptr failure;
    bool stopped = false;
    auto close = [&] { shutdown.request_stop(); if (rpc_server) rpc_server->close(); if (raw_server) raw_server->close(); signals.cancel(); };
    try {
        net::ip::tcp::endpoint endpoint;
        if (config.transport == "rpc") {
            rpc_server = std::make_unique<rpc_channel>(context, config);
            endpoint = rpc_server->server->listen({net::ip::address_v4::loopback(), 0});
        } else {
            raw_server = std::make_unique<raw_channel>(context, config);
            std::error_code error;
            endpoint = raw_server->acceptor->local_endpoint(error);
            if (error) throw std::system_error{error};
            raw_server->spawn([&] { return raw_echo(*raw_server); });
        }
        net::run_async(context.get_executor(), shutdown.get_token(), nullptr, [&] { stopped = true; close(); },
            [&](std::exception_ptr error) { failure = error; stopped = true; close(); })([&] { return wait_signal(signals); });
        std::cout << "{\"ready\":true,\"port\":" << endpoint.port() << ",\"pid\":" << ::getpid()
                  << ",\"backend\":\"" << net::to_string(config.backend) << "\"}\n" << std::flush;
        context.run_for(std::chrono::seconds{90});
        if (!stopped) throw std::runtime_error{"benchmark server watchdog expired"};
    } catch (...) { failure = std::current_exception(); }
    close();
    for (;;) { try { context.run(); break; } catch (...) { if (!failure) failure = std::current_exception(); close(); } }
    if (failure) std::rethrow_exception(failure);
    if (rpc_server) {
        auto const stats = rpc_server->server->stats();
        if (stats.connections || stats.active_calls || stats.request_bytes_in_use || stats.response_bytes_in_use || stats.control_bytes_in_use)
            throw std::runtime_error{"server retained live resources after drain"};
    }
}

} // namespace bench
