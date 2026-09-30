// Cost gates for the v2 hot path, each measured per call in steady state:
// heap allocations on the client and server threads, clock reads with and
// without a deadline, and socket writes with 64 calls in flight.
#include "backend.hpp"
#include "check.hpp"

#include <rpc/v2/client.hpp>
#include <rpc/v2/server.hpp>

#include <net/run_async.hpp>

#include <array>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <new>
#include <thread>
#include <vector>

namespace {
enum role : int { other = 0, server_thread = 1, client_thread = 2 };
thread_local int current_role = other;
std::atomic<std::uint64_t> allocations[3];

void *counted(std::size_t size) {
    allocations[current_role].fetch_add(1, std::memory_order_relaxed);
    if (void *pointer = std::malloc(size != 0 ? size : 1)) return pointer;
    throw std::bad_alloc{};
}
} // namespace

void *operator new(std::size_t size) { return counted(size); }
void *operator new[](std::size_t size) { return counted(size); }
void operator delete(void *pointer) noexcept { std::free(pointer); }
void operator delete[](void *pointer) noexcept { std::free(pointer); }
void operator delete(void *pointer, std::size_t) noexcept { std::free(pointer); }
void operator delete[](void *pointer, std::size_t) noexcept { std::free(pointer); }

namespace {

namespace v2 = rpc::v2;
using rpc::wire::bytes_view;
using rpc::wire::mutable_bytes_view;

net::backend_kind selected_backend = net::default_backend_t::kind;
constexpr std::size_t payload = 64;
constexpr unsigned workers = 64;
std::atomic<std::uint64_t> server_clock_reads{0};

struct counting_stream {
    net::tcp_socket *socket;
    std::atomic<std::uint64_t> *writes;
    template <class Buffers> auto read_some(Buffers const &buffers) { return socket->read_some(buffers); }
    template <class Buffers> auto write_some(Buffers const &buffers) {
        writes->fetch_add(1, std::memory_order_relaxed);
        return socket->write_some(buffers);
    }
};

struct counting_transport final : v2::transport {
    counting_transport(net::tcp_socket value, std::atomic<std::uint64_t> &writes)
        : socket(std::move(value)), counter{&socket, &writes}, stream_(&counter) {}
    net::any_stream &stream() noexcept override { return stream_; }
    void close() noexcept override { static_cast<void>(socket.close()); }
    net::tcp_socket socket;
    counting_stream counter;
    net::any_stream stream_;
};

auto respond(bytes_view request, v2::response_writer &response)
    CO2_BEG(net::task<v2::status_code>, (request, response)) {
    CO2_RETURN(response.assign(request) ? v2::status_code::ok : v2::status_code::internal);
}
CO2_END

struct echo_handler final : v2::method_handler {
    net::task<v2::status_code> invoke(v2::server_context &, bytes_view request, v2::response_writer &response) override {
        server_clock_reads.store(v2::debug::clock_reads(), std::memory_order_relaxed);
        return respond(request, response);
    }
};

struct server_side;
net::task<> accept_one(server_side &self);

// One io_context thread serving one counted connection.
struct server_side {
    server_side() {
        context.set_frame_allocator(&context.recycling_frame_allocator());
        shard.reset(new v2::shard(context));
        server.reset(new v2::server(*shard, {{"gate/Echo", &handler, payload}}));
        endpoint = acceptor.local_endpoint(error);
        CHECK(!error);
    }
    void start() {
        thread = std::thread{[this] {
            current_role = server_thread;
            try {
                net::run_async(context.get_executor())(accept_one(*this));
                context.run();
            } catch (...) {
                failure = std::current_exception();
            }
        }};
    }

    net::io_context context{selected_backend, net::single_thread_hint};
    std::unique_ptr<v2::shard> shard;
    echo_handler handler;
    std::unique_ptr<v2::server> server;
    net::tcp_acceptor acceptor{context, {net::ip::address_v4::loopback(), 0}};
    std::error_code error;
    net::ip::tcp::endpoint endpoint;
    std::atomic<std::uint64_t> writes{0};
    std::thread thread;
    std::exception_ptr failure;
};

auto accept_one_impl(server_side &self)
    CO2_BEG(net::task<>, (self), net::io_result<net::tcp_socket> accepted;) {
    CO2_AWAIT_SET(accepted, self.acceptor.accept());
    CHECK(!accepted.ec);
    CHECK(!accepted.value.set_option(net::socket_option::no_delay{true}));
    CHECK(self.server->attach(
        std::unique_ptr<v2::transport>(new counting_transport(std::move(accepted.value), self.writes))));
    CO2_RETURN();
}
CO2_END

net::task<> accept_one(server_side &self) { return accept_one_impl(self); }

auto close_server(server_side &self) CO2_BEG(net::task<>, (self)) {
    self.server->close();
    CO2_RETURN();
}
CO2_END

struct latch {
    struct awaiter {
        latch *self;
        bool await_ready() const noexcept { return self->remaining == 0; }
        net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
            self->waiter.h = handle;
            self->executor = env->executor;
            return net::noop_coroutine();
        }
        void await_resume() const noexcept {}
    };
    awaiter wait() noexcept { return {this}; }
    void count_down() noexcept {
        if (--remaining == 0 && waiter.h) executor.post(waiter);
    }
    unsigned remaining = 0;
    net::continuation waiter{};
    net::executor_ref executor{};
};

struct counters {
    std::uint64_t client_allocations, server_allocations, client_clock, server_clock, client_writes, server_writes;
};

struct client_side {
    explicit client_side(server_side &peer) : server(peer) {
        context.set_frame_allocator(&context.recycling_frame_allocator());
        shard.reset(new v2::shard(context));
        client.reset(new v2::client(*shard));
        method = client->bind("gate/Echo");
    }
    counters snapshot() const {
        return {allocations[client_thread].load(), allocations[server_thread].load(), v2::debug::clock_reads(),
                server_clock_reads.load(), writes.load(), server.writes.load()};
    }

    net::io_context context{selected_backend, net::single_thread_hint};
    server_side &server;
    std::unique_ptr<v2::shard> shard;
    std::unique_ptr<v2::client> client;
    v2::method_ref method;
    std::atomic<std::uint64_t> writes{0};
    std::array<std::array<std::uint8_t, payload>, workers> requests{};
    std::array<std::array<std::uint8_t, payload>, workers> replies{};
    latch done;
};

auto sequential(client_side &self, unsigned count, v2::call_spec const *spec)
    CO2_BEG(net::task<>, (self, count, spec), v2::call_result result; unsigned i = 0;) {
    for (i = 0; i < count; ++i) {
        CO2_AWAIT_SET(result, self.client->call(self.method, {self.requests[0].data(), payload},
                                                {self.replies[0].data(), payload}, spec));
        CHECK(result.code == v2::status_code::ok && result.size == payload);
    }
    CO2_RETURN();
}
CO2_END

auto worker(client_side &self, unsigned index, unsigned count)
    CO2_BEG(net::task<>, (self, index, count), v2::call_result result; unsigned i = 0;) {
    for (i = 0; i < count; ++i) {
        CO2_AWAIT_SET(result, self.client->call(self.method, {self.requests[index].data(), payload},
                                                {self.replies[index].data(), payload}));
        CHECK(result.code == v2::status_code::ok && self.replies[index] == self.requests[index]);
    }
    self.done.count_down();
    CO2_RETURN();
}
CO2_END

void report(char const *name, counters const &before, counters const &after, double calls) {
    std::cout << name << ": client allocs/call " << double(after.client_allocations - before.client_allocations) / calls
              << ", server allocs/call " << double(after.server_allocations - before.server_allocations) / calls
              << ", client clock/call " << double(after.client_clock - before.client_clock) / calls
              << ", server clock/call " << double(after.server_clock - before.server_clock) / calls
              << ", client writes/call " << double(after.client_writes - before.client_writes) / calls
              << ", server writes/call " << double(after.server_writes - before.server_writes) / calls << '\n';
}

auto drive(client_side &self)
    CO2_BEG(net::task<>, (self), std::unique_ptr<net::tcp_socket> socket; net::io_result<> connected;
            v2::status_code attached; v2::call_spec deadline; counters before{}; counters after{}; unsigned i = 0;
            unsigned round = 0;) {
    socket.reset(new net::tcp_socket(self.context));
    CO2_AWAIT_SET(connected, socket->connect(self.server.endpoint));
    CHECK(!connected.ec && !socket->set_option(net::socket_option::no_delay{true}));
    CO2_AWAIT_SET(attached, self.client->attach(std::unique_ptr<v2::transport>(
                                new counting_transport(std::move(*socket), self.writes))));
    CHECK(attached == v2::status_code::ok);
    for (i = 0; i < workers; ++i) self.requests[i].fill(static_cast<std::uint8_t>(i));
    deadline.timeout = std::chrono::seconds{5};

    // Warm every pool, cache and allocator class on both sides.
    CO2_AWAIT(sequential(self, 2000, nullptr));
    CO2_AWAIT(sequential(self, 2000, &deadline));

    before = self.snapshot();
    CO2_AWAIT(sequential(self, 5000, nullptr));
    after = self.snapshot();
    report("sequential", before, after, 5000);
    CHECK(after.client_allocations == before.client_allocations);
    CHECK(after.server_allocations == before.server_allocations);
    CHECK(after.client_clock == before.client_clock);
    CHECK(after.server_clock == before.server_clock);

    before = self.snapshot();
    CO2_AWAIT(sequential(self, 5000, &deadline));
    after = self.snapshot();
    report("deadline", before, after, 5000);
    CHECK(after.client_allocations == before.client_allocations);
    CHECK(after.server_allocations == before.server_allocations);
    CHECK(after.client_clock - before.client_clock == 5000);
    CHECK(after.server_clock - before.server_clock == 5000);

    for (round = 0; round < 2; ++round) { // The first round warms 64-deep pools.
        self.done.remaining = workers;
        for (i = 0; i < workers; ++i) net::run_async(self.context.get_executor())(worker(self, i, 200));
        before = self.snapshot(); // Workers are posted, not yet running.
        CO2_AWAIT(self.done.wait());
        after = self.snapshot();
    }
    report("64 in flight", before, after, workers * 200.0);
    CHECK(after.client_allocations == before.client_allocations);
    CHECK(after.server_allocations == before.server_allocations);
    CHECK(after.client_clock == before.client_clock);
    CHECK(double(after.client_writes - before.client_writes) <= 0.1 * workers * 200);
    CHECK(double(after.server_writes - before.server_writes) <= 0.1 * workers * 200);
    CO2_RETURN();
}
CO2_END

} // namespace

int main(int argc, char **argv) {
    try {
        selected_backend = test_backend(argc, argv);
        if (!net::backend_available(selected_backend)) {
            std::cout << "SKIP backend unavailable\n";
            return 77;
        }
        server_side server;
        server.start();
        std::exception_ptr failure;
        {
            client_side client{server};
            current_role = client_thread;
            net::run_async(client.context.get_executor(), [&] { client.client->close(); },
                           [&](std::exception_ptr error) {
                               failure = error;
                               client.client->close();
                           })(drive(client));
            client.context.run();
            current_role = other;
        }
        net::run_async(server.context.get_executor())(close_server(server));
        server.thread.join();
        if (failure) std::rethrow_exception(failure);
        if (server.failure) std::rethrow_exception(server.failure);
        std::cout << "PASS v2 cost gates\n";
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
