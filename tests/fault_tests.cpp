#include "client_fixture.hpp"
#include <cstdlib>
#include <iostream>
#include <new>

namespace {
std::size_t fail_after = 0;
std::size_t fail_size = 0;
bool injected = false;
void *allocate(std::size_t size) {
    if (fail_size && size == fail_size) { fail_size = 0; injected = true; throw std::bad_alloc{}; }
    if (fail_after && --fail_after == 0) { injected = true; throw std::bad_alloc{}; }
    if (auto *value = std::malloc(size ? size : 1)) return value;
    throw std::bad_alloc{};
}
}
void *operator new(std::size_t n) { return allocate(n); }
void *operator new[](std::size_t n) { return allocate(n); }
void operator delete(void *p) noexcept { std::free(p); }
void operator delete[](void *p) noexcept { std::free(p); }
void operator delete(void *p, std::size_t) noexcept { std::free(p); }
void operator delete[](void *p, std::size_t) noexcept { std::free(p); }

namespace {
struct failing_frames final : net::memory_resource {
    void *do_allocate(std::size_t bytes, std::size_t alignment) override {
        if (remaining && --remaining == 0) { failed = true; throw std::bad_alloc{}; }
        auto *pointer = net::new_delete_resource()->allocate(bytes, alignment); ++live; return pointer;
    }
    void do_deallocate(void *pointer, std::size_t bytes, std::size_t alignment) noexcept override {
        --live; net::new_delete_resource()->deallocate(pointer, bytes, alignment);
    }
    bool do_is_equal(net::memory_resource const &other) const noexcept override { return this == &other; }
    std::size_t remaining = 0, live = 0;
    bool failed = false;
};

auto wait_timer(net::steady_timer &timer)
    CO2_BEG(net::task<>, (timer), net::io_result<> result;) {
    CO2_AWAIT_SET(result, timer.wait()); CO2_RETURN();
}
CO2_END

// Explicit known-failure repro, intentionally excluded from the passing CTest
// suite: net::timer_heap::push allocates inside noexcept on a cold context.
void reproduce_net_timer_oom() {
    net::io_context context{net::epoll, net::single_thread_hint};
    net::steady_timer timer{context}; timer.expires_after(std::chrono::hours{1});
    net::run_async(context.get_executor())([&] { return wait_timer(timer); });
    fail_after = 1;
    context.poll(); // net 7dff859 currently terminates here (bad_alloc).
    fail_after = 0; timer.cancel(); context.run();
}

unsigned launch_failures() {
    unsigned exercised = 0;
    for (std::size_t nth = 1; nth <= 8; ++nth) {
        test_client::call a, b; test_client::fixture f;
        injected = false; fail_after = nth;
        bool synchronous_failure = false;
        try { f.start(a); } catch (std::bad_alloc const &) { synchronous_failure = true; }
        fail_after = 0;
        if (injected) ++exercised;
        f.context.poll();
        if (!synchronous_failure) { f.pipe->feed(test_client::reply(1)); f.context.poll(); CHECK(a.completions == 1); }
        else {
            CHECK(a.completions == 0 && f.client->stats().active_calls == 0);
            f.start(b); f.context.poll(); f.pipe->feed(test_client::reply(1)); f.context.poll();
            CHECK(b.completions == 1 && b.result.code == rpc::status_code::ok);
        }
        f.zero();
    }
    CHECK(exercised >= 2); return exercised;
}

unsigned handshake_failures() {
    unsigned exercised = 0;
    for (std::size_t nth = 1; nth <= 20; ++nth) {
        failing_frames frames;
        net::io_context context{net::default_backend, net::single_thread_hint};
        context.set_frame_allocator(&frames);
        auto client = rpc::make_client(context);
        auto pipe = std::make_shared<manual_pipe>();
        pipe->input.reserve(65536); pipe->output.reserve(65536); pipe->feed(test_client::peer_settings());
        unsigned completed = 0;
        net::run_async(context.get_executor(), [&](rpc::status_code) { ++completed; },
            [&](std::exception_ptr) { ++completed; })([&] { return client->attach(std::make_unique<manual_transport>(pipe)); });
        frames.remaining = nth;
        try { context.poll(); }
        catch (...) { frames.remaining = 0; client->close(); context.run(); throw; }
        frames.remaining = 0;
        if (frames.failed) ++exercised;
        client->close(); context.run();
        auto stats = client->stats();
        CHECK(completed == 1 && stats.active_calls == 0 && stats.request_bytes_in_use == 0 && stats.control_bytes_in_use == 0);
        CHECK(pipe->closed && frames.live == 0 && context.run() == 0);
    }
    CHECK(exercised >= 6); return exercised;
}

void receive_window_failure() {
    net::io_context context{net::default_backend, net::single_thread_hint};
    rpc::server_options options{}; options.max_connections = 1;
    options.connection.receive.max_frame_size = 1024;
    options.connection.receive_buffer_bytes = 65536;
    auto server = rpc::make_server(context, {}, options);
    auto pipe = std::make_shared<manual_pipe>();
    auto stream = std::make_unique<manual_transport>(pipe);
    injected = false; fail_size = options.connection.receive_buffer_bytes;
    auto accepted = server->attach(std::move(stream)); fail_size = 0;
    CHECK(injected && !accepted && pipe->closed && server->stats().connections == 0);
    pipe = std::make_shared<manual_pipe>();
    std::vector<std::uint8_t> preface(rpc::wire::preface_size);
    CHECK(rpc::wire::encode_preface({preface.data(), preface.size()}).code == rpc::wire::error::none);
    pipe->feed(preface); pipe->feed(test_client::peer_settings());
    CHECK(server->attach(std::make_unique<manual_transport>(pipe))); context.poll();
    CHECK(!pipe->closed && server->stats().connections == 1);
    server->close(); context.run(); CHECK(server->stats().connections == 0 && context.run() == 0);
}
}

int main(int argc, char **argv) {
    try {
        if (argc == 2 && std::string{argv[1]} == "--reproduce-net-timer-oom") {
            reproduce_net_timer_oom(); return 0;
        }
        CHECK(argc == 1);
        unsigned exercised = 0, recovered = 0;
        for (std::size_t nth = 1; nth <= 48; ++nth) {
            test_client::call a, b;
            rpc::client_options config{}; config.connection.receive.max_method_ids = 1;
            test_client::fixture f{config};
            f.start(a, "A");
            injected = false; fail_after = nth;
            try { f.context.poll(); } catch (...) { fail_after = 0; f.client->close(); f.context.run(); throw; }
            fail_after = 0;
            if (injected) ++exercised;
            if (a.completions == 0) { f.pipe->feed(test_client::reply(1)); f.context.poll(); }
            CHECK(a.completions == 1);
            CHECK(f.client->stats().active_calls == 0 && f.client->stats().request_bytes_in_use == 0);
            if (injected && (a.failure || a.result.code != rpc::status_code::ok) && f.client->ready()) {
                f.start(b, "B"); f.context.poll();
                // Failed admission must not permanently consume the sole method slot.
                if (b.completions == 0) { f.pipe->feed(test_client::reply(1)); f.context.poll(); }
                CHECK(b.completions == 1 && !b.failure && b.result.code == rpc::status_code::ok);
                ++recovered;
            }
            f.zero();
        }
        CHECK(exercised >= 4 && recovered >= 4);
        auto launches = launch_failures(); auto handshakes = handshake_failures(); receive_window_failure();
        std::cout << "PASS " << exercised << " admission failure points, " << recovered << " recoveries, "
                  << launches << " task launch failures, " << handshakes << " handshake/I/O frame failures, receive-window OOM recovery\n";
    } catch (std::exception const &error) { fail_after = 0; fail_size = 0; std::cerr << error.what() << '\n'; return 1; }
}
