#include "client_fixture.hpp"
#include "../unary/src/runtime.hpp"
#include <rpc/typed.hpp>
#include <cstdlib>
#include <iostream>
#include <new>

namespace {
std::size_t fail_after = 0;
std::size_t fail_size = 0;
std::size_t fail_size_after = 0;
bool injected = false;
void *allocate(std::size_t size) {
    if (fail_size && size == fail_size && (!fail_size_after || --fail_size_after == 0)) {
        fail_size = 0; injected = true; throw std::bad_alloc{};
    }
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

rpc::client_options small_client() {
    rpc::client_options options{};
    options.connection.receive.max_frame_size = 1024;
    options.connection.receive.max_message_size = 1024;
    options.connection.receive.max_concurrent_streams = 2;
    options.connection.receive.max_method_ids = 2;
    options.request_bytes = 32768; options.control_bytes = 4096;
    options.max_registered_methods = 2;
    return options;
}

void construction_rollback() {
    net::io_context context{net::default_backend, net::single_thread_hint};
    auto anchor = rpc::make_client(context, small_client());
    auto scheduler = rpc::detail::shard_deadlines(context);
    auto const capacity = rpc::detail::deadline_capacity(*scheduler);
    unsigned failures = 0;
    for (std::size_t nth = 1; nth <= 48; ++nth) {
        std::unique_ptr<rpc::client> candidate;
        injected = false; fail_after = nth;
        try { candidate = rpc::make_client(context, small_client()); }
        catch (std::bad_alloc const &) { ++failures; }
        fail_after = 0;
        if (candidate) { candidate->close(); candidate.reset(); }
        CHECK(rpc::detail::deadline_capacity(*scheduler) == capacity);
        CHECK(context.run() == 0);
    }
    CHECK(failures != 0);
}

void registry_rollback() {
    rpc::codec_ops const operations{
        [](void const *) -> std::size_t { return 0; },
        [](void const *, rpc::wire::mutable_bytes_view) { return true; },
        [](rpc::wire::bytes_view, void *) { return true; }};
    rpc::method_descriptor const first[] = {
        {"A", rpc::method_kind::unary, rpc::idempotency::unknown, &operations, &operations},
        {"B", rpc::method_kind::unary, rpc::idempotency::unknown, &operations, &operations}};
    rpc::method_descriptor const replacement[] = {
        {"C", rpc::method_kind::unary, rpc::idempotency::unknown, &operations, &operations},
        {"D", rpc::method_kind::unary, rpc::idempotency::unknown, &operations, &operations}};
    unsigned failures = 0;
    for (std::size_t nth = 1; nth <= 12; ++nth) {
        net::io_context context{net::default_backend, net::single_thread_hint};
        auto client = rpc::make_client(context, small_client());
        bool failed = false; injected = false; fail_after = nth;
        try { CHECK(client->bind({"first", first, 2}).size() == 2); }
        catch (std::bad_alloc const &) { failed = true; ++failures; }
        fail_after = 0;
        if (failed) CHECK(client->bind({"replacement", replacement, 2}).size() == 2);
        CHECK(context.run() == 0);
    }
    CHECK(failures != 0);
}

void listener_start_rollback() {
    unsigned failures = 0;
    // Fail each cold driver/worker/accept frame. Keep the facade alive while
    // draining so destruction cannot hide a failed-start work-count leak.
    for (std::size_t nth = 1; nth <= 8; ++nth) {
        failing_frames frames;
        net::io_context context{net::default_backend, net::single_thread_hint};
        context.set_frame_allocator(&frames);
        rpc::server_options options{}; options.max_active_calls = 2;
        auto server = rpc::make_server(context, {}, options);
        bool failed = false; frames.remaining = nth;
        try { server->listen({net::ip::address_v4::loopback(), 0}); }
        catch (std::bad_alloc const &) { failed = true; ++failures; }
        frames.remaining = 0;
        if (!failed) server->close();
        context.run();
        CHECK(server->stats().connections == 0 && server->stats().active_calls == 0);
        CHECK(frames.live == 0 && context.run() == 0);
    }
    CHECK(failures != 0);
}

void status_copy_failure() {
    std::string const message(200, 'x');
    std::vector<std::uint8_t> packet(256);
    auto head = rpc::wire::encode_end_head({{reinterpret_cast<std::uint8_t const *>(message.data()), message.size()}, {}},
        {packet.data() + 16, packet.size() - 16});
    CHECK(head.code == rpc::wire::error::none);
    CHECK(rpc::wire::encode_header({static_cast<std::uint32_t>(head.written), 1, rpc::wire::frame_type::end, 0,
        static_cast<std::uint16_t>(head.written), static_cast<std::uint32_t>(rpc::status_code::permission_denied)},
        {packet.data(), 16}).code == rpc::wire::error::none);
    packet.resize(16 + head.written);
    for (std::size_t nth : {std::size_t{1}, std::size_t{2}}) {
        test_client::fixture f; rpc::call_result result; unsigned completed = 0; std::exception_ptr error;
        net::run_async(f.context.get_executor(), [&](rpc::call_result value) {
            result = std::move(value); ++completed;
        },
            [&](std::exception_ptr value) { error = value; })([&] { return f.client->call("Echo", {}, {}); });
        f.context.poll(); f.pipe->feed(packet);
        // libstdc++'s owning string copy requests text length + its terminator.
        // Target that allocation, independently of transport read rearming.
        injected = false; fail_size = message.size() + 1; fail_size_after = nth;
        f.context.poll(); fail_size = 0; fail_size_after = 0;
        CHECK(completed == 1 && !error);
        if (nth == 1) CHECK(injected && result.code == rpc::status_code::resource_exhausted && result.message.empty());
        else {
            CHECK(!injected && result.code == rpc::status_code::permission_denied && result.message == message);
        }
        f.zero();
    }
}

void builder_retry() {
    unsigned failures = 0;
    for (std::size_t nth = 1; nth <= 16; ++nth) {
        net::io_context context{net::default_backend, net::single_thread_hint};
        rpc::server_options options{}; options.max_active_calls = 2;
        rpc::server_builder builder{context, options}; std::unique_ptr<rpc::server> server;
        injected = false; fail_after = nth;
        try { server = builder.build(); } catch (std::bad_alloc const &) { ++failures; }
        fail_after = 0;
        if (!server) server = builder.build();
        CHECK(server && context.run() == 0); server->close();
    }
    CHECK(failures != 0);
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
        // Count actual reachable failures, rather than requiring the allocation
        // topology of the former per-call node/timer implementation.
        CHECK(exercised != 0 && recovered == exercised);
        auto launches = launch_failures(); auto handshakes = handshake_failures(); receive_window_failure();
        construction_rollback(); registry_rollback(); listener_start_rollback();
        status_copy_failure(); builder_retry();
        std::cout << "PASS " << exercised << " admission failure points, " << recovered << " recoveries, "
                  << launches << " task launch failures, " << handshakes << " handshake/I/O frame failures, receive-window OOM recovery\n";
    } catch (std::exception const &error) { fail_after = 0; fail_size = 0; std::cerr << error.what() << '\n'; return 1; }
}
