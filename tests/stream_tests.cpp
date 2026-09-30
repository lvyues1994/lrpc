#include "backend.hpp"
#include "check.hpp"

#include <rpc/client.hpp>
#include <rpc/server.hpp>

#include <net/run_async.hpp>
#include <net/timeout.hpp>

#include <array>
#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

using rpc::wire::bytes_view;
using std::chrono::milliseconds;

net::backend_kind selected_backend = net::default_backend_t::kind;

bytes_view text(char const *value) noexcept {
    return {reinterpret_cast<std::uint8_t const *>(value), std::strlen(value)};
}
std::string text(bytes_view value) { return {reinterpret_cast<char const *>(value.data), value.size}; }

enum class mode { echo, fail_after_first, hang, sum, fan_out, silent };

struct stream_handler final : rpc::stream_method_handler {
    net::task<rpc::status_code> invoke(rpc::server_context &context, rpc::server_stream &stream) override;
    mode behaviour = mode::echo;
    unsigned read_delay_ms = 0;
    unsigned fan_out = 0;
    unsigned stopped = 0;
    unsigned finished = 0;
};

auto serve(stream_handler &self, rpc::server_context &context, rpc::server_stream &stream)
    CO2_BEG(net::task<rpc::status_code>, (self, context, stream), rpc::stream_read read; rpc::status_code wrote;
            net::io_result<> slept; std::uint64_t total = 0; std::array<std::uint8_t, 8> encoded{}; unsigned i = 0;
            std::vector<std::uint8_t> payload;) {
    for (;;) {
        if (self.read_delay_ms != 0) CO2_AWAIT_SET(slept, net::delay(milliseconds{self.read_delay_ms}));
        CO2_AWAIT_SET(read, stream.read());
        if (read.code != rpc::status_code::ok) {
            CHECK(context.stop_token.stop_requested());
            ++self.stopped;
            CO2_RETURN(read.code);
        }
        if (read.ended) break;
        if (self.behaviour == mode::fail_after_first) {
            CHECK(stream.set_trailer(text("broken")));
            CO2_RETURN(rpc::status_code::aborted);
        }
        total += read.message.size;
        if (self.behaviour == mode::echo) {
            CO2_AWAIT_SET(wrote, stream.write(read.message)); // The view lasts until the next read.
            if (wrote != rpc::status_code::ok) CO2_RETURN(wrote);
        }
    }
    if (self.behaviour == mode::sum) {
        std::memcpy(encoded.data(), &total, sizeof(total));
        CO2_AWAIT_SET(wrote, stream.write({encoded.data(), encoded.size()}));
        if (wrote != rpc::status_code::ok) CO2_RETURN(wrote);
    }
    if (self.behaviour == mode::fan_out) {
        for (i = 0; i < self.fan_out; ++i) {
            payload.assign(i * 97U, static_cast<std::uint8_t>(i));
            CO2_AWAIT_SET(wrote, stream.write({payload.data(), payload.size()}));
            if (wrote != rpc::status_code::ok) CO2_RETURN(wrote);
        }
    }
    static rpc::wire::metadata_entry const entries[] = {{text("sent"), text("yes")}};
    CHECK(stream.set_trailer({}, {entries, 1}));
    ++self.finished;
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

net::task<rpc::status_code> stream_handler::invoke(rpc::server_context &context, rpc::server_stream &stream) {
    return serve(*this, context, stream);
}

rpc::connection_options small_frames(std::uint32_t window = 1U << 20) {
    rpc::connection_options options{};
    options.receive.max_frame_size = 16U << 10; // Large messages fragment.
    options.receive.initial_stream_window = window;
    return options;
}

struct fixture {
    explicit fixture(rpc::connection_options connection = small_frames())
        : shard(context),
          server(shard,
                 {{"test/Bidi", nullptr, 1U << 20, 64, rpc::method_kind::bidirectional, &handler},
                  {"test/Sum", nullptr, 64, 64, rpc::method_kind::client_streaming, &handler},
                  {"test/Fan", nullptr, 1U << 20, 64, rpc::method_kind::server_streaming, &handler}},
                 server_options(connection)),
          client(shard, client_options(connection)), endpoint(server.listen({net::ip::address_v4::loopback(), 0})),
          bidi(client.bind("test/Bidi")), sum(client.bind("test/Sum")), fan(client.bind("test/Fan")) {}

    static rpc::server_options server_options(rpc::connection_options const &connection) {
        rpc::server_options options{};
        options.connection = connection;
        return options;
    }
    static rpc::client_options client_options(rpc::connection_options const &connection) {
        rpc::client_options options{};
        options.connection = connection;
        return options;
    }

    template <class F> void run(F factory) {
        std::exception_ptr failure;
        auto close = [&] {
            client.close();
            server.close();
        };
        net::run_async(context.get_executor(), close, [&](std::exception_ptr error) {
            failure = error;
            close();
        })(factory);
        context.run();
        if (failure) std::rethrow_exception(failure);
        auto const stats = server.stats();
        CHECK(stats.connections == 0 && stats.active_calls == 0 && stats.request_bytes == 0);
    }

    net::io_context context{selected_backend, net::single_thread_hint};
    rpc::shard shard;
    stream_handler handler{};
    rpc::server server;
    rpc::client client;
    net::ip::tcp::endpoint endpoint;
    rpc::method_ref bidi;
    rpc::method_ref sum;
    rpc::method_ref fan;
};

std::array<std::size_t, 7> const echo_sizes{{0, 1, 100, 5000, 16384, 70000, 300000}};

auto bidirectional(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::open_result opened; rpc::status_code wrote;
            rpc::stream_read read; rpc::call_result result; rpc::response_trailer trailer;
            std::array<std::uint8_t, 64> storage{}; std::vector<std::uint8_t> message; std::size_t i = 0;
            std::array<std::size_t, 7> sizes = echo_sizes;) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    CO2_AWAIT_SET(opened, f.client.open(f.bidi, rpc::method_kind::bidirectional));
    CHECK(opened.code == rpc::status_code::ok && opened.stream);
    for (i = 0; i < sizes.size(); ++i) {
        message.resize(sizes[i]);
        for (std::size_t j = 0; j < message.size(); ++j) message[j] = static_cast<std::uint8_t>(j * 7 + i);
        CO2_AWAIT_SET(wrote, opened.stream.write({message.data(), message.size()}));
        CHECK(wrote == rpc::status_code::ok);
        CO2_AWAIT_SET(read, opened.stream.read());
        CHECK(read.code == rpc::status_code::ok && !read.ended && read.message.size == message.size());
        CHECK(message.empty() || std::memcmp(read.message.data, message.data(), message.size()) == 0);
    }
    CO2_AWAIT_SET(wrote, opened.stream.writes_done());
    CHECK(wrote == rpc::status_code::ok);
    CO2_AWAIT_SET(wrote, opened.stream.write({}));
    CHECK(wrote == rpc::status_code::failed_precondition); // After the half-close.
    CO2_AWAIT_SET(read, opened.stream.read());
    CHECK(read.code == rpc::status_code::ok && read.ended);
    trailer.storage = {storage.data(), storage.size()};
    CO2_AWAIT_SET(result, opened.stream.finish(&trailer));
    CHECK(result.code == rpc::status_code::ok && trailer.metadata.count == 1);
    CHECK(f.handler.finished == 1);
    CO2_RETURN();
}
CO2_END

auto client_streaming(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::open_result opened; rpc::status_code wrote;
            rpc::stream_read read; rpc::call_result result; std::vector<std::uint8_t> message; std::uint64_t total = 0;
            std::uint64_t expected = 0; unsigned i = 0;) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    f.handler.behaviour = mode::sum;
    CO2_AWAIT_SET(opened, f.client.open(f.sum, rpc::method_kind::client_streaming));
    CHECK(opened.code == rpc::status_code::ok);
    for (i = 0; i < 40; ++i) {
        message.assign(i * 1000U, 1);
        expected += message.size();
        CO2_AWAIT_SET(wrote, opened.stream.write({message.data(), message.size()}));
        CHECK(wrote == rpc::status_code::ok);
    }
    CO2_AWAIT_SET(wrote, opened.stream.writes_done());
    CHECK(wrote == rpc::status_code::ok);
    CO2_AWAIT_SET(read, opened.stream.read());
    CHECK(read.code == rpc::status_code::ok && read.message.size == sizeof(total));
    std::memcpy(&total, read.message.data, sizeof(total));
    CHECK(total == expected);
    CO2_AWAIT_SET(result, opened.stream.finish());
    CHECK(result.code == rpc::status_code::ok);

    // A client_streaming handler that returns ok without its one reply.
    f.handler.behaviour = mode::silent;
    CO2_AWAIT_SET(opened, f.client.open(f.sum, rpc::method_kind::client_streaming));
    CO2_AWAIT_SET(wrote, opened.stream.writes_done());
    CO2_AWAIT_SET(result, opened.stream.finish());
    CHECK(result.code == rpc::status_code::invalid_argument);
    CO2_RETURN();
}
CO2_END

auto server_streaming(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::open_result opened; rpc::status_code wrote;
            rpc::stream_read read; rpc::call_result result; std::array<std::uint8_t, 4> request{}; unsigned count = 0;) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    f.handler.behaviour = mode::fan_out;
    f.handler.fan_out = 200;
    CO2_AWAIT_SET(opened, f.client.open(f.fan, rpc::method_kind::server_streaming));
    CHECK(opened.code == rpc::status_code::ok);
    CO2_AWAIT_SET(wrote, opened.stream.write({request.data(), request.size()}));
    CHECK(wrote == rpc::status_code::ok);
    CO2_AWAIT_SET(wrote, opened.stream.write({request.data(), request.size()}));
    CHECK(wrote == rpc::status_code::failed_precondition); // One request message.
    CO2_AWAIT_SET(wrote, opened.stream.writes_done());
    CHECK(wrote == rpc::status_code::ok);
    for (;;) {
        CO2_AWAIT_SET(read, opened.stream.read());
        CHECK(read.code == rpc::status_code::ok);
        if (read.ended) break;
        CHECK(read.message.size == count * 97U);
        CHECK(read.message.size == 0 || read.message.data[0] == static_cast<std::uint8_t>(count));
        ++count;
    }
    CHECK(count == 200);
    CO2_AWAIT_SET(result, opened.stream.finish());
    CHECK(result.code == rpc::status_code::ok);
    CO2_RETURN();
}
CO2_END

// A slow reader behind a 64 KiB window: writers wait for credit, nothing is lost.
auto flow_control(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::open_result opened; rpc::status_code wrote;
            rpc::stream_read read; rpc::call_result result; std::vector<std::uint8_t> message; unsigned i = 0;
            std::chrono::steady_clock::time_point started;) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    f.handler.behaviour = mode::sum;
    f.handler.read_delay_ms = 2;
    CO2_AWAIT_SET(opened, f.client.open(f.sum, rpc::method_kind::client_streaming));
    started = std::chrono::steady_clock::now();
    message.assign(30000, 5);
    for (i = 0; i < 20; ++i) {
        CO2_AWAIT_SET(wrote, opened.stream.write({message.data(), message.size()}));
        CHECK(wrote == rpc::status_code::ok);
    }
    // Twenty 30 KB messages through a 64 KiB window need the reader's pace.
    CHECK(std::chrono::steady_clock::now() - started >= milliseconds{20});
    message.assign(70000, 5);
    CO2_AWAIT_SET(wrote, opened.stream.write({message.data(), message.size()}));
    CHECK(wrote == rpc::status_code::resource_exhausted); // Larger than the whole window.
    CO2_AWAIT_SET(wrote, opened.stream.writes_done());
    CO2_AWAIT_SET(read, opened.stream.read());
    CHECK(read.code == rpc::status_code::ok && read.message.size == 8);
    CO2_AWAIT_SET(result, opened.stream.finish());
    CHECK(result.code == rpc::status_code::ok);
    CO2_RETURN();
}
CO2_END

auto failures(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::open_result opened; rpc::status_code wrote;
            rpc::stream_read read; rpc::call_result result; rpc::response_trailer trailer;
            std::array<std::uint8_t, 64> storage{}; std::array<std::uint8_t, 4> message{}; rpc::call_spec spec;
            net::stop_source stop; std::thread canceller;) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);

    f.handler.behaviour = mode::fail_after_first;
    CO2_AWAIT_SET(opened, f.client.open(f.bidi, rpc::method_kind::bidirectional));
    CO2_AWAIT_SET(wrote, opened.stream.write({message.data(), message.size()}));
    CHECK(wrote == rpc::status_code::ok);
    CO2_AWAIT_SET(read, opened.stream.read());
    CHECK(read.code == rpc::status_code::aborted && read.ended);
    trailer.storage = {storage.data(), storage.size()};
    CO2_AWAIT_SET(result, opened.stream.finish(&trailer));
    CHECK(result.code == rpc::status_code::aborted && text(trailer.message) == "broken");

    f.handler.behaviour = mode::hang;
    CO2_AWAIT_SET(opened, f.client.open(f.bidi, rpc::method_kind::bidirectional));
    CO2_AWAIT(net::delay(milliseconds{10}));
    opened.stream.cancel();
    CO2_AWAIT_SET(result, opened.stream.finish());
    CHECK(result.code == rpc::status_code::cancelled);
    opened.stream = {};

    spec.timeout = std::chrono::microseconds{40000};
    CO2_AWAIT_SET(opened, f.client.open(f.bidi, rpc::method_kind::bidirectional, &spec));
    CO2_AWAIT_SET(read, opened.stream.read());
    CHECK(read.code == rpc::status_code::deadline_exceeded && read.ended);
    CO2_AWAIT_SET(result, opened.stream.finish());
    CHECK(result.code == rpc::status_code::deadline_exceeded);
    opened.stream = {};
    CO2_RETURN();
}
CO2_END

auto open_bidi(fixture &f, rpc::open_result *out) CO2_BEG(net::task<>, (f, out)) {
    CO2_AWAIT_SET(*out, f.client.open(f.bidi, rpc::method_kind::bidirectional));
    CO2_RETURN();
}
CO2_END

// Opens on a coroutine whose environment carries `token`.
void open_with_token(fixture &f, net::stop_token token, rpc::open_result *out, bool *done) {
    net::run_async(f.context.get_executor(), std::move(token), nullptr, [done] { *done = true; },
                   [](std::exception_ptr error) { std::rethrow_exception(error); })(open_bidi(f, out));
}

// The opening coroutine's stop token covers the stream, from any thread.
auto cross_thread(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code connected; rpc::open_result opened; rpc::call_result result;
            net::stop_source stop; std::thread canceller; bool opened_done = false; int spins = 0;) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == rpc::status_code::ok);
    f.handler.behaviour = mode::hang;
    open_with_token(f, stop.get_token(), &opened, &opened_done);
    for (spins = 0; !opened_done; ++spins) {
        CHECK(spins < 1000);
        CO2_AWAIT(net::delay(milliseconds{1}));
    }
    CHECK(opened.code == rpc::status_code::ok);
    canceller = std::thread{[&] {
        std::this_thread::sleep_for(milliseconds{10});
        stop.request_stop();
    }};
    CO2_AWAIT_SET(result, opened.stream.finish());
    canceller.join();
    CHECK(result.code == rpc::status_code::cancelled);
    opened.stream = {};
    for (spins = 0; f.handler.stopped < 1; ++spins) { // The server heard the CANCEL.
        CHECK(spins < 2000);
        CO2_AWAIT(net::delay(milliseconds{1}));
    }
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
        { fixture f; f.run([&] { return bidirectional(f); }); std::cout << "PASS bidirectional echo, fragments\n"; }
        { fixture f; f.run([&] { return client_streaming(f); }); std::cout << "PASS client streaming\n"; }
        { fixture f; f.run([&] { return server_streaming(f); }); std::cout << "PASS server streaming\n"; }
        { fixture f{small_frames(64U << 10)}; f.run([&] { return flow_control(f); }); std::cout << "PASS flow control\n"; }
        { fixture f; f.run([&] { return failures(f); }); std::cout << "PASS error, cancel, deadline\n"; }
        { fixture f; f.run([&] { return cross_thread(f); }); std::cout << "PASS cross-thread stop\n"; }
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
