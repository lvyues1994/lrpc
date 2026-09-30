#include "backend.hpp"
#include "check.hpp"

#include <rpc/v2/client.hpp>
#include <rpc/v2/server.hpp>

#include <net/run_async.hpp>
#include <net/timeout.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

namespace v2 = rpc::v2;
using rpc::wire::bytes_view;
using rpc::wire::mutable_bytes_view;
using std::chrono::milliseconds;

net::backend_kind selected_backend = net::default_backend_t::kind;
constexpr std::size_t max_response = 1U << 20;

struct echo_handler final : v2::method_handler {
    net::task<v2::status_code> invoke(v2::server_context &context, bytes_view request,
                                      v2::response_writer &response) override;
    unsigned calls = 0;
    unsigned stopped = 0;
    bool throws = false;
    bool delayed = false; // Sleep request[0] milliseconds.
    bool hang = false;    // Sleep until stopped.
    bool doubled = false; // Reply twice the request, exceeding small method limits.
    int trailer = 0;      // 1: metadata with ok, 2: message with an error, 3: oversized.
    std::vector<unsigned> order{};
};

bytes_view text(char const *value) noexcept {
    return {reinterpret_cast<std::uint8_t const *>(value), std::strlen(value)};
}
std::string text(bytes_view value) { return {reinterpret_cast<char const *>(value.data), value.size}; }

auto echo(echo_handler &self, v2::server_context &context, bytes_view request, v2::response_writer &response)
    CO2_BEG(net::task<v2::status_code>, (self, context, request, response), net::io_result<> slept;
            std::vector<std::uint8_t> twice;) {
    ++self.calls;
    if (self.throws) throw std::runtime_error{"handler failure must not escape"};
    if (self.delayed && request.size != 0 && request.data[0] != 0) {
        CO2_AWAIT_SET(slept, net::delay(milliseconds{request.data[0]}));
    } else if (self.hang) {
        CO2_AWAIT_SET(slept, net::delay(std::chrono::seconds{30}));
    }
    if (slept.ec) {
        CHECK(context.stop_token.stop_requested());
        ++self.stopped;
        CO2_RETURN(v2::status_code::cancelled);
    }
    if (request.size != 0) self.order.push_back(request.data[0]);
    if (self.trailer == 1) {
        static rpc::wire::metadata_entry const entries[] = {{text("k"), text("v")}};
        CHECK(response.set_trailer(text("ok-note"), {entries, 1}));
    } else if (self.trailer == 2) {
        CHECK(response.set_trailer(text("bad input")));
        CO2_RETURN(v2::status_code::invalid_argument);
    } else if (self.trailer == 3) {
        twice.assign(200, std::uint8_t{'x'});
        if (!response.set_trailer({twice.data(), twice.size()})) CO2_RETURN(v2::status_code::internal);
    }
    if (self.doubled) {
        twice.assign(request.data, request.data + request.size);
        twice.insert(twice.end(), request.data, request.data + request.size);
        if (!response.assign({twice.data(), twice.size()})) CO2_RETURN(v2::status_code::internal);
        CO2_RETURN(v2::status_code::ok);
    }
    if (!response.assign(request)) CO2_RETURN(v2::status_code::internal);
    CO2_RETURN(v2::status_code::ok);
}
CO2_END

net::task<v2::status_code> echo_handler::invoke(v2::server_context &context, bytes_view request,
                                                v2::response_writer &response) {
    return echo(*this, context, request, response);
}

auto call_once(v2::client &client, v2::method_ref method, bytes_view request, mutable_bytes_view response,
               v2::call_spec const *spec)
    CO2_BEG(net::task<v2::call_result>, (client, method, request, response, spec), v2::call_result result;) {
    CO2_AWAIT_SET(result, client.call(method, request, response, spec));
    CO2_RETURN(result);
}
CO2_END

struct fixture {
    explicit fixture(v2::server_options server_config = {}, v2::client_options client_config = {},
                     std::size_t response_limit = max_response, std::size_t trailer_limit = 0)
        : shard(context), server(shard, {{"test/Echo", &handler, response_limit, trailer_limit}}, server_config),
          client(shard, client_config), endpoint(server.listen({net::ip::address_v4::loopback(), 0})),
          echo_method(client.bind("test/Echo")) {}

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
        CHECK(stats.connections == 0 && stats.active_calls == 0);
        CHECK(stats.request_bytes == 0 && stats.response_bytes == 0);
        CHECK(client.pending() == 0);
    }

    // Runs one call on its own coroutine; done counts completions.
    void spawn(bytes_view request, mutable_bytes_view response, v2::call_spec const *spec, v2::call_result &out,
               unsigned &done, net::stop_token token = {}) {
        net::run_async(context.get_executor(), std::move(token), nullptr,
                       [&out, &done](v2::call_result value) {
                           out = value;
                           ++done;
                       },
                       [](std::exception_ptr error) { std::rethrow_exception(error); })([this, request, response, spec] {
            return call_once(client, echo_method, request, response, spec);
        });
    }

    net::io_context context{selected_backend, net::single_thread_hint};
    v2::shard shard;
    echo_handler handler{};
    v2::server server;
    v2::client client;
    net::ip::tcp::endpoint endpoint;
    v2::method_ref echo_method;
};

auto wait_until(unsigned const &done, unsigned target)
    CO2_BEG(net::task<>, (done, target), int spins = 0;) {
    for (spins = 0; done < target; ++spins) {
        CHECK(spins < 5000);
        CO2_AWAIT(net::delay(milliseconds{1}));
    }
    CO2_RETURN();
}
CO2_END

auto basic(fixture &f)
    CO2_BEG(net::task<>, (f), v2::status_code connected; v2::call_result result;
            std::array<std::uint8_t, 64> request{}; std::array<std::uint8_t, 64> response{};
            std::array<std::uint8_t, 8> small{}; v2::method_ref missing; int i = 0;) {
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {}, {}));
    CHECK(result.code == v2::status_code::unavailable);
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == v2::status_code::ok && f.client.ready());
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == v2::status_code::failed_precondition);
    request.fill(0x5a);
    for (i = 0; i < 3; ++i) {
        response.fill(0);
        CO2_AWAIT_SET(result, f.client.call(f.echo_method, {request.data(), request.size()},
                                            {response.data(), response.size()}));
        CHECK(result.code == v2::status_code::ok && result.size == request.size());
        CHECK(request == response);
    }
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {}, {}));
    CHECK(result.code == v2::status_code::ok && result.size == 0);
    missing = f.client.bind("missing/Method");
    CHECK(f.client.bind("missing/Method").index == missing.index);
    for (i = 0; i < 2; ++i) {
        CO2_AWAIT_SET(result, f.client.call(missing, {}, {}));
        CHECK(result.code == v2::status_code::unimplemented);
    }
    CO2_AWAIT_SET(result, f.client.call(v2::method_ref{}, {}, {}));
    CHECK(result.code == v2::status_code::invalid_argument);
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {request.data(), request.size()}, {small.data(), small.size()}));
    CHECK(result.code == v2::status_code::resource_exhausted);
    f.handler.throws = true;
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {}, {}));
    CHECK(result.code == v2::status_code::internal);
    f.handler.throws = false;
    CHECK(f.handler.calls == 6);
    CO2_RETURN();
}
CO2_END

auto response_limit(fixture &f)
    CO2_BEG(net::task<>, (f), v2::status_code connected; v2::call_result result;
            std::array<std::uint8_t, 32> request{}; std::array<std::uint8_t, 64> response{};) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == v2::status_code::ok);
    f.handler.doubled = true; // 64 bytes against a 48-byte method limit.
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {request.data(), request.size()},
                                        {response.data(), response.size()}));
    CHECK(result.code == v2::status_code::internal);
    f.handler.doubled = false;
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {request.data(), request.size()},
                                        {response.data(), response.size()}));
    CHECK(result.code == v2::status_code::ok && result.size == request.size());
    CO2_RETURN();
}
CO2_END

auto trailers(fixture &f)
    CO2_BEG(net::task<>, (f), v2::status_code connected; v2::call_result result; v2::response_trailer trailer;
            std::array<std::uint8_t, 128> storage{}; std::array<std::uint8_t, 4> tiny{};
            std::array<std::uint8_t, 8> request{}; std::array<std::uint8_t, 8> response{};
            rpc::wire::decode_result<rpc::wire::metadata_entry> entry;) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == v2::status_code::ok);
    request.fill(9);
    trailer.storage = {storage.data(), storage.size()};
    f.handler.trailer = 1;
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {request.data(), request.size()},
                                        {response.data(), response.size()}, nullptr, &trailer));
    CHECK(result.code == v2::status_code::ok && result.size == request.size() && response == request);
    // The wire carries a status message only with an error.
    CHECK(trailer.message.size == 0 && trailer.metadata.count == 1 && !trailer.truncated);
    entry = rpc::wire::decode_metadata_entry(trailer.metadata.entries);
    CHECK(entry.code == rpc::wire::error::none && text(entry.value.key) == "k" && text(entry.value.value) == "v");

    f.handler.trailer = 2;
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {}, {}, nullptr, &trailer));
    CHECK(result.code == v2::status_code::invalid_argument);
    CHECK(text(trailer.message) == "bad input" && trailer.metadata.count == 0);
    trailer.storage = {tiny.data(), tiny.size()};
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {}, {}, nullptr, &trailer));
    CHECK(result.code == v2::status_code::invalid_argument && trailer.truncated && trailer.message.size == 0);
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {}, {}));
    CHECK(result.code == v2::status_code::invalid_argument);

    f.handler.trailer = 3; // Beyond the method's 64-byte trailer limit.
    trailer.storage = {storage.data(), storage.size()};
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {}, {}, nullptr, &trailer));
    CHECK(result.code == v2::status_code::internal && trailer.message.size == 0 && !trailer.truncated);
    f.handler.trailer = 0;
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {}, {}, nullptr, &trailer));
    CHECK(result.code == v2::status_code::ok && trailer.message.size == 0 && trailer.metadata.count == 0);
    CO2_RETURN();
}
CO2_END

auto concurrent(fixture &f)
    CO2_BEG(net::task<>, (f), v2::status_code connected; unsigned done = 0; std::array<std::uint8_t, 8> requests{};
            std::array<std::uint8_t, 8> replies{}; std::array<v2::call_result, 8> results{}; std::size_t i = 0;) {
    f.handler.delayed = true;
    requests = {{40, 35, 30, 25, 20, 15, 10, 5}};
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == v2::status_code::ok);
    for (i = 0; i < requests.size(); ++i) f.spawn({&requests[i], 1}, {&replies[i], 1}, nullptr, results[i], done);
    CO2_AWAIT(wait_until(done, 8));
    for (i = 0; i < requests.size(); ++i) {
        CHECK(results[i].code == v2::status_code::ok && results[i].size == 1);
        CHECK(replies[i] == requests[i]);
    }
    CHECK((f.handler.order == std::vector<unsigned>{5, 10, 15, 20, 25, 30, 35, 40}));
    CO2_RETURN();
}
CO2_END

auto large(fixture &f)
    CO2_BEG(net::task<>, (f), v2::status_code connected; v2::call_result result; std::vector<std::uint8_t> request;
            std::vector<std::uint8_t> response; unsigned done = 0; std::array<v2::call_result, 4> results{};
            std::vector<std::vector<std::uint8_t>> replies; std::size_t i = 0;) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == v2::status_code::ok);
    request.resize(700U * 1024U); // Beyond the 64 KiB receive window on both sides.
    for (i = 0; i < request.size(); ++i) request[i] = static_cast<std::uint8_t>(i * 131U);
    response.resize(request.size());
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {request.data(), request.size()},
                                        {response.data(), response.size()}));
    CHECK(result.code == v2::status_code::ok && result.size == request.size() && response == request);
    // Several large frames interleaved with small ones in one burst.
    replies.assign(results.size(), std::vector<std::uint8_t>(request.size()));
    for (i = 0; i < results.size(); ++i) {
        auto const size = i % 2 == 0 ? request.size() : std::size_t{3};
        f.spawn({request.data(), size}, {replies[i].data(), replies[i].size()}, nullptr, results[i], done);
    }
    CO2_AWAIT(wait_until(done, static_cast<unsigned>(results.size())));
    for (i = 0; i < results.size(); ++i) {
        auto const size = i % 2 == 0 ? request.size() : std::size_t{3};
        CHECK(results[i].code == v2::status_code::ok && results[i].size == size);
        CHECK(std::equal(replies[i].begin(), replies[i].begin() + static_cast<std::ptrdiff_t>(size), request.begin()));
    }
    request.resize(2U << 20); // Larger than the peer's frame limit: refused locally.
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {request.data(), request.size()}, {}));
    CHECK(result.code == v2::status_code::resource_exhausted);
    CO2_RETURN();
}
CO2_END

auto deadlines(fixture &f)
    CO2_BEG(net::task<>, (f), v2::status_code connected; v2::call_result result; v2::call_spec spec;
            std::chrono::steady_clock::time_point started; unsigned calls = 0;
            std::array<std::uint8_t, 1> request{}; std::array<std::uint8_t, 1> response{};) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == v2::status_code::ok);
    spec.deadline = std::chrono::steady_clock::now() - milliseconds{1};
    calls = f.handler.calls;
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {}, {}, &spec));
    CHECK(result.code == v2::status_code::deadline_exceeded && f.handler.calls == calls);
    f.handler.hang = true;
    spec = {};
    spec.timeout = std::chrono::microseconds{50000};
    started = std::chrono::steady_clock::now();
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {}, {}, &spec));
    CHECK(result.code == v2::status_code::deadline_exceeded);
    CHECK(std::chrono::steady_clock::now() - started >= milliseconds{50});
    CHECK(std::chrono::steady_clock::now() - started < milliseconds{2000});
    // The server hears CANCEL (or its own deadline) and stops the handler.
    CO2_AWAIT(wait_until(f.handler.stopped, 1));
    f.handler.hang = false;
    f.handler.delayed = true;
    request[0] = 5;
    CO2_AWAIT_SET(result, f.client.call(f.echo_method, {request.data(), 1}, {response.data(), 1}, &spec));
    CHECK(result.code == v2::status_code::ok && response[0] == 5);
    CO2_RETURN();
}
CO2_END

auto cancellation(fixture &f)
    CO2_BEG(net::task<>, (f), v2::status_code connected; unsigned done = 0; v2::call_result local{};
            v2::call_result remote{}; v2::call_result early{}; net::stop_source local_stop;
            net::stop_source remote_stop; net::stop_source early_stop; std::thread canceller;) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == v2::status_code::ok);
    f.handler.hang = true;
    early_stop.request_stop();
    f.spawn({}, {}, nullptr, early, done, early_stop.get_token());
    f.spawn({}, {}, nullptr, local, done, local_stop.get_token());
    f.spawn({}, {}, nullptr, remote, done, remote_stop.get_token());
    CO2_AWAIT(wait_until(done, 1));
    CHECK(early.code == v2::status_code::cancelled);
    CO2_AWAIT(net::delay(milliseconds{20}));
    local_stop.request_stop();
    canceller = std::thread{[&] {
        std::this_thread::sleep_for(milliseconds{20});
        remote_stop.request_stop();
    }};
    CO2_AWAIT(wait_until(done, 3));
    canceller.join();
    CHECK(local.code == v2::status_code::cancelled && remote.code == v2::status_code::cancelled);
    CO2_AWAIT(wait_until(f.handler.stopped, 2));
    CO2_RETURN();
}
CO2_END

// Stop requests from another thread race with END frames and hook reuse.
auto cancel_race(fixture &f)
    CO2_BEG(net::task<>, (f), v2::status_code connected; unsigned done = 0; std::vector<net::stop_source> sources;
            std::vector<v2::call_result> results; std::vector<std::array<std::uint8_t, 16>> requests;
            std::vector<std::array<std::uint8_t, 16>> replies; std::atomic<std::size_t> started{0};
            std::thread canceller; std::size_t i = 0; std::size_t wave = 0; unsigned ok = 0; unsigned cancelled = 0;) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == v2::status_code::ok);
    f.handler.delayed = true; // 0..3 ms per call against stops 0..4 ms into each wave.
    sources.resize(2000);
    results.resize(sources.size());
    requests.resize(sources.size());
    replies.resize(sources.size());
    for (i = 0; i < requests.size(); ++i) requests[i].fill(static_cast<std::uint8_t>(i % 4));
    canceller = std::thread{[&] {
        std::uint32_t seed = 12345;
        std::vector<std::pair<std::uint32_t, std::size_t>> plan(250);
        for (std::size_t first = 0; first < sources.size(); first += plan.size()) {
            for (std::size_t index = 0; index != plan.size(); ++index) {
                seed = seed * 1664525U + 1013904223U;
                plan[index] = {(seed >> 8) % 4000, first + index};
            }
            std::sort(plan.begin(), plan.end());
            while (started.load() <= first) std::this_thread::yield();
            auto const origin = std::chrono::steady_clock::now();
            for (auto const &step : plan) {
                std::this_thread::sleep_until(origin + std::chrono::microseconds{step.first});
                sources[step.second].request_stop();
            }
        }
    }};
    for (wave = 0; wave < sources.size(); wave += 250) {
        for (i = wave; i < wave + 250; ++i)
            f.spawn({requests[i].data(), requests[i].size()}, {replies[i].data(), replies[i].size()}, nullptr,
                    results[i], done, sources[i].get_token());
        started.store(wave + 1);
        CO2_AWAIT(wait_until(done, static_cast<unsigned>(wave + 250)));
    }
    canceller.join();
    for (i = 0; i < results.size(); ++i) {
        if (results[i].code == v2::status_code::ok) {
            CHECK(replies[i] == requests[i]);
            ++ok;
        } else {
            CHECK(results[i].code == v2::status_code::cancelled);
            ++cancelled;
        }
    }
    CHECK(ok + cancelled == results.size());
    CO2_RETURN();
}
CO2_END

auto closing(fixture &f)
    CO2_BEG(net::task<>, (f), v2::status_code connected; unsigned done = 0; std::array<v2::call_result, 3> results{};
            v2::call_result after; std::size_t i = 0;) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == v2::status_code::ok);
    f.handler.hang = true;
    for (i = 0; i < results.size(); ++i) f.spawn({}, {}, nullptr, results[i], done);
    CO2_AWAIT(net::delay(milliseconds{20}));
    f.client.close();
    CO2_AWAIT(wait_until(done, 3));
    for (auto const &result : results) CHECK(result.code == v2::status_code::unavailable);
    CO2_AWAIT_SET(after, f.client.call(f.echo_method, {}, {}));
    CHECK(after.code == v2::status_code::unavailable);
    CO2_AWAIT(wait_until(f.handler.stopped, 3));
    CO2_RETURN();
}
CO2_END

auto draining(fixture &f)
    CO2_BEG(net::task<>, (f), v2::status_code connected; unsigned done = 0; v2::call_result slow{};
            v2::call_result after; std::array<std::uint8_t, 1> request{}; std::array<std::uint8_t, 1> response{};
            int spins = 0;) {
    CO2_AWAIT_SET(connected, f.client.connect(f.endpoint));
    CHECK(connected == v2::status_code::ok);
    f.handler.delayed = true;
    request[0] = 30;
    f.spawn({request.data(), 1}, {response.data(), 1}, nullptr, slow, done);
    CO2_AWAIT(net::delay(milliseconds{5}));
    f.server.drain();
    CO2_AWAIT(wait_until(done, 1));
    CHECK(slow.code == v2::status_code::ok && response[0] == 30); // Admitted before GOAWAY: finishes.
    CHECK(!f.client.ready());
    CO2_AWAIT_SET(after, f.client.call(f.echo_method, {}, {}));
    CHECK(after.code == v2::status_code::unavailable);
    for (spins = 0; f.server.stats().connections != 0; ++spins) {
        CHECK(spins < 2000);
        CO2_AWAIT(net::delay(milliseconds{1}));
    }
    CO2_RETURN();
}
CO2_END

// Delivers at most `limit` bytes per read and per write.
struct trickle_stream {
    net::tcp_socket *socket;
    std::size_t limit;
    template <class Buffers> net::socket_read_awaitable read_some(Buffers const &buffers) {
        net::mutable_buffer const first{*net::buffer_sequence_begin(buffers)};
        return socket->read_some(net::mutable_buffer{first.data(), std::min(first.size(), limit)});
    }
    template <class Buffers> net::socket_write_awaitable write_some(Buffers const &buffers) {
        net::const_buffer const first{*net::buffer_sequence_begin(buffers)};
        return socket->write_some(net::const_buffer{first.data(), std::min(first.size(), limit)});
    }
};

struct trickle_transport final : v2::transport {
    trickle_transport(net::tcp_socket value, std::size_t limit)
        : socket(std::move(value)), trickle{&socket, limit}, stream_(&trickle) {}
    net::any_stream &stream() noexcept override { return stream_; }
    void close() noexcept override { static_cast<void>(socket.close()); }
    net::tcp_socket socket;
    trickle_stream trickle;
    net::any_stream stream_;
};

auto fragmented(fixture &f)
    CO2_BEG(net::task<>, (f), std::unique_ptr<net::tcp_socket> socket; net::io_result<> connected;
            v2::status_code attached; unsigned done = 0; std::array<v2::call_result, 6> results{};
            std::array<std::array<std::uint8_t, 300>, 6> requests{}; std::array<std::array<std::uint8_t, 300>, 6> replies{};
            std::size_t i = 0;) {
    socket.reset(new net::tcp_socket(f.context));
    CO2_AWAIT_SET(connected, socket->connect(f.endpoint));
    CHECK(!connected.ec);
    CO2_AWAIT_SET(attached, f.client.attach(std::unique_ptr<v2::transport>(new trickle_transport(std::move(*socket), 5))));
    CHECK(attached == v2::status_code::ok);
    for (i = 0; i < requests.size(); ++i) {
        requests[i].fill(static_cast<std::uint8_t>(i + 1));
        f.spawn({requests[i].data(), requests[i].size()}, {replies[i].data(), replies[i].size()}, nullptr, results[i],
                done);
    }
    CO2_AWAIT(wait_until(done, static_cast<unsigned>(results.size())));
    for (i = 0; i < results.size(); ++i) CHECK(results[i].code == v2::status_code::ok && replies[i] == requests[i]);
    CO2_RETURN();
}
CO2_END

// A peer that accepts TCP but never answers SETTINGS.
auto handshake_timeout(fixture &f)
    CO2_BEG(net::task<>, (f), std::unique_ptr<net::tcp_acceptor> silent; std::unique_ptr<net::tcp_socket> socket;
            net::io_result<> connected; v2::status_code attached; std::error_code error;
            std::chrono::steady_clock::time_point started;) {
    silent.reset(new net::tcp_acceptor(f.context, {net::ip::address_v4::loopback(), 0}));
    socket.reset(new net::tcp_socket(f.context));
    CO2_AWAIT_SET(connected, socket->connect(silent->local_endpoint(error)));
    CHECK(!connected.ec && !error);
    started = std::chrono::steady_clock::now();
    CO2_AWAIT_SET(attached, f.client.attach(v2::make_tcp_transport(std::move(*socket))));
    CHECK(attached == v2::status_code::deadline_exceeded);
    CHECK(std::chrono::steady_clock::now() - started >= milliseconds{100});
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
        { fixture f; f.run([&] { return basic(f); }); std::cout << "PASS basic/unknown/small buffer/exception\n"; }
        { fixture f{{}, {}, 48}; f.run([&] { return response_limit(f); }); std::cout << "PASS declared response limit\n"; }
        { fixture f{{}, {}, 64, 64}; f.run([&] { return trailers(f); }); std::cout << "PASS status message and metadata\n"; }
        { fixture f; f.run([&] { return concurrent(f); }); std::cout << "PASS multiplexed reverse completion\n"; }
        { fixture f; f.run([&] { return large(f); }); std::cout << "PASS large frames beyond the window\n"; }
        { fixture f; f.run([&] { return deadlines(f); }); std::cout << "PASS deadlines and server cancel\n"; }
        { fixture f; f.run([&] { return cancellation(f); }); std::cout << "PASS early/same-thread/cross-thread cancel\n"; }
        {
            fixture f{{}, {}, 64}; // The ledger charges each call its method's declared maximum.
            f.run([&] { return cancel_race(f); });
            std::cout << "PASS cross-thread cancel racing completion\n";
        }
        { fixture f; f.run([&] { return closing(f); }); std::cout << "PASS close fails in-flight calls\n"; }
        { fixture f; f.run([&] { return draining(f); }); std::cout << "PASS GOAWAY drain\n"; }
        { fixture f; f.run([&] { return fragmented(f); }); std::cout << "PASS fragmented reads and writes\n"; }
        {
            v2::client_options config{};
            config.connection.handshake_timeout = milliseconds{100};
            fixture f{{}, config};
            f.run([&] { return handshake_timeout(f); });
            std::cout << "PASS handshake timeout\n";
        }
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
