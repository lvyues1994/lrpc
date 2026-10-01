#include "backend.hpp"
#include "check.hpp"

#include <rpc/channel.hpp>
#include <rpc/server.hpp>

#include <net/run_async.hpp>
#include <net/timeout.hpp>

#include <array>
#include <chrono>
#include <iostream>
#include <memory>
#include <vector>

namespace {

using rpc::wire::bytes_view;
using rpc::wire::mutable_bytes_view;
using std::chrono::milliseconds;

net::backend_kind selected_backend = net::default_backend_t::kind;

auto echo(bytes_view request, rpc::response_writer &response)
    CO2_BEG(net::task<rpc::status_code>, (request, response)) {
    CO2_RETURN(response.assign(request) ? rpc::status_code::ok : rpc::status_code::internal);
}
CO2_END

struct counting_handler final : rpc::method_handler {
    net::task<rpc::status_code> invoke(rpc::server_context &, bytes_view request, rpc::response_writer &response) override {
        ++calls;
        return echo(request, response);
    }
    unsigned calls = 0;
};

struct backend {
    backend(rpc::shard &shard, rpc::server_options options = {}, net::ip::tcp::endpoint where = {net::ip::address_v4::loopback(), 0})
        : server(new rpc::server(shard, {{"test/Echo", &handler, 64}}, options)), endpoint(server->listen(where)) {}
    void restart(rpc::shard &shard) {
        server.reset(new rpc::server(shard, {{"test/Echo", &handler, 64}}));
        server->listen(endpoint);
    }
    counting_handler handler{};
    std::unique_ptr<rpc::server> server;
    net::ip::tcp::endpoint endpoint;
};

rpc::server_options refusing() {
    rpc::server_options options{};
    options.max_active_calls = 0; // Admits nothing: every call is refused, provably not executed.
    return options;
}

struct fixture {
    explicit fixture(rpc::server_options first = {}) : a(shard, first), b(shard) {}

    std::unique_ptr<rpc::channel> open(std::vector<net::ip::tcp::endpoint> endpoints, rpc::channel_options options = {}) {
        options.initial_backoff = milliseconds{5};
        options.max_backoff = milliseconds{40};
        std::unique_ptr<rpc::channel> created{new rpc::channel(shard, std::move(endpoints), options)};
        echo_method = created->bind("test/Echo");
        return created;
    }
    std::unique_ptr<rpc::channel> open(std::vector<rpc::channel_target> targets, rpc::channel_options options = {}) {
        options.initial_backoff = milliseconds{5};
        options.max_backoff = milliseconds{40};
        std::unique_ptr<rpc::channel> created{new rpc::channel(shard, std::move(targets), options)};
        echo_method = created->bind("test/Echo");
        return created;
    }
    static std::string port(backend const &server) { return std::to_string(server.endpoint.port()); }

    template <class F> void run(F factory) {
        std::exception_ptr failure;
        auto close = [&] {
            if (channel) channel->close();
            if (a.server) a.server->close();
            if (b.server) b.server->close();
        };
        net::run_async(context.get_executor(), close, [&](std::exception_ptr error) {
            failure = error;
            close();
        })(factory);
        context.run();
        if (failure) std::rethrow_exception(failure);
    }

    net::io_context context{selected_backend, net::single_thread_hint};
    rpc::shard shard{context};
    backend a;
    backend b;
    std::unique_ptr<rpc::channel> channel;
    rpc::method_ref echo_method;
    std::vector<std::vector<net::ip::tcp::endpoint>> answers; // One per lookup through the hook; the last repeats.
    std::size_t lookups = 0;
};

auto answer(fixture &f)
    CO2_BEG(net::task<std::vector<net::ip::tcp::endpoint>>, (f), std::vector<net::ip::tcp::endpoint> result;) {
    result = f.answers[std::min(f.lookups, f.answers.size() - 1)];
    ++f.lookups;
    CO2_RETURN(std::move(result));
}
CO2_END

auto call_once(rpc::channel &channel, rpc::method_ref method, bytes_view request, mutable_bytes_view response)
    CO2_BEG(net::task<rpc::call_result>, (channel, method, request, response), rpc::call_result result;) {
    CO2_AWAIT_SET(result, channel.call(method, request, response));
    CO2_RETURN(result);
}
CO2_END

void spawn_call(fixture &f, bytes_view request, mutable_bytes_view reply, rpc::call_result &result, unsigned &done) {
    net::run_async(f.context.get_executor(), [&result, &done](rpc::call_result value) {
        result = value;
        ++done;
    }, [](std::exception_ptr error) { std::rethrow_exception(error); })([&f, request, reply] {
        return call_once(*f.channel, f.echo_method, request, reply);
    });
}

// Issues `count` calls, at most `width` at a time, and checks every echo.
auto burst(fixture &f, unsigned count, unsigned width, unsigned *failed)
    CO2_BEG(net::task<>, (f, count, width, failed), unsigned issued = 0; unsigned done = 0;
            std::vector<std::array<std::uint8_t, 8>> requests; std::vector<std::array<std::uint8_t, 8>> replies;
            std::vector<rpc::call_result> results; std::size_t i = 0;) {
    requests.resize(count);
    replies.resize(count);
    results.resize(count);
    for (i = 0; i < count; ++i) requests[i].fill(static_cast<std::uint8_t>(i));
    while (done < count) {
        for (; issued < count && issued - done < width; ++issued)
            spawn_call(f, {requests[issued].data(), 8}, {replies[issued].data(), 8}, results[issued], done);
        CO2_AWAIT(net::delay(milliseconds{1}));
    }
    *failed = 0;
    for (i = 0; i < count; ++i) {
        if (results[i].code != rpc::status_code::ok) {
            ++*failed;
            continue;
        }
        CHECK(replies[i] == requests[i]);
    }
    CO2_RETURN();
}
CO2_END

auto until_ready(rpc::channel &channel, std::size_t connections)
    CO2_BEG(net::task<>, (channel, connections), int spins = 0;) {
    for (spins = 0; channel.ready_connections() < connections; ++spins) {
        CHECK(spins < 3000);
        CO2_AWAIT(net::delay(milliseconds{1}));
    }
    CO2_RETURN();
}
CO2_END

auto balance(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code ready; unsigned failed = 0; rpc::call_result early;) {
    f.channel = f.open({f.a.endpoint, f.b.endpoint});
    CO2_AWAIT_SET(early, f.channel->call(f.echo_method, {}, {}));
    CHECK(early.code == rpc::status_code::unavailable && early.not_executed);
    CO2_AWAIT_SET(ready, f.channel->wait_ready());
    CHECK(ready == rpc::status_code::ok);
    CO2_AWAIT(until_ready(*f.channel, 2));
    CO2_AWAIT(burst(f, 2000, 64, &failed));
    CHECK(failed == 0 && f.a.handler.calls + f.b.handler.calls == 2000);
    CHECK(f.a.handler.calls > 500 && f.b.handler.calls > 500);
    CO2_RETURN();
}
CO2_END

auto refused_retry(fixture &f)
    CO2_BEG(net::task<>, (f), unsigned failed = 0;) {
    f.channel = f.open({f.a.endpoint, f.b.endpoint});
    CO2_AWAIT(until_ready(*f.channel, 2));
    CO2_AWAIT(burst(f, 1000, 32, &failed));
    CHECK(failed == 0 && f.a.handler.calls == 0 && f.b.handler.calls == 1000);
    CO2_RETURN();
}
CO2_END

auto failover(fixture &f)
    CO2_BEG(net::task<>, (f), unsigned failed = 0; rpc::call_result result; std::array<std::uint8_t, 8> request{};
            std::array<std::uint8_t, 8> reply{}; int i = 0;) {
    f.channel = f.open({f.a.endpoint, f.b.endpoint});
    CO2_AWAIT(until_ready(*f.channel, 2));
    f.a.server->close();
    for (i = 0; f.channel->ready_connections() != 1; ++i) {
        CHECK(i < 3000);
        CO2_AWAIT(net::delay(milliseconds{1}));
    }
    f.a.handler.calls = f.b.handler.calls = 0;
    CO2_AWAIT(burst(f, 500, 16, &failed));
    CHECK(failed == 0 && f.b.handler.calls == 500);
    f.a.restart(f.shard); // Same port: the channel reconnects with backoff.
    CO2_AWAIT(until_ready(*f.channel, 2));
    CO2_AWAIT_SET(result, f.channel->call(f.echo_method, {request.data(), 8}, {reply.data(), 8}));
    CHECK(result.code == rpc::status_code::ok);
    CO2_RETURN();
}
CO2_END

auto drained(fixture &f)
    CO2_BEG(net::task<>, (f), unsigned failed = 0; int i = 0;) {
    f.channel = f.open({f.a.endpoint, f.b.endpoint});
    CO2_AWAIT(until_ready(*f.channel, 2));
    f.a.server->drain(); // GOAWAY: the channel stops routing to it, then reconnects once it closes.
    for (i = 0; f.a.server->stats().connections != 0; ++i) {
        CHECK(i < 3000);
        CO2_AWAIT(net::delay(milliseconds{1}));
    }
    f.a.handler.calls = f.b.handler.calls = 0;
    CO2_AWAIT(burst(f, 300, 16, &failed));
    CHECK(failed == 0 && f.a.handler.calls == 0 && f.b.handler.calls == 300);
    CO2_RETURN();
}
CO2_END

auto waiting(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::status_code ready; std::chrono::steady_clock::time_point started;
            std::unique_ptr<net::tcp_acceptor> probe; net::ip::tcp::endpoint dead; std::error_code error;) {
    probe.reset(new net::tcp_acceptor(f.context, {net::ip::address_v4::loopback(), 0}));
    dead = probe->local_endpoint(error);
    CHECK(!error);
    probe.reset(); // Nothing listens on `dead` now.
    f.channel = f.open({dead});
    started = std::chrono::steady_clock::now();
    CO2_AWAIT_SET(ready, f.channel->wait_ready(started + milliseconds{60}));
    CHECK(ready == rpc::status_code::deadline_exceeded);
    CHECK(std::chrono::steady_clock::now() - started >= milliseconds{60});
    f.channel->close();
    CO2_AWAIT_SET(ready, f.channel->wait_ready());
    CHECK(ready == rpc::status_code::unavailable);
    CO2_RETURN();
}
CO2_END

// "localhost" may also answer ::1, where nothing listens: its connections move on to 127.0.0.1.
auto named(fixture &f)
    CO2_BEG(net::task<>, (f), unsigned failed = 0; rpc::channel_options options;) {
    options.connections_per_endpoint = 2;
    f.channel = f.open({{"localhost", fixture::port(f.a)}, {"127.0.0.1", fixture::port(f.b)}}, options);
    CO2_AWAIT(until_ready(*f.channel, 4));
    CO2_AWAIT(burst(f, 1000, 32, &failed));
    CHECK(failed == 0 && f.a.handler.calls + f.b.handler.calls == 1000);
    CHECK(f.a.handler.calls > 200 && f.b.handler.calls > 200);
    CO2_RETURN();
}
CO2_END

// A target that does not resolve leaves the channel to the others, and never becomes ready alone.
auto unresolved(fixture &f)
    CO2_BEG(net::task<>, (f), unsigned failed = 0; rpc::status_code ready; std::unique_ptr<rpc::channel> lone;
            std::chrono::steady_clock::time_point started;) {
    f.channel = f.open({{"localhost", "no-such-service-lrpc"}, {"127.0.0.1", fixture::port(f.a)}});
    CO2_AWAIT(until_ready(*f.channel, 1));
    CO2_AWAIT(burst(f, 200, 16, &failed));
    CHECK(failed == 0 && f.a.handler.calls == 200);
    lone.reset(new rpc::channel(f.shard, std::vector<rpc::channel_target>{{"localhost", "no-such-service-lrpc"}}));
    started = std::chrono::steady_clock::now();
    CO2_AWAIT_SET(ready, lone->wait_ready(started + milliseconds{60}));
    CHECK(ready == rpc::status_code::deadline_exceeded && lone->ready_connections() == 0);
    lone->close();
    CO2_RETURN();
}
CO2_END

rpc::channel_options discovery(fixture &f) {
    rpc::channel_options options{};
    options.resolve = [&f](rpc::channel_target const &target) {
        CHECK(target.host == "pool" && target.service == "echo");
        return answer(f);
    };
    return options;
}

// Through the hook: a dead first address is skipped, and once every address
// of the answer has failed, the target is looked up again.
auto discovered(fixture &f)
    CO2_BEG(net::task<>, (f), unsigned failed = 0; std::unique_ptr<net::tcp_acceptor> probe;
            net::ip::tcp::endpoint dead; std::error_code error; int i = 0;) {
    probe.reset(new net::tcp_acceptor(f.context, {net::ip::address_v4::loopback(), 0}));
    dead = probe->local_endpoint(error);
    CHECK(!error);
    probe.reset();
    f.answers = {{dead, f.a.endpoint}, {f.b.endpoint}};
    f.channel = f.open({{"pool", "echo"}}, discovery(f));
    CO2_AWAIT(until_ready(*f.channel, 1));
    CHECK(f.lookups == 1);
    CO2_AWAIT(burst(f, 100, 8, &failed));
    CHECK(failed == 0 && f.a.handler.calls == 100);
    f.a.server->close();
    for (i = 0; f.channel->ready_connections() != 0; ++i) {
        CHECK(i < 3000);
        CO2_AWAIT(net::delay(milliseconds{1}));
    }
    CO2_AWAIT(until_ready(*f.channel, 1));
    CHECK(f.lookups == 2);
    CO2_AWAIT(burst(f, 100, 8, &failed));
    CHECK(failed == 0 && f.b.handler.calls == 100);
    CO2_RETURN();
}
CO2_END

// A named target reconnects through a fresh lookup after its server restarts.
auto named_reconnect(fixture &f)
    CO2_BEG(net::task<>, (f), rpc::call_result result; std::array<std::uint8_t, 8> request{};
            std::array<std::uint8_t, 8> reply{}; rpc::channel_options options; int i = 0;) {
    options.resolve_interval = milliseconds{1};
    f.channel = f.open({{"127.0.0.1", fixture::port(f.a)}}, options);
    CO2_AWAIT(until_ready(*f.channel, 1));
    f.a.server->close();
    for (i = 0; f.channel->ready_connections() != 0; ++i) {
        CHECK(i < 3000);
        CO2_AWAIT(net::delay(milliseconds{1}));
    }
    f.a.restart(f.shard);
    CO2_AWAIT(until_ready(*f.channel, 1));
    CO2_AWAIT_SET(result, f.channel->call(f.echo_method, {request.data(), 8}, {reply.data(), 8}));
    CHECK(result.code == rpc::status_code::ok);
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
        { fixture f; f.run([&] { return balance(f); }); std::cout << "PASS least-loaded balance\n"; }
        { fixture f{refusing()}; f.run([&] { return refused_retry(f); }); std::cout << "PASS retry after refusal\n"; }
        { fixture f; f.run([&] { return failover(f); }); std::cout << "PASS failover and reconnect\n"; }
        { fixture f; f.run([&] { return drained(f); }); std::cout << "PASS GOAWAY reroutes\n"; }
        { fixture f; f.run([&] { return waiting(f); }); std::cout << "PASS wait_ready deadline and close\n"; }
        { fixture f; f.run([&] { return named(f); }); std::cout << "PASS named targets\n"; }
        { fixture f; f.run([&] { return unresolved(f); }); std::cout << "PASS unresolvable target\n"; }
        { fixture f; f.run([&] { return discovered(f); }); std::cout << "PASS lookup hook, address failover\n"; }
        { fixture f; f.run([&] { return named_reconnect(f); }); std::cout << "PASS named target reconnects\n"; }
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
