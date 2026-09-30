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
};

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
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
