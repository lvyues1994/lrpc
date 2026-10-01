// Unary latency on a connection that also carries a bulk stream. The client
// keeps one bidirectional stream busy with --bulk-bytes messages while it makes
// --bytes echo calls one at a time; --bulk-bytes 0 measures the calls alone.
// --window is the stream window, and so bounds the bulk bytes in flight ahead
// of a call. Server and client each run one thread over loopback TCP.
#include "event.hpp"

#include <rpc/client.hpp>
#include <rpc/server.hpp>

#include <net/run_async.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

using rpc::wire::bytes_view;

struct config {
    net::backend_kind backend = net::backend_kind::epoll;
    std::size_t bytes = 64;
    std::size_t bulk_bytes = 1U << 20;
    std::size_t window = 1U << 20;
    std::size_t calls = 20000;
    std::size_t warmup = 2000;
    rpc::connection_options defaults{};
};

config parse(int argc, char **argv) {
    config result{};
    for (int i = 1; i + 1 < argc; i += 2) {
        std::string const key = argv[i];
        std::string const value = argv[i + 1];
        if (key == "--backend") {
            if (value == "epoll") result.backend = net::backend_kind::epoll;
            else if (value == "poll") result.backend = net::backend_kind::poll;
            else if (value == "select") result.backend = net::backend_kind::select;
            else if (value == "io_uring") result.backend = net::backend_kind::io_uring;
            else throw std::invalid_argument{"unknown backend"};
            continue;
        }
        if (value.empty() || value.find_first_not_of("0123456789") != std::string::npos)
            throw std::invalid_argument{"nonnegative integer required: " + key};
        auto const number = static_cast<std::size_t>(std::stoull(value));
        if (key == "--bytes") result.bytes = number;
        else if (key == "--bulk-bytes") result.bulk_bytes = number;
        else if (key == "--window") result.window = number;
        else if (key == "--fragment-bytes") result.defaults.stream_fragment_bytes = number;
        else if (key == "--send-budget") result.defaults.stream_send_budget = number;
        else if (key == "--receive-buffer-bytes") result.defaults.receive_buffer_bytes = number;
        else if (key == "--calls") result.calls = number;
        else if (key == "--warmup") result.warmup = number;
        else throw std::invalid_argument{"unknown option: " + key};
    }
    if (argc % 2 == 0) throw std::invalid_argument{"missing option value"};
    if (result.bytes == 0 || result.bytes > (64U << 10) || result.calls == 0 || result.bulk_bytes > (64U << 20) ||
        result.window < std::max<std::size_t>(result.bulk_bytes, 1) || result.window > (1U << 30))
        throw std::invalid_argument{"invalid benchmark configuration"};
    if (!net::backend_available(result.backend)) throw std::runtime_error{"backend unavailable"};
    return result;
}

rpc::connection_options connection(config const &c) {
    auto options = c.defaults;
    options.receive.max_message_size =
        std::max(options.receive.max_message_size, static_cast<std::uint32_t>(c.bulk_bytes));
    options.receive.initial_stream_window = static_cast<std::uint32_t>(c.window);
    return options;
}

auto respond(bytes_view request, rpc::response_writer &response)
    CO2_BEG(net::task<rpc::status_code>, (request, response)) {
    CO2_RETURN(response.assign(request) ? rpc::status_code::ok : rpc::status_code::internal);
}
CO2_END

struct echo_handler final : rpc::method_handler {
    net::task<rpc::status_code> invoke(rpc::server_context &, bytes_view request,
                                       rpc::response_writer &response) override {
        return respond(request, response);
    }
};

auto drain(rpc::server_stream &stream) CO2_BEG(net::task<rpc::status_code>, (stream), rpc::stream_read read;) {
    for (;;) {
        CO2_AWAIT_SET(read, stream.read());
        if (read.code != rpc::status_code::ok) CO2_RETURN(read.code);
        if (read.ended) CO2_RETURN(rpc::status_code::ok);
    }
}
CO2_END

struct sink_handler final : rpc::stream_method_handler {
    net::task<rpc::status_code> invoke(rpc::server_context &, rpc::server_stream &stream) override {
        return drain(stream);
    }
};

struct server_side {
    explicit server_side(config const &c)
        : context(c.backend, net::single_thread_hint), shard(context),
          server(shard,
                 {{"bench/Echo", &echo, c.bytes},
                  {"bench/Sink", nullptr, 0, 0, rpc::method_kind::bidirectional, &sink}},
                 options(c)),
          endpoint(server.listen({net::ip::address_v4::loopback(), 0})) {}
    static rpc::server_options options(config const &c) {
        rpc::server_options result{};
        result.connection = connection(c);
        return result;
    }

    net::io_context context;
    rpc::shard shard;
    echo_handler echo;
    sink_handler sink;
    rpc::server server;
    net::ip::tcp::endpoint endpoint;
};

auto close_server(server_side &self) CO2_BEG(net::task<>, (self)) {
    self.server.close();
    CO2_RETURN();
}
CO2_END

struct client_side {
    client_side(config const &c, net::ip::tcp::endpoint server)
        : settings(c), context(c.backend, net::single_thread_hint), shard(context), client(shard, options(c)),
          endpoint(server), echo(client.bind("bench/Echo")), sink(client.bind("bench/Sink")), request(c.bytes, 7),
          reply(c.bytes), bulk(c.bulk_bytes, 3) {
        latencies.reserve(c.calls);
    }
    static rpc::client_options options(config const &c) {
        rpc::client_options result{};
        result.connection = connection(c);
        return result;
    }

    config settings;
    net::io_context context;
    rpc::shard shard;
    rpc::client client;
    net::ip::tcp::endpoint endpoint;
    rpc::method_ref echo;
    rpc::method_ref sink;
    std::vector<std::uint8_t> request;
    std::vector<std::uint8_t> reply;
    std::vector<std::uint8_t> bulk;
    std::vector<double> latencies; // Microseconds.
    std::uint64_t bulk_messages = 0;
    bool stopping = false;
    bench::event bulk_finished;
};

auto bulk_loop(client_side &self)
    CO2_BEG(net::task<>, (self), rpc::open_result opened; rpc::status_code wrote; rpc::call_result result;) {
    CO2_AWAIT_SET(opened, self.client.open(self.sink, rpc::method_kind::bidirectional));
    if (opened.code != rpc::status_code::ok) throw std::runtime_error{"open failed"};
    while (!self.stopping) {
        CO2_AWAIT_SET(wrote, opened.stream.write({self.bulk.data(), self.bulk.size()}));
        if (wrote != rpc::status_code::ok) throw std::runtime_error{"bulk write failed"};
        ++self.bulk_messages;
    }
    CO2_AWAIT_SET(wrote, opened.stream.writes_done());
    CO2_AWAIT_SET(result, opened.stream.finish());
    if (wrote != rpc::status_code::ok || result.code != rpc::status_code::ok)
        throw std::runtime_error{"bulk stream failed"};
    self.bulk_finished.signal();
    CO2_RETURN();
}
CO2_END

double percentile(std::vector<double> const &sorted, double fraction) {
    return sorted[static_cast<std::size_t>(std::ceil(fraction * static_cast<double>(sorted.size()))) - 1];
}

auto drive(client_side &self)
    CO2_BEG(net::task<>, (self), rpc::status_code connected; rpc::call_result result; std::size_t i = 0;
            rpc::clock::time_point started; rpc::clock::time_point measured; std::uint64_t first_bulk = 0;
            double seconds = 0;) {
    CO2_AWAIT_SET(connected, self.client.connect(self.endpoint));
    if (connected != rpc::status_code::ok) throw std::runtime_error{"connect failed"};
    if (self.settings.bulk_bytes != 0) net::run_async(self.context.get_executor())(bulk_loop(self));
    for (i = 0; i < self.settings.warmup + self.settings.calls; ++i) {
        if (i == self.settings.warmup) {
            measured = rpc::clock::now();
            first_bulk = self.bulk_messages;
        }
        started = rpc::clock::now();
        CO2_AWAIT_SET(result, self.client.call(self.echo, {self.request.data(), self.request.size()},
                                               {self.reply.data(), self.reply.size()}));
        if (result.code != rpc::status_code::ok || result.size != self.request.size())
            throw std::runtime_error{"call failed"};
        if (i >= self.settings.warmup)
            self.latencies.push_back(std::chrono::duration<double, std::micro>(rpc::clock::now() - started).count());
    }
    seconds = std::chrono::duration<double>(rpc::clock::now() - measured).count();
    self.stopping = true;
    if (self.settings.bulk_bytes != 0) CO2_AWAIT(self.bulk_finished.wait());
    std::sort(self.latencies.begin(), self.latencies.end());
    std::cout << "{\"bytes\":" << self.settings.bytes << ",\"bulk_bytes\":" << self.settings.bulk_bytes
              << ",\"window\":" << self.settings.window
              << ",\"fragment_bytes\":" << self.settings.defaults.stream_fragment_bytes
              << ",\"send_budget\":" << self.settings.defaults.stream_send_budget
              << ",\"receive_buffer_bytes\":" << self.settings.defaults.receive_buffer_bytes
              << ",\"calls\":" << self.settings.calls
              << ",\"p50_us\":" << percentile(self.latencies, 0.5)
              << ",\"p99_us\":" << percentile(self.latencies, 0.99)
              << ",\"p999_us\":" << percentile(self.latencies, 0.999) << ",\"max_us\":" << self.latencies.back()
              << ",\"bulk_mib_per_second\":"
              << static_cast<double>(self.bulk_messages - first_bulk) * static_cast<double>(self.settings.bulk_bytes) /
                     (1024.0 * 1024.0) / seconds
              << "}\n";
    CO2_RETURN();
}
CO2_END

} // namespace

int main(int argc, char **argv) {
    try {
        auto const settings = parse(argc, argv);
        server_side server{settings};
        std::exception_ptr server_failure;
        std::thread serving{[&] {
            try {
                server.context.run();
            } catch (...) {
                server_failure = std::current_exception();
            }
        }};
        std::exception_ptr failure;
        {
            client_side client{settings, server.endpoint};
            net::run_async(client.context.get_executor(), [&] { client.client.close(); },
                           [&](std::exception_ptr error) {
                               failure = error;
                               client.client.close();
                           })(drive(client));
            client.context.run();
        }
        net::run_async(server.context.get_executor())(close_server(server));
        serving.join();
        if (failure) std::rethrow_exception(failure);
        if (server_failure) std::rethrow_exception(server_failure);
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
