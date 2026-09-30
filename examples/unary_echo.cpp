#include <rpc/client.hpp>
#include <rpc/server.hpp>

#include <net/run_async.hpp>

#include <array>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {

auto echo(rpc::wire::bytes_view request, rpc::response_writer &response)
    CO2_BEG(net::task<rpc::status_code>, (request, response)) {
    CO2_RETURN(response.assign(request) ? rpc::status_code::ok : rpc::status_code::internal);
}
CO2_END

struct echo_handler final : rpc::method_handler {
    net::task<rpc::status_code> invoke(rpc::server_context &, rpc::wire::bytes_view request,
                                       rpc::response_writer &response) override {
        return echo(request, response);
    }
};

auto round_trip(rpc::client &client, net::ip::tcp::endpoint endpoint)
    CO2_BEG(net::task<>, (client, endpoint), rpc::status_code connected; rpc::call_result result; rpc::call_spec spec;
            std::array<std::uint8_t, 64> reply{}; std::string message{"hello lrpc"};) {
    CO2_AWAIT_SET(connected, client.connect(endpoint));
    if (connected != rpc::status_code::ok) throw std::runtime_error{"connect failed"};
    spec.timeout = std::chrono::seconds{1};
    CO2_AWAIT_SET(result, client.call(client.bind("example/Echo"), {message.data(), message.size()},
                                      {reply.data(), reply.size()}, &spec));
    if (result.code != rpc::status_code::ok) throw std::runtime_error{"echo call failed"};
    auto const text = std::string{reinterpret_cast<char const *>(reply.data()), result.size};
    if (text != message) throw std::runtime_error{"echo response mismatch"};
    std::cout << text << '\n';
    CO2_RETURN();
}
CO2_END

} // namespace

int main() {
    try {
        net::io_context context{net::default_backend, net::single_thread_hint};
        rpc::shard shard{context};
        echo_handler handler;
        rpc::server server{shard, {{"example/Echo", &handler, 64}}};
        rpc::client client{shard};
        auto const endpoint = server.listen({net::ip::address_v4::loopback(), 0});
        std::exception_ptr failure;
        auto close = [&] {
            client.close();
            server.close();
        };
        net::run_async(context.get_executor(), close, [&](std::exception_ptr error) {
            failure = error;
            close();
        })([&] { return round_trip(client, endpoint); });
        context.run(); // close() cancels; run() still consumes every completion.
        if (failure) std::rethrow_exception(failure);
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
