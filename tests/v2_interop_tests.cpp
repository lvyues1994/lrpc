// v1 and v2 speak the same wire: each client calls the other's server.
#include "backend.hpp"
#include "check.hpp"

#include <rpc/unary.hpp>
#include <rpc/v2/client.hpp>
#include <rpc/v2/server.hpp>

#include <net/run_async.hpp>

#include <array>
#include <iostream>

namespace {

namespace v2 = rpc::v2;
using rpc::wire::bytes_view;

net::backend_kind selected_backend = net::default_backend_t::kind;

auto v1_echo(bytes_view request, rpc::response_writer &response)
    CO2_BEG(net::task<rpc::status_code>, (request, response)) {
    CO2_RETURN(response.assign(request) ? rpc::status_code::ok : rpc::status_code::internal);
}
CO2_END

struct v1_handler final : rpc::method_handler {
    net::task<rpc::status_code> invoke(rpc::server_context &, bytes_view request, rpc::response_writer &response) override {
        return v1_echo(request, response);
    }
};

auto v2_echo(bytes_view request, v2::response_writer &response)
    CO2_BEG(net::task<v2::status_code>, (request, response)) {
    CO2_RETURN(response.assign(request) ? v2::status_code::ok : v2::status_code::internal);
}
CO2_END

struct v2_handler final : v2::method_handler {
    net::task<v2::status_code> invoke(v2::server_context &, bytes_view request, v2::response_writer &response) override {
        return v2_echo(request, response);
    }
};

struct fixture {
    net::io_context context{selected_backend, net::single_thread_hint};
    v2::shard shard{context};
    v1_handler old_handler{};
    v2_handler new_handler{};
    std::unique_ptr<rpc::server> old_server{rpc::make_server(context, {{"interop/Echo", 256, &old_handler}})};
    v2::server new_server{shard, {{"interop/Echo", &new_handler, 256}}};
    std::unique_ptr<rpc::client> old_client{rpc::make_client(context)};
    v2::client new_client{shard};
};

auto exercise(fixture &f)
    CO2_BEG(net::task<>, (f), v2::status_code connected; rpc::status_code old_connected; v2::call_result result;
            rpc::call_result old_result; v2::method_ref echo; v2::method_ref missing;
            std::array<std::uint8_t, 48> request{}; std::array<std::uint8_t, 48> response{}; int i = 0;) {
    request.fill(0x3c);
    CO2_AWAIT_SET(connected, f.new_client.connect(f.old_server->listen({net::ip::address_v4::loopback(), 0})));
    CHECK(connected == v2::status_code::ok);
    echo = f.new_client.bind("interop/Echo");
    missing = f.new_client.bind("interop/Missing");
    for (i = 0; i < 3; ++i) { // Interned after the first call.
        response.fill(0);
        CO2_AWAIT_SET(result, f.new_client.call(echo, {request.data(), request.size()}, {response.data(), response.size()}));
        CHECK(result.code == v2::status_code::ok && result.size == request.size() && response == request);
    }
    CO2_AWAIT_SET(result, f.new_client.call(missing, {}, {}));
    CHECK(result.code == v2::status_code::unimplemented);

    CO2_AWAIT_SET(old_connected, f.old_client->connect(f.new_server.listen({net::ip::address_v4::loopback(), 0})));
    CHECK(old_connected == rpc::status_code::ok);
    for (i = 0; i < 3; ++i) {
        response.fill(0);
        CO2_AWAIT_SET(old_result, f.old_client->call("interop/Echo", {request.data(), request.size()},
                                                     {response.data(), response.size()}));
        CHECK(old_result.code == rpc::status_code::ok && old_result.response_size == request.size() && response == request);
    }
    CO2_AWAIT_SET(old_result, f.old_client->call("interop/Missing", {}, {}));
    CHECK(old_result.code == rpc::status_code::unimplemented);
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
        fixture f;
        std::exception_ptr failure;
        auto close = [&] {
            f.new_client.close();
            f.old_client->close();
            f.new_server.close();
            f.old_server->close();
        };
        net::run_async(f.context.get_executor(), close, [&](std::exception_ptr error) {
            failure = error;
            close();
        })([&] { return exercise(f); });
        f.context.run();
        if (failure) std::rethrow_exception(failure);
        std::cout << "PASS v2 client with v1 server, v1 client with v2 server\n";
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
