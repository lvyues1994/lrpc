#include "echo.rpc.hpp"
#include <net/run_async.hpp>

#include <array>
#include <iostream>
#include <stdexcept>

namespace {
auto echo(rpc::server_context *context, example::unary::Request const *request, example::unary::Reply *reply)
    CO2_BEG(net::task<rpc::status_code>, (context, request, reply)) {
    reply->set_text(request->text());
    if (context->metadata.count != 0) {
        auto const item = rpc::wire::decode_metadata_entry(context->metadata.entries);
        if (!context->response_metadata.assign({&item.value, 1})) CO2_RETURN(rpc::status_code::internal);
    }
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

struct echo_service final : example::unary::EchoService {
    net::task<rpc::status_code> Echo(rpc::server_context &context, example::unary::Request const &request,
                                    example::unary::Reply &reply) override { return echo(&context, &request, &reply); }
};

auto round_trip(rpc::client *client, net::ip::tcp::endpoint endpoint, example::unary::EchoStub stub)
    CO2_BEG(net::task<>, (client, endpoint, stub), rpc::status_code connected; rpc::call_result result;
        example::unary::Request request; example::unary::Reply reply;
        std::string key{"trace-id"}, value{"example-1"}; rpc::wire::metadata_entry metadata{};
        std::array<std::uint8_t, 128> response_metadata{}; rpc::call_options options{};) {
    CO2_AWAIT_SET(connected, client->connect(endpoint));
    if (connected != rpc::status_code::ok) throw std::runtime_error{"connect failed"};
    request.set_text("hello protobuf");
    metadata = {{reinterpret_cast<std::uint8_t const *>(key.data()), key.size()},
                {reinterpret_cast<std::uint8_t const *>(value.data()), value.size()}};
    options.timeout = std::chrono::seconds{1}; options.metadata = {&metadata, 1};
    options.response_metadata = {response_metadata.data(), response_metadata.size()};
    CO2_AWAIT_SET(result, stub.Echo(request, reply, options));
    if (result.code != rpc::status_code::ok || reply.text() != request.text() || result.response_metadata.count != 1)
        throw std::runtime_error{"protobuf echo mismatch"};
    std::cout << reply.text() << ", metadata=" << result.response_metadata.count << '\n';
    CO2_RETURN();
}
CO2_END
} // namespace

int main() {
    try {
        net::io_context context{net::default_backend, net::single_thread_hint};
        echo_service service;
        example::unary::EchoLimits limits{}; limits.Echo = {1024, 128};
        auto server = rpc::make_server(context, example::unary::Echo_bindings(service, limits));
        auto client = rpc::make_client(context);
        auto const endpoint = server->listen({net::ip::address_v4::loopback(), 0});
        std::exception_ptr failure;
        auto close = [&] { client->close(); server->close(); };
        net::run_async(context.get_executor(), close, [&](std::exception_ptr error) {
            failure = error; close();
        })([&] { return round_trip(client.get(), endpoint, example::unary::EchoStub{*client}); });
        context.run();
        if (failure) std::rethrow_exception(failure);
    } catch (std::exception const &error) { std::cerr << error.what() << '\n'; return 1; }
}
