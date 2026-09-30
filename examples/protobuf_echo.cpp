#include "echo.rpc.hpp"

#include <net/run_async.hpp>

#include <array>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {

auto echo(rpc::server_context *context, example::unary::Request const *request, example::unary::Reply *reply)
    CO2_BEG(net::task<rpc::status_code>, (context, request, reply)) {
    reply->set_text(request->text());
    if (context->metadata.count != 0) { // Echo the first request metadata entry back.
        auto const item = rpc::wire::decode_metadata_entry(context->metadata.entries);
        if (!context->set_trailer({}, {&item.value, 1})) CO2_RETURN(rpc::status_code::internal);
    }
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

struct echo_service final : example::unary::EchoService {
    net::task<rpc::status_code> Echo(rpc::server_context &context, example::unary::Request const &request,
                                     example::unary::Reply &reply) override {
        return echo(&context, &request, &reply);
    }
};

auto round_trip(rpc::client *client, net::ip::tcp::endpoint endpoint)
    CO2_BEG(net::task<>, (client, endpoint), rpc::status_code connected; rpc::call_result result;
            example::unary::Request request; example::unary::Reply reply; std::string key{"trace-id"};
            std::string value{"example-1"}; rpc::wire::metadata_entry metadata{}; rpc::call_spec spec;
            rpc::response_trailer trailer; std::array<std::uint8_t, 128> storage{};) {
    CO2_AWAIT_SET(connected, client->connect(endpoint));
    if (connected != rpc::status_code::ok) throw std::runtime_error{"connect failed"};
    request.set_text("hello protobuf");
    metadata = {{reinterpret_cast<std::uint8_t const *>(key.data()), key.size()},
                {reinterpret_cast<std::uint8_t const *>(value.data()), value.size()}};
    spec.timeout = std::chrono::seconds{1};
    spec.metadata = {&metadata, 1};
    trailer.storage = {storage.data(), storage.size()};
    CO2_AWAIT_SET(result, example::unary::EchoStub{*client}.Echo(request, reply, &spec, &trailer));
    if (result.code != rpc::status_code::ok || reply.text() != request.text() || trailer.metadata.count != 1)
        throw std::runtime_error{"protobuf echo mismatch"};
    std::cout << reply.text() << ", metadata=" << trailer.metadata.count << '\n';
    CO2_RETURN();
}
CO2_END

} // namespace

int main() {
    try {
        net::io_context context{net::default_backend, net::single_thread_hint};
        rpc::shard shard{context};
        echo_service service;
        rpc::server server{shard, example::unary::Echo_bindings(service)};
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
        })([&] { return round_trip(&client, endpoint); });
        context.run();
        if (failure) std::rethrow_exception(failure);
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
