#include "json_users.hpp"
#ifdef LRPC_USERS_PROTOBUF
#include "protobuf_users.hpp"
#endif

#include <net/run_async.hpp>

#include <array>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {

auto get_user(rpc::server_context *context, example::GetUserRequest const *request, example::GetUserReply *reply,
              std::string const *name)
    CO2_BEG(net::task<rpc::status_code>, (context, request, reply, name)) {
    if (request->id != 7) CO2_RETURN(rpc::status_code::not_found);
    reply->user = {request->id, *name, {"reader", "writer"}};
    if (context->metadata.count != 0) { // Echo the first request metadata entry back.
        auto const item = rpc::wire::decode_metadata_entry(context->metadata.entries);
        if (!context->set_trailer({}, {&item.value, 1})) CO2_RETURN(rpc::status_code::internal);
    }
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

auto rename_user(example::RenameUserRequest const *request, example::RenameUserReply *reply, std::string *name)
    CO2_BEG(net::task<rpc::status_code>, (request, reply, name)) {
    if (request->id != 7) CO2_RETURN(rpc::status_code::not_found);
    *name = request->name;
    reply->updated = true;
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END

struct UserService {
    std::string name{"Alice"};
    net::task<rpc::status_code> GetUser(rpc::server_context &context, example::GetUserRequest const &request,
                                        example::GetUserReply &reply) {
        return get_user(&context, &request, &reply, &name);
    }
    net::task<rpc::status_code> RenameUser(rpc::server_context &, example::RenameUserRequest const &request,
                                           example::RenameUserReply &reply) {
        return rename_user(&request, &reply, &name);
    }
};

template <class Policy>
auto run(rpc::client *client, net::ip::tcp::endpoint endpoint, example::UserStub<Policy> const *users)
    CO2_BEG(net::task<>, (client, endpoint, users), rpc::status_code connected; rpc::call_result result;
            example::GetUserRequest request{7}; example::GetUserReply reply; example::RenameUserRequest rename{7, "张三"};
            example::RenameUserReply renamed; rpc::call_spec spec; rpc::response_trailer trailer;
            std::array<std::uint8_t, 128> storage{}; rpc::wire::metadata_entry trace{}; std::string key{"trace-id"};
            std::string value{"json-example"};) {
    CO2_AWAIT_SET(connected, client->connect(endpoint));
    if (connected != rpc::status_code::ok) throw std::runtime_error{"connect failed"};
    trace = {{reinterpret_cast<std::uint8_t const *>(key.data()), key.size()},
             {reinterpret_cast<std::uint8_t const *>(value.data()), value.size()}};
    spec.timeout = std::chrono::seconds{1};
    spec.metadata = {&trace, 1};
    trailer.storage = {storage.data(), storage.size()};
    CO2_AWAIT_SET(result, users->GetUser(request, reply, &spec, &trailer));
    if (result.code != rpc::status_code::ok || reply.user.name != "Alice" || trailer.metadata.count != 1)
        throw std::runtime_error{"GetUser mismatch"};
    CO2_AWAIT_SET(result, users->RenameUser(rename, renamed));
    if (result.code != rpc::status_code::ok || !renamed.updated) throw std::runtime_error{"RenameUser mismatch"};
    CO2_AWAIT_SET(result, users->GetUser(request, reply));
    if (result.code != rpc::status_code::ok || reply.user.name != rename.name)
        throw std::runtime_error{"renamed user mismatch"};
    std::cout << reply.user.id << ": " << reply.user.name << ", roles=" << reply.user.roles.size() << '\n';
    CO2_RETURN();
}
CO2_END

template <class Policy> void serve(Policy policy) {
    net::io_context context{net::default_backend, net::single_thread_hint};
    rpc::shard shard{context};
    UserService service;
    auto const contract = example::users_contract(policy);
    rpc::server server{shard, example::users_bindings(contract, service)};
    rpc::client client{shard};
    example::UserStub<Policy> const users{client, contract};
    auto const endpoint = server.listen({net::ip::address_v4::loopback(), 0});
    std::exception_ptr failure;
    auto close = [&] {
        client.close();
        server.close();
    };
    net::run_async(context.get_executor(), close, [&](std::exception_ptr error) {
        failure = error;
        close();
    })([&] { return run(&client, endpoint, &users); });
    context.run();
    if (failure) std::rethrow_exception(failure);
}

} // namespace

int main(int argc, char **argv) {
    try {
#ifdef LRPC_USERS_PROTOBUF
        if (argc == 2 && std::string{argv[1]} == "protobuf") {
            serve(rpc::mapped_protobuf_codec_policy{});
            return 0;
        }
#endif
        static_cast<void>(argv);
        if (argc != 1) throw std::invalid_argument{"usage: json_users [protobuf]"};
        serve(rpc::json_codec_policy{});
    } catch (std::exception const &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
