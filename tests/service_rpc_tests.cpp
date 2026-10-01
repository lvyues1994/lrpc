#include "check.hpp"
#include "../examples/json_users.hpp"
#include "../examples/protobuf_users.hpp"
#include <rpc/channel.hpp>
#include <net/run_async.hpp>
#include <net/timeout.hpp>
#include <array>
#include <iostream>

struct measured_message { std::string text; bool valid = true; bool fail_allocation = false; };
unsigned conversions = 0;
namespace rpc {
template <> struct protobuf_mapping<measured_message> {
    using message_type = example::wire::RenameUserRequest;
    static bool to_protobuf(measured_message const &v, message_type &out) {
        ++conversions;
        if (v.fail_allocation) throw std::bad_alloc{};
        out.set_name(v.text); return v.valid;
    }
    static bool from_protobuf(message_type const &v, measured_message &out) { out.text = v.name(); return v.id() == 0; }
    static std::size_t upper_bound(measured_message const &v) { return example::protobuf_bound_add(11, v.text.size()); }
};
}
namespace {
template <class Message> void oracle(Message const &source) {
    using mapping = rpc::protobuf_mapping<Message>;
    using wire_type = typename mapping::message_type;
    wire_type native; CHECK(mapping::to_protobuf(source, native));
    auto const size = native.ByteSizeLong(); CHECK(size <= mapping::upper_bound(source));
    std::vector<std::uint8_t> reference(size), actual(mapping::upper_bound(source));
    CHECK(rpc::codec<wire_type>::encode(native, {reference.data(), reference.size()}));
    auto const result = rpc::mapped_protobuf_codec<Message>::encode_bounded(source, {actual.data(), actual.size()});
    CHECK(result.code == rpc::wire::error::none && result.written == size);
    actual.resize(result.written); CHECK(actual == reference);
    Message decoded; CHECK(rpc::mapped_protobuf_codec<Message>::decode({reference.data(), reference.size()}, decoded));
    wire_type restored; CHECK(mapping::to_protobuf(decoded, restored));
    auto const restored_size = restored.ByteSizeLong(); CHECK(restored_size == size);
    std::vector<std::uint8_t> restored_bytes(restored_size);
    CHECK(rpc::codec<wire_type>::encode(restored, {restored_bytes.data(), restored_bytes.size()}));
    CHECK(restored_bytes == reference);
}
void mapping_checks() {
    for (auto id : {std::uint64_t{0}, std::numeric_limits<std::uint64_t>::max()}) {
        oracle(example::GetUserRequest{id}); oracle(example::RenameUserReply{true}); oracle(example::RenameUserReply{false});
        for (std::size_t length : {std::size_t{0}, std::size_t{64}, std::size_t{600}, std::size_t{4096}}) {
            std::string text(length, 'x'); text += std::string{"张\0三", 7};
            oracle(example::RenameUserRequest{id, text});
            oracle(example::GetUserReply{{id, text, {"", "writer", std::string{"a\0b", 3}}}});
        }
    }
    measured_message source{"payload"}, decoded; std::array<std::uint8_t, 64> bytes{};
    conversions = 0;
    auto result = rpc::encode_into(rpc::mapped_protobuf_codec_policy::operations<measured_message>(), &source, {bytes.data(), bytes.size()});
    CHECK(result.code == rpc::wire::error::none && conversions == 1);
    CHECK(rpc::mapped_protobuf_codec<measured_message>::decode({bytes.data(), result.written}, decoded) && decoded.text == source.text);
    bytes.fill(0xa5);
    result = rpc::mapped_protobuf_codec<measured_message>::encode_bounded(source, {bytes.data(), 1});
    CHECK(result.code == rpc::wire::error::output_too_small && result.written == 0 && bytes[0] == 0xa5);
    source.valid = false;
    result = rpc::mapped_protobuf_codec<measured_message>::encode_bounded(source, {bytes.data(), bytes.size()});
    CHECK(result.code == rpc::wire::error::invalid_argument && result.written == 0);
    source.valid = true; source.fail_allocation = true; bool exhausted = false;
    try { rpc::mapped_protobuf_codec<measured_message>::encode_bounded(source, {bytes.data(), bytes.size()}); }
    catch (std::bad_alloc const &) { exhausted = true; }
    CHECK(exhausted);
    example::GetUserReply reply; std::uint8_t const malformed = 0xff;
    CHECK(!rpc::mapped_protobuf_codec<example::GetUserReply>::decode({&malformed, 1}, reply));
    CHECK(!rpc::mapped_protobuf_codec<example::GetUserReply>::decode({}, reply)); // Mapping requires the nested user.
}
struct service {
    std::string name = "Alice";
    net::task<rpc::status_code> GetUser(rpc::server_context &, example::GetUserRequest const &, example::GetUserReply &);
    net::task<rpc::status_code> RenameUser(rpc::server_context &, example::RenameUserRequest const &, example::RenameUserReply &);
};
auto get(service *self, rpc::server_context *context, example::GetUserRequest const *request, example::GetUserReply *reply)
    CO2_BEG(net::task<rpc::status_code>, (self, context, request, reply)) {
    CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    reply->user = {request->id, self->name, {"reader", "writer"}};
    if (context->metadata.count) {
        auto const item = rpc::wire::decode_metadata_entry(context->metadata.entries);
        if (!context->set_trailer({}, {&item.value, 1})) CO2_RETURN(rpc::status_code::internal);
    }
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END
auto rename(service *self, example::RenameUserRequest const *request, example::RenameUserReply *reply)
    CO2_BEG(net::task<rpc::status_code>, (self, request, reply)) {
    self->name = request->name;
    reply->updated = true; CO2_RETURN(rpc::status_code::ok);
}
CO2_END
net::task<rpc::status_code> service::GetUser(rpc::server_context &c, example::GetUserRequest const &r, example::GetUserReply &v) { return get(this, &c, &r, &v); }
net::task<rpc::status_code> service::RenameUser(rpc::server_context &, example::RenameUserRequest const &r, example::RenameUserReply &v) { return rename(this, &r, &v); }
template <class Policy>
auto run(rpc::client *client, rpc::channel *channel, net::ip::tcp::endpoint endpoint, example::UserStub<Policy> stub)
    CO2_BEG(net::task<>, (client, channel, endpoint, stub), rpc::status_code connected; rpc::call_result result;
        example::GetUserRequest request{std::numeric_limits<std::uint64_t>::max()}; example::GetUserReply reply;
        example::RenameUserRequest change; example::RenameUserReply changed;
        std::array<std::uint8_t, 64> metadata{}; rpc::wire::metadata_entry entry;
        std::string key{"trace"}, value{"service-contract"}; rpc::call_spec spec; rpc::response_trailer trailer;) {
    if (channel) { CO2_AWAIT_SET(connected, channel->wait_ready(rpc::clock::now() + std::chrono::seconds{5})); }
    else { CO2_AWAIT_SET(connected, client->connect(endpoint)); }
    CHECK(connected == rpc::status_code::ok);
    entry = {{reinterpret_cast<std::uint8_t const *>(key.data()), key.size()}, {reinterpret_cast<std::uint8_t const *>(value.data()), value.size()}};
    spec.timeout = std::chrono::seconds{2}; spec.metadata = {&entry, 1};
    trailer.storage = {metadata.data(), metadata.size()};
    CO2_AWAIT_SET(result, stub.GetUser(request, reply, &spec, &trailer));
    CHECK(result.code == rpc::status_code::ok && reply.user.id == request.id && reply.user.name == "Alice" && reply.user.roles.size() == 2);
    CHECK(trailer.metadata.count == 1);
    change.id = request.id; change.name = std::string(600, 'x') + std::string{"张\0三", 7};
    CO2_AWAIT_SET(result, stub.RenameUser(change, changed, &spec));
    CHECK(result.code == rpc::status_code::ok && changed.updated);
    CO2_AWAIT_SET(result, stub.GetUser(request, reply, &spec));
    CHECK(result.code == rpc::status_code::ok && reply.user.name == change.name && reply.user.roles[1] == "writer");
    if (channel) channel->close();
    CO2_RETURN();
}
CO2_END
// One server answers the same names in both encodings; each stub's label picks its own.
auto mixed(rpc::client *client, net::ip::tcp::endpoint endpoint, example::UserStub<rpc::json_codec_policy> json,
           example::UserStub<rpc::mapped_protobuf_codec_policy> proto)
    CO2_BEG(net::task<>, (client, endpoint, json, proto), rpc::status_code connected; rpc::call_result result;
            example::GetUserRequest request{7}; example::GetUserReply reply; example::RenameUserRequest change;
            example::RenameUserReply changed;) {
    CO2_AWAIT_SET(connected, client->connect(endpoint));
    CHECK(connected == rpc::status_code::ok);
    CO2_AWAIT_SET(result, json.GetUser(request, reply));
    CHECK(result.code == rpc::status_code::ok && reply.user.name == "Alice");
    change.id = 7; change.name = "Bob";
    CO2_AWAIT_SET(result, proto.RenameUser(change, changed));
    CHECK(result.code == rpc::status_code::ok && changed.updated);
    reply = {};
    CO2_AWAIT_SET(result, json.GetUser(request, reply));
    CHECK(result.code == rpc::status_code::ok && reply.user.name == "Bob" && reply.user.id == 7);
    reply = {};
    CO2_AWAIT_SET(result, proto.GetUser(request, reply));
    CHECK(result.code == rpc::status_code::ok && reply.user.name == "Bob" && reply.user.roles.size() == 2);
    CO2_RETURN();
}
CO2_END
void both_encodings() {
    net::io_context context{net::default_backend, net::single_thread_hint};
    rpc::shard shard{context};
    service implementation;
    auto const json = example::users_contract(rpc::json_codec_policy{});
    auto const proto = example::users_contract(rpc::mapped_protobuf_codec_policy{});
    auto methods = example::users_bindings(json, implementation);
    for (auto &binding : example::users_bindings(proto, implementation)) methods.push_back(std::move(binding));
    rpc::server server{shard, std::move(methods)};
    auto const endpoint = server.listen({net::ip::address_v4::loopback(), 0});
    rpc::client client{shard};
    std::exception_ptr failure;
    auto close = [&] { client.close(); server.close(); };
    net::run_async(context.get_executor(), close, [&](std::exception_ptr e) { failure = e; close(); })([&] {
        return mixed(&client, endpoint, example::UserStub<rpc::json_codec_policy>{client, json},
                     example::UserStub<rpc::mapped_protobuf_codec_policy>{client, proto});
    });
    context.run(); if (failure) std::rethrow_exception(failure);
}
template <class Policy> void round_trip(Policy policy, bool use_channel) {
    net::io_context context{net::default_backend, net::single_thread_hint};
    rpc::shard shard{context};
    service implementation;
    auto const contract = example::users_contract(policy);
    rpc::server server{shard, example::users_bindings(contract, implementation)};
    auto const endpoint = server.listen({net::ip::address_v4::loopback(), 0});
    rpc::client client{shard};
    rpc::channel channel{shard, {endpoint}};
    auto const target = use_channel ? rpc::call_target{channel} : rpc::call_target{client};
    std::exception_ptr failure;
    auto close = [&] { channel.close(); client.close(); server.close(); };
    net::run_async(context.get_executor(), close, [&](std::exception_ptr e) { failure = e; close(); })
        ([&] { return run(&client, use_channel ? &channel : nullptr, endpoint, example::UserStub<Policy>{target, contract}); });
    context.run(); if (failure) std::rethrow_exception(failure);
    CHECK(server.stats().active_calls == 0 && server.stats().response_bytes == 0);
}
}
int main() {
    try {
        mapping_checks();
        for (bool c : {false, true}) {
            round_trip(rpc::json_codec_policy{}, c);
            round_trip(rpc::mapped_protobuf_codec_policy{}, c);
        }
        std::cout << "PASS shared struct service over JSON and mapped protobuf, client and channel, trailer\n";
        both_encodings();
        std::cout << "PASS one server, both encodings under the same names\n";
    } catch (std::exception const &e) { std::cerr << e.what() << '\n'; return 1; }
}
