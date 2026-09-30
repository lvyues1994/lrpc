#include "check.hpp"
#include "../examples/json_users.hpp"
#include "../examples/protobuf_users.hpp"
#include <rpc/channel.hpp>
#include <rpc/runtime.hpp>
#include <net/run_async.hpp>
#include <net/timeout.hpp>
#include <net/test/run_blocking.hpp>
#include <array>
#include <iostream>
#include <mutex>

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
struct user_state {
    std::mutex mutex;
    std::string name = "Alice";
};
struct service {
    std::shared_ptr<user_state> state = std::make_shared<user_state>();
    net::task<rpc::status_code> GetUser(rpc::server_context &, example::GetUserRequest const &, example::GetUserReply &);
    net::task<rpc::status_code> RenameUser(rpc::server_context &, example::RenameUserRequest const &, example::RenameUserReply &);
};
auto get(service *self, rpc::server_context *context, example::GetUserRequest const *request, example::GetUserReply *reply)
    CO2_BEG(net::task<rpc::status_code>, (self, context, request, reply)) {
    CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    { std::lock_guard<std::mutex> lock(self->state->mutex);
      reply->user = {request->id, self->state->name, {"reader", "writer"}}; }
    if (context->metadata.count) {
        auto const item = rpc::wire::decode_metadata_entry(context->metadata.entries);
        if (!context->response_metadata.assign({&item.value, 1})) CO2_RETURN(rpc::status_code::internal);
    }
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END
auto rename(service *self, example::RenameUserRequest const *request, example::RenameUserReply *reply)
    CO2_BEG(net::task<rpc::status_code>, (self, request, reply)) {
    { std::lock_guard<std::mutex> lock(self->state->mutex); self->state->name = request->name; }
    reply->updated = true; CO2_RETURN(rpc::status_code::ok);
}
CO2_END
net::task<rpc::status_code> service::GetUser(rpc::server_context &c, example::GetUserRequest const &r, example::GetUserReply &v) { return get(this, &c, &r, &v); }
net::task<rpc::status_code> service::RenameUser(rpc::server_context &, example::RenameUserRequest const &r, example::RenameUserReply &v) { return rename(this, &r, &v); }
auto run(rpc::client *client, rpc::channel *channel, net::ip::tcp::endpoint endpoint, example::UserStub stub)
    CO2_BEG(net::task<>, (client, channel, endpoint, stub), rpc::status_code connected; rpc::call_result result;
        example::GetUserRequest request{std::numeric_limits<std::uint64_t>::max()}; example::GetUserReply reply;
        example::RenameUserRequest change; example::RenameUserReply changed;
        std::array<std::uint8_t, 64> metadata{}; rpc::wire::metadata_entry entry;
        std::string key{"trace"}, value{"service-contract"}; rpc::call_options options;) {
    options.timeout = std::chrono::seconds{2};
    if (channel) { CO2_AWAIT_SET(connected, channel->warmup(options)); }
    else { CO2_AWAIT_SET(connected, client->connect(endpoint)); }
    CHECK(connected == rpc::status_code::ok);
    entry = {{reinterpret_cast<std::uint8_t const *>(key.data()), key.size()}, {reinterpret_cast<std::uint8_t const *>(value.data()), value.size()}};
    options.metadata = {&entry, 1}; options.response_metadata = {metadata.data(), metadata.size()};
    CO2_AWAIT_SET(result, stub.GetUser(request, reply, options));
    CHECK(result.code == rpc::status_code::ok && reply.user.id == request.id && reply.user.name == "Alice" && reply.user.roles.size() == 2);
    CHECK(result.response_metadata.count == 1);
    change.id = request.id; change.name = std::string(600, 'x') + std::string{"张\0三", 7};
    CO2_AWAIT_SET(result, stub.RenameUser(change, changed, options));
    CHECK(result.code == rpc::status_code::ok && changed.updated);
    CO2_AWAIT_SET(result, stub.GetUser(request, reply, options));
    CHECK(result.code == rpc::status_code::ok && reply.user.name == change.name && reply.user.roles[1] == "writer");
    if (channel) CHECK(channel->metrics().replay_bytes_in_use == 0);
    CO2_RETURN();
}
CO2_END
void round_trip(bool protobuf, bool use_channel, bool streaming) {
    net::io_context ctx{net::epoll, net::single_thread_hint}; service impl;
    auto definition = example::users_contract(rpc::json_codec_policy{});
    if (protobuf) definition = example::users_contract(rpc::mapped_protobuf_codec_policy{});
    rpc::server_options so; so.connection.receive.max_frame_size = streaming ? 256 : 4096;
    so.connection.receive.max_message_size = 4096;
    so.connection.receive.features = streaming ? rpc::wire::streaming : 0;
    if (streaming) so.connection.receive.initial_stream_window = 4096;
    rpc::server_builder builder{ctx, so}; example::add_users_service(builder, definition, impl);
    auto server = builder.build(); auto endpoint = server->listen({net::ip::address_v4::loopback(), 0});
    std::unique_ptr<rpc::client> direct; std::unique_ptr<rpc::channel> channel;
    rpc::client_options co; co.connection = so.connection;
    if (use_channel) { rpc::channel_options options; options.connection = co;
        options.resolve = rpc::make_static_resolver({endpoint}); channel = rpc::make_channel(ctx, options); }
    else direct = rpc::make_client(ctx, co);
    auto &client = use_channel ? static_cast<rpc::client &>(*channel) : *direct;
    auto stub = example::UserStub{client, definition};
    // Reassigning the original contract cannot change bound server/stub codecs.
    definition = example::users_contract(rpc::json_codec_policy{});
    std::exception_ptr failure; auto close = [&] { client.close(); server->close(); };
    net::run_async(ctx.get_executor(), close, [&](std::exception_ptr e) { failure = e; close(); })
        ([&] { return run(&client, channel.get(), endpoint, stub); });
    ctx.run(); if (failure) std::rethrow_exception(failure);
    CHECK(client.stats().active_calls == 0 && client.stats().request_bytes_in_use == 0);
    CHECK(server->stats().active_calls == 0 && server->stats().response_bytes_in_use == 0);
}
void runtime_round_trip(bool protobuf) {
    auto rt = rpc::make_runtime({2, net::backend_kind::epoll, 1024, {}});
    std::array<service, 2> services;
    // External facade calls may choose different shards. The business database
    // is shared and synchronized, while each shard has its own service adapter.
    services[1].state = services[0].state;
    auto definition = example::users_contract(rpc::json_codec_policy{});
    if (protobuf) definition = example::users_contract(rpc::mapped_protobuf_codec_policy{});
    example::UserLimits limits;
    std::vector<std::vector<rpc::method_binding>> bindings;
    for (auto &s : services)
        bindings.push_back(definition.bindings(s, {limits.GetUser, limits.RenameUser}, &service::GetUser, &service::RenameUser));
    auto server = rpc::make_server(*rt, std::move(bindings));
    auto endpoint = server->listen({net::ip::address_v4::loopback(), 0});
    rpc::channel_options options; options.resolve = rpc::make_static_resolver({endpoint});
    auto channel = rpc::make_channel(*rt, options);
    auto stub = example::UserStub{*channel, definition}; // Cold batch binding before start.
    rt->start(); bool rejected = false;
    try { example::UserStub late{*channel, definition}; } catch (std::logic_error const &) { rejected = true; }
    CHECK(rejected);
    net::io_context caller;
    net::test::run_blocking(caller, run(channel.get(), channel.get(), endpoint, stub));
    net::test::run_blocking(caller, channel->shutdown(std::chrono::milliseconds{100}));
    net::test::run_blocking(caller, server->shutdown(std::chrono::milliseconds{100}));
    rt->shutdown(); CHECK(channel->quiescent() && rt->stopped());
}
}
int main() {
    try { mapping_checks(); for (bool p : {false, true}) {
            for (bool c : {false, true}) for (bool s : {false, true}) round_trip(p, c, s);
            runtime_round_trip(p);
        }
        std::cout << "PASS shared struct service JSON/protobuf, channel/streaming, metadata and native wire oracle\n"; }
    catch (std::exception const &e) { std::cerr << e.what() << '\n'; return 1; }
}
