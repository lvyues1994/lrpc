#include "client_fixture.hpp"
#include <rpc/service.hpp>
#include <net/timeout.hpp>
#include <iostream>

struct number { std::uint8_t value = 0; };
namespace rpc {
template <> struct codec<number> {
    static std::size_t size(number const &) { return 1; }
    static bool encode(number const &value, wire::mutable_bytes_view out) {
        if (out.size != 1) return false;
        out.data[0] = value.value; return true;
    }
    static bool decode(wire::bytes_view in, number &value) {
        if (in.size != 1) return false;
        value.value = in.data[0]; return true;
    }
};
}
namespace {
struct alternate_policy {
    template <class T> static rpc::codec_ops const &operations() {
        static auto const ops = rpc::codec_for<T>(); return ops;
    }
};
struct invalid_policy {
    template <class T> static rpc::codec_ops const &operations() {
        static auto const ops = [] { auto v = rpc::codec_for<T>(); v.encode = nullptr; return v; }(); return ops;
    }
};
using declaration = rpc::unary_method<number, number>;
template <class Policy = rpc::default_codec_policy>
auto contract(Policy policy = {}) {
    return rpc::make_service_contract("numbers", policy, declaration{"A"}, declaration{"B"});
}
template <class Function> void rejects(Function fn) {
    bool failed = false; try { fn(); } catch (std::invalid_argument const &) { failed = true; }
    CHECK(failed);
}
void validation() {
    rejects([] { rpc::make_service_contract("", rpc::default_codec_policy{}, declaration{"A"}); });
    rejects([] { rpc::make_service_contract("s", rpc::default_codec_policy{}, declaration{std::string{"A\0B", 3}}); });
    rejects([] { rpc::make_service_contract("s", rpc::default_codec_policy{}, declaration{"A"}, declaration{"A"}); });
    rejects([] { rpc::make_service_contract("s", rpc::default_codec_policy{}, declaration{"A", static_cast<rpc::idempotency>(9)}); });
    rejects([] { contract(invalid_policy{}); });
    test_client::fixture f;
    auto empty = rpc::make_service_contract("empty", rpc::default_codec_policy{});
    auto bound = rpc::bind_service(*f.client, empty); (void)bound;
    f.zero();
}
void rollback() {
    rpc::client_options options; options.max_registered_methods = 2;
    test_client::fixture f{options};
    auto existing = rpc::bind(*f.client, rpc::method<number, number>{"B"});
    rejects([&] { rpc::bind_service(*f.client, contract(alternate_policy{})); });
    auto remaining = rpc::bind(*f.client, rpc::method<number, number>{"C"}); (void)remaining;
    number request{42}, response; unsigned done = 0;
    net::run_async(f.context.get_executor(), [&](rpc::call_result r) { CHECK(r.code == rpc::status_code::ok); ++done; },
        [](std::exception_ptr e) { std::rethrow_exception(e); })([&] { return existing(request, response); });
    f.context.poll(); f.pipe->feed(test_client::reply(1, 73)); f.context.poll();
    CHECK(done == 1 && response.value == 73); f.zero();
    options.max_registered_methods = 1; test_client::fixture limited{options};
    rejects([&] { rpc::bind_service(*limited.client, contract()); });
    auto available = rpc::bind(*limited.client, rpc::method<number, number>{"C"}); (void)available;
    limited.zero();
}
struct service {
    net::task<rpc::status_code> increment(rpc::server_context &, number const &, number &);
};
auto increment(number const *request, number *response)
    CO2_BEG(net::task<rpc::status_code>, (request, response)) {
    CO2_AWAIT(net::delay(std::chrono::milliseconds{1}));
    response->value = static_cast<std::uint8_t>(request->value + 1);
    CO2_RETURN(rpc::status_code::ok);
}
CO2_END
net::task<rpc::status_code> service::increment(rpc::server_context &, number const &r, number &s) { return ::increment(&r, &s); }
void lifetime_and_server_atomicity() {
    test_client::fixture f;
    auto bound = [&] { auto original = contract(); auto copy = original; auto moved = std::move(copy);
                      return rpc::bind_service(*f.client, moved); }();
    number request{9}, response; bool done = false;
    net::run_async(f.context.get_executor(), [&](rpc::call_result r) { CHECK(r.code == rpc::status_code::ok); done = true; },
        [](std::exception_ptr e) { std::rethrow_exception(e); })([&] { return bound.call<1>(request, response); });
    f.context.poll(); f.pipe->feed(test_client::reply(1, 19)); f.context.poll();
    CHECK(done && response.value == 19); f.zero();

    service impl; rpc::server_builder builder{f.context};
    auto definition = contract(); decltype(definition)::limits_type limits{{{1, 2}, {1, 2}}};
    using function = net::task<rpc::status_code> (service::*)(rpc::server_context &, number const &, number &);
    rejects([&] { rpc::add_service(builder, definition, impl, limits, &service::increment, function{}); });
    limits[1].message_cache_entries = 1;
    rejects([&] { rpc::add_service(builder, definition, impl, limits, &service::increment, &service::increment); });
    limits[1].message_cache_entries = 0;
    // Re-adding A/B and building proves the failed batches did not append A.
    rpc::add_service(builder, contract(), impl, limits, &service::increment, &service::increment);
    auto server = builder.build(); server->close(); f.context.run();
    auto bindings = contract().bindings(impl, limits, &service::increment, &service::increment);
    rpc::server_context call; std::uint8_t in = 9, out = 0; rpc::response_writer writer{{&out, 1}}; done = false;
    net::run_async(f.context.get_executor(), [&](rpc::status_code code) { CHECK(code == rpc::status_code::ok); done = true; },
        [](std::exception_ptr e) { std::rethrow_exception(e); })([&] { return bindings[0].handler->invoke(call, {&in, 1}, writer); });
    f.context.run(); CHECK(done && out == 10 && writer.size() == 1);
}
}
int main() {
    try { validation(); rollback(); lifetime_and_server_atomicity(); std::cout << "PASS typed service validation, lifetimes and atomic registration\n"; }
    catch (std::exception const &e) { std::cerr << e.what() << '\n'; return 1; }
}
