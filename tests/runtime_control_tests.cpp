#include "check.hpp"
#include <rpc/runtime.hpp>
#include <net/test/run_blocking.hpp>
#include <atomic>
#include <cerrno>
#include <future>
#include <iostream>
#include <pthread.h>
#include <dlfcn.h>

namespace {
std::atomic<int> fail_thread{-1};
}
#if defined(LRPC_TEST_ASAN)
extern "C" int __interceptor_pthread_create(pthread_t *, pthread_attr_t const *, void *(*)(void *), void *);
#endif
extern "C" int pthread_create(pthread_t *thread, pthread_attr_t const *attr, void *(*fn)(void *), void *arg) noexcept {
#if !defined(LRPC_TEST_ASAN)
    using create = int (*)(pthread_t *, pthread_attr_t const *, void *(*)(void *), void *);
    static auto real_create = reinterpret_cast<create>(dlsym(RTLD_NEXT, "pthread_create"));
#endif
    auto remaining = fail_thread.load();
    if (remaining >= 0 && fail_thread.fetch_sub(1) == 0) return EAGAIN;
#if defined(LRPC_TEST_ASAN)
    // Preserve ASan's thread registration; RTLD_NEXT here points to libc and
    // bypasses the sanitizer, making a valid join appear to join an old thread.
    return __interceptor_pthread_create(thread, attr, fn, arg);
#else
    return real_create(thread, attr, fn, arg);
#endif
}
namespace {
rpc::codec_ops const ops{[](void const *) { return std::size_t{0}; },
    [](void const *, rpc::wire::mutable_bytes_view) { return true; },
    [](rpc::wire::bytes_view, void *) { return true; }};
void registration_rollback() {
    auto rt = rpc::make_runtime({2, net::backend_kind::epoll, 1, {}});
    rpc::channel_options options; options.connection.max_registered_methods = 1;
    options.resolve = rpc::make_static_resolver({{net::ip::make_address("127.0.0.1"), 1}});
    auto c = rpc::make_channel(*rt, options);
    rpc::method_descriptor methods[]{{"A", rpc::method_kind::unary, rpc::idempotency::unknown, &ops, &ops}, {}};
    bool failed = false; try { c->bind({"service", methods, 2}); } catch (std::invalid_argument const &) { failed = true; }
    CHECK(failed);
    CHECK(c->bind({"C", rpc::method_kind::unary, rpc::idempotency::unknown, &ops, &ops}));
    rt->shutdown();
}
void startup_failure() {
    auto rt = rpc::make_runtime({2, net::backend_kind::epoll, 4, {}});
    auto s = rpc::make_server(*rt, {{}, {}});
    s->listen({net::ip::make_address("127.0.0.1"), 0});
    fail_thread.store(1); bool failed = false;
    try { rt->start(); } catch (std::system_error const &) { failed = true; }
    fail_thread.store(-1); CHECK(failed && !rt->accepting()); rt->shutdown();
}
void full_control_capacity() {
    auto rt = rpc::make_runtime({2, net::backend_kind::epoll, 1, {}});
    rpc::channel_options options; options.resolve = rpc::make_static_resolver({{net::ip::make_address("127.0.0.1"), 1}});
    auto c = rpc::make_channel(*rt, options); auto s = rpc::make_server(*rt, {{}, {}});
    auto late = rpc::make_channel(*rt, options);
    s->listen({net::ip::make_address("127.0.0.1"), 0}); rt->start();
    CHECK(rt->acquire_call()); CHECK(!rt->acquire_call());
    net::io_context caller;
    net::test::run_blocking(caller, c->shutdown(std::chrono::milliseconds{0}));
    net::test::run_blocking(caller, s->shutdown(std::chrono::milliseconds{0}));
    CHECK(c->quiescent()); rt->release_call();
    rpc::call_options warmup; warmup.timeout = std::chrono::milliseconds{10};
    CHECK(net::test::run_blocking(caller, late->warmup(warmup)) == rpc::status_code::deadline_exceeded);
    std::promise<void> entered, release; auto gate = release.get_future().share();
    CHECK(rt->post(0, [&entered, gate] { entered.set_value(); gate.wait(); })); entered.get_future().get();
    CHECK(!rt->post(1, [] {}));
    late->close(); s->close(); release.set_value();
    auto until = rpc::clock::now() + std::chrono::milliseconds{500};
    while (!late->quiescent() && rpc::clock::now() < until) std::this_thread::sleep_for(std::chrono::milliseconds{1});
    CHECK(late->quiescent()); rt->shutdown(); CHECK(late->quiescent() && rt->stopped());
}
}
int main() {
    try { registration_rollback(); startup_failure(); full_control_capacity(); std::cout << "runtime control tests passed\n"; }
    catch (std::exception const &e) { fail_thread.store(-1); std::cerr << e.what() << '\n'; return 1; }
}
