#pragma once

#include <net/io_env.hpp>
#include <net/continuation.hpp>

#include <cassert>
#include <utility>

namespace bench {

// One waiter on one executor thread. Also latches a signal before the wait.
class event {
public:
    struct awaiter {
        event *owner;
        net::continuation continuation{};
        bool await_ready() noexcept { return std::exchange(owner->signaled_, false); }
        net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
            assert(owner->waiter_ == nullptr);
            continuation.h = handle;
            owner->waiter_ = &continuation;
            owner->executor_ = env->executor;
            return net::noop_coroutine();
        }
        void await_resume() const noexcept {}
    };
    awaiter wait() noexcept { return {this, {}}; }
    void signal() noexcept {
        auto *waiter = std::exchange(waiter_, nullptr);
        if (waiter) executor_.post(*waiter);
        else signaled_ = true;
    }
private:
    bool signaled_ = false;
    net::continuation *waiter_ = nullptr;
    net::executor_ref executor_{};
};

struct yield {
    net::continuation continuation{};
    bool await_ready() const noexcept { return false; }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) noexcept {
        continuation.h = handle; env->executor.post(continuation); return net::noop_coroutine();
    }
    void await_resume() const noexcept {}
};

} // namespace bench
