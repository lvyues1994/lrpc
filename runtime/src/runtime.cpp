#include <rpc/runtime.hpp>
#include <net/detail/completion_frame.hpp>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <stdexcept>
#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif

namespace rpc {
namespace {
thread_local bool runtime_worker = false;
struct runtime_impl;
struct posted_operation {
    runtime_impl *owner;
    net::io_context::executor_type executor;
    std::function<void()> operation;
    net::detail::completion_frame frame;
    net::continuation continuation{};
    posted_operation(runtime_impl &owner, net::io_context &context, std::function<void()> operation);
    static net::coroutine_handle<> resume(void *value) noexcept;
};

struct runtime_impl final : runtime {
    explicit runtime_impl(runtime_options options) : options_(std::move(options)) {
        contexts_.reserve(options_.shards); threads_.reserve(options_.shards);
        for (std::size_t i = 0; i < options_.shards; ++i)
            contexts_.push_back(std::make_unique<net::io_context>(options_.backend, net::single_thread_hint));
    }
    ~runtime_impl() override {
        try { shutdown(); } catch (...) { std::terminate(); }
    }
    std::size_t size() const noexcept override { return contexts_.size(); }
    net::io_context &context(std::size_t shard) override { return *contexts_.at(shard); }
    bool running_in_this_thread() const noexcept override {
        for (auto const &context : contexts_) if (context->get_executor().running_in_this_thread()) return true;
        return false;
    }
    bool accepting() const noexcept override { return accepting_.load(std::memory_order_acquire); }
    bool started() const noexcept override { return started_.load(std::memory_order_acquire); }
    bool stopped() const noexcept override { return shutdown_done_.load(std::memory_order_acquire); }
    void start() override {
        std::unique_lock<std::mutex> lock(mutex_);
        if (started()) return;
        if (!accepting()) throw std::logic_error{"runtime is shut down"};
        for (auto const &context : contexts_) context->get_executor().on_work_started();
        try {
            for (std::size_t i = 0; i < size(); ++i) threads_.emplace_back([this, i] {
                { std::unique_lock<std::mutex> guard(mutex_); changed_.wait(guard, [this] { return launch_ready_; }); }
                runtime_worker = true;
#if defined(__linux__)
                if (!options_.cpu_affinity.empty()) {
                    cpu_set_t cpus; CPU_ZERO(&cpus); CPU_SET(options_.cpu_affinity[i], &cpus);
                    auto error = pthread_setaffinity_np(pthread_self(), sizeof(cpus), &cpus);
                    if (error != 0) { std::lock_guard<std::mutex> guard(mutex_); if (!error_) error_ = std::make_exception_ptr(std::system_error{error, std::generic_category()}); }
                }
#endif
                // An escaped user exception is recorded; the same fixed thread
                // continues draining rather than abandoning other I/O chains.
                for (;;) {
                    try { contexts_[i]->run(); break; }
                    catch (...) { std::lock_guard<std::mutex> guard(mutex_); if (!error_) error_ = std::current_exception(); }
                }
                runtime_worker = false;
            });
        } catch (...) {
            auto failure = std::current_exception();
            accepting_.store(false, std::memory_order_release);
            // Workers are still behind the barrier: cold sockets can be closed
            // before any context has established its backend thread identity.
            lock.unlock();
            for (auto &hook : hooks_) { try { hook->operation(); } catch (...) {} }
            hooks_.clear();
            for (auto const &context : contexts_) context->get_executor().on_work_finished();
            lock.lock(); launch_ready_ = true; changed_.notify_all(); lock.unlock();
            // Contexts without a worker have never run; drain their cold roots
            // here. Created workers retain their own backend ownership.
            for (std::size_t i = threads_.size(); i < size(); ++i) {
                for (;;) { try { contexts_[i]->run(); break; } catch (...) {} }
            }
            for (auto &thread : threads_) thread.join();
            threads_.clear(); shutdown_done_ = true; std::rethrow_exception(failure);
        }
        started_.store(true, std::memory_order_release);
        launch_ready_ = true; changed_.notify_all();
    }
    bool post(std::size_t shard, std::function<void()> operation) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!started() || !accepting() || shard >= size() || !operation || pending_ >= options_.max_pending_posts) return false;
        enqueue(shard, std::move(operation)); return true;
    }
    void on_shutdown(std::size_t shard, std::function<void()> operation) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (started() || !accepting() || shard >= size() || !operation) throw std::invalid_argument{"invalid cold runtime shutdown hook"};
        hooks_.push_back(std::make_unique<posted_operation>(*this, *contexts_[shard], std::move(operation)));
    }
    bool acquire_call(bool control) noexcept override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!accepting() || (!control && calls_ >= options_.max_pending_posts)) return false;
        ++calls_; return true;
    }
    void release_call() noexcept override {
        std::lock_guard<std::mutex> lock(mutex_); --calls_; changed_.notify_all();
    }
    void shutdown() override {
        if (running_in_this_thread()) throw std::logic_error{"runtime shutdown cannot join its own shard"};
        std::lock_guard<std::mutex> serial(shutdown_mutex_);
        if (shutdown_done_) return;
        if (!started()) start(); // Cold registered sockets/tasks must be drained too.
        {
            std::lock_guard<std::mutex> lock(mutex_);
            accepting_.store(false, std::memory_order_release);
            for (auto &hook : hooks_) enqueue(std::move(hook));
            hooks_.clear();
        }
        {
            std::unique_lock<std::mutex> lock(mutex_);
            changed_.wait(lock, [this] { return pending_ == 0 && calls_ == 0; });
        }
        for (auto const &context : contexts_) context->get_executor().on_work_finished();
        for (auto &thread : threads_) thread.join();
        threads_.clear();
        shutdown_done_ = true;
        if (error_) std::rethrow_exception(error_);
    }
    void enqueue(std::size_t shard, std::function<void()> operation) {
        auto node = std::make_unique<posted_operation>(*this, *contexts_[shard], std::move(operation));
        enqueue(std::move(node));
    }
    void enqueue(std::unique_ptr<posted_operation> node) {
        ++pending_; node->executor.on_work_started(); node->executor.post(node->continuation); node.release();
    }
    void finish(std::exception_ptr error) noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        if (error && !error_) error_ = error;
        --pending_; changed_.notify_all();
    }
private:
    runtime_options options_;
    std::vector<std::unique_ptr<net::io_context>> contexts_{};
    std::vector<std::thread> threads_{};
    std::vector<std::unique_ptr<posted_operation>> hooks_{};
    std::mutex mutex_;
    std::mutex shutdown_mutex_;
    std::condition_variable changed_;
    std::atomic<bool> accepting_{true}, started_{false};
    bool launch_ready_ = false;
    std::atomic<bool> shutdown_done_{false};
    std::size_t pending_ = 0, calls_ = 0;
    std::exception_ptr error_{};
};
posted_operation::posted_operation(runtime_impl &owner, net::io_context &context, std::function<void()> operation)
    : owner(&owner), executor(context.get_executor()), operation(std::move(operation)), frame(&resume, this) { continuation.h = frame.handle(); }
net::coroutine_handle<> posted_operation::resume(void *value) noexcept {
    std::unique_ptr<posted_operation> self{static_cast<posted_operation *>(value)};
    std::exception_ptr error;
    try { self->operation(); } catch (...) { error = std::current_exception(); }
    auto *owner = self->owner; auto executor = self->executor;
    self.reset(); owner->finish(error); executor.on_work_finished();
    return nullptr;
}
} // namespace

namespace detail { bool is_runtime_thread() noexcept { return runtime_worker; } }

std::unique_ptr<runtime> make_runtime(runtime_options options) {
    if (options.shards == 0 || options.shards > 256 || options.max_pending_posts == 0 ||
        (!options.cpu_affinity.empty() && options.cpu_affinity.size() != options.shards))
        throw std::invalid_argument{"invalid runtime capacity/affinity"};
#if defined(__linux__)
    for (auto cpu : options.cpu_affinity) if (cpu >= CPU_SETSIZE) throw std::invalid_argument{"CPU index exceeds affinity mask"};
#else
    if (!options.cpu_affinity.empty()) throw std::invalid_argument{"CPU affinity is unsupported on this platform"};
#endif
    return std::make_unique<runtime_impl>(std::move(options));
}
} // namespace rpc
