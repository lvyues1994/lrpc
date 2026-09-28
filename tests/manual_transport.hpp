#pragma once

#include <rpc/unary.hpp>
#include <net/error.hpp>

#include <algorithm>
#include <cstring>
#include <functional>
#include <limits>
#include <stdexcept>

// Deterministic single-thread test stream. Each pending direction retains its
// descriptors until manual completion (or close/stop), matching real net I/O.
// Stop requests for this fake must also originate on its one test thread.
struct manual_pipe {
    using completion = std::function<void(net::io_result<std::size_t>)>;
    std::vector<std::uint8_t> input{};
    std::vector<std::uint8_t> output{};
    std::size_t cursor = 0;
    net::mutable_buffer_array<> read_buffers{};
    net::const_buffer_array<> write_buffers{};
    completion reader{};
    completion writer{};
    bool closed = false;
    bool block_writes = false;
    bool delay_close_write = false;
    bool throw_reads = false;
    std::size_t read_chunk = std::numeric_limits<std::size_t>::max();

    std::size_t copy_read() {
        if (read_chunk == 0) throw std::logic_error{"zero read progress"};
        std::size_t count = 0;
        auto available = std::min(input.size() - cursor, read_chunk);
        for (auto buffer : read_buffers.to_span()) {
            auto const size = std::min(available, buffer.size());
            if (size != 0) std::memcpy(buffer.data(), input.data() + cursor, size);
            count += size; cursor += size; available -= size;
        }
        return count;
    }
    std::size_t copy_write(std::size_t maximum) {
        std::size_t count = 0;
        for (auto buffer : write_buffers.to_span()) {
            auto const size = std::min(maximum - count, buffer.size());
            auto const *bytes = static_cast<std::uint8_t const *>(buffer.data());
            if (size != 0) output.insert(output.end(), bytes, bytes + size);
            count += size;
        }
        return count;
    }
    void feed(std::vector<std::uint8_t> const &bytes) {
        input.insert(input.end(), bytes.begin(), bytes.end());
        if (reader && cursor != input.size()) {
            auto done = std::move(reader); reader = {};
            done({{}, copy_read()});
        }
    }
    void finish_write(std::size_t maximum, std::error_code error = {}) {
        if (!error && maximum == 0) throw std::logic_error{"zero write progress"};
        auto done = std::move(writer); writer = {};
        if (done) done({error, error ? 0 : copy_write(maximum)});
    }
    void abort(bool writing) {
        auto done = writing ? std::move(writer) : std::move(reader);
        if (writing) writer = {}; else reader = {};
        if (done) done({net::make_error_code(net::error::operation_aborted), 0});
    }
    void close() { closed = true; abort(false); if (!delay_close_write) abort(true); }
};

struct manual_operation {
    manual_pipe *pipe;
    bool writing;
    net::io_result<std::size_t> result{};
    net::continuation continuation{};
    net::executor_ref executor{};
    std::unique_ptr<net::stop_callback<std::function<void()>>> stop{};

    bool await_ready() { return await_ready(nullptr); }
    bool await_ready(net::io_env const *env) {
        bool empty = true;
        if (writing) { for (auto buffer : pipe->write_buffers.to_span()) empty = empty && buffer.size() == 0; }
        else { for (auto buffer : pipe->read_buffers.to_span()) empty = empty && buffer.size() == 0; }
        if (empty) return true;
        if (pipe->closed || (env != nullptr && env->stop_token.stop_requested())) {
            result.ec = net::make_error_code(net::error::operation_aborted); return true;
        }
        if (writing && !pipe->block_writes) { result.value = pipe->copy_write(std::numeric_limits<std::size_t>::max()); return true; }
        if (!writing && pipe->cursor != pipe->input.size()) { result.value = pipe->copy_read(); return true; }
        return false;
    }
    net::coroutine_handle<> await_suspend(net::coroutine_handle<> handle, net::io_env const *env) {
        continuation.h = handle; executor = env->executor;
        auto complete = [this](net::io_result<std::size_t> value) {
            result = value; executor.post(continuation);
        };
        if (writing) pipe->writer = complete; else pipe->reader = complete;
        if (env->stop_token.stop_possible()) {
            auto *state = pipe; auto const direction = writing;
            try {
                stop = std::make_unique<net::stop_callback<std::function<void()>>>(env->stop_token,
                    [state, direction] { state->abort(direction); });
            } catch (...) {
                if (writing) pipe->writer = {}; else pipe->reader = {};
                throw;
            }
        }
        return net::noop_coroutine();
    }
    net::io_result<std::size_t> await_resume() { stop.reset(); return result; }
};

struct manual_stream {
    manual_pipe *pipe;
    template <class B> manual_operation read_some(B const &buffers) {
        if (pipe->throw_reads) throw std::runtime_error{"injected handshake read failure"};
        pipe->read_buffers = net::mutable_buffer_array<>{buffers};
        return {pipe, false, {}, {}, {}, {}};
    }
    template <class B> manual_operation write_some(B const &buffers) {
        pipe->write_buffers = net::const_buffer_array<>{buffers};
        return {pipe, true, {}, {}, {}, {}};
    }
};

struct manual_transport final : rpc::transport {
    explicit manual_transport(std::shared_ptr<manual_pipe> pipe)
        : pipe_(std::move(pipe)), raw_{pipe_.get()}, stream_(&raw_) {}
    net::any_stream &stream() noexcept override { return stream_; }
    void close() noexcept override { pipe_->close(); }
private:
    std::shared_ptr<manual_pipe> pipe_;
    manual_stream raw_;
    net::any_stream stream_;
};
