#pragma once

#include <net/io_context.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>

namespace rpc {
namespace v2 {

struct shard_options {
    // Idle blocks kept per size class; physical memory grows only with load.
    std::size_t slab_cache_bytes_per_class = 4U << 20;
};

namespace detail {
struct shard_state;
}

// Per-core resources for every client and server bound to one io_context:
// size-classed memory, the deadline wheel and its single timer, and small
// object pools. Everything is single-threaded; only stop requests may come
// from other threads. Destroy it after its endpoints are closed and the
// context has drained.
class shard {
public:
    explicit shard(net::io_context &context, shard_options options = {});
    ~shard();
    shard(shard const &) = delete;
    shard &operator=(shard const &) = delete;

    net::io_context &context() const noexcept;
    detail::shard_state &state() noexcept { return *state_; }

private:
    std::unique_ptr<detail::shard_state> state_;
};

namespace debug {
// Clock reads performed by this library on the calling thread (a test gate).
std::uint64_t clock_reads() noexcept;
} // namespace debug

} // namespace v2
} // namespace rpc
