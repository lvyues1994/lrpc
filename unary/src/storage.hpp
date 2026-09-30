#pragma once

#include <rpc/unary.hpp>
#include "runtime.hpp"

#include <vector>

namespace rpc {
namespace detail {

struct client_call;
class block_pool;

struct byte_budget {
    std::size_t limit = 0;
    std::size_t used = 0;
};

struct block {
    block_pool *pool = nullptr;
    byte_budget *budget = nullptr;
    std::uint8_t *data = nullptr;
    block *next = nullptr;
    std::size_t size = 0;
    std::size_t body_offset = 0;
    std::size_t body_size = 0;
    slot_handle request{};
    bool is_request = false;
#ifdef LRPC_ENABLE_DIAGNOSTICS
    clock::time_point queued_at{};
#endif
};

// Stable, nonmoving pools; one lease owns one whole block. Moving it to a tx
// queue does not release quota. No allocator calls on acquire/release.
class block_lease {
public:
    block_lease() noexcept = default;
    explicit block_lease(block *value) noexcept;
    ~block_lease();
    block_lease(block_lease &&other) noexcept;
    block_lease &operator=(block_lease &&other) noexcept;
    block_lease(block_lease const &) = delete;
    block_lease &operator=(block_lease const &) = delete;
    block *get() const noexcept;
    block *release() noexcept;
    void reset() noexcept;
private:
    block *value_ = nullptr;
};

class block_pool {
public:
    block_pool(std::size_t block_size, std::size_t budget);
    block_pool(block_pool const &) = delete;
    block_pool &operator=(block_pool const &) = delete;
    block_pool(block_pool &&) = delete;
    block_pool &operator=(block_pool &&) = delete;
    block_lease acquire(byte_budget &budget) noexcept;
    void release(block &value) noexcept;
    std::size_t block_size() const noexcept;
    std::size_t charge() const noexcept;
    std::size_t in_use() const noexcept;
    std::size_t allocated() const noexcept;
private:
    std::size_t block_size_ = 0;
    std::vector<std::uint8_t> bytes_{};
    std::vector<block> blocks_{};
    block *free_ = nullptr;
    std::size_t used_ = 0;
};

class tx_queue {
public:
    tx_queue() noexcept = default;
    ~tx_queue();
    tx_queue(tx_queue const &) = delete;
    tx_queue &operator=(tx_queue const &) = delete;
    tx_queue(tx_queue &&) = delete;
    tx_queue &operator=(tx_queue &&) = delete;
    void push(block_lease value) noexcept;
    void push_front(block_lease value) noexcept;
    block_lease pop() noexcept;
    bool empty() const noexcept;
    void clear() noexcept;
private:
    block *head_ = nullptr;
    block *tail_ = nullptr;
};

} // namespace detail
} // namespace rpc
