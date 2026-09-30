#pragma once

#include <rpc/unary.hpp>
#include <net/continuation.hpp>
#include <net/timer.hpp>

#include <cassert>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace rpc {
namespace detail {

// Stable in-place ownership for immovable callbacks and other late resources.
template <class T> class inplace {
public:
    inplace() noexcept = default;
    ~inplace() { reset(); }
    inplace(inplace const &) = delete;
    inplace &operator=(inplace const &) = delete;
    template <class... Args> T &emplace(Args &&...args) {
        assert(!engaged_);
        auto *value = ::new (static_cast<void *>(&storage_)) T(std::forward<Args>(args)...);
        engaged_ = true;
        return *value;
    }
    void reset() noexcept {
        if (engaged_) { get()->~T(); engaged_ = false; }
    }
    T *get() noexcept { return engaged_ ? reinterpret_cast<T *>(&storage_) : nullptr; }
private:
    typename std::aligned_storage<sizeof(T), alignof(T)>::type storage_;
    bool engaged_ = false;
};

struct slot_handle {
    std::size_t index = std::numeric_limits<std::size_t>::max();
    std::uint64_t generation = 0;
};

template <class T> class slot_pool {
    struct entry {
        T value{};
        std::size_t next = 0;
        std::uint64_t generation = 0;
        bool occupied = false;
    };
public:
    explicit slot_pool(std::size_t capacity) : entries_(capacity), free_(capacity ? 0 : none) {
        for (std::size_t i = 0; i < capacity; ++i) entries_[i].next = i + 1 < capacity ? i + 1 : none;
    }
    slot_handle acquire() noexcept {
        if (free_ == none) return {};
        auto const index = free_;
        auto &entry = entries_[index];
        free_ = entry.next; entry.occupied = true; ++used_;
        // Never wrap an externally observable generation.
        if (++entry.generation == 0) std::terminate();
        return {index, entry.generation};
    }
    T *get(slot_handle handle) noexcept {
        if (handle.index >= entries_.size()) return nullptr;
        auto &entry = entries_[handle.index];
        return entry.occupied && entry.generation == handle.generation ? &entry.value : nullptr;
    }
    void release(slot_handle handle) noexcept {
        assert(get(handle));
        auto &entry = entries_[handle.index];
        entry.occupied = false; entry.next = free_; free_ = handle.index; --used_;
    }
    std::size_t size() const noexcept { return used_; }
    T &at(std::size_t index) noexcept { return entries_[index].value; }
    std::size_t capacity() const noexcept { return entries_.size(); }
private:
    static constexpr std::size_t none = std::numeric_limits<std::size_t>::max();
    std::vector<entry> entries_;
    std::size_t free_;
    std::size_t used_ = 0;
};

// Fixed, <= 1/2 loaded open-addressing stream index. Back-shift deletion does
// not retain tombstones; neither admission nor completion allocates a node.
template <class T> class stream_index {
    struct entry { std::uint32_t id = 0; T *value = nullptr; };
public:
    explicit stream_index(std::size_t capacity) : entries_(table_size(capacity)), limit_(capacity) {}
    T *find(std::uint32_t id) const noexcept {
        auto i = static_cast<std::size_t>(id) & mask();
        while (entries_[i].id != 0) {
            if (entries_[i].id == id) return entries_[i].value;
            i = (i + 1) & mask();
        }
        return nullptr;
    }
    bool insert(std::uint32_t id, T &value) noexcept {
        assert(id != 0);
        if (size_ == limit_ || find(id)) return false;
        auto i = static_cast<std::size_t>(id) & mask();
        while (entries_[i].id != 0) i = (i + 1) & mask();
        entries_[i] = {id, &value}; ++size_; return true;
    }
    void erase(std::uint32_t id) noexcept {
        auto hole = static_cast<std::size_t>(id) & mask();
        while (entries_[hole].id != 0 && entries_[hole].id != id) hole = (hole + 1) & mask();
        if (entries_[hole].id == 0) return;
        for (auto i = (hole + 1) & mask(); entries_[i].id != 0; i = (i + 1) & mask()) {
            auto const home = static_cast<std::size_t>(entries_[i].id) & mask();
            if (((hole - home) & mask()) < ((i - home) & mask())) { entries_[hole] = entries_[i]; hole = i; }
        }
        entries_[hole] = {}; --size_;
    }
    T *first() const noexcept { for (auto const &entry : entries_) if (entry.id != 0) return entry.value; return nullptr; }
    template <class Function> void each(Function function) const { for (auto const &entry : entries_) if (entry.id != 0) function(*entry.value); }
    std::size_t size() const noexcept { return size_; }
    bool empty() const noexcept { return size_ == 0; }
private:
    static std::size_t table_size(std::size_t capacity) {
        if (capacity > std::numeric_limits<std::size_t>::max() / 4) throw std::invalid_argument{"stream capacity overflow"};
        std::size_t size = 2;
        while (size < capacity * 2) size *= 2;
        return size;
    }
    std::size_t mask() const noexcept { return entries_.size() - 1; }
    std::vector<entry> entries_;
    std::size_t limit_;
    std::size_t size_ = 0;
};

struct deadline_node {
    clock::time_point when = clock::time_point::max();
    std::size_t index = std::numeric_limits<std::size_t>::max();
    void (*expire)(void *, status_code) = nullptr;
    void *user = nullptr;
};

struct deadline_scheduler;
std::shared_ptr<deadline_scheduler> shard_deadlines(net::io_context &context);
void reserve_deadlines(deadline_scheduler &scheduler, std::size_t capacity);
void release_deadlines(deadline_scheduler &scheduler, std::size_t capacity) noexcept;
void activate_deadlines(std::shared_ptr<deadline_scheduler> const &scheduler);
void deactivate_deadlines(deadline_scheduler &scheduler) noexcept;
bool schedule(deadline_scheduler &scheduler, deadline_node &node, clock::time_point when,
              void (*expire)(void *, status_code), void *user) noexcept;
void unschedule(deadline_scheduler &scheduler, deadline_node &node) noexcept;
void fail_deadlines(deadline_scheduler &scheduler) noexcept;
std::size_t deadline_capacity(deadline_scheduler const &scheduler) noexcept;

class deadline_reservation {
public:
    deadline_reservation(deadline_scheduler &scheduler, std::size_t capacity) : scheduler_(scheduler), capacity_(capacity) {
        reserve_deadlines(scheduler_, capacity_);
    }
    ~deadline_reservation() { release_deadlines(scheduler_, capacity_); }
    deadline_reservation(deadline_reservation const &) = delete;
    deadline_reservation &operator=(deadline_reservation const &) = delete;
private:
    deadline_scheduler &scheduler_;
    std::size_t capacity_;
};

} // namespace detail
} // namespace rpc
