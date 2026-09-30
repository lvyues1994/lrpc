#include "storage.hpp"

#include <cassert>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>

namespace rpc {

metadata_writer::metadata_writer(wire::mutable_bytes_view storage) noexcept : storage_(storage) {
    if (storage_.size != 0 && storage_.data != nullptr) storage_.data[0] = 0;
    assign({});
}
bool metadata_writer::assign(wire::metadata_list entries) noexcept {
    if (overflowed_) return false;
    if (storage_.size < message_end_ || storage_.data == nullptr) { overflowed_ = true; return false; }
    auto const result = wire::encode_metadata(entries, {storage_.data + message_end_, storage_.size - message_end_});
    if (result.code != wire::error::none) { overflowed_ = true; return false; }
    size_ = message_end_ + result.written;
    return true;
}
bool metadata_writer::assign_message(wire::bytes_view message) noexcept {
    if (overflowed_) return false;
    if (message.size != 0 && message.data == nullptr) { overflowed_ = true; return false; }
    std::uint8_t prefix[10]{};
    auto const encoded = wire::encode_varint(message.size, {prefix, sizeof(prefix)});
    auto const tail = size_ - message_end_;
    if (message.size > storage_.size || encoded.written > storage_.size - message.size ||
        tail > storage_.size - message.size - encoded.written || storage_.data == nullptr) {
        overflowed_ = true; return false;
    }
    auto const end = encoded.written + message.size;
    std::memmove(storage_.data + end, storage_.data + message_end_, tail);
    if (message.size != 0) std::memcpy(storage_.data + encoded.written, message.data, message.size);
    std::memcpy(storage_.data, prefix, encoded.written);
    message_end_ = end; size_ = end + tail;
    return true;
}
bool metadata_writer::has_message() const noexcept { return message_end_ != 1; }
status_code set_status(server_context &context, status const &result) noexcept {
    if (result.code == status_code::ok && !result.message.empty()) return status_code::internal;
    if (!context.response_metadata.assign_message({reinterpret_cast<std::uint8_t const *>(result.message.data()), result.message.size()}))
        return status_code::internal;
    return result.code;
}
std::size_t metadata_writer::size() const noexcept { return size_; }
bool metadata_writer::overflowed() const noexcept { return overflowed_; }

response_writer::response_writer(wire::mutable_bytes_view storage) noexcept : storage_(storage) {}
wire::mutable_bytes_view response_writer::buffer() const noexcept { return storage_; }
bool response_writer::assign(wire::bytes_view bytes) noexcept {
    if (bytes.size > storage_.size || (bytes.size != 0 && bytes.data == nullptr)) {
        overflowed_ = true;
        return false;
    }
    if (bytes.size != 0) std::memmove(storage_.data, bytes.data, bytes.size);
    return commit(bytes.size);
}
bool response_writer::commit(std::size_t size) noexcept {
    if (size > storage_.size) overflowed_ = true;
    if (overflowed_) return false;
    size_ = size;
    return true;
}
std::size_t response_writer::size() const noexcept { return size_; }
bool response_writer::overflowed() const noexcept { return overflowed_; }

namespace detail {

block_lease::block_lease(block *value) noexcept : value_(value) {}
block_lease::~block_lease() { reset(); }
block_lease::block_lease(block_lease &&other) noexcept : value_(other.release()) {}
block_lease &block_lease::operator=(block_lease &&other) noexcept {
    if (this != &other) { reset(); value_ = other.release(); }
    return *this;
}
block *block_lease::get() const noexcept { return value_; }
block *block_lease::release() noexcept { return std::exchange(value_, nullptr); }
void block_lease::reset() noexcept {
    auto *value = release();
    if (value != nullptr) value->pool->release(*value);
}

block_pool::block_pool(std::size_t size, std::size_t budget) : block_size_(size) {
    if (size == 0 || size > std::numeric_limits<std::size_t>::max() - sizeof(block))
        throw std::invalid_argument{"invalid block size"};
    auto const count = budget / charge();
    if (count == 0) throw std::invalid_argument{"budget cannot hold one block and descriptor"};
    bytes_.resize(count * size);
    blocks_.resize(count);
    if (allocated() > budget) throw std::invalid_argument{"allocator capacity exceeds pool budget"};
    for (std::size_t i = 0; i < count; ++i) {
        blocks_[i].pool = this;
        blocks_[i].data = bytes_.data() + i * size;
        blocks_[i].next = free_;
        free_ = &blocks_[i];
    }
}
block_lease block_pool::acquire(byte_budget &budget) noexcept {
    if (free_ == nullptr || budget.used > budget.limit || charge() > budget.limit - budget.used) return {};
    auto *value = free_;
    free_ = value->next;
    value->next = nullptr;
    value->budget = &budget;
    value->size = 0;
    value->body_offset = 0;
    value->body_size = 0;
    value->is_request = false;
    budget.used += charge();
    used_ += charge();
    return block_lease{value};
}
void block_pool::release(block &value) noexcept {
    assert(value.pool == this && value.budget != nullptr && used_ >= charge());
    assert(value.budget->used >= charge());
    value.budget->used -= charge();
    value.budget = nullptr;
    used_ -= charge();
    value.request = {};
    value.next = free_;
    free_ = &value;
}
std::size_t block_pool::block_size() const noexcept { return block_size_; }
std::size_t block_pool::charge() const noexcept { return block_size_ + sizeof(block); }
std::size_t block_pool::in_use() const noexcept { return used_; }
std::size_t block_pool::allocated() const noexcept {
    return bytes_.capacity() + blocks_.capacity() * sizeof(block);
}

tx_queue::~tx_queue() { clear(); }
void tx_queue::push(block_lease value) noexcept {
    auto *node = value.release();
    assert(node != nullptr && node->next == nullptr);
    if (tail_ != nullptr) tail_->next = node;
    else head_ = node;
    tail_ = node;
}
void tx_queue::push_front(block_lease value) noexcept {
    auto *node = value.release();
    assert(node != nullptr && node->next == nullptr);
    node->next = head_;
    head_ = node;
    if (tail_ == nullptr) tail_ = node;
}
block_lease tx_queue::pop() noexcept {
    if (head_ == nullptr) return {};
    auto *node = head_;
    head_ = node->next;
    if (head_ == nullptr) tail_ = nullptr;
    node->next = nullptr;
    return block_lease{node};
}
bool tx_queue::empty() const noexcept { return head_ == nullptr; }
void tx_queue::clear() noexcept { while (!empty()) pop(); }

} // namespace detail
} // namespace rpc
