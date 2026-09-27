#include "memshare/shadow_queue.h"

namespace memshare {

void ShadowQueue::insert(uint64_t keyHash, uint32_t bytes) {
    if (capacity_ == 0) return;
    auto it = index_.find(keyHash);
    if (it != index_.end()) {
        bytes_ -= it->second->bytes;
        lru_.erase(it->second);
        index_.erase(it);
    }
    lru_.push_front(Node{keyHash, bytes});
    index_.emplace(keyHash, lru_.begin());
    bytes_ += bytes;
    while (bytes_ > capacity_ && !lru_.empty()) {
        const Node& old = lru_.back();
        bytes_ -= old.bytes;
        index_.erase(old.hash);
        lru_.pop_back();
    }
}

bool ShadowQueue::take(uint64_t keyHash) {
    auto it = index_.find(keyHash);
    if (it == index_.end()) return false;
    bytes_ -= it->second->bytes;
    lru_.erase(it->second);
    index_.erase(it);
    return true;
}

}  // namespace memshare
