// Shadow queue (paper §4, §5.2): an LRU list of the keys most recently evicted from one
// application, holding only each key's 64-bit hash and item size. A GET that misses the cache
// but hits the shadow queue would have hit if the application had shadowQueueBytes more memory,
// so the rate of shadow hits approximates the gradient of the application's hit-rate curve.
// Not thread safe; the arbiter locks it.
#pragma once

#include <cstdint>
#include <list>
#include <unordered_map>

namespace memshare {

class ShadowQueue {
public:
    explicit ShadowQueue(uint64_t capacityBytes) : capacity_(capacityBytes) {}

    // Adds an evicted item at the most-recent end, dropping the oldest keys beyond capacity.
    void insert(uint64_t keyHash, uint32_t bytes);

    // If the key is present, removes it (it is about to be re-inserted into the cache) and
    // returns true.
    bool take(uint64_t keyHash);

    uint64_t bytes() const { return bytes_; }
    size_t size() const { return index_.size(); }

private:
    struct Node {
        uint64_t hash;
        uint32_t bytes;
    };
    uint64_t capacity_;
    uint64_t bytes_ = 0;
    std::list<Node> lru_;  // front = most recently evicted
    std::unordered_map<uint64_t, std::list<Node>::iterator> index_;
};

}  // namespace memshare
