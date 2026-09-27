// Hash table mapping (application, key) to the item's current location in the log.
//
// Buckets have their own spin locks, like memcached's fine-grained bucket locks. Entries also
// hold each item's ranking metadata (last access and access count), so that GETs never write to
// the log and the cleaner can copy items without coordinating with readers.
#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string_view>
#include <vector>

#include "memshare/config.h"
#include "memshare/log.h"
#include "memshare/util.h"

namespace memshare {

struct AccessInfo {
    uint64_t lastSeq = 0;   // global access sequence number of the last access (LRU order)
    uint32_t lastTime = 0;  // clock second of the last access (idle detection)
    uint32_t freq = 0;      // number of accesses
};

class HashTable {
public:
    explicit HashTable(uint64_t buckets);

    static uint64_t bucketHash(AppId app, uint64_t keyHash) {
        return mix64(keyHash ^ (uint64_t(app) * 0x9e3779b97f4a7c15ULL));
    }

    // Returns the item and records an access, or nullptr if absent.
    const ItemHeader* lookup(AppId app, std::string_view key, uint64_t keyHash, uint64_t seq,
                             uint32_t now);

    // Points (app, key) at `item`. Returns the item it replaced, or nullptr.
    const ItemHeader* upsert(AppId app, std::string_view key, uint64_t keyHash,
                             const ItemHeader* item, uint64_t seq, uint32_t now);

    // Removes (app, key). Returns the removed item, or nullptr.
    const ItemHeader* erase(AppId app, std::string_view key, uint64_t keyHash);

    // Removes the entry only if it still points at `item` (it may have expired).
    bool eraseIf(const ItemHeader* item, uint64_t keyHash);

    // ---- Cleaner interface: entries are identified by item address ----

    // If `item` is the live copy of its key, fills `info` and returns true.
    bool liveInfo(const ItemHeader* item, uint64_t keyHash, AccessInfo* info);
    // If the entry still points at `from`, repoints it at `to` and returns true.
    bool relocate(const ItemHeader* from, const ItemHeader* to, uint64_t keyHash);

    uint64_t numBuckets() const { return mask_ + 1; }

    // Calls fn(item, info) for every entry in buckets [begin, end), one bucket lock at a time.
    template <typename F>
    void forEachInBuckets(uint64_t begin, uint64_t end, F&& fn) {
        for (uint64_t b = begin; b < end; b++) {
            Bucket& bk = buckets_[b];
            std::lock_guard<SpinLock> g(bk.lock);
            for (const Entry& e : bk.entries) fn(e.item, e.info);
        }
    }

private:
    struct Entry {
        uint64_t hash;
        const ItemHeader* item;
        AccessInfo info;
    };
    struct alignas(64) Bucket {
        SpinLock lock;
        std::vector<Entry> entries;
    };

    Bucket& bucketFor(uint64_t h) { return buckets_[h & mask_]; }
    static bool matches(const Entry& e, uint64_t h, AppId app, std::string_view key) {
        return e.hash == h && e.item->app == app && e.item->keyLen == key.size() &&
               std::memcmp(e.item->key(), key.data(), key.size()) == 0;
    }

    uint64_t mask_;
    std::unique_ptr<Bucket[]> buckets_;
};

}  // namespace memshare
