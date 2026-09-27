#include "memshare/hash_table.h"

#include <mutex>

namespace memshare {

HashTable::HashTable(uint64_t buckets) {
    uint64_t n = 1;
    while (n < buckets) n <<= 1;
    mask_ = n - 1;
    buckets_.reset(new Bucket[n]);
}

const ItemHeader* HashTable::lookup(AppId app, std::string_view key, uint64_t keyHash,
                                    uint64_t seq, uint32_t now) {
    uint64_t h = bucketHash(app, keyHash);
    Bucket& b = bucketFor(h);
    std::lock_guard<SpinLock> g(b.lock);
    for (Entry& e : b.entries) {
        if (matches(e, h, app, key)) {
            e.info.lastSeq = seq;
            e.info.lastTime = now;
            e.info.freq++;
            return e.item;
        }
    }
    return nullptr;
}

const ItemHeader* HashTable::upsert(AppId app, std::string_view key, uint64_t keyHash,
                                    const ItemHeader* item, uint64_t seq, uint32_t now) {
    uint64_t h = bucketHash(app, keyHash);
    Bucket& b = bucketFor(h);
    std::lock_guard<SpinLock> g(b.lock);
    for (Entry& e : b.entries) {
        if (matches(e, h, app, key)) {
            const ItemHeader* old = e.item;
            e.item = item;
            e.info.lastSeq = seq;
            e.info.lastTime = now;
            e.info.freq++;
            return old;
        }
    }
    b.entries.push_back(Entry{h, item, AccessInfo{seq, now, 1}});
    return nullptr;
}

const ItemHeader* HashTable::erase(AppId app, std::string_view key, uint64_t keyHash) {
    uint64_t h = bucketHash(app, keyHash);
    Bucket& b = bucketFor(h);
    std::lock_guard<SpinLock> g(b.lock);
    for (Entry& e : b.entries) {
        if (matches(e, h, app, key)) {
            const ItemHeader* old = e.item;
            e = b.entries.back();
            b.entries.pop_back();
            return old;
        }
    }
    return nullptr;
}

bool HashTable::eraseIf(const ItemHeader* item, uint64_t keyHash) {
    Bucket& b = bucketFor(bucketHash(item->app, keyHash));
    std::lock_guard<SpinLock> g(b.lock);
    for (Entry& e : b.entries) {
        if (e.item == item) {
            e = b.entries.back();
            b.entries.pop_back();
            return true;
        }
    }
    return false;
}

bool HashTable::liveInfo(const ItemHeader* item, uint64_t keyHash, AccessInfo* info) {
    Bucket& b = bucketFor(bucketHash(item->app, keyHash));
    std::lock_guard<SpinLock> g(b.lock);
    for (const Entry& e : b.entries) {
        if (e.item == item) {
            *info = e.info;
            return true;
        }
    }
    return false;
}

bool HashTable::relocate(const ItemHeader* from, const ItemHeader* to, uint64_t keyHash) {
    Bucket& b = bucketFor(bucketHash(from->app, keyHash));
    std::lock_guard<SpinLock> g(b.lock);
    for (Entry& e : b.entries) {
        if (e.item == from) {
            e.item = to;
            return true;
        }
    }
    return false;
}

}  // namespace memshare
