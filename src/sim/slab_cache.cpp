#include "sim/slab_cache.h"

namespace sim {

SlabCache::SlabCache(const std::vector<uint64_t>& appBytes, uint32_t slabBytes, uint32_t itemOverhead)
    : slabBytes_(slabBytes), overhead_(itemOverhead) {
    for (uint64_t bytes : appBytes) {
        App a;
        a.maxSlabs = bytes / slabBytes;
        for (uint32_t c = 64; c <= slabBytes; c *= 2) a.classes.push_back(Class{c, 0, {}});
        apps_.push_back(std::move(a));
    }
}

void SlabCache::remove(App& a, std::unordered_map<std::string, Entry>::iterator it) {
    a.classes[it->second.cls].lru.erase(it->second.pos);
    a.used -= it->second.bytes;
    a.index.erase(it);
}

bool SlabCache::get(uint32_t app, const std::string& key) {
    App& a = apps_[app];
    auto it = a.index.find(key);
    if (it == a.index.end()) return false;
    Class& c = a.classes[it->second.cls];
    c.lru.splice(c.lru.begin(), c.lru, it->second.pos);
    return true;
}

void SlabCache::set(uint32_t app, const std::string& key, uint32_t valueBytes) {
    App& a = apps_[app];
    auto it = a.index.find(key);
    if (it != a.index.end()) remove(a, it);

    uint32_t bytes = static_cast<uint32_t>(key.size()) + valueBytes + overhead_;
    uint32_t cls = 0;
    while (cls < a.classes.size() && a.classes[cls].chunkBytes < bytes) cls++;
    if (cls == a.classes.size()) return;  // larger than a slab
    Class& c = a.classes[cls];

    if (c.lru.size() >= c.capacity) {
        if (a.slabs < a.maxSlabs) {
            a.slabs++;
            c.capacity += slabBytes_ / c.chunkBytes;
        } else if (!c.lru.empty()) {
            remove(a, a.index.find(c.lru.back()));
        } else {
            return;  // no slab for this class and none left to assign: memcached fails the store
        }
    }
    c.lru.push_front(key);
    a.index.emplace(key, Entry{cls, bytes, c.lru.begin()});
    a.used += bytes;
}

void SlabCache::del(uint32_t app, const std::string& key) {
    App& a = apps_[app];
    auto it = a.index.find(key);
    if (it != a.index.end()) remove(a, it);
}

uint64_t SlabCache::usedBytes(uint32_t app) const { return apps_[app].used; }

}  // namespace sim
