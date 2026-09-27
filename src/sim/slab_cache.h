// Baseline for comparison: memcached's slab allocator with per-application static partitions, as
// run by Memcachier (paper §2). Each app gets a fixed number of 1 MB slabs. A slab is assigned to
// a size class (64 B * 2^i) the first time that class needs space and is never reassigned; each
// class evicts in LRU order on its own.
#pragma once

#include <cstdint>
#include <list>
#include <string>
#include <unordered_map>
#include <vector>

namespace sim {

class SlabCache {
public:
    // itemOverhead: per-item header bytes memcached stores alongside key and value.
    SlabCache(const std::vector<uint64_t>& appBytes, uint32_t slabBytes = 1 << 20,
              uint32_t itemOverhead = 48);

    bool get(uint32_t app, const std::string& key);
    void set(uint32_t app, const std::string& key, uint32_t valueBytes);
    void del(uint32_t app, const std::string& key);

    uint64_t usedBytes(uint32_t app) const;  // bytes of items (not chunks) stored

private:
    struct Class {
        uint32_t chunkBytes;
        uint64_t capacity = 0;  // chunks in assigned slabs
        std::list<std::string> lru;  // front = most recent
    };
    struct Entry {
        uint32_t cls;
        uint32_t bytes;
        std::list<std::string>::iterator pos;
    };
    struct App {
        uint64_t maxSlabs;
        uint64_t slabs = 0;
        uint64_t used = 0;
        std::vector<Class> classes;
        std::unordered_map<std::string, Entry> index;
    };

    void remove(App& a, std::unordered_map<std::string, Entry>::iterator it);

    uint32_t slabBytes_;
    uint32_t overhead_;
    std::vector<App> apps_;
};

}  // namespace sim
