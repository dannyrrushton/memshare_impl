// Memshare: a multi-tenant key-value cache that shares memory dynamically between applications
// (Cidon, Rushton, Rumble, Stutsman, USENIX ATC 2017).
//
//                      +-> HashTable (key -> item in log, access info)
//   get/set/delete ----+-> Log (segments; writers append at the head)
//                      +-> Arbiter (per-app actualMem/targetMem/need, shadow queues)
//                               ^
//                      Cleaner -+ (evicts by need and rank, compacts segments)
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "memshare/arbiter.h"
#include "memshare/cleaner.h"
#include "memshare/config.h"
#include "memshare/epoch.h"
#include "memshare/hash_table.h"
#include "memshare/log.h"

namespace memshare {

enum class SetResult { Stored, TooLarge, NoMemory };

struct CacheStats {
    std::vector<AppStats> apps;
    CleanerStats cleaner;
    uint32_t segmentBytes = 0;
    uint32_t totalSegments = 0, freeSegments = 0, closedSegments = 0, reserveSegments = 0;
    uint64_t itemCapacity = 0;
};

class Cache {
public:
    static constexpr uint32_t kMaxKeyBytes = 65535;

    explicit Cache(Config cfg);
    ~Cache();
    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;

    uint32_t numApps() const { return arbiter_->numApps(); }
    std::optional<AppId> findApp(std::string_view name) const;

    // Looks up key for app. On a hit copies the value (and flags) out and returns true.
    bool get(AppId app, std::string_view key, std::string* value, uint32_t* flags = nullptr);
    // Stores key for app. exptime is seconds from now; 0 means never expire.
    SetResult set(AppId app, std::string_view key, std::string_view value, uint32_t flags = 0,
                  uint32_t exptime = 0);
    bool del(AppId app, std::string_view key);

    // Runs one cleaning pass on the calling thread. Returns false if nothing could be cleaned.
    bool cleanOnce();
    // Recomputes each application's idle memory and, under the idle-tax policy, its target.
    // Background mode does this every second; otherwise the caller drives it.
    void updateIdleTax();

    CacheStats stats() const;
    const Arbiter& arbiter() const { return *arbiter_; }

private:
    uint32_t now() const;
    bool needsCleaning() const;
    // Called when a writer cannot get space. Returns false if no space can be made.
    bool makeRoom();
    void backgroundLoop(uint64_t seed);
    void maintenanceLoop();
    void dropItem(const ItemHeader* item);

    Config cfg_;
    std::chrono::steady_clock::time_point start_;
    EpochManager epochs_;
    std::unique_ptr<Log> log_;
    std::unique_ptr<HashTable> table_;
    std::unique_ptr<Arbiter> arbiter_;
    std::unique_ptr<Cleaner> cleaner_;
    uint32_t lowWater_ = 1;
    std::atomic<uint64_t> seq_{1};

    // Inline cleaning (backgroundCleaner = false).
    std::mutex inlineMu_;
    std::mt19937_64 inlineRng_;

    // Background threads.
    std::atomic<bool> stop_{false};
    std::mutex wakeMu_;
    std::condition_variable wakeCv_;
    std::vector<std::thread> threads_;
};

}  // namespace memshare
