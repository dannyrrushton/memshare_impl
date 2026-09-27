// Configuration for a Memshare cache instance.
#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace memshare {

using AppId = uint32_t;

// Per-application eviction policy, expressed as a ranking function over an item's last-access
// time t and access count f (§3.1). Higher rank = more worth keeping.
enum class RankPolicy {
    LRU,           // rank(t) = t
    LFU,           // rank(f) = f, ties broken by t
    SegmentedLRU,  // items accessed more than once rank above all items accessed once, then by t
};

// How the arbiter sets each application's targetMem (§4).
enum class SharingPolicy {
    Static,   // targetMem = reservedMem + an equal share of pooled memory, never changes
    Pooled,   // pooled memory moves between apps in credits on shadow-queue hits (Algorithm 2)
    IdleTax,  // reserved memory that sits idle is taxed and made available to others (Algorithm 3)
};

struct AppConfig {
    std::string name;
    // Memory the app is guaranteed. The rest of the cache is pooled.
    uint64_t reservedBytes = 0;
    // Pooled memory moved to this app on each shadow-queue hit (§4.1 allocation priority).
    uint64_t creditBytes = 64 << 10;
    RankPolicy rank = RankPolicy::LRU;
    // Item bytes represented by this app's shadow queue of evicted keys.
    uint64_t shadowQueueBytes = 10 << 20;
};

struct Config {
    // Total DRAM given to the log.
    uint64_t totalBytes = 64ull << 20;
    // Segment size; also the largest item accepted.
    uint32_t segmentBytes = 1 << 20;
    // n: segments examined per cleaning pass, which writes survivors into at most n-1 segments.
    uint32_t cleanSegments = 100;
    // Fraction of the n candidate segments chosen at random; the rest are chosen by the most
    // data from applications with the lowest need.
    double randomSegmentFraction = 0.5;
    // The cleaner runs while fewer than this fraction of segments are free.
    double freeFraction = 0.01;
    // Background cleaner threads. With backgroundCleaner = false, writers clean inline when memory
    // runs low; this is deterministic and is what the trace simulator uses.
    bool backgroundCleaner = true;
    uint32_t cleanerThreads = 1;

    SharingPolicy policy = SharingPolicy::Pooled;
    // Idle-tax parameters (Algorithm 3).
    double taxRate = 0.5;
    double idleSeconds = 5 * 3600;

    // Wall-clock seconds, used for item idle times and expiry. The simulator substitutes trace
    // time. Defaults to a steady clock.
    std::function<double()> clock;
    uint64_t seed = 42;
    // Hash table buckets (rounded up to a power of two). 0 picks a size from totalBytes.
    uint64_t hashBuckets = 0;

    std::vector<AppConfig> apps;
};

}  // namespace memshare
