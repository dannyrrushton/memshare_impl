// The arbiter (paper §3.1, §4, §5.2): per-application memory accounting, shadow queues, the
// sharing policy that sets each application's targetMem, and the ranking decisions the cleaner
// asks for.
#pragma once

#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <vector>

#include "memshare/config.h"
#include "memshare/hash_table.h"
#include "memshare/shadow_queue.h"
#include "memshare/util.h"

namespace memshare {

// Result of an application's ranking function. Compared lexicographically; higher is kept.
struct RankKey {
    uint64_t major = 0;
    uint64_t minor = 0;
    bool operator<(const RankKey& o) const {
        return major != o.major ? major < o.major : minor < o.minor;
    }
};

RankKey rankItem(RankPolicy policy, const AccessInfo& info);

// A live item in a segment being cleaned.
struct CleanCandidate {
    const ItemHeader* item;
    uint64_t keyHash;
    uint32_t bytes;
    AppId app;
    uint32_t segment;  // index into the pass's candidate segment list
    RankKey rank;
    bool keep = false;
};

struct AppStats {
    std::string name;
    uint64_t gets = 0, hits = 0, misses = 0, sets = 0;
    uint64_t shadowHits = 0;
    uint64_t evictedItems = 0, evictedBytes = 0;
    int64_t actualBytes = 0, targetBytes = 0, reservedBytes = 0, pooledBytes = 0, idleBytes = 0;
    double need = 0;
};

class Arbiter {
public:
    // itemCapacity: bytes of log space available to items (excluding the cleaner's reserve and
    // the free-segment headroom).
    Arbiter(const Config& cfg, uint64_t itemCapacity);

    uint32_t numApps() const { return static_cast<uint32_t>(apps_.size()); }
    const AppConfig& appConfig(AppId a) const { return apps_[a]->cfg; }
    uint64_t itemCapacity() const { return itemCapacity_; }

    // need(app) = targetMem / actualMem. Above 1 the app deserves more memory; below 1 it holds
    // more than its share.
    static double need(int64_t target, int64_t actual) {
        if (actual <= 0) return target > 0 ? std::numeric_limits<double>::infinity() : 0.0;
        return double(target) / double(actual);
    }
    double need(AppId a) const { return need(target(a), actual(a)); }
    int64_t actual(AppId a) const { return apps_[a]->actual.load(std::memory_order_relaxed); }
    int64_t target(AppId a) const { return apps_[a]->target.load(std::memory_order_relaxed); }

    // ---- Request path ----
    void onSet(AppId a, int64_t bytesAdded) {
        apps_[a]->actual.fetch_add(bytesAdded, std::memory_order_relaxed);
        apps_[a]->sets.fetch_add(1, std::memory_order_relaxed);
    }
    void onRemove(AppId a, int64_t bytes) {
        apps_[a]->actual.fetch_sub(bytes, std::memory_order_relaxed);
    }
    void onHit(AppId a) {
        apps_[a]->gets.fetch_add(1, std::memory_order_relaxed);
        apps_[a]->hits.fetch_add(1, std::memory_order_relaxed);
    }
    // GET miss: checks the shadow queue and, under the pooled policy, moves a credit of pooled
    // memory to this app (Algorithm 2). Uses try_lock so a cleaner filling the queue never
    // blocks a request; a busy queue just means the check is skipped.
    void onMiss(AppId a, uint64_t keyHash);

    // ---- Cleaner interface ----

    // Per-app weights for choosing segments: higher for apps with lower need.
    std::vector<double> cleaningWeights() const;
    RankKey rank(AppId a, const AccessInfo& info) const { return rankItem(apps_[a]->cfg.rank, info); }
    // Algorithm 1: decides which candidates survive a cleaning pass. Repeatedly takes the
    // application with the highest need and keeps its highest-ranked remaining item, until the
    // next item does not fit in budgetBytes. Sets `keep` on the survivors and returns their
    // indices in the order they were chosen, most important first.
    std::vector<size_t> chooseSurvivors(std::vector<CleanCandidate>& items,
                                        uint64_t budgetBytes) const;
    // Installs the result of a pass with one lock acquisition per app: subtracts evicted bytes
    // and pushes evicted keys (lowest rank first) into the shadow queue.
    void recordEvictions(AppId a, const std::vector<std::pair<uint64_t, uint32_t>>& evicted,
                         int64_t bytes);

    // ---- Idle tax (Algorithm 3) ----
    // idleBytes[a]: bytes of app a's items not accessed within idleSeconds.
    void updateIdle(const std::vector<int64_t>& idleBytes);

    AppStats stats(AppId a) const;

private:
    struct AppState {
        AppConfig cfg;
        std::atomic<int64_t> actual{0};
        std::atomic<int64_t> target{0};
        int64_t pooled = 0;  // guarded by poolLock_
        int64_t idle = 0;    // guarded by poolLock_
        SpinLock shadowLock;
        ShadowQueue shadow;
        std::atomic<uint64_t> gets{0}, hits{0}, misses{0}, sets{0}, shadowHits{0};
        std::atomic<uint64_t> evictedItems{0}, evictedBytes{0};
        explicit AppState(const AppConfig& c) : cfg(c), shadow(c.shadowQueueBytes) {}
    };

    void recomputeTargetsLocked();

    SharingPolicy policy_;
    double taxRate_;
    uint64_t itemCapacity_;
    std::vector<std::unique_ptr<AppState>> apps_;
    mutable SpinLock poolLock_;
    std::mt19937_64 rng_;  // guarded by poolLock_
};

}  // namespace memshare
