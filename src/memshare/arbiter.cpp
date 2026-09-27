#include "memshare/arbiter.h"

#include <algorithm>
#include <mutex>
#include <queue>
#include <stdexcept>

namespace memshare {

RankKey rankItem(RankPolicy policy, const AccessInfo& info) {
    switch (policy) {
        case RankPolicy::LRU: return {info.lastSeq, 0};
        case RankPolicy::LFU: return {info.freq, info.lastSeq};
        case RankPolicy::SegmentedLRU: return {info.freq > 1 ? 1u : 0u, info.lastSeq};
    }
    return {info.lastSeq, 0};
}

Arbiter::Arbiter(const Config& cfg, uint64_t itemCapacity)
    : policy_(cfg.policy), taxRate_(cfg.taxRate), itemCapacity_(itemCapacity), rng_(cfg.seed) {
    if (cfg.apps.empty()) throw std::invalid_argument("at least one application is required");
    if (taxRate_ < 0 || taxRate_ > 1) throw std::invalid_argument("taxRate must be in [0, 1]");
    uint64_t reserved = 0;
    for (const auto& a : cfg.apps) {
        reserved += a.reservedBytes;
        apps_.push_back(std::make_unique<AppState>(a));
    }
    if (reserved > itemCapacity)
        throw std::invalid_argument("reserved memory exceeds the cache's item capacity");

    // Each application starts with an equal portion of the pooled memory.
    uint64_t pooled = itemCapacity - reserved;
    uint64_t share = pooled / apps_.size();
    for (size_t i = 0; i < apps_.size(); i++) apps_[i]->pooled = static_cast<int64_t>(share);
    apps_[0]->pooled += static_cast<int64_t>(pooled - share * apps_.size());
    std::lock_guard<SpinLock> g(poolLock_);
    recomputeTargetsLocked();
}

void Arbiter::recomputeTargetsLocked() {
    for (auto& ap : apps_) {
        int64_t base = static_cast<int64_t>(ap->cfg.reservedBytes) + ap->pooled;
        int64_t target = base;
        if (policy_ == SharingPolicy::IdleTax) {
            // Algorithm 3: targetMem = reservedMem / tau, with
            // tau = (1 - activeFraction * taxRate) / (1 - taxRate).
            int64_t actual = ap->actual.load(std::memory_order_relaxed);
            double activeFraction =
                actual > 0 ? 1.0 - double(std::min(ap->idle, actual)) / double(actual) : 1.0;
            if (taxRate_ >= 1.0) {
                // Limit of the formula as taxRate -> 1: all idle memory can be reassigned.
                target = activeFraction >= 1.0 ? base : 0;
            } else {
                double tau = (1.0 - activeFraction * taxRate_) / (1.0 - taxRate_);
                target = static_cast<int64_t>(double(base) / tau);
            }
        }
        ap->target.store(target, std::memory_order_relaxed);
    }
}

void Arbiter::onMiss(AppId a, uint64_t keyHash) {
    AppState& app = *apps_[a];
    app.gets.fetch_add(1, std::memory_order_relaxed);
    app.misses.fetch_add(1, std::memory_order_relaxed);

    if (!app.shadowLock.try_lock()) return;
    bool shadowHit = app.shadow.take(keyHash);
    app.shadowLock.unlock();
    if (!shadowHit) return;
    app.shadowHits.fetch_add(1, std::memory_order_relaxed);
    if (policy_ != SharingPolicy::Pooled) return;

    // Algorithm 2: take one credit of pooled memory from a random app that has at least a
    // credit's worth, and give it to this one.
    int64_t credit = static_cast<int64_t>(app.cfg.creditBytes);
    std::lock_guard<SpinLock> g(poolLock_);
    std::vector<AppState*> candidates;
    for (auto& other : apps_) {
        if (other.get() != &app && other->pooled >= credit) candidates.push_back(other.get());
    }
    if (candidates.empty()) return;
    AppState* donor =
        candidates[std::uniform_int_distribution<size_t>(0, candidates.size() - 1)(rng_)];
    donor->pooled -= credit;
    app.pooled += credit;
    recomputeTargetsLocked();
}

std::vector<double> Arbiter::cleaningWeights() const {
    std::vector<double> w(apps_.size());
    for (size_t a = 0; a < apps_.size(); a++) {
        double n = need(static_cast<AppId>(a));
        w[a] = 1.0 / std::max(n, 1e-6);
    }
    return w;
}

std::vector<size_t> Arbiter::chooseSurvivors(std::vector<CleanCandidate>& items,
                                             uint64_t budgetBytes) const {
    uint32_t numApps = this->numApps();
    // Each app's candidates, best rank first.
    std::vector<std::vector<CleanCandidate*>> byApp(numApps);
    std::vector<int64_t> localActual(numApps);
    std::vector<int64_t> inPass(numApps, 0);
    for (auto& c : items) {
        byApp[c.app].push_back(&c);
        inPass[c.app] += c.bytes;
    }
    // The items being cleaned no longer count towards actualMem until they are relocated.
    for (uint32_t a = 0; a < numApps; a++)
        localActual[a] = std::max<int64_t>(0, actual(a) - inPass[a]);

    using HeapEntry = std::pair<double, AppId>;
    std::priority_queue<HeapEntry> heap;  // max-need first
    std::vector<size_t> next(numApps, 0);
    std::vector<int64_t> targets(numApps);
    for (uint32_t a = 0; a < numApps; a++) {
        targets[a] = target(a);
        if (byApp[a].empty()) continue;
        std::sort(byApp[a].begin(), byApp[a].end(),
                  [](const CleanCandidate* x, const CleanCandidate* y) { return y->rank < x->rank; });
        heap.emplace(need(targets[a], localActual[a]), a);
    }

    std::vector<size_t> order;
    uint64_t relocated = 0;
    while (!heap.empty()) {
        AppId a = heap.top().second;
        heap.pop();
        CleanCandidate* c = byApp[a][next[a]++];
        if (relocated + c->bytes > budgetBytes) break;
        c->keep = true;
        order.push_back(static_cast<size_t>(c - items.data()));
        relocated += c->bytes;
        localActual[a] += c->bytes;
        if (next[a] < byApp[a].size()) heap.emplace(need(targets[a], localActual[a]), a);
    }
    return order;
}

void Arbiter::recordEvictions(AppId a, const std::vector<std::pair<uint64_t, uint32_t>>& evicted,
                              int64_t bytes) {
    AppState& app = *apps_[a];
    app.actual.fetch_sub(bytes, std::memory_order_relaxed);
    app.evictedItems.fetch_add(evicted.size(), std::memory_order_relaxed);
    app.evictedBytes.fetch_add(static_cast<uint64_t>(bytes), std::memory_order_relaxed);
    std::lock_guard<SpinLock> g(app.shadowLock);
    for (const auto& [hash, size] : evicted) app.shadow.insert(hash, size);
}

void Arbiter::updateIdle(const std::vector<int64_t>& idleBytes) {
    std::lock_guard<SpinLock> g(poolLock_);
    for (size_t a = 0; a < apps_.size() && a < idleBytes.size(); a++) apps_[a]->idle = idleBytes[a];
    recomputeTargetsLocked();
}

AppStats Arbiter::stats(AppId a) const {
    const AppState& app = *apps_[a];
    AppStats s;
    s.name = app.cfg.name;
    s.gets = app.gets.load();
    s.hits = app.hits.load();
    s.misses = app.misses.load();
    s.sets = app.sets.load();
    s.shadowHits = app.shadowHits.load();
    s.evictedItems = app.evictedItems.load();
    s.evictedBytes = app.evictedBytes.load();
    s.actualBytes = app.actual.load();
    s.targetBytes = app.target.load();
    s.reservedBytes = static_cast<int64_t>(app.cfg.reservedBytes);
    {
        std::lock_guard<SpinLock> g(poolLock_);
        s.pooledBytes = app.pooled;
        s.idleBytes = app.idle;
    }
    s.need = need(s.targetBytes, s.actualBytes);
    return s;
}

}  // namespace memshare
