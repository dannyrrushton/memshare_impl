#include "memshare/cleaner.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <thread>

namespace memshare {

Cleaner::Cleaner(Log& log, HashTable& table, Arbiter& arbiter, EpochManager& epochs,
                 uint32_t segmentsPerPass, double randomFraction)
    : log_(log),
      table_(table),
      arbiter_(arbiter),
      epochs_(epochs),
      n_(std::max<uint32_t>(1, segmentsPerPass)),
      randomFraction_(randomFraction) {}

Segment* Cleaner::acquireOutput() {
    for (;;) {
        if (Segment* s = log_.takeFreeForCleaner()) return s;
        // Free segments exist but requests from before they were cleaned are still running;
        // those finish in microseconds.
        if (log_.freeSegments() == 0) return nullptr;
        std::this_thread::yield();
    }
}

bool Cleaner::cleanPass(std::mt19937_64& rng, uint32_t now) {
    auto start = std::chrono::steady_clock::now();

    // Choose segments: some at random, the rest holding the most data from low-need apps.
    // Dead space is reclaimed for free, so it counts double the most over-provisioned app.
    std::vector<double> weights = arbiter_.cleaningWeights();
    double deadWeight = 2.0 * *std::max_element(weights.begin(), weights.end());
    std::vector<Segment*> segs = log_.takeCandidates(n_, randomFraction_, weights, deadWeight, rng);
    if (segs.empty()) return false;

    uint32_t numApps = arbiter_.numApps();
    std::vector<std::vector<std::pair<uint64_t, uint32_t>>> evicted(numApps);
    std::vector<std::vector<RankKey>> evictedRank(numApps);
    std::vector<int64_t> evictedBytes(numApps, 0);
    uint64_t itemsEvicted = 0;

    auto evict = [&](const CleanCandidate& c) {
        bool removed;
        {
            EpochGuard g(epochs_);
            removed = table_.eraseIf(c.item, c.keyHash);
        }
        if (!removed) return;  // overwritten or deleted meanwhile
        evicted[c.app].emplace_back(c.keyHash, c.bytes);
        evictedRank[c.app].push_back(c.rank);
        evictedBytes[c.app] += c.bytes;
        itemsEvicted++;
    };

    // 1. Find the live items: those the hash table still points at.
    std::vector<CleanCandidate> items;
    std::vector<CleanCandidate> expired;
    for (uint32_t i = 0; i < segs.size(); i++) {
        Segment* s = segs[i];
        uint32_t used = s->used.load(std::memory_order_acquire);
        EpochGuard g(epochs_);
        for (uint32_t off = 0; off < used;) {
            const auto* h = reinterpret_cast<const ItemHeader*>(s->data + off);
            uint32_t bytes = itemBytes(h);
            uint64_t keyHash = hashKey(std::string_view(h->key(), h->keyLen));
            AccessInfo info;
            if (table_.liveInfo(h, keyHash, &info)) {
                CleanCandidate c{h, keyHash, bytes, h->app, i, arbiter_.rank(h->app, info)};
                if (h->expiry != 0 && h->expiry <= now) expired.push_back(c);
                else items.push_back(c);
            }
            off += bytes;
        }
    }

    // Expired items are simply dropped; they were not evicted by the policy, so they do not go
    // into the shadow queues.
    for (const auto& c : expired) {
        bool removed;
        {
            EpochGuard g(epochs_);
            removed = table_.eraseIf(c.item, c.keyHash);
        }
        if (removed) arbiter_.onRemove(c.app, c.bytes);
    }

    // 2. Algorithm 1: the arbiter picks the survivors that fit in n-1 segments.
    uint32_t maxOutputs = static_cast<uint32_t>(segs.size() - 1);
    uint64_t budget = uint64_t(maxOutputs) * log_.segmentBytes();
    std::vector<size_t> keepOrder = arbiter_.chooseSurvivors(items, budget);

    // Items cannot straddle segments, so the survivors may need slightly more than their byte
    // total. Simulate the packing step 4 will do and, while it overflows, give up the survivors
    // chosen last (the least important) rather than whatever happens to be copied last.
    for (;;) {
        uint32_t outs = 0;
        uint64_t outUsed = 0, overflow = 0;
        for (const auto& c : items) {
            if (!c.keep) continue;
            if (outs == 0 || outUsed + c.bytes > log_.segmentBytes()) {
                outs++;
                outUsed = 0;
            }
            outUsed += c.bytes;
            if (outs > maxOutputs) overflow += c.bytes;
        }
        if (overflow == 0) break;
        for (uint64_t dropped = 0; dropped < overflow && !keepOrder.empty(); keepOrder.pop_back()) {
            items[keepOrder.back()].keep = false;
            dropped += items[keepOrder.back()].bytes;
        }
    }

    // 3. Evict everything else.
    for (const auto& c : items) {
        if (!c.keep) evict(c);
    }

    // 4. Copy the survivors out, one source segment at a time, so each source segment can be
    // freed (and reused as an output) as soon as its survivors are safe. Which items survive is
    // decided entirely by step 2; only their physical order differs from rank order.
    uint32_t outputs = 0;
    Segment* out = nullptr;
    uint32_t outUsed = 0;
    uint64_t itemsRelocated = 0, bytesRelocated = 0;
    size_t k = 0;
    for (uint32_t i = 0; i < segs.size(); i++) {
        for (; k < items.size() && items[k].segment == i; k++) {
            const CleanCandidate& c = items[k];
            if (!c.keep) continue;
            if (out == nullptr || outUsed + c.bytes > log_.segmentBytes()) {
                if (out != nullptr) log_.addClosed(out);
                out = nullptr;
                // Step 2 guarantees the survivors fit; this only fails if the free list is
                // somehow empty, in which case the item is evicted instead.
                if (outputs < maxOutputs) out = acquireOutput();
                if (out == nullptr) {
                    evict(c);
                    continue;
                }
                outputs++;
                outUsed = 0;
            }
            auto* dst = reinterpret_cast<ItemHeader*>(out->data + outUsed);
            std::memcpy(dst, c.item, c.bytes);
            outUsed += c.bytes;
            out->used.store(outUsed, std::memory_order_release);
            bool moved;
            {
                EpochGuard g(epochs_);
                moved = table_.relocate(c.item, dst, c.keyHash);
            }
            if (moved) {
                out->live[c.app].fetch_add(c.bytes, std::memory_order_relaxed);
                itemsRelocated++;
                bytesRelocated += c.bytes;
            }
        }
        log_.release(segs[i]);
    }
    if (out != nullptr) log_.addClosed(out);

    // 5. Install evictions in the arbiter with one lock acquisition per app, lowest rank first
    // so the most valuable evicted keys are the most recent in the shadow queue.
    uint64_t bytesEvicted = 0;
    for (uint32_t a = 0; a < numApps; a++) {
        if (evicted[a].empty()) continue;
        std::vector<size_t> order(evicted[a].size());
        for (size_t j = 0; j < order.size(); j++) order[j] = j;
        std::sort(order.begin(), order.end(),
                  [&](size_t x, size_t y) { return evictedRank[a][x] < evictedRank[a][y]; });
        std::vector<std::pair<uint64_t, uint32_t>> sorted;
        sorted.reserve(order.size());
        for (size_t j : order) sorted.push_back(evicted[a][j]);
        arbiter_.recordEvictions(a, sorted, evictedBytes[a]);
        bytesEvicted += static_cast<uint64_t>(evictedBytes[a]);
    }

    passes_.fetch_add(1, std::memory_order_relaxed);
    segmentsCleaned_.fetch_add(segs.size(), std::memory_order_relaxed);
    itemsRelocated_.fetch_add(itemsRelocated, std::memory_order_relaxed);
    bytesRelocated_.fetch_add(bytesRelocated, std::memory_order_relaxed);
    itemsEvicted_.fetch_add(itemsEvicted, std::memory_order_relaxed);
    bytesEvicted_.fetch_add(bytesEvicted, std::memory_order_relaxed);
    nanos_.fetch_add(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                               std::chrono::steady_clock::now() - start)
                                               .count()),
                     std::memory_order_relaxed);
    return true;
}

CleanerStats Cleaner::stats() const {
    CleanerStats s;
    s.passes = passes_.load();
    s.segmentsCleaned = segmentsCleaned_.load();
    s.itemsRelocated = itemsRelocated_.load();
    s.bytesRelocated = bytesRelocated_.load();
    s.itemsEvicted = itemsEvicted_.load();
    s.bytesEvicted = bytesEvicted_.load();
    s.seconds = double(nanos_.load()) * 1e-9;
    return s;
}

}  // namespace memshare
