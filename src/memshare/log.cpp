#include "memshare/log.h"

#include <algorithm>
#include <new>
#include <stdexcept>

namespace memshare {

Log::Log(uint64_t totalBytes, uint32_t segmentBytes, uint32_t numApps, uint32_t reserveSegments,
         EpochManager& epochs)
    : segBytes_(segmentBytes), numApps_(numApps), reserve_(reserveSegments), epochs_(epochs) {
    if (segmentBytes < 4096 || segmentBytes % 8 != 0)
        throw std::invalid_argument("segment size must be a multiple of 8 and at least 4 KB");
    uint64_t n = totalBytes / segmentBytes;
    if (n < uint64_t(reserveSegments) + 2)
        throw std::invalid_argument("cache too small for its segment size and cleaner reserve");

    arenaBytes_ = static_cast<size_t>(n) * segmentBytes;
    arena_ = static_cast<uint8_t*>(::operator new(arenaBytes_, std::align_val_t(4096)));
    segments_.reserve(n);
    for (uint64_t i = 0; i < n; i++) {
        auto s = std::make_unique<Segment>();
        s->id = static_cast<uint32_t>(i);
        s->data = arena_ + i * segmentBytes;
        s->live.reset(new std::atomic<int64_t>[numApps]);
        for (uint32_t a = 0; a < numApps; a++) s->live[a].store(0, std::memory_order_relaxed);
        segments_.push_back(std::move(s));
    }
    // Hand out low-numbered segments first.
    for (size_t i = segments_.size(); i-- > 0;) free_.push_back(segments_[i].get());
    freeCount_.store(static_cast<uint32_t>(free_.size()));
}

Log::~Log() { ::operator delete(arena_, std::align_val_t(4096)); }

void Log::resetSegment(Segment* seg) {
    seg->used.store(0, std::memory_order_relaxed);
    seg->pendingWriters.store(0, std::memory_order_relaxed);
    for (uint32_t a = 0; a < numApps_; a++) seg->live[a].store(0, std::memory_order_relaxed);
}

Segment* Log::popSafeFreeLocked() {
    // Segments released longest ago sit at the front and are the most likely to be safe, but
    // popping from the back is O(1); try the back first, then scan.
    for (size_t i = free_.size(); i-- > 0;) {
        Segment* s = free_[i];
        if (epochs_.safeToReuse(s->retireEpoch)) {
            free_[i] = free_.back();
            free_.pop_back();
            freeCount_.store(static_cast<uint32_t>(free_.size()), std::memory_order_release);
            resetSegment(s);
            return s;
        }
    }
    return nullptr;
}

bool Log::allocHead(uint32_t bytes, Allocation* out) {
    std::lock_guard<SpinLock> g(headLock_);
    if (head_ == nullptr || head_->used.load(std::memory_order_relaxed) + bytes > segBytes_) {
        std::lock_guard<SpinLock> lg(listLock_);
        if (free_.size() <= reserve_) return false;
        Segment* next = popSafeFreeLocked();
        if (next == nullptr) return false;
        if (head_ != nullptr) closed_.push_back(head_);
        head_ = next;
    }
    uint32_t off = head_->used.load(std::memory_order_relaxed);
    head_->used.store(off + bytes, std::memory_order_relaxed);
    head_->pendingWriters.fetch_add(1, std::memory_order_relaxed);
    out->seg = head_;
    out->ptr = head_->data + off;
    return true;
}

std::vector<Segment*> Log::takeCandidates(uint32_t n, double randomFraction,
                                          const std::vector<double>& appWeight,
                                          double deadWeight, std::mt19937_64& rng) {
    std::lock_guard<SpinLock> g(listLock_);
    // Only segments whose writers have all finished can be cleaned.
    std::vector<size_t> eligible;
    eligible.reserve(closed_.size());
    for (size_t i = 0; i < closed_.size(); i++) {
        if (closed_[i]->pendingWriters.load(std::memory_order_acquire) == 0) eligible.push_back(i);
    }
    if (eligible.empty()) return {};
    n = std::min<uint32_t>(n, static_cast<uint32_t>(eligible.size()));

    auto numRandom = static_cast<uint32_t>(n * randomFraction + 0.5);
    std::vector<size_t> chosen;
    chosen.reserve(n);
    // Random picks: partial Fisher-Yates over the eligible indices.
    for (uint32_t k = 0; k < numRandom; k++) {
        std::uniform_int_distribution<size_t> d(k, eligible.size() - 1);
        std::swap(eligible[k], eligible[d(rng)]);
        chosen.push_back(eligible[k]);
    }
    // The rest: most data from low-need applications, counting dead space as the most
    // attractive data of all.
    uint32_t numScored = n - numRandom;
    if (numScored > 0) {
        std::vector<std::pair<double, size_t>> scored;
        scored.reserve(eligible.size() - numRandom);
        for (size_t k = numRandom; k < eligible.size(); k++) {
            const Segment* s = closed_[eligible[k]];
            double score = 0;
            int64_t liveTotal = 0;
            for (uint32_t a = 0; a < numApps_; a++) {
                int64_t l = s->live[a].load(std::memory_order_relaxed);
                liveTotal += l;
                score += double(l) * appWeight[a];
            }
            int64_t dead = int64_t(s->used.load(std::memory_order_relaxed)) - liveTotal;
            if (dead > 0) score += double(dead) * deadWeight;
            scored.emplace_back(score, eligible[k]);
        }
        std::partial_sort(scored.begin(), scored.begin() + numScored, scored.end(),
                          [](const auto& x, const auto& y) { return x.first > y.first; });
        for (uint32_t k = 0; k < numScored; k++) chosen.push_back(scored[k].second);
    }

    std::vector<Segment*> out;
    out.reserve(chosen.size());
    for (size_t idx : chosen) out.push_back(closed_[idx]);
    // Remove the chosen segments from the closed list, highest index first.
    std::sort(chosen.begin(), chosen.end(), std::greater<>());
    for (size_t idx : chosen) {
        closed_[idx] = closed_.back();
        closed_.pop_back();
    }
    return out;
}

Segment* Log::takeFreeForCleaner() {
    std::lock_guard<SpinLock> g(listLock_);
    return popSafeFreeLocked();
}

void Log::addClosed(Segment* seg) {
    std::lock_guard<SpinLock> g(listLock_);
    closed_.push_back(seg);
}

void Log::release(Segment* seg) {
    seg->retireEpoch = epochs_.retire();
    {
        std::lock_guard<SpinLock> g(listLock_);
        free_.insert(free_.begin(), seg);
        freeCount_.store(static_cast<uint32_t>(free_.size()), std::memory_order_release);
    }
    {
        std::lock_guard<std::mutex> g(waitMu_);
    }
    releaseCv_.notify_all();
}

uint32_t Log::closedSegments() const {
    std::lock_guard<SpinLock> g(listLock_);
    return static_cast<uint32_t>(closed_.size());
}

void Log::waitForRelease(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lk(waitMu_);
    releaseCv_.wait_for(lk, timeout);
}

}  // namespace memshare
