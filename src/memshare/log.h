// The segmented in-memory log (paper §3, §5.1).
//
// All items, of every size and every application, live intermixed in fixed-size segments carved
// out of one arena. Writers append to the current head segment; when it fills, a free segment
// becomes the new head and the old one joins the list of closed segments that the cleaner
// may compact.
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <vector>

#include "memshare/epoch.h"
#include "memshare/util.h"

namespace memshare {

// Layout of an item in the log: header, key bytes, value bytes, padded to 8 bytes.
// Items are immutable once written; the cleaner moves them by copying.
struct ItemHeader {
    uint32_t app;
    uint32_t valueLen;
    uint32_t flags;
    uint32_t expiry;  // absolute clock second, 0 = never
    uint16_t keyLen;
    uint16_t unused;
    uint32_t unused2;

    const char* key() const { return reinterpret_cast<const char*>(this + 1); }
    const char* value() const { return key() + keyLen; }
};
static_assert(sizeof(ItemHeader) == 24);

inline uint32_t itemBytes(uint32_t keyLen, uint32_t valueLen) {
    return static_cast<uint32_t>(alignUp(sizeof(ItemHeader) + keyLen + valueLen, 8));
}
inline uint32_t itemBytes(const ItemHeader* h) { return itemBytes(h->keyLen, h->valueLen); }

struct Segment {
    uint32_t id = 0;
    uint8_t* data = nullptr;
    // Bytes handed out from this segment.
    std::atomic<uint32_t> used{0};
    // Writers that have been given space here but have not finished writing. The cleaner will
    // not touch a segment until this drops to zero.
    std::atomic<int32_t> pendingWriters{0};
    // Live bytes per application. used - sum(live) is dead space left by overwrites, deletes and
    // evictions. Used to pick segments to clean.
    std::unique_ptr<std::atomic<int64_t>[]> live;
    // Epoch this segment was retired in; it may be reused once no request from that epoch or
    // earlier is still running.
    uint64_t retireEpoch = 0;
};

class Log {
public:
    struct Allocation {
        Segment* seg = nullptr;
        uint8_t* ptr = nullptr;
    };

    // reserveSegments free segments are held back from writers so that the cleaner can always
    // make progress.
    Log(uint64_t totalBytes, uint32_t segmentBytes, uint32_t numApps, uint32_t reserveSegments,
        EpochManager& epochs);
    ~Log();

    uint32_t segmentBytes() const { return segBytes_; }
    uint32_t numSegments() const { return static_cast<uint32_t>(segments_.size()); }
    uint32_t reserveSegments() const { return reserve_; }
    uint32_t numApps() const { return numApps_; }

    // Allocates space for a new item from the head segment. Fails when no free segment is
    // available to become the next head; the caller should then clean and retry. On success the
    // caller must call commit() once the item is written.
    bool allocHead(uint32_t bytes, Allocation* out);
    void commit(Segment* seg) { seg->pendingWriters.fetch_sub(1, std::memory_order_release); }

    Segment* segmentOf(const void* p) const {
        auto off = static_cast<const uint8_t*>(p) - arena_;
        return segments_[static_cast<size_t>(off) / segBytes_].get();
    }

    // ---- Cleaner interface ----

    // Removes up to n closed segments from the closed list and returns them. A fraction
    // `randomFraction` is chosen uniformly at random; the rest are the segments with the highest
    // score sum_app(live[app] * appWeight[app]) + dead * deadWeight.
    std::vector<Segment*> takeCandidates(uint32_t n, double randomFraction,
                                         const std::vector<double>& appWeight, double deadWeight,
                                         std::mt19937_64& rng);
    // Returns a free segment for the cleaner to write survivors into, ignoring the reserve.
    // Returns nullptr if there is none, or none that is safe to reuse yet.
    Segment* takeFreeForCleaner();
    // Adds a segment the cleaner has finished filling to the closed list.
    void addClosed(Segment* seg);
    // Returns a segment to the free list. Every hash table reference into it must already be
    // gone; it is tagged with the current epoch so readers still inside it are protected.
    void release(Segment* seg);

    uint32_t freeSegments() const { return freeCount_.load(std::memory_order_acquire); }
    uint32_t closedSegments() const;

    // Blocks until a segment is released or the timeout passes.
    void waitForRelease(std::chrono::milliseconds timeout);

private:
    Segment* popSafeFreeLocked();
    void resetSegment(Segment* seg);

    uint32_t segBytes_;
    uint32_t numApps_;
    uint32_t reserve_;
    EpochManager& epochs_;
    uint8_t* arena_ = nullptr;
    size_t arenaBytes_ = 0;
    std::vector<std::unique_ptr<Segment>> segments_;

    SpinLock headLock_;
    Segment* head_ = nullptr;

    // Protects free_ and closed_. Lock order: headLock_ before listLock_.
    mutable SpinLock listLock_;
    std::vector<Segment*> free_;
    std::vector<Segment*> closed_;
    std::atomic<uint32_t> freeCount_{0};

    std::mutex waitMu_;
    std::condition_variable releaseCv_;
};

}  // namespace memshare
