// Epoch-based protection for segment reuse (paper §5.3).
//
// GET requests read items straight out of log segments without holding the segment-list lock,
// so a concurrent cleaning pass may free a segment that a request is still reading. Each request
// publishes the global epoch it started in; a cleaned segment is tagged with the global epoch
// and may only be reused once every in-flight request started in a later epoch.
#pragma once

#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>

namespace memshare {

class EpochManager {
public:
    static constexpr uint64_t kInactive = std::numeric_limits<uint64_t>::max();
    // Upper bound on threads that can use any EpochManager at the same time.
    static constexpr int kMaxThreads = 1024;

    EpochManager();

    // Marks the calling thread as inside a request. Guards must not be nested, and a thread
    // must never wait for segment reuse while inside one.
    void enter();
    void exit();

    // Returns the epoch to tag freshly cleaned segments with, and advances the global epoch.
    // Call only after every reference to the segments has been removed from the hash table.
    uint64_t retire();

    // A segment tagged with `tag` may be reused once this returns true.
    bool safeToReuse(uint64_t tag) const;

private:
    std::atomic<uint64_t> global_{1};
    std::unique_ptr<std::atomic<uint64_t>[]> slots_;
};

class EpochGuard {
public:
    explicit EpochGuard(EpochManager& m) : m_(m) { m_.enter(); }
    ~EpochGuard() { m_.exit(); }
    EpochGuard(const EpochGuard&) = delete;
    EpochGuard& operator=(const EpochGuard&) = delete;

private:
    EpochManager& m_;
};

}  // namespace memshare
