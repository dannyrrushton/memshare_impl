// The cleaner (paper §3.1, §5.3, §5.4). A pass takes n closed segments, asks the arbiter which
// of their live items to keep (Algorithm 1), evicts the rest into their applications' shadow
// queues, and compacts the survivors into at most n-1 segments, freeing at least one.
//
// The cleaner knows nothing about applications or ranking; the arbiter makes every keep/evict
// decision. Passes are independent and can run in parallel on different segments.
#pragma once

#include <atomic>
#include <cstdint>
#include <random>

#include "memshare/arbiter.h"
#include "memshare/epoch.h"
#include "memshare/hash_table.h"
#include "memshare/log.h"

namespace memshare {

struct CleanerStats {
    uint64_t passes = 0;
    uint64_t segmentsCleaned = 0;
    uint64_t itemsRelocated = 0, bytesRelocated = 0;
    uint64_t itemsEvicted = 0, bytesEvicted = 0;
    double seconds = 0;  // time spent cleaning
};

class Cleaner {
public:
    Cleaner(Log& log, HashTable& table, Arbiter& arbiter, EpochManager& epochs,
            uint32_t segmentsPerPass, double randomFraction);

    // Runs one cleaning pass. `now` is the current clock second, used to drop expired items.
    // Returns false if there was nothing to clean. Must not be called inside an EpochGuard.
    bool cleanPass(std::mt19937_64& rng, uint32_t now);

    CleanerStats stats() const;

private:
    // Gets a free segment to write survivors into. Returns nullptr if the free list is empty.
    Segment* acquireOutput();

    Log& log_;
    HashTable& table_;
    Arbiter& arbiter_;
    EpochManager& epochs_;
    uint32_t n_;
    double randomFraction_;

    std::atomic<uint64_t> passes_{0}, segmentsCleaned_{0};
    std::atomic<uint64_t> itemsRelocated_{0}, bytesRelocated_{0};
    std::atomic<uint64_t> itemsEvicted_{0}, bytesEvicted_{0};
    std::atomic<uint64_t> nanos_{0};
};

}  // namespace memshare
