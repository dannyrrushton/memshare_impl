#include "memshare/epoch.h"

#include <mutex>
#include <stdexcept>
#include <vector>

namespace memshare {

namespace {

// Each thread gets a process-wide slot index, shared by all EpochManager instances, and gives it
// back when the thread exits.
class SlotRegistry {
public:
    static SlotRegistry& instance() {
        static SlotRegistry r;
        return r;
    }
    int acquire() {
        std::lock_guard<std::mutex> g(mu_);
        if (!free_.empty()) {
            int s = free_.back();
            free_.pop_back();
            return s;
        }
        if (next_ >= EpochManager::kMaxThreads)
            throw std::runtime_error("memshare: too many threads for EpochManager");
        int s = next_++;
        highWater_.store(next_, std::memory_order_release);
        return s;
    }
    void release(int s) {
        std::lock_guard<std::mutex> g(mu_);
        free_.push_back(s);
    }
    int highWater() const { return highWater_.load(std::memory_order_acquire); }

private:
    std::mutex mu_;
    std::vector<int> free_;
    int next_ = 0;
    std::atomic<int> highWater_{0};
};

struct ThreadSlot {
    int index;
    ThreadSlot() : index(SlotRegistry::instance().acquire()) {}
    ~ThreadSlot() { SlotRegistry::instance().release(index); }
};

int mySlot() {
    thread_local ThreadSlot slot;
    return slot.index;
}

}  // namespace

EpochManager::EpochManager() : slots_(new std::atomic<uint64_t>[kMaxThreads]) {
    for (int i = 0; i < kMaxThreads; i++) slots_[i].store(kInactive, std::memory_order_relaxed);
}

void EpochManager::enter() {
    auto& slot = slots_[mySlot()];
    slot.store(global_.load(std::memory_order_seq_cst), std::memory_order_seq_cst);
    // The published epoch must be visible before this request reads the hash table.
    std::atomic_thread_fence(std::memory_order_seq_cst);
}

void EpochManager::exit() { slots_[mySlot()].store(kInactive, std::memory_order_release); }

uint64_t EpochManager::retire() { return global_.fetch_add(1, std::memory_order_seq_cst); }

bool EpochManager::safeToReuse(uint64_t tag) const {
    std::atomic_thread_fence(std::memory_order_seq_cst);
    int n = SlotRegistry::instance().highWater();
    for (int i = 0; i < n; i++) {
        if (slots_[i].load(std::memory_order_seq_cst) <= tag) return false;
    }
    return true;
}

}  // namespace memshare
