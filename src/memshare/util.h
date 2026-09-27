// Small shared helpers: a spin lock, key hashing and alignment.
#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <thread>

namespace memshare {

// Test-and-test-and-set spin lock. Satisfies Lockable, so it works with std::lock_guard and
// std::unique_lock. Memshare's critical sections (head allocation, segment lists, bucket
// updates, shadow queues) are all a few instructions long, which is what the paper relies on.
class SpinLock {
public:
    void lock() noexcept {
        for (;;) {
            if (!flag_.exchange(true, std::memory_order_acquire)) return;
            int spins = 0;
            while (flag_.load(std::memory_order_relaxed)) {
                if (++spins > 64) {
                    std::this_thread::yield();
                    spins = 0;
                }
            }
        }
    }
    bool try_lock() noexcept {
        return !flag_.load(std::memory_order_relaxed) &&
               !flag_.exchange(true, std::memory_order_acquire);
    }
    void unlock() noexcept { flag_.store(false, std::memory_order_release); }

private:
    std::atomic<bool> flag_{false};
};

inline uint64_t mix64(uint64_t x) {
    // splitmix64 finalizer.
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

// 64-bit key hash. Shadow queues store only this value (§5.2), so it must be well mixed.
inline uint64_t hashKey(std::string_view key) {
    uint64_t h = 0xcbf29ce484222325ULL ^ key.size();
    size_t i = 0;
    for (; i + 8 <= key.size(); i += 8) {
        uint64_t w;
        std::memcpy(&w, key.data() + i, 8);
        h = mix64(h ^ w) * 0x9e3779b97f4a7c15ULL;
    }
    uint64_t tail = 0;
    std::memcpy(&tail, key.data() + i, key.size() - i);
    return mix64(h ^ tail ^ (uint64_t(key.size()) << 56));
}

constexpr uint64_t alignUp(uint64_t v, uint64_t a) { return (v + a - 1) & ~(a - 1); }

}  // namespace memshare
