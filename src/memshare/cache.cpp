#include "memshare/cache.h"

#include <cmath>
#include <cstring>
#include <stdexcept>

namespace memshare {

Cache::Cache(Config cfg) : cfg_(std::move(cfg)), start_(std::chrono::steady_clock::now()) {
    if (cfg_.apps.empty()) throw std::invalid_argument("at least one application is required");
    if (cfg_.cleanerThreads == 0) cfg_.cleanerThreads = 1;
    // Each concurrent cleaning pass holds at most two output segments beyond the source segments
    // it has already freed, so this reserve guarantees the cleaner can always make progress.
    uint32_t concurrentPasses = cfg_.backgroundCleaner ? cfg_.cleanerThreads : 1;
    uint32_t reserve = 2 * concurrentPasses + 1;

    auto numSegments = static_cast<uint32_t>(cfg_.totalBytes / cfg_.segmentBytes);
    lowWater_ = std::max<uint32_t>(1, static_cast<uint32_t>(std::ceil(cfg_.freeFraction * numSegments)));
    if (numSegments < reserve + lowWater_ + 4)
        throw std::invalid_argument("cache too small: need more segments for the given segment size");

    log_ = std::make_unique<Log>(cfg_.totalBytes, cfg_.segmentBytes,
                                 static_cast<uint32_t>(cfg_.apps.size()), reserve, epochs_);
    uint64_t buckets = cfg_.hashBuckets ? cfg_.hashBuckets : std::max<uint64_t>(1024, cfg_.totalBytes / 128);
    table_ = std::make_unique<HashTable>(buckets);
    uint64_t itemCapacity = uint64_t(numSegments - reserve - lowWater_) * cfg_.segmentBytes;
    arbiter_ = std::make_unique<Arbiter>(cfg_, itemCapacity);
    cleaner_ = std::make_unique<Cleaner>(*log_, *table_, *arbiter_, epochs_, cfg_.cleanSegments,
                                         cfg_.randomSegmentFraction);
    inlineRng_.seed(cfg_.seed);

    if (cfg_.backgroundCleaner) {
        for (uint32_t i = 0; i < cfg_.cleanerThreads; i++)
            threads_.emplace_back([this, i] { backgroundLoop(cfg_.seed + 1 + i); });
        threads_.emplace_back([this] { maintenanceLoop(); });
    }
}

Cache::~Cache() {
    stop_.store(true);
    {
        std::lock_guard<std::mutex> g(wakeMu_);
    }
    wakeCv_.notify_all();
    for (auto& t : threads_) t.join();
}

std::optional<AppId> Cache::findApp(std::string_view name) const {
    for (AppId a = 0; a < arbiter_->numApps(); a++) {
        if (arbiter_->appConfig(a).name == name) return a;
    }
    return std::nullopt;
}

uint32_t Cache::now() const {
    double t = cfg_.clock ? cfg_.clock()
                          : std::chrono::duration<double>(std::chrono::steady_clock::now() - start_).count();
    return static_cast<uint32_t>(t);
}

bool Cache::needsCleaning() const {
    return log_->freeSegments() < log_->reserveSegments() + lowWater_;
}

// Removes an item that was just unlinked from the hash table from the accounting. Must run inside
// the EpochGuard that covered the unlink, so the item's segment cannot be reused meanwhile.
void Cache::dropItem(const ItemHeader* item) {
    uint32_t bytes = itemBytes(item);
    log_->segmentOf(item)->live[item->app].fetch_sub(bytes, std::memory_order_relaxed);
    arbiter_->onRemove(item->app, bytes);
}

bool Cache::get(AppId app, std::string_view key, std::string* value, uint32_t* flags) {
    if (app >= arbiter_->numApps()) return false;
    uint64_t keyHash = hashKey(key);
    uint32_t t = now();
    {
        EpochGuard g(epochs_);
        const ItemHeader* item =
            table_->lookup(app, key, keyHash, seq_.fetch_add(1, std::memory_order_relaxed), t);
        if (item != nullptr) {
            if (item->expiry == 0 || item->expiry > t) {
                // Items are immutable and their segment cannot be reused while we are inside the
                // guard, so the copy is safe even if the cleaner moves the item meanwhile.
                if (value) value->assign(item->value(), item->valueLen);
                if (flags) *flags = item->flags;
                arbiter_->onHit(app);
                return true;
            }
            if (table_->eraseIf(item, keyHash)) dropItem(item);
        }
    }
    arbiter_->onMiss(app, keyHash);
    return false;
}

SetResult Cache::set(AppId app, std::string_view key, std::string_view value, uint32_t flags,
                     uint32_t exptime) {
    if (app >= arbiter_->numApps()) throw std::out_of_range("unknown application");
    if (key.size() > kMaxKeyBytes || value.size() > cfg_.segmentBytes) return SetResult::TooLarge;
    uint32_t bytes = itemBytes(static_cast<uint32_t>(key.size()), static_cast<uint32_t>(value.size()));
    if (bytes > cfg_.segmentBytes) return SetResult::TooLarge;

    Log::Allocation a;
    while (!log_->allocHead(bytes, &a)) {
        if (!makeRoom()) return SetResult::NoMemory;
    }

    uint32_t t = now();
    auto* h = reinterpret_cast<ItemHeader*>(a.ptr);
    h->app = app;
    h->valueLen = static_cast<uint32_t>(value.size());
    h->flags = flags;
    h->expiry = exptime ? t + exptime : 0;
    h->keyLen = static_cast<uint16_t>(key.size());
    h->unused = 0;
    h->unused2 = 0;
    std::memcpy(a.ptr + sizeof(ItemHeader), key.data(), key.size());
    std::memcpy(a.ptr + sizeof(ItemHeader) + key.size(), value.data(), value.size());

    uint64_t keyHash = hashKey(key);
    {
        EpochGuard g(epochs_);
        a.seg->live[app].fetch_add(bytes, std::memory_order_relaxed);
        arbiter_->onSet(app, bytes);
        const ItemHeader* old =
            table_->upsert(app, key, keyHash, h, seq_.fetch_add(1, std::memory_order_relaxed), t);
        if (old != nullptr) dropItem(old);
    }
    log_->commit(a.seg);

    if (needsCleaning()) {
        if (cfg_.backgroundCleaner) {
            wakeCv_.notify_one();
        } else if (inlineMu_.try_lock()) {
            // Clean while under the free-segment threshold (§5.3), without blocking other writers.
            std::lock_guard<std::mutex> g(inlineMu_, std::adopt_lock);
            while (needsCleaning() && cleaner_->cleanPass(inlineRng_, now())) {}
        }
    }
    return SetResult::Stored;
}

bool Cache::del(AppId app, std::string_view key) {
    if (app >= arbiter_->numApps()) return false;
    EpochGuard g(epochs_);
    const ItemHeader* old = table_->erase(app, key, hashKey(key));
    if (old == nullptr) return false;
    dropItem(old);
    return true;
}

bool Cache::makeRoom() {
    // Every pass frees at least one segment once there is anything to clean, so these bounds are
    // only reached when the cache is wedged (for example, every segment is still being written).
    constexpr int kAttempts = 10000;
    if (cfg_.backgroundCleaner) {
        for (int i = 0; i < kAttempts; i++) {
            wakeCv_.notify_one();
            if (log_->freeSegments() > log_->reserveSegments()) return true;
            log_->waitForRelease(std::chrono::milliseconds(1));
        }
        return false;
    }
    for (int i = 0; i < kAttempts; i++) {
        {
            std::lock_guard<std::mutex> g(inlineMu_);
            if (log_->freeSegments() > log_->reserveSegments()) return true;
            if (cleaner_->cleanPass(inlineRng_, now())) return true;
        }
        // Nothing cleanable yet: the only closed segments still have writers finishing up.
        std::this_thread::yield();
    }
    return false;
}

bool Cache::cleanOnce() {
    std::lock_guard<std::mutex> g(inlineMu_);
    return cleaner_->cleanPass(inlineRng_, now());
}

void Cache::backgroundLoop(uint64_t seed) {
    std::mt19937_64 rng(seed);
    while (!stop_.load()) {
        if (needsCleaning()) {
            if (!cleaner_->cleanPass(rng, now())) std::this_thread::yield();
            continue;
        }
        std::unique_lock<std::mutex> lk(wakeMu_);
        wakeCv_.wait_for(lk, std::chrono::milliseconds(5),
                         [this] { return stop_.load() || needsCleaning(); });
    }
}

void Cache::maintenanceLoop() {
    while (!stop_.load()) {
        {
            std::unique_lock<std::mutex> lk(wakeMu_);
            wakeCv_.wait_for(lk, std::chrono::seconds(1), [this] { return stop_.load(); });
        }
        if (!stop_.load() && cfg_.policy == SharingPolicy::IdleTax) updateIdleTax();
    }
}

void Cache::updateIdleTax() {
    uint32_t t = now();
    double cutoff = double(t) - cfg_.idleSeconds;
    std::vector<int64_t> idle(arbiter_->numApps(), 0);
    constexpr uint64_t kChunk = 1024;
    for (uint64_t b = 0; b < table_->numBuckets(); b += kChunk) {
        // Guard per chunk, not per scan, so a long scan does not hold up segment reuse.
        EpochGuard g(epochs_);
        table_->forEachInBuckets(b, std::min(b + kChunk, table_->numBuckets()),
                                 [&](const ItemHeader* item, const AccessInfo& info) {
                                     if (double(info.lastTime) < cutoff)
                                         idle[item->app] += itemBytes(item);
                                 });
    }
    arbiter_->updateIdle(idle);
}

CacheStats Cache::stats() const {
    CacheStats s;
    for (AppId a = 0; a < arbiter_->numApps(); a++) s.apps.push_back(arbiter_->stats(a));
    s.cleaner = cleaner_->stats();
    s.segmentBytes = log_->segmentBytes();
    s.totalSegments = log_->numSegments();
    s.freeSegments = log_->freeSegments();
    s.closedSegments = log_->closedSegments();
    s.reserveSegments = log_->reserveSegments();
    s.itemCapacity = arbiter_->itemCapacity();
    return s;
}

}  // namespace memshare
