// Tests for the Memshare cache. Plain assertions, no framework: run the binary, nonzero exit on
// failure. An optional argument runs only tests whose name contains it.
#include <atomic>
#include <cstdio>
#include <cstring>
#include <functional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "memshare/cache.h"
#include "memshare/shadow_queue.h"

using namespace memshare;

namespace {

int failures = 0;

#define CHECK(cond)                                                              \
    do {                                                                         \
        if (!(cond)) {                                                           \
            std::fprintf(stderr, "  FAILED %s:%d: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                          \
        }                                                                        \
    } while (0)

#define CHECK_MSG(cond, ...)                                                     \
    do {                                                                         \
        if (!(cond)) {                                                           \
            std::fprintf(stderr, "  FAILED %s:%d: %s: ", __FILE__, __LINE__, #cond); \
            std::fprintf(stderr, __VA_ARGS__);                                   \
            std::fprintf(stderr, "\n");                                          \
            failures++;                                                          \
        }                                                                        \
    } while (0)

constexpr uint64_t KB = 1024, MB = 1024 * 1024;

std::string keyOf(int i) { return "key:" + std::to_string(i); }

// Values carry their key and a version so readers can detect torn or misdirected reads.
std::string valueOf(const std::string& key, uint64_t version, size_t len) {
    std::string v = key + "#" + std::to_string(version) + "|";
    while (v.size() < len) v.push_back(static_cast<char>('a' + (v.size() + version) % 26));
    return v;
}
bool valueValid(const std::string& key, const std::string& v) {
    auto hash = v.find('#');
    auto bar = v.find('|');
    if (hash == std::string::npos || bar == std::string::npos) return false;
    if (v.compare(0, hash, key) != 0) return false;
    uint64_t version = std::stoull(v.substr(hash + 1, bar - hash - 1));
    for (size_t i = bar + 1; i < v.size(); i++)
        if (v[i] != static_cast<char>('a' + (i + version) % 26)) return false;
    return true;
}

Config smallConfig(std::vector<AppConfig> apps, SharingPolicy policy, double* clock = nullptr) {
    Config c;
    c.totalBytes = 8 * MB;
    c.segmentBytes = 64 * KB;
    c.cleanSegments = 16;
    c.backgroundCleaner = false;
    c.policy = policy;
    c.apps = std::move(apps);
    if (clock) c.clock = [clock] { return *clock; };
    return c;
}

AppConfig app(const std::string& name, uint64_t reserved, uint64_t shadow = 1 * MB) {
    AppConfig a;
    a.name = name;
    a.reservedBytes = reserved;
    a.shadowQueueBytes = shadow;
    a.creditBytes = 16 * KB;
    return a;
}

// ---------------------------------------------------------------------------------------------

void testBasicOps() {
    double clock = 100;
    Cache c(smallConfig({app("a", 0), app("b", 0)}, SharingPolicy::Static, &clock));
    std::string v;
    uint32_t flags = 0;
    CHECK(!c.get(0, "x", &v));
    CHECK(c.set(0, "x", "hello", 7) == SetResult::Stored);
    CHECK(c.get(0, "x", &v, &flags) && v == "hello" && flags == 7);
    // Applications have separate key spaces.
    CHECK(!c.get(1, "x", &v));
    CHECK(c.set(1, "x", "other") == SetResult::Stored);
    CHECK(c.get(0, "x", &v) && v == "hello");
    CHECK(c.get(1, "x", &v) && v == "other");
    // Overwrite replaces the value and the accounting.
    int64_t before = c.stats().apps[0].actualBytes;
    CHECK(c.set(0, "x", std::string(1000, 'z')) == SetResult::Stored);
    CHECK(c.get(0, "x", &v) && v == std::string(1000, 'z'));
    CHECK(c.stats().apps[0].actualBytes == before - itemBytes(1, 5) + itemBytes(1, 1000));
    // Delete.
    CHECK(c.del(0, "x"));
    CHECK(!c.del(0, "x"));
    CHECK(!c.get(0, "x", &v));
    CHECK(c.stats().apps[0].actualBytes == 0);
    // Expiry.
    CHECK(c.set(0, "e", "soon", 0, 10) == SetResult::Stored);
    CHECK(c.get(0, "e", &v));
    clock += 11;
    CHECK(!c.get(0, "e", &v));
    CHECK(c.stats().apps[0].actualBytes == 0);
    // Too large for a segment.
    CHECK(c.set(0, "big", std::string(64 * KB, 'x')) == SetResult::TooLarge);
    CHECK(c.findApp("b") == AppId(1));
    CHECK(!c.findApp("nope"));
}

void testShadowQueue() {
    ShadowQueue q(1000);
    q.insert(1, 400);
    q.insert(2, 400);
    q.insert(3, 400);  // pushes out key 1
    CHECK(q.bytes() == 800);
    CHECK(!q.take(1));
    CHECK(q.take(2));
    CHECK(!q.take(2));
    CHECK(q.size() == 1);
}

// Algorithm 1 in isolation: the highest-need app gets its best items first, need is updated
// after every relocation, and the pass stops at the first item that does not fit.
void testChooseSurvivors() {
    Config cfg = smallConfig({app("a", 600), app("b", 300)}, SharingPolicy::Static);
    Arbiter arb(cfg, 900);
    // Fixed pooled share goes to targets too: 900 - 900 reserved = 0 pooled.
    CHECK(arb.target(0) == 600 && arb.target(1) == 300);
    arb.onSet(0, 600);  // a: actual 600 (all in the pass)
    arb.onSet(1, 600);  // b: actual 600 (all in the pass), over target

    std::vector<CleanCandidate> items;
    for (int i = 0; i < 6; i++) items.push_back({nullptr, 0, 100, 0, 0, RankKey{uint64_t(i), 0}});
    for (int i = 0; i < 6; i++) items.push_back({nullptr, 0, 100, 1, 0, RankKey{uint64_t(10 + i), 0}});
    arb.chooseSurvivors(items, 600);

    int keptA = 0, keptB = 0;
    for (auto& c : items) {
        if (c.keep) (c.app == 0 ? keptA : keptB)++;
    }
    // Both start with need = inf (no memory outside the pass). a must end with twice b's bytes.
    CHECK_MSG(keptA == 4 && keptB == 2, "keptA=%d keptB=%d", keptA, keptB);
    // The survivors are each app's highest-ranked items.
    for (auto& c : items) {
        if (c.app == 0) CHECK(c.keep == (c.rank.major >= 2));
        if (c.app == 1) CHECK(c.keep == (c.rank.major >= 14));
    }
}

// A single app writing far more than fits: LRU should keep a hot set that is re-read often,
// and every hit must return the right bytes.
void testEvictionByRank() {
    Cache c(smallConfig({app("a", 0)}, SharingPolicy::Static));
    std::string v;
    const int hot = 200;
    for (int i = 0; i < hot; i++) c.set(0, keyOf(i), valueOf(keyOf(i), 0, 500));
    int hotMisses = 0;
    for (int i = hot; i < 60000; i++) {
        c.set(0, keyOf(i), valueOf(keyOf(i), 0, 500));
        if (i % 50 == 0) {
            for (int h = 0; h < hot; h++) {
                if (!c.get(0, keyOf(h), &v)) {
                    hotMisses++;
                    c.set(0, keyOf(h), valueOf(keyOf(h), 0, 500));
                } else {
                    CHECK(valueValid(keyOf(h), v));
                }
            }
        }
    }
    auto s = c.stats();
    CHECK(s.cleaner.passes > 0);
    CHECK(s.cleaner.itemsEvicted > 0);
    CHECK_MSG(hotMisses == 0, "hot items missed %d times", hotMisses);
    // Everything recent is present and intact; cold old keys are gone.
    int recentHits = 0;
    for (int i = 59900; i < 60000; i++) recentHits += c.get(0, keyOf(i), &v) && valueValid(keyOf(i), v);
    CHECK_MSG(recentHits == 100, "recentHits=%d", recentHits);
    CHECK(!c.get(0, keyOf(hot + 1), &v));
    // Memory in use stays within capacity.
    CHECK(uint64_t(s.apps[0].actualBytes) <= uint64_t(s.totalSegments) * s.segmentBytes);
}

// Static partitioning: an app flooding the cache cannot push another app below its reservation.
void testStaticIsolation() {
    Config cfg = smallConfig({app("quiet", 0), app("flood", 0)}, SharingPolicy::Static);
    Cache probe(cfg);
    uint64_t cap = probe.stats().itemCapacity;
    cfg.apps[0].reservedBytes = cap / 2;
    cfg.apps[1].reservedBytes = cap / 2;
    Cache c(cfg);

    // quiet stores ~35% of the cache and then only reads.
    std::vector<std::string> quietKeys;
    for (int i = 0; uint64_t(c.stats().apps[0].actualBytes) < cap * 35 / 100; i++) {
        quietKeys.push_back("q" + std::to_string(i));
        c.set(0, quietKeys.back(), valueOf(quietKeys.back(), 0, 400));
    }
    std::string v;
    for (int i = 0; i < 100000; i++) {
        std::string k = "f" + std::to_string(i);
        c.set(1, k, valueOf(k, 0, 400));
    }
    int hits = 0;
    for (auto& k : quietKeys) hits += c.get(0, k, &v) && valueValid(k, v);
    auto s = c.stats();
    CHECK_MSG(hits == int(quietKeys.size()), "quiet kept %d of %zu", hits, quietKeys.size());
    CHECK(s.apps[1].evictedItems > 0);
    CHECK(s.apps[0].evictedItems == 0);
    // flood is held near its target, and could use what quiet is not using.
    CHECK(s.apps[1].actualBytes > int64_t(cap) / 2);
}

// Pooled memory (Algorithm 2): the app with shadow-queue hits gains pooled memory from the one
// without, never taking the other below its reservation.
void testPooledSharing() {
    Config cfg = smallConfig({app("big", 0, 2 * MB), app("small", 0, 2 * MB)}, SharingPolicy::Pooled);
    Cache probe(cfg);
    uint64_t cap = probe.stats().itemCapacity;
    cfg.apps[1].reservedBytes = cap / 5;
    Cache c(cfg);
    auto initial = c.stats();
    int64_t smallInitialTarget = initial.apps[1].targetBytes;

    // big cycles over a working set of ~90% of the cache; small over ~10%.
    std::mt19937_64 rng(1);
    std::string v;
    const int bigKeys = int(cap * 9 / 10 / itemBytes(8, 500));
    const int smallKeys = int(cap / 10 / itemBytes(8, 500));
    uint64_t bigHitsEarly = 0, bigGetsEarly = 0, bigHitsLate = 0, bigGetsLate = 0;
    const int rounds = 400000;
    for (int i = 0; i < rounds; i++) {
        bool isBig = rng() % 4 != 0;
        AppId a = isBig ? 0 : 1;
        int k = int(rng() % (isBig ? bigKeys : smallKeys));
        std::string key = keyOf(k);
        bool hit = c.get(a, key, &v);
        if (hit) CHECK(valueValid(key, v));
        else c.set(a, key, valueOf(key, 0, 500));
        if (isBig && i < rounds / 4) { bigGetsEarly++; bigHitsEarly += hit; }
        if (isBig && i >= rounds * 3 / 4) { bigGetsLate++; bigHitsLate += hit; }
    }
    auto s = c.stats();
    CHECK(s.apps[0].shadowHits > 0);
    CHECK_MSG(s.apps[0].targetBytes > initial.apps[0].targetBytes,
              "big target %lld -> %lld", (long long)initial.apps[0].targetBytes,
              (long long)s.apps[0].targetBytes);
    CHECK(s.apps[1].targetBytes < smallInitialTarget);
    CHECK(s.apps[1].targetBytes >= int64_t(cfg.apps[1].reservedBytes));
    CHECK(s.apps[0].targetBytes + s.apps[1].targetBytes == int64_t(cap));
    double early = double(bigHitsEarly) / bigGetsEarly, late = double(bigHitsLate) / bigGetsLate;
    CHECK_MSG(late > early, "big hit rate early %.3f late %.3f", early, late);
}

// Idle tax (Algorithm 3): an app sitting on idle reserved memory has it reassigned.
void testIdleTax() {
    double clock = 0;
    Config cfg = smallConfig({app("idle", 0), app("busy", 0)}, SharingPolicy::IdleTax, &clock);
    cfg.taxRate = 0.5;
    cfg.idleSeconds = 100;
    Cache probe(cfg);
    uint64_t cap = probe.stats().itemCapacity;
    cfg.apps[0].reservedBytes = cap / 2;
    cfg.apps[1].reservedBytes = cap / 2;
    Cache c(cfg);

    std::vector<std::string> idleKeys;
    for (int i = 0; uint64_t(c.stats().apps[0].actualBytes) < cap * 45 / 100; i++) {
        idleKeys.push_back("i" + std::to_string(i));
        c.set(0, idleKeys.back(), valueOf(idleKeys.back(), 0, 400));
    }
    clock = 1000;  // everything idle has now been idle for longer than idleSeconds
    c.updateIdleTax();
    auto s = c.stats();
    CHECK(s.apps[0].idleBytes == s.apps[0].actualBytes);
    // All memory idle: target = reserved * (1 - taxRate).
    CHECK_MSG(std::abs(double(s.apps[0].targetBytes) - double(cap / 2) * 0.5) < 2,
              "idle target %lld", (long long)s.apps[0].targetBytes);
    CHECK(s.apps[1].targetBytes == int64_t(cap / 2));

    for (int i = 0; i < 100000; i++) {
        std::string k = "b" + std::to_string(i);
        c.set(1, k, valueOf(k, 0, 400));
        if (i % 5000 == 0) c.updateIdleTax();
    }
    s = c.stats();
    CHECK(s.apps[0].evictedItems > 0);
    // The idle app is squeezed towards its taxed target.
    CHECK_MSG(s.apps[0].actualBytes < int64_t(cap) * 35 / 100, "idle actual %lld of cap %llu",
              (long long)s.apps[0].actualBytes, (unsigned long long)cap);
}

// LFU ranking: frequently read items survive even when they are not the most recent.
void testLfu() {
    Config cfg = smallConfig({app("a", 0)}, SharingPolicy::Static);
    cfg.apps[0].rank = RankPolicy::LFU;
    Cache c(cfg);
    std::string v;
    for (int i = 0; i < 100; i++) {
        c.set(0, keyOf(i), valueOf(keyOf(i), 0, 500));
        for (int r = 0; r < 5; r++) c.get(0, keyOf(i), &v);
    }
    for (int i = 100; i < 50000; i++) c.set(0, keyOf(i), valueOf(keyOf(i), 0, 500));
    int kept = 0;
    for (int i = 0; i < 100; i++) kept += c.get(0, keyOf(i), &v);
    CHECK_MSG(kept == 100, "kept %d frequent items", kept);
}

// Many threads reading and writing while background cleaners compact the log. Every value read
// must be intact and belong to the requested key.
void testConcurrentStress() {
    Config cfg;
    cfg.totalBytes = 16 * MB;
    cfg.segmentBytes = 64 * KB;
    cfg.cleanSegments = 8;
    cfg.backgroundCleaner = true;
    cfg.cleanerThreads = 2;
    cfg.policy = SharingPolicy::Pooled;
    cfg.apps = {app("a", 0), app("b", 0), app("c", 0)};
    Cache c(cfg);

    std::atomic<int> bad{0};
    std::atomic<uint64_t> hits{0}, gets{0}, noMem{0};
    std::vector<std::thread> workers;
    for (int t = 0; t < 8; t++) {
        workers.emplace_back([&, t] {
            std::mt19937_64 rng(t);
            std::string v;
            for (int i = 0; i < 60000; i++) {
                AppId a = AppId(rng() % 3);
                std::string key = keyOf(int(rng() % 20000));
                if (rng() % 4 == 0) {
                    size_t len = 50 + rng() % 1500;
                    if (c.set(a, key, valueOf(key, rng() % 1000, len)) == SetResult::NoMemory) noMem++;
                } else if (rng() % 50 == 0) {
                    c.del(a, key);
                } else {
                    gets++;
                    if (c.get(a, key, &v)) {
                        hits++;
                        if (!valueValid(key, v)) bad++;
                    }
                }
            }
        });
    }
    for (auto& w : workers) w.join();
    auto s = c.stats();
    CHECK(bad.load() == 0);
    CHECK(noMem.load() == 0);
    CHECK(s.cleaner.passes > 0);
    CHECK(hits.load() > 0);
    int64_t total = 0;
    for (auto& a : s.apps) total += a.actualBytes;
    CHECK(total > 0 && uint64_t(total) <= uint64_t(s.totalSegments) * s.segmentBytes);
    std::printf("  %llu gets, %.1f%% hits, %llu cleaning passes, %llu MB relocated\n",
                (unsigned long long)gets.load(), 100.0 * hits.load() / gets.load(),
                (unsigned long long)s.cleaner.passes,
                (unsigned long long)(s.cleaner.bytesRelocated / MB));
}

}  // namespace

int main(int argc, char** argv) {
    std::vector<std::pair<const char*, std::function<void()>>> tests = {
        {"basic_ops", testBasicOps},
        {"shadow_queue", testShadowQueue},
        {"choose_survivors", testChooseSurvivors},
        {"eviction_by_rank", testEvictionByRank},
        {"static_isolation", testStaticIsolation},
        {"pooled_sharing", testPooledSharing},
        {"idle_tax", testIdleTax},
        {"lfu", testLfu},
        {"concurrent_stress", testConcurrentStress},
    };
    for (auto& [name, fn] : tests) {
        if (argc > 1 && !std::strstr(name, argv[1])) continue;
        int before = failures;
        std::printf("%s\n", name);
        fn();
        std::printf("  %s\n", failures == before ? "ok" : "FAILED");
    }
    std::printf(failures ? "%d check(s) failed\n" : "all tests passed\n", failures);
    return failures ? 1 : 0;
}
