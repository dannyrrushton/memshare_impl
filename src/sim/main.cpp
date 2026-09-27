// memshare_sim: replays a multi-tenant trace against Memshare under different sharing policies and
// against a memcached-style partitioned slab cache, and compares their hit rates (paper §6).
//
// The cache is a lookaside cache: a GET that misses is followed by a SET of the item, as the
// application would do after reading it from the database.
#include <chrono>
#include <cstdio>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "memshare/cache.h"
#include "sim/slab_cache.h"
#include "sim/trace.h"

using namespace sim;

namespace {

struct Options {
    std::string tracePath;
    std::vector<std::pair<std::string, double>> appMem;  // for CSV traces
    uint64_t requests = 12'000'000;
    uint64_t seed = 1;
    std::vector<std::string> policies = {"memcached", "static", "pooled", "pooled0", "idletax"};
    double reserved = 0.75;   // pooled policy: fraction of each app's memory reserved
    double creditKB = 64;
    double taxRate = 0.5;
    double idleSeconds = 900;
    uint32_t segmentKB = 1024;
    uint32_t cleanSegments = 40;
    double shadowMB = 10;
    std::string dumpPath;
};

struct Result {
    std::string policy;
    std::vector<uint64_t> gets, hits;
    std::vector<int64_t> finalBytes, targetBytes;
    std::vector<uint64_t> shadowHits, evictions;
    double traceSeconds = 0, wallSeconds = 0;
    uint64_t relocatedBytes = 0;
    uint64_t cleanerPasses = 0;
};

std::vector<std::string> splitList(const std::string& s, char sep) {
    std::vector<std::string> v;
    std::stringstream ss(s);
    std::string x;
    while (std::getline(ss, x, sep)) v.push_back(x);
    return v;
}

void usage() {
    std::cerr <<
        "usage: memshare_sim [options]\n"
        "  --trace FILE            CSV trace (time,app,op,key,value_bytes); default: synthetic\n"
        "  --app-mem NAME:MB       memory each app bought (required per app with --trace)\n"
        "  --requests N            synthetic trace length (default 12000000)\n"
        "  --seed N                synthetic trace seed (default 1)\n"
        "  --dump FILE             write the synthetic trace as CSV and exit\n"
        "  --policies LIST         any of memcached,static,pooled,pooled0,idletax (default all)\n"
        "  --reserved F            pooled: reserved fraction of each app's memory (default 0.75)\n"
        "  --credit KB             pooled: credit size (default 64)\n"
        "  --tax-rate F            idletax: tax rate (default 0.5)\n"
        "  --idle-seconds S        idletax: idle time (default 900)\n"
        "  --segment KB            segment size (default 1024)\n"
        "  --clean-segments N      n, segments per cleaning pass (default 40)\n"
        "  --shadow MB             shadow queue size in item bytes (default 10)\n";
}

using TraceFactory = std::function<std::unique_ptr<TraceSource>()>;

Result runMemcached(const TraceFactory& make, const std::vector<double>& memMB) {
    Result r;
    r.policy = "memcached";
    std::vector<uint64_t> bytes;
    for (double m : memMB) bytes.push_back(static_cast<uint64_t>(m * (1 << 20)));
    SlabCache cache(bytes);
    r.gets.assign(memMB.size(), 0);
    r.hits.assign(memMB.size(), 0);
    auto trace = make();
    Request q;
    auto start = std::chrono::steady_clock::now();
    while (trace->next(&q)) {
        r.traceSeconds = q.time;
        switch (q.op) {
            case Request::Op::Get:
                r.gets[q.app]++;
                if (cache.get(q.app, q.key)) r.hits[q.app]++;
                else if (q.valueBytes) cache.set(q.app, q.key, q.valueBytes);
                break;
            case Request::Op::Set: cache.set(q.app, q.key, q.valueBytes); break;
            case Request::Op::Delete: cache.del(q.app, q.key); break;
        }
    }
    r.wallSeconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    for (size_t a = 0; a < memMB.size(); a++) {
        r.finalBytes.push_back(static_cast<int64_t>(cache.usedBytes(static_cast<uint32_t>(a))));
        r.targetBytes.push_back(static_cast<int64_t>(bytes[a]));
    }
    return r;
}

Result runMemshare(const std::string& policy, const TraceFactory& make,
                   const std::vector<std::string>& names, const std::vector<double>& memMB,
                   const Options& o) {
    using namespace memshare;
    double simNow = 0;
    Config cfg;
    double totalMB = 0;
    for (double m : memMB) totalMB += m;
    cfg.totalBytes = static_cast<uint64_t>(totalMB * (1 << 20));
    cfg.segmentBytes = o.segmentKB << 10;
    cfg.cleanSegments = o.cleanSegments;
    cfg.backgroundCleaner = false;
    cfg.clock = [&simNow] { return simNow; };
    cfg.taxRate = o.taxRate;
    cfg.idleSeconds = o.idleSeconds;
    double reservedFraction = 1.0;
    if (policy == "static") cfg.policy = SharingPolicy::Static;
    else if (policy == "pooled") { cfg.policy = SharingPolicy::Pooled; reservedFraction = o.reserved; }
    else if (policy == "pooled0") { cfg.policy = SharingPolicy::Pooled; reservedFraction = 0; }
    else if (policy == "idletax") cfg.policy = SharingPolicy::IdleTax;
    else throw std::invalid_argument("unknown policy " + policy);
    for (const auto& n : names) {
        AppConfig a;
        a.name = n;
        a.creditBytes = static_cast<uint64_t>(o.creditKB * 1024);
        a.shadowQueueBytes = static_cast<uint64_t>(o.shadowMB * (1 << 20));
        cfg.apps.push_back(a);
    }
    // Memshare pays for its cleaner reserve and free-segment headroom out of the same total, so
    // reservations are each app's share of the item capacity.
    uint64_t itemCapacity = Cache(cfg).stats().itemCapacity;
    for (size_t a = 0; a < names.size(); a++)
        cfg.apps[a].reservedBytes =
            static_cast<uint64_t>(double(itemCapacity) * reservedFraction * memMB[a] / totalMB);

    Cache cache(cfg);
    Result r;
    r.policy = policy;
    r.gets.assign(names.size(), 0);
    r.hits.assign(names.size(), 0);
    auto trace = make();
    Request q;
    double nextIdleScan = 60;
    auto start = std::chrono::steady_clock::now();
    while (trace->next(&q)) {
        simNow = q.time;
        if (cfg.policy == SharingPolicy::IdleTax && simNow >= nextIdleScan) {
            cache.updateIdleTax();
            nextIdleScan = simNow + 60;
        }
        switch (q.op) {
            case Request::Op::Get:
                r.gets[q.app]++;
                if (cache.get(q.app, q.key, nullptr)) r.hits[q.app]++;
                else if (q.valueBytes) cache.set(q.app, q.key, std::string(q.valueBytes, 'v'));
                break;
            case Request::Op::Set: cache.set(q.app, q.key, std::string(q.valueBytes, 'v')); break;
            case Request::Op::Delete: cache.del(q.app, q.key); break;
        }
    }
    r.wallSeconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    r.traceSeconds = simNow;
    auto s = cache.stats();
    for (const auto& a : s.apps) {
        r.finalBytes.push_back(a.actualBytes);
        r.targetBytes.push_back(a.targetBytes);
        r.shadowHits.push_back(a.shadowHits);
        r.evictions.push_back(a.evictedItems);
    }
    r.relocatedBytes = s.cleaner.bytesRelocated;
    r.cleanerPasses = s.cleaner.passes;
    return r;
}

double rate(uint64_t hits, uint64_t gets) { return gets ? 100.0 * double(hits) / double(gets) : 0; }

void printResults(const std::vector<Result>& results, const std::vector<std::string>& names,
                  const std::vector<double>& memMB) {
    std::printf("\nHit rate (%%) per application\n%-12s %8s", "app", "mem MB");
    for (const auto& r : results) std::printf(" %10s", r.policy.c_str());
    std::printf("\n");
    for (size_t a = 0; a < names.size(); a++) {
        std::printf("%-12s %8.1f", names[a].c_str(), memMB[a]);
        for (const auto& r : results) std::printf(" %10.2f", rate(r.hits[a], r.gets[a]));
        std::printf("\n");
    }
    std::vector<double> combined, missRate;
    for (const auto& r : results) {
        uint64_t h = 0, g = 0;
        for (size_t a = 0; a < names.size(); a++) { h += r.hits[a]; g += r.gets[a]; }
        combined.push_back(rate(h, g));
    }
    std::printf("%-12s %8s", "combined", "");
    for (double c : combined) std::printf(" %10.2f", c);
    std::printf("\n");

    // Miss reduction relative to the first policy (memcached when it is run).
    std::printf("\n%-10s %12s %12s %14s %14s %10s\n", "policy", "hit rate", "miss reduc.",
                "cleaner MB/s", "cleaner passes", "wall s");
    for (size_t i = 0; i < results.size(); i++) {
        const Result& r = results[i];
        double missBase = 100 - combined[0], miss = 100 - combined[i];
        double bw = r.traceSeconds > 0 ? double(r.relocatedBytes) / (1 << 20) / r.traceSeconds : 0;
        std::printf("%-10s %11.2f%% %11.2f%% %14.3f %14llu %10.1f\n", r.policy.c_str(), combined[i],
                    missBase > 0 ? 100.0 * (missBase - miss) / missBase : 0.0, bw,
                    (unsigned long long)r.cleanerPasses, r.wallSeconds);
    }

    std::printf("\nMemory at end of trace, MB used / target\n%-12s", "app");
    for (const auto& r : results) std::printf(" %17s", r.policy.c_str());
    std::printf("\n");
    for (size_t a = 0; a < names.size(); a++) {
        std::printf("%-12s", names[a].c_str());
        for (const auto& r : results)
            std::printf("    %6.1f / %6.1f", r.finalBytes[a] / 1048576.0, r.targetBytes[a] / 1048576.0);
        std::printf("\n");
    }

    std::printf("\nShadow-queue hits / evictions (thousands)\n%-12s", "app");
    for (const auto& r : results)
        if (!r.shadowHits.empty()) std::printf(" %17s", r.policy.c_str());
    std::printf("\n");
    for (size_t a = 0; a < names.size(); a++) {
        std::printf("%-12s", names[a].c_str());
        for (const auto& r : results)
            if (!r.shadowHits.empty())
                std::printf("    %6.1f / %6.1f", r.shadowHits[a] / 1e3, r.evictions[a] / 1e3);
        std::printf("\n");
    }
}

}  // namespace

int main(int argc, char** argv) {
    Options o;
    try {
        for (int i = 1; i < argc; i++) {
            std::string a = argv[i];
            auto next = [&]() -> std::string {
                if (i + 1 >= argc) throw std::invalid_argument("missing value for " + a);
                return argv[++i];
            };
            if (a == "--trace") o.tracePath = next();
            else if (a == "--app-mem") {
                auto parts = splitList(next(), ':');
                if (parts.size() != 2) throw std::invalid_argument("--app-mem expects NAME:MB");
                o.appMem.emplace_back(parts[0], std::stod(parts[1]));
            } else if (a == "--requests") o.requests = std::stoull(next());
            else if (a == "--seed") o.seed = std::stoull(next());
            else if (a == "--dump") o.dumpPath = next();
            else if (a == "--policies") o.policies = splitList(next(), ',');
            else if (a == "--reserved") o.reserved = std::stod(next());
            else if (a == "--credit") o.creditKB = std::stod(next());
            else if (a == "--tax-rate") o.taxRate = std::stod(next());
            else if (a == "--idle-seconds") o.idleSeconds = std::stod(next());
            else if (a == "--segment") o.segmentKB = static_cast<uint32_t>(std::stoul(next()));
            else if (a == "--clean-segments") o.cleanSegments = static_cast<uint32_t>(std::stoul(next()));
            else if (a == "--shadow") o.shadowMB = std::stod(next());
            else if (a == "-h" || a == "--help") { usage(); return 0; }
            else throw std::invalid_argument("unknown option: " + a);
        }
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        usage();
        return 2;
    }

    std::vector<std::string> names;
    std::vector<double> memMB;
    TraceFactory make;
    if (o.tracePath.empty()) {
        auto apps = SyntheticTrace::defaultScenario();
        for (const auto& a : apps) {
            names.push_back(a.name);
            memMB.push_back(a.memoryMB);
        }
        make = [apps, &o] { return std::make_unique<SyntheticTrace>(apps, o.requests, o.seed); };
    } else {
        if (o.appMem.empty()) {
            std::cerr << "--trace requires --app-mem for every app\n";
            return 2;
        }
        for (const auto& [n, m] : o.appMem) {
            names.push_back(n);
            memMB.push_back(m);
        }
        make = [&o, names] { return std::make_unique<CsvTrace>(o.tracePath, names); };
    }

    try {
        if (!o.dumpPath.empty()) {
            FILE* f = std::fopen(o.dumpPath.c_str(), "w");
            if (!f) throw std::runtime_error("cannot write " + o.dumpPath);
            std::fprintf(f, "# time,app,op,key,value_bytes\n");
            auto trace = make();
            Request q;
            static const char* ops[] = {"get", "set", "delete"};
            while (trace->next(&q))
                std::fprintf(f, "%.6f,%s,%s,%s,%u\n", q.time, names[q.app].c_str(),
                             ops[static_cast<int>(q.op)], q.key.c_str(), q.valueBytes);
            std::fclose(f);
            return 0;
        }

        std::printf("%zu apps, %.0f MB total, %s\n", names.size(),
                    [&] { double t = 0; for (double m : memMB) t += m; return t; }(),
                    o.tracePath.empty() ? "synthetic trace" : o.tracePath.c_str());
        std::vector<Result> results;
        for (const auto& p : o.policies) {
            std::fprintf(stderr, "running %s...\n", p.c_str());
            if (p == "memcached") results.push_back(runMemcached(make, memMB));
            else results.push_back(runMemshare(p, make, names, memMB, o));
        }
        printResults(results, names, memMB);
    } catch (const std::exception& e) {
        std::cerr << "memshare_sim: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
