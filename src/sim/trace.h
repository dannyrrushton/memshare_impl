// Request traces for the simulator: a CSV reader and a synthetic multi-tenant generator.
#pragma once

#include <cstdint>
#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

namespace sim {

struct Request {
    enum class Op { Get, Set, Delete };
    double time = 0;  // seconds
    uint32_t app = 0;
    Op op = Op::Get;
    std::string key;
    uint32_t valueBytes = 0;  // for sets, and for the fill after a GET miss
};

class TraceSource {
public:
    virtual ~TraceSource() = default;
    virtual bool next(Request* r) = 0;
    virtual const std::vector<std::string>& appNames() const = 0;
};

// CSV lines: time,app,op,key,value_bytes   (op is get, set or delete; '#' starts a comment).
// Apps are named by the second column and numbered by their position in appNames.
class CsvTrace : public TraceSource {
public:
    CsvTrace(const std::string& path, std::vector<std::string> appNames);
    bool next(Request* r) override;
    const std::vector<std::string>& appNames() const override { return names_; }

private:
    std::ifstream in_;
    std::vector<std::string> names_;
    std::unordered_map<std::string, uint32_t> ids_;
    uint64_t line_ = 0;
};

struct SyntheticApp {
    std::string name;
    double memoryMB = 16;  // memory the app bought (partitioned allocation)
    double rate = 100;     // requests per second while active
    uint64_t keys = 100000;
    double zipf = 0.9;
    uint32_t minValue = 100, maxValue = 1000;  // log-uniform value sizes, fixed per key
    double writeFraction = 0.02;               // explicit sets (updates) among requests
    // Bursts: every burstPeriod seconds, for burstLength seconds, the rate is multiplied by
    // burstRate and a fraction burstMix of requests go to a separate set of burstKeys keys.
    double burstPeriod = 0, burstLength = 0, burstRate = 1, burstMix = 0;
    uint64_t burstKeys = 0;
    // The app stops sending requests after this many seconds (0 = never).
    double activeUntil = 0;
};

class SyntheticTrace : public TraceSource {
public:
    SyntheticTrace(std::vector<SyntheticApp> apps, uint64_t requests, uint64_t seed);
    bool next(Request* r) override;
    const std::vector<std::string>& appNames() const override { return names_; }
    const std::vector<SyntheticApp>& apps() const { return apps_; }

    // The built-in scenario: a mix of steady, bursty, fading, small-item and large-item tenants.
    static std::vector<SyntheticApp> defaultScenario();

private:
    struct Zipf {
        std::vector<double> cdf;
        uint64_t sample(std::mt19937_64& rng) const;
    };
    double rateAt(size_t app, double t) const;
    bool inBurst(size_t app, double t) const;
    uint32_t valueSize(size_t app, uint64_t key, bool burst) const;

    std::vector<SyntheticApp> apps_;
    std::vector<std::string> names_;
    std::vector<Zipf> zipf_, burstZipf_;
    uint64_t remaining_;
    double now_ = 0;
    std::mt19937_64 rng_;
};

}  // namespace sim
