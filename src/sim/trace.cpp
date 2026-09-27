#include "sim/trace.h"

#include <algorithm>
#include <cmath>
#include <sstream>
#include <stdexcept>

#include "memshare/util.h"

namespace sim {

CsvTrace::CsvTrace(const std::string& path, std::vector<std::string> appNames)
    : in_(path), names_(std::move(appNames)) {
    if (!in_) throw std::runtime_error("cannot open trace " + path);
    for (uint32_t i = 0; i < names_.size(); i++) ids_[names_[i]] = i;
}

bool CsvTrace::next(Request* r) {
    std::string line;
    while (std::getline(in_, line)) {
        line_++;
        if (line.empty() || line[0] == '#') continue;
        std::stringstream ss(line);
        std::string time, app, op, key, bytes;
        if (!std::getline(ss, time, ',') || !std::getline(ss, app, ',') ||
            !std::getline(ss, op, ',') || !std::getline(ss, key, ',')) {
            throw std::runtime_error("trace line " + std::to_string(line_) + ": expected time,app,op,key,bytes");
        }
        std::getline(ss, bytes, ',');
        r->time = std::stod(time);
        auto it = ids_.find(app);
        if (it == ids_.end())
            throw std::runtime_error("trace line " + std::to_string(line_) + ": app " + app +
                                     " has no --app-mem");
        r->app = it->second;
        if (op == "get") r->op = Request::Op::Get;
        else if (op == "set") r->op = Request::Op::Set;
        else if (op == "delete") r->op = Request::Op::Delete;
        else throw std::runtime_error("trace line " + std::to_string(line_) + ": unknown op " + op);
        r->key = key;
        r->valueBytes = bytes.empty() ? 0 : static_cast<uint32_t>(std::stoul(bytes));
        return true;
    }
    return false;
}

uint64_t SyntheticTrace::Zipf::sample(std::mt19937_64& rng) const {
    double u = std::uniform_real_distribution<double>(0, cdf.back())(rng);
    return static_cast<uint64_t>(std::lower_bound(cdf.begin(), cdf.end(), u) - cdf.begin());
}

static void buildZipf(std::vector<double>* cdf, uint64_t n, double s) {
    cdf->resize(n);
    double sum = 0;
    for (uint64_t i = 0; i < n; i++) {
        sum += 1.0 / std::pow(double(i + 1), s);
        (*cdf)[i] = sum;
    }
}

SyntheticTrace::SyntheticTrace(std::vector<SyntheticApp> apps, uint64_t requests, uint64_t seed)
    : apps_(std::move(apps)), remaining_(requests), rng_(seed) {
    zipf_.resize(apps_.size());
    burstZipf_.resize(apps_.size());
    for (size_t i = 0; i < apps_.size(); i++) {
        names_.push_back(apps_[i].name);
        buildZipf(&zipf_[i].cdf, std::max<uint64_t>(1, apps_[i].keys), apps_[i].zipf);
        if (apps_[i].burstKeys > 0) buildZipf(&burstZipf_[i].cdf, apps_[i].burstKeys, apps_[i].zipf);
    }
}

bool SyntheticTrace::inBurst(size_t a, double t) const {
    const SyntheticApp& app = apps_[a];
    return app.burstPeriod > 0 && std::fmod(t, app.burstPeriod) < app.burstLength;
}

double SyntheticTrace::rateAt(size_t a, double t) const {
    const SyntheticApp& app = apps_[a];
    if (app.activeUntil > 0 && t >= app.activeUntil) return 0;
    return app.rate * (inBurst(a, t) ? app.burstRate : 1.0);
}

uint32_t SyntheticTrace::valueSize(size_t a, uint64_t key, bool burst) const {
    const SyntheticApp& app = apps_[a];
    uint64_t h = memshare::mix64(key * 2 + burst + (uint64_t(a) << 40));
    double u = double(h >> 11) / double(1ull << 53);
    double lo = std::log(double(app.minValue)), hi = std::log(double(app.maxValue));
    return static_cast<uint32_t>(std::exp(lo + u * (hi - lo)));
}

bool SyntheticTrace::next(Request* r) {
    if (remaining_ == 0) return false;
    std::vector<double> rates(apps_.size());
    double total = 0;
    for (size_t a = 0; a < apps_.size(); a++) total += rates[a] = rateAt(a, now_);
    if (total <= 0) return false;
    now_ += std::exponential_distribution<double>(total)(rng_);
    double u = std::uniform_real_distribution<double>(0, total)(rng_);
    size_t a = 0;
    while (a + 1 < apps_.size() && u >= rates[a]) u -= rates[a++];

    const SyntheticApp& app = apps_[a];
    bool burst = inBurst(a, now_) && app.burstKeys > 0 &&
                 std::uniform_real_distribution<double>(0, 1)(rng_) < app.burstMix;
    uint64_t key = burst ? burstZipf_[a].sample(rng_) : zipf_[a].sample(rng_);
    r->time = now_;
    r->app = static_cast<uint32_t>(a);
    r->op = std::uniform_real_distribution<double>(0, 1)(rng_) < app.writeFraction ? Request::Op::Set
                                                                                  : Request::Op::Get;
    r->key = (burst ? "b" : "k") + std::to_string(key);
    r->valueBytes = valueSize(a, key, burst);
    remaining_--;
    return true;
}

std::vector<SyntheticApp> SyntheticTrace::defaultScenario() {
    std::vector<SyntheticApp> v;
    // A large, steady app whose working set is bigger than what it bought.
    SyntheticApp a;
    a.name = "steady";
    a.memoryMB = 48;
    a.rate = 300;
    a.keys = 400000;
    a.zipf = 0.85;
    a.minValue = 200;
    a.maxValue = 2000;
    v.push_back(a);
    // Over-provisioned: small, hot working set.
    SyntheticApp b;
    b.name = "overprov";
    b.memoryMB = 48;
    b.rate = 250;
    b.keys = 30000;
    b.zipf = 1.0;
    b.minValue = 100;
    b.maxValue = 1500;
    v.push_back(b);
    // Bursty: periodically floods the cache with a new set of keys (like app 7 in the paper).
    SyntheticApp c;
    c.name = "bursty";
    c.memoryMB = 24;
    c.rate = 60;
    c.keys = 50000;
    c.zipf = 0.9;
    c.minValue = 300;
    c.maxValue = 3000;
    c.burstPeriod = 3600;
    c.burstLength = 900;
    c.burstRate = 8;
    c.burstMix = 0.8;
    c.burstKeys = 150000;
    v.push_back(c);
    // Tiny items; slab allocators waste the most memory on these.
    SyntheticApp d;
    d.name = "tiny";
    d.memoryMB = 16;
    d.rate = 200;
    d.keys = 300000;
    d.zipf = 0.95;
    d.minValue = 10;
    d.maxValue = 80;
    v.push_back(d);
    // Stops sending requests after an hour but keeps its memory.
    SyntheticApp e;
    e.name = "fading";
    e.memoryMB = 32;
    e.rate = 150;
    e.keys = 60000;
    e.zipf = 0.8;
    e.minValue = 400;
    e.maxValue = 4000;
    e.activeUntil = 3600;
    v.push_back(e);
    return v;
}

}  // namespace sim
