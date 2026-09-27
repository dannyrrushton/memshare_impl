// memshare_server: Memshare behind the memcached text protocol.
//
// Supported commands: get, gets, set, add, replace, delete, stats, version, quit, plus
// `app <name>`, which selects the tenant for the rest of the connection (the paper's Memcachier
// deployment identifies tenants by login). Connections start on the first configured app.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "memshare/cache.h"

using namespace memshare;

namespace {

constexpr size_t kMaxKey = 250;             // memcached's key limit
constexpr uint32_t kRelativeExpiryMax = 60 * 60 * 24 * 30;  // memcached: larger = unix time

class Connection {
public:
    Connection(int fd, Cache& cache) : fd_(fd), cache_(cache) {}
    ~Connection() { ::close(fd_); }

    void run() {
        std::string line;
        while (readLine(&line)) {
            if (!handle(line)) break;
            if (!flush()) break;
        }
    }

private:
    bool fill() {
        char buf[16384];
        ssize_t n;
        do {
            n = ::recv(fd_, buf, sizeof(buf), 0);
        } while (n < 0 && errno == EINTR);
        if (n <= 0) return false;
        in_.append(buf, static_cast<size_t>(n));
        return true;
    }
    bool readLine(std::string* line) {
        for (;;) {
            auto pos = in_.find("\r\n", inPos_);
            if (pos != std::string::npos) {
                line->assign(in_, inPos_, pos - inPos_);
                inPos_ = pos + 2;
                return true;
            }
            if (in_.size() - inPos_ > 4096) return false;  // no command line is this long
            compact();
            if (!fill()) return false;
        }
    }
    bool readBytes(size_t n, std::string* out) {
        while (in_.size() - inPos_ < n) {
            compact();
            if (!fill()) return false;
        }
        out->assign(in_, inPos_, n);
        inPos_ += n;
        return true;
    }
    void compact() {
        in_.erase(0, inPos_);
        inPos_ = 0;
    }
    bool flush() {
        size_t off = 0;
        while (off < out_.size()) {
            ssize_t n = ::send(fd_, out_.data() + off, out_.size() - off, MSG_NOSIGNAL);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) return false;
            off += static_cast<size_t>(n);
        }
        out_.clear();
        return true;
    }

    static std::vector<std::string> split(const std::string& s) {
        std::vector<std::string> v;
        std::istringstream is(s);
        std::string w;
        while (is >> w) v.push_back(w);
        return v;
    }

    static bool parseU32(const std::string& s, uint32_t* out) {
        char* end = nullptr;
        errno = 0;
        unsigned long long v = std::strtoull(s.c_str(), &end, 10);
        if (errno || end == s.c_str() || *end || v > UINT32_MAX) return false;
        *out = static_cast<uint32_t>(v);
        return true;
    }

    bool handle(const std::string& line) {
        auto w = split(line);
        if (w.empty()) {
            out_ += "ERROR\r\n";
            return true;
        }
        const std::string& cmd = w[0];
        if (cmd == "get" || cmd == "gets") {
            std::string value;
            uint32_t flags;
            for (size_t i = 1; i < w.size(); i++) {
                if (!cache_.get(app_, w[i], &value, &flags)) continue;
                out_ += "VALUE " + w[i] + " " + std::to_string(flags) + " " +
                        std::to_string(value.size());
                if (cmd == "gets") out_ += " 0";
                out_ += "\r\n" + value + "\r\n";
            }
            out_ += "END\r\n";
        } else if (cmd == "set" || cmd == "add" || cmd == "replace") {
            return handleStore(w);
        } else if (cmd == "delete") {
            if (w.size() < 2) {
                out_ += "ERROR\r\n";
            } else {
                bool ok = cache_.del(app_, w[1]);
                if (!(w.size() > 2 && w.back() == "noreply"))
                    out_ += ok ? "DELETED\r\n" : "NOT_FOUND\r\n";
            }
        } else if (cmd == "app") {
            auto id = w.size() == 2 ? cache_.findApp(w[1]) : std::nullopt;
            if (id) {
                app_ = *id;
                out_ += "OK\r\n";
            } else {
                out_ += "CLIENT_ERROR unknown app\r\n";
            }
        } else if (cmd == "stats") {
            writeStats();
        } else if (cmd == "version") {
            out_ += "VERSION memshare-1.0\r\n";
        } else if (cmd == "quit") {
            return false;
        } else {
            out_ += "ERROR\r\n";
        }
        return true;
    }

    // <cmd> <key> <flags> <exptime> <bytes> [noreply]\r\n<data>\r\n
    bool handleStore(const std::vector<std::string>& w) {
        uint32_t flags, exptime, bytes;
        if (w.size() < 5 || w.size() > 6 || !parseU32(w[2], &flags) || !parseU32(w[3], &exptime) ||
            !parseU32(w[4], &bytes)) {
            out_ += "CLIENT_ERROR bad command line format\r\n";
            return true;
        }
        bool noreply = w.size() == 6 && w[5] == "noreply";
        std::string data;
        if (!readBytes(size_t(bytes) + 2, &data)) return false;
        if (data.compare(bytes, 2, "\r\n") != 0) {
            out_ += "CLIENT_ERROR bad data chunk\r\n";
            return true;
        }
        data.resize(bytes);
        const std::string& key = w[1];
        if (key.size() > kMaxKey) {
            if (!noreply) out_ += "CLIENT_ERROR key too long\r\n";
            return true;
        }
        if (exptime > kRelativeExpiryMax) {
            auto now = static_cast<uint32_t>(std::time(nullptr));
            if (exptime <= now) {  // already expired: memcached treats this as a delete
                cache_.del(app_, key);
                if (!noreply) out_ += "STORED\r\n";
                return true;
            }
            exptime -= now;
        }
        // add/replace check existence first; like memcached's text protocol without CAS, a racing
        // writer can slip in between the check and the store.
        if (w[0] != "set") {
            bool exists = cache_.get(app_, key, nullptr);
            if (exists != (w[0] == "replace")) {
                if (!noreply) out_ += "NOT_STORED\r\n";
                return true;
            }
        }
        SetResult r = cache_.set(app_, key, data, flags, exptime);
        if (noreply) return true;
        switch (r) {
            case SetResult::Stored: out_ += "STORED\r\n"; break;
            case SetResult::TooLarge: out_ += "SERVER_ERROR object too large for cache\r\n"; break;
            case SetResult::NoMemory: out_ += "SERVER_ERROR out of memory storing object\r\n"; break;
        }
        return true;
    }

    void stat(const std::string& name, const std::string& value) {
        out_ += "STAT " + name + " " + value + "\r\n";
    }

    void writeStats() {
        CacheStats s = cache_.stats();
        stat("segment_bytes", std::to_string(s.segmentBytes));
        stat("segments_total", std::to_string(s.totalSegments));
        stat("segments_free", std::to_string(s.freeSegments));
        stat("item_capacity", std::to_string(s.itemCapacity));
        stat("cleaner_passes", std::to_string(s.cleaner.passes));
        stat("cleaner_bytes_relocated", std::to_string(s.cleaner.bytesRelocated));
        stat("cleaner_bytes_evicted", std::to_string(s.cleaner.bytesEvicted));
        stat("cleaner_seconds", std::to_string(s.cleaner.seconds));
        for (const AppStats& a : s.apps) {
            std::string p = "app:" + a.name + ":";
            stat(p + "get_hits", std::to_string(a.hits));
            stat(p + "get_misses", std::to_string(a.misses));
            stat(p + "sets", std::to_string(a.sets));
            stat(p + "shadow_hits", std::to_string(a.shadowHits));
            stat(p + "evictions", std::to_string(a.evictedItems));
            stat(p + "actual_bytes", std::to_string(a.actualBytes));
            stat(p + "target_bytes", std::to_string(a.targetBytes));
            stat(p + "reserved_bytes", std::to_string(a.reservedBytes));
            stat(p + "pooled_bytes", std::to_string(a.pooledBytes));
            stat(p + "idle_bytes", std::to_string(a.idleBytes));
            stat(p + "need", std::to_string(a.need));
        }
        out_ += "END\r\n";
    }

    int fd_;
    Cache& cache_;
    AppId app_ = 0;
    std::string in_;
    size_t inPos_ = 0;
    std::string out_;
};

void usage() {
    std::cerr <<
        "usage: memshare_server [options]\n"
        "  -p, --port N            TCP port (default 11211)\n"
        "  -m, --memory MB         cache memory (default 1024)\n"
        "  --segment KB            segment size (default 1024)\n"
        "  --clean-segments N      segments per cleaning pass, n (default 100)\n"
        "  --cleaner-threads N     background cleaner threads (default 1)\n"
        "  --policy P              static | pooled | idletax (default pooled)\n"
        "  --tax-rate F            idle tax rate in [0,1] (default 0.5)\n"
        "  --idle-seconds S        idle time for the tax (default 18000)\n"
        "  --app NAME:RESERVED_MB[:CREDIT_KB[:lru|lfu|slru[:SHADOW_MB]]]\n"
        "                          add a tenant (repeatable; default: one app \"default\")\n";
}

AppConfig parseApp(const std::string& spec) {
    std::vector<std::string> parts;
    std::stringstream ss(spec);
    std::string p;
    while (std::getline(ss, p, ':')) parts.push_back(p);
    if (parts.size() < 2) throw std::invalid_argument("bad --app spec: " + spec);
    AppConfig a;
    a.name = parts[0];
    a.reservedBytes = static_cast<uint64_t>(std::stod(parts[1]) * (1 << 20));
    if (parts.size() > 2) a.creditBytes = static_cast<uint64_t>(std::stod(parts[2]) * 1024);
    if (parts.size() > 3) {
        if (parts[3] == "lru") a.rank = RankPolicy::LRU;
        else if (parts[3] == "lfu") a.rank = RankPolicy::LFU;
        else if (parts[3] == "slru") a.rank = RankPolicy::SegmentedLRU;
        else throw std::invalid_argument("unknown rank policy: " + parts[3]);
    }
    if (parts.size() > 4) a.shadowQueueBytes = static_cast<uint64_t>(std::stod(parts[4]) * (1 << 20));
    return a;
}

}  // namespace

int main(int argc, char** argv) {
    Config cfg;
    cfg.totalBytes = 1024ull << 20;
    int port = 11211;
    try {
        for (int i = 1; i < argc; i++) {
            std::string a = argv[i];
            auto next = [&]() -> std::string {
                if (i + 1 >= argc) throw std::invalid_argument("missing value for " + a);
                return argv[++i];
            };
            if (a == "-p" || a == "--port") port = std::stoi(next());
            else if (a == "-m" || a == "--memory") cfg.totalBytes = std::stoull(next()) << 20;
            else if (a == "--segment") cfg.segmentBytes = static_cast<uint32_t>(std::stoul(next()) << 10);
            else if (a == "--clean-segments") cfg.cleanSegments = static_cast<uint32_t>(std::stoul(next()));
            else if (a == "--cleaner-threads") cfg.cleanerThreads = static_cast<uint32_t>(std::stoul(next()));
            else if (a == "--tax-rate") cfg.taxRate = std::stod(next());
            else if (a == "--idle-seconds") cfg.idleSeconds = std::stod(next());
            else if (a == "--app") cfg.apps.push_back(parseApp(next()));
            else if (a == "--policy") {
                std::string p = next();
                if (p == "static") cfg.policy = SharingPolicy::Static;
                else if (p == "pooled") cfg.policy = SharingPolicy::Pooled;
                else if (p == "idletax") cfg.policy = SharingPolicy::IdleTax;
                else throw std::invalid_argument("unknown policy: " + p);
            } else if (a == "-h" || a == "--help") {
                usage();
                return 0;
            } else {
                throw std::invalid_argument("unknown option: " + a);
            }
        }
        if (cfg.apps.empty()) cfg.apps.push_back(AppConfig{"default"});
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        usage();
        return 2;
    }

    std::unique_ptr<Cache> cache;
    try {
        cache = std::make_unique<Cache>(cfg);
    } catch (const std::exception& e) {
        std::cerr << "memshare_server: " << e.what() << "\n";
        return 1;
    }

    int ls = ::socket(AF_INET6, SOCK_STREAM, 0);
    int one = 1, zero = 0;
    ::setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    ::setsockopt(ls, IPPROTO_IPV6, IPV6_V6ONLY, &zero, sizeof(zero));
    sockaddr_in6 addr{};
    addr.sin6_family = AF_INET6;
    addr.sin6_addr = in6addr_any;
    addr.sin6_port = htons(static_cast<uint16_t>(port));
    if (::bind(ls, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0 || ::listen(ls, 128) < 0) {
        std::perror("memshare_server: bind/listen");
        return 1;
    }
    ::signal(SIGPIPE, SIG_IGN);
    auto st = cache->stats();
    std::cerr << "memshare_server listening on port " << port << ": " << (cfg.totalBytes >> 20)
              << " MB, " << st.totalSegments << " segments, " << cfg.apps.size() << " app(s)\n";

    for (;;) {
        int fd = ::accept(ls, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EINTR) continue;
            std::perror("accept");
            continue;
        }
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        std::thread([fd, &cache] { Connection(fd, *cache).run(); }).detach();
    }
}
