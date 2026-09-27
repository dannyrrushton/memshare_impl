# memshare

A C++17 implementation of **Memshare**, the multi-tenant key-value cache from
[*Memshare: a Dynamic Multi-tenant Key-value Cache*](https://www.usenix.org/system/files/conference/atc17/atc17-cidon.pdf)
(Cidon, Rushton, Rumble, Stutsman; USENIX ATC 2017).

Memshare replaces memcached's slab allocator with a segmented in-memory log, so memory can move
freely between applications (tenants) and between item sizes. Each application is guaranteed its
reserved memory. An arbiter lends the rest to whichever application would gain the most hits,
judged by hits in per-application shadow queues. A cleaner enforces those decisions by choosing
what to evict and compacting the log.

It builds three programs:

| Program | What it is |
|---|---|
| `memshare_server` | Memshare behind the memcached text protocol (multi-threaded, background cleaner) |
| `memshare_sim` | Replays a trace against Memshare's policies and a memcached-style slab baseline and compares hit rates |
| `memshare_tests` | Unit, policy and concurrency tests |

## Building

Requirements: CMake 3.20+, a C++17 compiler, Linux (the server uses POSIX sockets).

```bash
cmake -S . -B build -G Ninja
cmake --build build
./build/memshare_tests
```

To build with a sanitizer, add `-DMEMSHARE_SANITIZE=tsan` or `-DMEMSHARE_SANITIZE=asan` to the
configure step, using a separate build directory.

## How it maps to the paper

| Paper | Code |
|---|---|
| Segmented log, head segment, free list (§3, §5.1) | `src/memshare/log.{h,cpp}` |
| Hash table with bucket locks | `src/memshare/hash_table.{h,cpp}` |
| need = targetMem / actualMem, ranking functions (§3.1) | `src/memshare/arbiter.{h,cpp}` |
| Shadow queues of 64-bit key hashes (§4, §5.2) | `src/memshare/shadow_queue.{h,cpp}` |
| Algorithm 1: which items survive a cleaning pass | `Arbiter::chooseSurvivors` |
| Algorithm 2: pooled memory credits | `Arbiter::onMiss` |
| Algorithm 3: idle tax | `Arbiter::recomputeTargetsLocked`, `Cache::updateIdleTax` |
| Cleaning passes, segment selection (half random, half by lowest need) (§3.1, §5.3) | `src/memshare/cleaner.{h,cpp}`, `Log::takeCandidates` |
| Epochs protecting segment reuse from in-flight GETs (§5.3) | `src/memshare/epoch.{h,cpp}` |
| Cleaner kept separate from apps and ranking (§5.4) | the cleaner only calls the arbiter and the hash table |
| memcached front end (§5) | `src/server/main.cpp` |

### Sharing policies

- **Static** (`static`): targetMem = reservedMem plus an equal share of any unreserved memory, and
  never changes. Memory an application is not using can still be used by others; the cleaner takes
  it back from whichever application is furthest over its target.
- **Pooled** (`pooled`, Algorithm 2): unreserved memory starts split equally. Each GET miss that
  hits the application's shadow queue moves one credit (default 64 KB, configurable per
  application, §4.1) of pooled memory to it from a random application that has at least a credit's
  worth. Applications never drop below their reservation.
- **Idle tax** (`idletax`, Algorithm 3): targetMem = reservedMem / τ, where
  τ = (1 − activeFraction·taxRate) / (1 − taxRate). Idle memory is memory not accessed within
  `idleSeconds`.

### Ranking functions

Each application picks a ranking function `rank(t, f)` of its items' last access `t` and access
count `f`: `LRU` (rank = t, the default), `LFU` (rank = f, ties broken by t) or `SegmentedLRU`
(items accessed more than once outrank all items accessed once, then by t).

### The cleaner

When fewer than 1% of segments are free, a cleaning pass runs:

1. Pick *n* closed segments (default 100): half at random, half with the most data from
   applications with the lowest need. Dead space counts as the most attractive data.
2. Find their live items and ask the arbiter which to keep (Algorithm 1). It repeatedly takes the
   application with the highest need and keeps that application's highest-ranked remaining item.
   Each application's need is recalculated after every kept item, and the pass stops at the first
   item that does not fit in *n − 1* segments.
3. Evict the rest. Their keys go into their applications' shadow queues, and each application's
   byte count is updated with one lock acquisition per pass.
4. Copy the survivors into free segments and free the *n* source segments.

## Implementation choices the paper leaves open

- **Copying survivors in source order.** Algorithm 1 decides which items survive exactly as the
  paper describes. The copy step (4) then moves them one source segment at a time, not in rank
  order. Each source segment can then be freed and reused as an output as soon as its survivors are
  copied. As a result a pass needs only about two spare segments, not *n − 1*: the cleaner reserve
  is `2 × cleaner threads + 1` segments. Items cannot straddle segments, so before copying, the
  pass simulates the packing. If the survivors would not fit, it drops the ones Algorithm 1 chose
  last.
- **Rank metadata lives in the hash table**, not in the log. GETs never write to log memory, so
  items are immutable once written, as §5.3 requires.
- **Per-segment, per-application live-byte counters** drive the "most data from low-need apps"
  segment choice. A segment's score is Σ live(app) / need(app), plus dead bytes weighted at twice
  the highest application weight.
- **Epoch protocol.** Each request publishes the global epoch it started in. A freed segment is
  tagged with the epoch in which its last hash table reference was removed. It is reused only once
  no in-flight request started in that epoch or earlier.
- **Shadow queue hits consume the entry.** The key is about to be reinserted, so one missing item
  earns only one credit.
- **Cleaner threads** are a fixed pool (`--cleaner-threads`), not started on demand as in §5.3.
- **Skipped optimization:** a pass whose last output segment is under 50% full does not evict that
  segment's items (end of §3.2).
- **Expiry and `add`/`replace`.** Items can expire (memcached `exptime`). The server's
  `add`/`replace` check for the key and then store it, so they are not atomic.

## Running the server

```bash
./build/memshare_server -p 11211 -m 1024 --policy pooled \
    --app shop:256 --app search:256:128:lfu --app batch:0
```

`--app NAME:RESERVED_MB[:CREDIT_KB[:lru|lfu|slru[:SHADOW_MB]]]` adds a tenant. With no `--app`,
there is a single tenant named `default`. The protocol is memcached's text protocol (`get`,
`gets`, `set`, `add`, `replace`, `delete`, `stats`, `version`, `quit`) plus `app <name>`, which
switches the connection's tenant. `stats` reports per-tenant hits, misses, shadow-queue hits,
evictions, and actual, target, reserved, pooled and idle bytes, along with cleaner work.

```
$ printf 'app search\r\nset k 0 0 5\r\nhello\r\nget k\r\n' | nc -q1 localhost 11211
OK
STORED
VALUE k 0 5
hello
END
```

## Running the simulator

```bash
./build/memshare_sim                     # built-in synthetic 5-tenant scenario, all policies
./build/memshare_sim --trace t.csv --app-mem web:64 --app-mem api:32
```

A trace is CSV: `time,app,op,key,value_bytes`, where op is `get`, `set` or `delete`. The cache is
treated as a lookaside cache, so a GET miss is followed by a SET of the item.
`--dump FILE` writes the synthetic trace in this format.

It compares these configurations:

- `memcached`: slab allocator, 1 MB slabs, 64 B·2ⁱ size classes, per-class LRU, each tenant
  statically limited to the memory it bought (Memcachier's scheme, §2)
- `static`: Memshare with 100% of memory reserved
- `pooled`: 75% reserved (`--reserved`)
- `pooled0`: no reservations
- `idletax`: 100% reserved, 50% tax

All Memshare configurations get the same total memory as the baseline. The cleaner reserve and
free headroom come out of that total.

The built-in scenario has five tenants:

- **steady:** its working set is larger than the memory it bought.
- **overprov:** a small, hot working set.
- **bursty:** hourly 15-minute bursts of new keys at 8× the request rate.
- **tiny:** 10–80 byte values, which the slab allocator stores worst.
- **fading:** stops sending requests after an hour but keeps its memory.

Results for the default 12M-request run (about 3.5 simulated hours):

```
app            mem MB  memcached     static     pooled    pooled0    idletax
steady           48.0      56.24      65.78      67.06      70.77      68.28
overprov         48.0      99.02      99.02      99.02      96.63      99.02
bursty           24.0      60.85      68.94      74.36      75.58      69.83
tiny             16.0      85.19      90.13      89.68      86.83      89.94
fading           32.0      63.76      76.75      78.15      79.73      77.43
combined                   74.31      80.35      81.73      81.98      81.28

policy         hit rate  miss reduc.   cleaner MB/s cleaner passes
memcached        74.31%        0.00%          0.000              0
static           80.35%       23.52%          6.214           1958
pooled           81.73%       28.89%          5.630           1774
pooled0          81.98%       29.83%          5.294           1668
idletax          81.28%       27.12%          5.903           1860
```

The ordering matches the paper's (§4, Tables 3 and 6):

- The log allocator alone (`static` against `memcached`, with the same partitions) removes about
  a quarter of the misses. The gain comes from eliminating slab fragmentation and letting tenants
  use memory others leave unused.
- Pooling adds more. `overprov` keeps its hit rate under `pooled` because it keeps 75% of its
  memory reserved. With no reservations (`pooled0`) it gives up some hit rate for the combined
  total.
- The idle tax reclaims memory from `fading` after it goes quiet.

Miss reduction is measured against the first policy listed. The cleaner's bandwidth is the bytes
it relocated per second of trace time.

Under the pooled policy, credits flow until the tenants' shadow-queue hit rates are roughly equal
(the local hit-rate gradients match, §4). A tenant that is already below its target can keep
collecting credits it does not use. That is harmless: eviction is driven by each tenant's need
relative to the others, so the unused target does not hurt anyone.

These numbers come from a synthetic workload, not the Memcachier trace. The paper's week-long
trace is not public, so the absolute numbers are not comparable to the paper's.
