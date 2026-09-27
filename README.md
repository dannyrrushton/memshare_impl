# memshare

A C++17 implementation of **Memshare**, the multi-tenant key-value cache described in
[*Memshare: a Dynamic Multi-tenant Key-value Cache*](https://www.usenix.org/system/files/conference/atc17/atc17-cidon.pdf)
by Asaf Cidon, Daniel Rushton, Stephen M. Rumble and Ryan Stutsman (USENIX ATC 2017).

This document explains:

- what Memshare is and why it exists
- how this implementation works, component by component
- how requests, cleaning and memory sharing interact, including the concurrency design
- every configuration option
- how to use the library, the server, the simulator and the tests
- where the implementation departs from the paper

## Contents

1. [The problem Memshare solves](#1-the-problem-memshare-solves)
2. [Quick start](#2-quick-start)
3. [Repository layout](#3-repository-layout)
4. [Core concepts](#4-core-concepts)
5. [Architecture](#5-architecture)
6. [The log](#6-the-log)
7. [The hash table](#7-the-hash-table)
8. [The arbiter and sharing policies](#8-the-arbiter-and-sharing-policies)
9. [The cleaner](#9-the-cleaner)
10. [Life of a request](#10-life-of-a-request)
11. [Concurrency](#11-concurrency)
12. [Memory accounting and overhead](#12-memory-accounting-and-overhead)
13. [Configuration reference](#13-configuration-reference)
14. [Using the library](#14-using-the-library)
15. [The server](#15-the-server)
16. [The simulator](#16-the-simulator)
17. [Tests](#17-tests)
18. [Differences from the paper](#18-differences-from-the-paper)
19. [Limitations](#19-limitations)

---

## 1. The problem Memshare solves

Web applications put DRAM key-value caches such as memcached in front of their databases. Hit rate
is what matters: with a 100 µs cache and a 10 ms database, each point of hit rate cuts average
latency and database load substantially.

Operators that run one cache for many applications (tenants) usually **partition memory
statically**. Each tenant buys or is assigned a fixed amount. This wastes memory in two ways:

- **Across tenants.** Some tenants sit on memory they barely use while others are starved,
  especially during bursts.
- **Inside the allocator.** memcached's *slab allocator* sorts items into size classes
  (64 B, 128 B, …, 1 MB). Each 1 MB slab is permanently assigned to one class. Memory can only
  move between tenants or sizes 1 MB at a time, which means evicting a whole slab of possibly hot
  items, and each class wastes up to half of every chunk.

Memshare fixes both problems:

- **A log-structured allocator.** Items of every size and every tenant are packed together in
  segments of an in-memory log, so memory can be reassigned a few bytes at a time.
- **A sharing model.** Each tenant has *reserved* memory it is guaranteed. The rest is *pooled*
  and lent to whichever tenants would gain the most hits.
- **An arbiter and a cleaner.** The arbiter decides how much memory each tenant should have. The
  cleaner enforces those decisions by choosing which items to evict, using each tenant's own
  eviction policy, and by compacting the log.

The log costs some CPU and memory bandwidth to compact. Caches are almost always limited by memory
capacity, not CPU, so trading idle CPU for hit rate is a good deal.

## 2. Quick start

Requirements: Linux, CMake 3.20+, and a C++17 compiler (tested with GCC 16 and Clang 22). Ninja is
optional.

```bash
cmake -S . -B build -G Ninja
cmake --build build

./build/memshare_tests                      # run the test suite
./build/memshare_sim --requests 2000000     # compare policies on a synthetic workload (~30 s)
./build/memshare_server -m 256 --app alice:64 --app bob:64   # memcached-compatible server on :11211
```

Sanitizer builds:

```bash
cmake -S . -B build-tsan -G Ninja -DMEMSHARE_SANITIZE=tsan -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake -S . -B build-asan -G Ninja -DMEMSHARE_SANITIZE=asan -DCMAKE_BUILD_TYPE=RelWithDebInfo
```

## 3. Repository layout

```
CMakeLists.txt
src/memshare/            the cache library (libmemshare.a)
  config.h               Config, AppConfig, RankPolicy, SharingPolicy
  cache.{h,cpp}          Cache: the public API that ties everything together
  log.{h,cpp}            segmented log, item layout, free/closed segment lists
  hash_table.{h,cpp}     key -> item location, plus per-item access info
  arbiter.{h,cpp}        per-tenant accounting, need, policies, Algorithm 1 decisions
  shadow_queue.{h,cpp}   per-tenant LRU list of evicted key hashes
  cleaner.{h,cpp}        cleaning passes: select, decide, evict, compact
  epoch.{h,cpp}          epoch-based protection for segment reuse
  util.h                 spin lock, key hashing, alignment
src/server/main.cpp      memshare_server: memcached text protocol front end
src/sim/                 memshare_sim: trace replay and policy comparison
  trace.{h,cpp}          CSV trace reader and synthetic multi-tenant workload generator
  slab_cache.{h,cpp}     memcached-style partitioned slab cache, the baseline
  main.cpp               driver and report
tests/tests.cpp          memshare_tests
```

## 4. Core concepts

**Application (tenant).** Every item belongs to one application, identified by an `AppId` (its
index in `Config::apps`). Applications have separate key spaces: key `x` of app 0 and key `x` of
app 1 are different items.

**reservedMem.** Memory an application is guaranteed (`AppConfig::reservedBytes`). The arbiter
never sets its target below this, except under the idle tax, which exists to reclaim unused
reservations.

**pooledMem.** Everything not reserved: the cache's item capacity minus the sum of reservations.
It starts split equally among applications, and the pooled policy moves it around.

**targetMem.** How much memory the application *should* have right now. Its value depends on the
policy (§8).

**actualMem.** Bytes the application's live items currently occupy in the log, including item
headers and padding.

**need.** The quantity everything else revolves around:

```
need(app) = targetMem(app) / actualMem(app)
```

need > 1 means the application has less than its share; need < 1 means it has more. If actualMem
is 0, need is infinite (or 0 when the target is also 0). When memory must be freed, the cleaner
takes it from applications with the lowest need.

**Rank.** Each application chooses how its own items are prioritized for eviction, as a function
`rank(t, f)` of an item's last access time `t` and access count `f`. Higher rank means more worth
keeping. See §8.4.

**Shadow queue.** For each application, an LRU list of the keys most recently evicted (only 64-bit
hashes plus sizes, no values), covering a configured number of item bytes (10 MB by default). A GET
that misses the cache but hits the shadow queue *would have been a hit* if the application had that
much more memory. The rate of shadow hits therefore estimates how many extra hits more memory would
buy the application: the local slope of its hit-rate curve.

**Segments.** The log is one arena cut into fixed-size segments (1 MB by default). A segment is:

- **free**, on the free list;
- the **head**, currently receiving new items; or
- **closed**, full and eligible for cleaning.

## 5. Architecture

```
                    get / set / delete  (any number of threads)
                               |
                               v
   +---------------------------------------------------------------+
   |  Cache                                                        |
   |                                                               |
   |   HashTable ---------------------------> Log                  |
   |   (app,key) -> item address               [seg][seg][head]... |
   |   + last access, access count             free list, closed   |
   |        ^                                   list, reserve      |
   |        |                                        ^             |
   |        |           Cleaner ---------------------+             |
   |        +---------- (select segments, evict, compact)          |
   |                        |                                      |
   |                        v                                      |
   |   Arbiter: per-app actualMem / targetMem / need,              |
   |            shadow queues, policy, Algorithm 1 decisions       |
   +---------------------------------------------------------------+
        background: cleaner thread(s), idle-tax scan (1 s)
```

The components are deliberately decoupled, as in §5.4 of the paper:

- **Log.** Knows nothing about keys, applications or policies. It hands out bytes and manages
  segment lists.
- **HashTable.** Maps keys to locations and remembers access information.
- **Arbiter.** Owns every policy decision: how much memory each application should have, and which
  items survive cleaning.
- **Cleaner.** Mechanics only. It finds live items, asks the arbiter what to keep, evicts the rest,
  copies survivors and frees segments.
- **Cache.** The public API and the glue: request paths, when to clean, and background threads.

## 6. The log

Source: `src/memshare/log.{h,cpp}`.

### 6.1 Arena and segments

At construction the log allocates `floor(totalBytes / segmentBytes)` segments as one contiguous,
4 KB-aligned arena. Because the arena is contiguous, finding an item's segment is a division:
`(item_address - arena) / segmentBytes`.

Each `Segment` holds:

| Field | Purpose |
|---|---|
| `data` | pointer to its bytes in the arena |
| `used` | bytes handed out so far |
| `pendingWriters` | writers given space here that have not finished writing. The cleaner never touches a segment while this is non-zero. |
| `live[app]` | live bytes per application. `used − Σ live` is dead space. Used to choose segments to clean. |
| `retireEpoch` | epoch in which the segment was freed; it may not be reused before that epoch drains (§11.3) |

### 6.2 Item layout

Items are laid out contiguously and padded to 8 bytes:

```
+---------------------- ItemHeader (24 bytes) ----------------------+-----+-------+-----+
| app u32 | valueLen u32 | flags u32 | expiry u32 | keyLen u16 | pad | key | value | pad |
+-------------------------------------------------------------------+-----+-------+-----+
```

`itemBytes(keyLen, valueLen) = align8(24 + keyLen + valueLen)`. An item must fit in one segment,
so the segment size is also the maximum item size. Items are **immutable** once written. An
overwrite writes a new copy and repoints the hash table, and the cleaner moves items by copying
them. Access metadata lives in the hash table (§7), so GETs never write to the log.

### 6.3 Allocation

`Log::allocHead(bytes)`, under a spin lock:

1. If the head segment has room, bump `used`, increment `pendingWriters` and return the address.
2. Otherwise, take a free segment that is safe to reuse (§11.3) and make it the head. The old head
   moves to the closed list. This fails if the free list holds no more than the **cleaner
   reserve**, which is held back so the cleaner can always make progress.

The writer copies its bytes outside the lock, publishes the item in the hash table, and then
calls `commit()`, which decrements `pendingWriters`.

### 6.4 Segment lists and the reserve

- The **free list** holds segments ready for use. Freed segments are added at the front and taken
  from the back, so the oldest (most likely epoch-safe) ones are reused first.
- The **closed list** holds full segments the cleaner can choose from.
- The **cleaner reserve** is `2 × concurrent cleaning passes + 1` segments: 3 with one cleaner
  thread, or when cleaning inline. Writers may not take the last `reserve` free segments; the
  cleaner may.

## 7. The hash table

Source: `src/memshare/hash_table.{h,cpp}`.

- **Buckets.** A fixed number, a power of two. By default `max(1024, totalBytes / 128)` rounded up.
  Each bucket is cache-line aligned and has its own spin lock and a small vector of entries.
- **Entries** hold:
  - a 64-bit bucket hash, mixing the key hash with the app id
  - a pointer to the item in the log
  - `AccessInfo`, the item's access metadata:
    - `lastSeq`: a global sequence number bumped on every access, which gives an exact LRU order
    - `lastTime`: the clock second of the last access, used for idle detection
    - `freq`: the number of accesses
- **Key comparison** checks the hash, then the app, then compares key bytes against the item in the
  log.
- **Cleaner operations** identify an entry by the item's address, which is unique:
  - `liveInfo`: is this item still the live copy, and what is its access info?
  - `relocate`: repoint the entry, only if it still points at the old address.
  - `eraseIf`: remove the entry, only if it still points at this address.

  These conditional operations are what make the cleaner safe against concurrent overwrites and
  deletes.

## 8. The arbiter and sharing policies

Source: `src/memshare/arbiter.{h,cpp}`.

### 8.1 Item capacity

The memory available for items is smaller than the arena:

```
itemCapacity = (numSegments − reserve − lowWater) × segmentBytes
lowWater     = max(1, ceil(freeFraction × numSegments))       // 1% by default
```

This is the pool that targets are carved from: `pooled = itemCapacity − Σ reservedBytes`.
Construction fails if the reservations exceed the item capacity.

### 8.2 Policies

`Config::policy` selects how targetMem is set.

**Static.** `targetMem = reservedMem + pooledShare`, fixed forever. With all memory reserved this is
plain static partitioning. It is still better than memcached's version, because:

- the log has no slab fragmentation;
- a tenant can temporarily *use* memory another tenant is not using. Writes are always allowed;
  the target only decides who gets evicted when memory runs out.

**Pooled (Algorithm 2).** On each GET miss, the application's shadow queue is checked. On a shadow
hit:

1. Choose, uniformly at random, another application whose `pooledMem ≥ credit`.
2. Move `credit` bytes of pooled memory from that application to this one.
3. Recompute `targetMem = reservedMem + pooledMem` for all applications.

Credits flow toward applications that keep missing on recently evicted keys, and away from those
that do not. Equilibrium is reached when shadow-hit rates, the hit-rate gradients, are about equal,
which is the condition for maximizing the combined hit rate. Reserved memory is never lent, so no
application falls below its reservation. The credit size is set per application
(`AppConfig::creditBytes`, 64 KB by default). A larger credit gives an application priority when it
bursts (§4.1 of the paper).

**Idle tax (Algorithm 3).** Periodically the cache scans the hash table and computes each
application's `idleBytes`: bytes in items not accessed within `idleSeconds`. Then:

```
activeFraction = 1 − idleBytes / actualMem
τ              = (1 − activeFraction · taxRate) / (1 − taxRate)
targetMem      = (reservedMem + pooledShare) / τ
```

- A fully active application keeps its full target (τ = 1).
- A fully idle one drops to `(1 − taxRate)` of it.
- `taxRate = 1` lets all idle memory be reclaimed.

Tenants that are still active can then take what idle tenants are sitting on. When several active
tenants compete for it, they get it in proportion to their targets. That follows directly from
comparing needs.

With a background cleaner the scan runs every second. When `backgroundCleaner = false`, the caller
calls `Cache::updateIdleTax()`; the simulator does so every 60 seconds of trace time.

### 8.3 Need and segment weights

The arbiter provides:

- `need(app)`, and
- `cleaningWeights()`: `1 / max(need, 10⁻⁶)` per application. The cleaner uses these to prefer
  segments full of data from over-provisioned applications.

### 8.4 Ranking functions

`AppConfig::rank` selects one of three functions. Ranks compare lexicographically as
`(major, minor)`.

| Policy | rank | Behaviour |
|---|---|---|
| `LRU` (default) | `(lastSeq, 0)` | least recently used is evicted first |
| `LFU` | `(freq, lastSeq)` | least frequently used first, ties broken by recency |
| `SegmentedLRU` | `(freq > 1 ? 1 : 0, lastSeq)` | items accessed once go first, then LRU within each group |

### 8.5 Algorithm 1: choosing survivors

`Arbiter::chooseSurvivors(items, budget)` decides which live items in a cleaning pass are kept.

1. For each application, subtract the bytes of its items in this pass from its actualMem. These
   items are "in limbo" until relocated.
2. Sort each application's items by rank, highest first.
3. Put applications into a max-heap keyed by need.
4. Loop:
   1. Pop the application with the highest need, and take its best remaining item.
   2. If the item would exceed `budget = (n − 1) × segmentBytes`, stop.
   3. Otherwise keep the item, add its bytes back to the application's actualMem, and push the
      application back with its recomputed need.

Everything not kept is evicted. Each step costs O(log apps), so passes over hundreds of thousands of
small items stay cheap. The function also returns the survivors in the order they were chosen,
most important first; the cleaner uses this if it must drop some (§9.3).

### 8.6 Shadow queues

Source: `src/memshare/shadow_queue.{h,cpp}`.

- **Contents.** An LRU list plus a hash index of `(keyHash, itemBytes)` pairs, trimmed from the
  oldest end until the total item bytes are within `shadowQueueBytes`.
- **Insertion.** The cleaner inserts each pass's evictions per application, lowest rank first, so
  the most valuable evicted keys are the most recent.
- **Lookup.** A GET miss removes the key if present, since the application is about to reinsert it.
  So each lost item is counted once.
- **Locking.** Each queue has a spin lock. GET misses use `try_lock`: if the cleaner is filling the
  queue, the check is skipped rather than blocking a request (§5.3 of the paper).
- **Scope.** Shadow hits are counted for every policy, but only move memory under `Pooled`.

## 9. The cleaner

Source: `src/memshare/cleaner.{h,cpp}`, with segment selection in `Log::takeCandidates`.

### 9.1 When it runs

The cache needs cleaning while `freeSegments < reserve + lowWater`, that is, while fewer than about
1% of segments are free beyond the reserve.

- **Background mode** (default). `cleanerThreads` threads run passes back to back while cleaning is
  needed, and otherwise sleep on a condition variable. A writer that finds the cache short of free
  segments wakes them. A writer that cannot allocate at all waits for a segment to be released.
- **Inline mode** (`backgroundCleaner = false`). The writer that notices the shortage runs passes
  itself, on the calling thread. At most one thread cleans at a time; others do not block. This mode
  is deterministic for a given seed, which is why the simulator uses it.

### 9.2 Selecting segments

A pass takes `n = cleanSegments` closed segments, or fewer if fewer are eligible. A segment is
eligible when it has no pending writers.

- `round(n × randomSegmentFraction)` of them (half by default) are chosen **uniformly at random**.
  This avoids pathologies such as one over-provisioned tenant hiding behind a worse one
  indefinitely.
- The rest are the segments with the highest **score**:

  ```
  score(segment) = Σ_app live(segment, app) × weight(app)  +  dead(segment) × deadWeight
  weight(app)    = 1 / need(app)
  deadWeight     = 2 × max weight
  ```

  Segments full of data from low-need applications score highest. Dead space (overwritten,
  deleted or expired items) is reclaimed for free, so it scores highest of all.

### 9.3 A cleaning pass

1. **Find live items.** Walk each chosen segment item by item. An item is live if the hash table
   still points at that exact address. Expired live items are dropped immediately, without going
   into a shadow queue.
2. **Decide.** Algorithm 1 (§8.5) marks survivors within the budget of `n − 1` segments.
3. **Check packing.** Items cannot straddle segments, so survivors totalling `n − 1` segments of
   bytes may need slightly more space. The pass simulates the copy. If it would overflow, it drops
   survivors from the end of Algorithm 1's order (the least important) until everything fits.
4. **Evict.** Unlink every non-survivor from the hash table, but only if the entry still points at
   it. Collect the evicted `(keyHash, bytes)` per application.
5. **Compact.** For each source segment in turn:
   1. Copy its survivors into an output segment taken from the free list.
   2. Repoint each entry with `relocate`. If a writer replaced the key meanwhile, the copy is just
      dead space.
   3. Free the source segment.

   Freed sources go back to the free list and can become the next outputs. A full output segment
   joins the closed list.
6. **Account.** For each application, with one lock acquisition each:
   - subtract its evicted bytes from actualMem;
   - add its evicted keys to its shadow queue, lowest rank first.

A pass reads n segments and writes at most `n − 1`, so it frees at least one. When the cache is
full of live data it frees exactly one; cleaning more aggressively means more copying.

The paper studies this trade-off with n (its Table 7):

- **Larger n** gives more accurate eviction, because the lowest-ranked items are found among more
  candidates, but more copying.
- **Smaller n** is cheaper but less accurate.
- **n = 1** degenerates to evicting whole segments in allocation order, which is roughly FIFO.

### 9.4 Cleaner statistics

`CacheStats::cleaner` reports:

- passes and segments cleaned
- items and bytes relocated (memory bandwidth spent)
- items and bytes evicted
- total seconds spent cleaning

## 10. Life of a request

### GET

1. Hash the key, then enter an epoch guard (§11.3).
2. Look up `(app, key)` under its bucket lock. On a match, update the entry's access info
   (`lastSeq`, `lastTime`, `freq`).
3. On a hit, copy the value out of the log. The item is immutable and its segment cannot be reused
   while the guard is held, so this is safe even if the cleaner relocates or evicts the item at the
   same moment.
4. If the item has expired, unlink it, fix up the accounting and treat the GET as a miss.
5. On a miss, after leaving the guard, call `Arbiter::onMiss`. It counts the miss, checks the shadow
   queue (with `try_lock`) and, under the pooled policy, transfers a credit.

### SET

1. Reject keys over 65,535 bytes and items larger than a segment (`SetResult::TooLarge`).
2. `allocHead`. If there is no space, clean or wait, then retry. After 10,000 failed attempts,
   return `NoMemory`. That only happens if the cache is wedged, for example because it is far too
   small.
3. Write the header, key and value into the log.
4. In an epoch guard:
   - add the bytes to the segment's `live[app]` and the application's actualMem;
   - upsert the hash table entry;
   - if it replaced an older copy, subtract that copy's bytes from its segment and application.
5. `commit()` the segment, then trigger cleaning if free segments are low.

### DELETE

In an epoch guard, erase the entry and subtract the item's bytes from its segment and application.

## 11. Concurrency

### 11.1 Locks

| Lock | Type | Protects | Held for |
|---|---|---|---|
| `Log::headLock_` | spin | head segment and its offset | a pointer bump, or a head switch |
| `Log::listLock_` | spin | free and closed lists | list operations; candidate scoring |
| bucket locks | spin, one per bucket | entries in that bucket | a short scan of one bucket |
| `Arbiter::poolLock_` | spin | pooled bytes, idle bytes, target recomputation | O(apps) |
| shadow-queue lock | spin, one per app | that app's shadow queue | one insert per eviction, or one lookup |
| `Cache::inlineMu_` | mutex | serializes inline cleaning | one pass |

Lock order: `headLock_` before `listLock_`. No other lock is held while taking another.

Counters (`actual`, `target`, `live[app]`, statistics) are atomics. Intermediate values can be
briefly inconsistent across counters. That is harmless, because the values only steer heuristics.

### 11.2 Pending writers

Writers allocate under the head lock but copy their bytes after releasing it. A segment can
therefore be closed (a new head chosen) while earlier writers are still copying into it. The
cleaner only considers segments with `pendingWriters == 0`. The acquire load of that counter pairs
with the writers' release decrement, so the cleaner sees fully written items.

### 11.3 Epochs: safe segment reuse without reader locks

A GET finds an item pointer under a bucket lock, releases the lock, and then copies the value. The
cleaner could meanwhile relocate or evict the item, free its segment, and hand the segment to a
writer. Epochs prevent the reader from ever seeing reused memory (`src/memshare/epoch.{h,cpp}`):

- A global epoch counter starts at 1. Every thread has a slot, one of 1,024 shared process-wide,
  assigned on first use and released when the thread exits.
- `EpochGuard` publishes the current epoch into the thread's slot, followed by a sequentially
  consistent fence. On exit the slot is marked inactive.
- When the cleaner frees a segment (after removing all hash table references into it), it tags the
  segment with the current epoch and increments the global epoch.
- A free segment tagged `E` is reused only when every active slot shows an epoch greater than `E`.
  A request that started later cannot have found a reference to the segment.

Rules:

- Guards cover every hash table access, because comparing keys reads other items in the log.
- Guards are never nested.
- A thread never waits for a segment to become reusable while inside a guard. That is why SET
  allocates *before* entering its guard, and why the cleaner takes a guard per item rather than per
  pass.

The cleaner itself is safe without holding segments locked, because a segment it is cleaning has
been removed from the closed list and no one else can select it.

### 11.4 Races the design tolerates

**A SET overwrites a key while the cleaner is moving it.**

1. The SET's upsert replaces the entry.
2. The cleaner's `relocate` finds the entry no longer points at the old copy and fails.
3. The cleaner's copy becomes dead space in its output segment.

**A DELETE or overwrite races with eviction.** Whoever unlinks the entry first does the accounting.
`eraseIf` on the other side finds nothing to remove.

**A GET races with relocation or eviction.** The reader may copy the old item. It is identical to
the new copy, and its memory is protected by the epoch.

**Parallel cleaning passes.** Each pass removes its segments from the closed list under
`listLock_`, so passes never share segments.

## 12. Memory accounting and overhead

- `actualMem` counts `itemBytes` (header, key, value and padding) of live items.
- Space not available to items:
  - the cleaner reserve (3 segments by default);
  - about 1% free headroom (`lowWater`);
  - dead space in closed segments until they are cleaned;
  - the tails of segments where the next item did not fit.
- Per item, a hash table entry costs 32 bytes plus vector slack, and the log header costs 24 bytes.
  memcached's header is about 48 to 56 bytes.
- Each shadow-queue entry costs one list node and one hash-map node. The paper stores only 8 bytes
  per entry and estimates 640 KB per application for a 10 MB queue of 128-byte items. This
  implementation uses standard containers, so its real cost is several times higher.
- Each segment keeps an 8-byte live counter per application.

## 13. Configuration reference

`memshare::Config` (`src/memshare/config.h`):

| Field | Default | Meaning |
|---|---|---|
| `totalBytes` | 64 MB | arena size; the segment count is `totalBytes / segmentBytes` |
| `segmentBytes` | 1 MB | segment size and maximum item size; multiple of 8, at least 4 KB |
| `cleanSegments` | 100 | *n*, segments per cleaning pass |
| `randomSegmentFraction` | 0.5 | fraction of the *n* segments chosen at random |
| `freeFraction` | 0.01 | clean while free segments (beyond the reserve) are below this fraction |
| `backgroundCleaner` | true | background cleaner threads; false cleans inline on writers |
| `cleanerThreads` | 1 | number of background cleaner threads |
| `policy` | `Pooled` | `Static`, `Pooled` or `IdleTax` |
| `taxRate` | 0.5 | idle tax rate, in [0, 1] |
| `idleSeconds` | 18000 (5 h) | idle time for the idle tax |
| `clock` | steady clock | returns seconds; drives expiry and idle times; the simulator substitutes trace time |
| `seed` | 42 | seed for random segment selection and credit donors |
| `hashBuckets` | 0 (auto) | hash table buckets; 0 = `max(1024, totalBytes / 128)` |
| `apps` | — | at least one `AppConfig` is required |

`memshare::AppConfig`:

| Field | Default | Meaning |
|---|---|---|
| `name` | — | used by `Cache::findApp` and the server's `app` command |
| `reservedBytes` | 0 | guaranteed memory |
| `creditBytes` | 64 KB | pooled memory gained per shadow hit (Pooled policy) |
| `rank` | `LRU` | `LRU`, `LFU` or `SegmentedLRU` |
| `shadowQueueBytes` | 10 MB | item bytes represented by the shadow queue; 0 disables it |

Construction throws `std::invalid_argument` if:

- there are no apps;
- the segment size is invalid;
- the cache has fewer than `reserve + lowWater + 4` segments;
- the reservations exceed the item capacity;
- `taxRate` is outside [0, 1].

## 14. Using the library

Link against `libmemshare.a` (target `memshare`) and include `memshare/cache.h`.

```cpp
#include "memshare/cache.h"
#include <iostream>

int main() {
    memshare::Config cfg;
    cfg.totalBytes = 256ull << 20;
    cfg.policy = memshare::SharingPolicy::Pooled;
    cfg.apps = {
        {"shop", /*reservedBytes=*/64ull << 20},
        {"search", 64ull << 20, /*creditBytes=*/128 << 10, memshare::RankPolicy::LFU},
        {"batch", 0},
    };
    memshare::Cache cache(cfg);

    memshare::AppId shop = *cache.findApp("shop");
    cache.set(shop, "user:42", "{\"name\":\"Ada\"}", /*flags=*/0, /*exptime=*/3600);

    std::string value;
    if (cache.get(shop, "user:42", &value)) std::cout << value << "\n";
    cache.del(shop, "user:42");

    for (const auto& a : cache.stats().apps)
        std::cout << a.name << " hits=" << a.hits << " target=" << a.targetBytes
                  << " actual=" << a.actualBytes << " need=" << a.need << "\n";
}
```

API summary (`class Cache`):

| Method | Description |
|---|---|
| `get(app, key, &value, &flags)` | true on a hit; copies the value and flags. Pass `nullptr` to skip the copy. |
| `set(app, key, value, flags, exptime)` | returns `Stored`, `TooLarge` or `NoMemory`. `exptime` is seconds from now; 0 never expires. |
| `del(app, key)` | true if the key existed |
| `findApp(name)` | `std::optional<AppId>` |
| `cleanOnce()` | run one cleaning pass on the calling thread |
| `updateIdleTax()` | rescan idle memory and recompute idle-tax targets |
| `stats()` | per-app `AppStats`, `CleanerStats`, segment counts, item capacity |

All methods are thread safe. `AppStats` contains:

- request counts: `gets`, `hits`, `misses`, `sets`, `shadowHits`
- evictions: `evictedItems`, `evictedBytes`
- memory: `actualBytes`, `targetBytes`, `reservedBytes`, `pooledBytes`, `idleBytes`
- `need`

## 15. The server

`memshare_server` puts the cache behind memcached's text protocol. It runs one thread per
connection, plus the background cleaner threads and the idle-tax scanner.

### Options

| Option | Default | Meaning |
|---|---|---|
| `-p`, `--port N` | 11211 | TCP port (IPv4 and IPv6) |
| `-m`, `--memory MB` | 1024 | cache memory |
| `--segment KB` | 1024 | segment size |
| `--clean-segments N` | 100 | *n* |
| `--cleaner-threads N` | 1 | background cleaner threads |
| `--policy P` | pooled | `static`, `pooled` or `idletax` |
| `--tax-rate F` | 0.5 | idle tax rate |
| `--idle-seconds S` | 18000 | idle time for the tax |
| `--app NAME:RESERVED_MB[:CREDIT_KB[:lru\|lfu\|slru[:SHADOW_MB]]]` | one app, `default` | add a tenant; repeatable |

Example:

```bash
./build/memshare_server -m 1024 --policy pooled \
    --app shop:256 --app search:256:128:lfu --app batch:0
```

### Protocol

| Command | Notes |
|---|---|
| `get <key>*`, `gets <key>*` | `gets` reports a CAS value of 0; CAS itself is not supported |
| `set/add/replace <key> <flags> <exptime> <bytes> [noreply]` | `exptime` follows memcached: values above 30 days are absolute Unix times, and a time in the past deletes the key |
| `delete <key> [noreply]` | |
| `app <name>` | **extension:** switches this connection's tenant; replies `OK` or `CLIENT_ERROR unknown app` |
| `stats` | see below |
| `version`, `quit` | |

Connections start on the first configured tenant. Keys are limited to 250 bytes, as in memcached.

```
$ printf 'app search\r\nset k 0 0 5\r\nhello\r\nget k\r\n' | nc -q1 localhost 11211
OK
STORED
VALUE k 0 5
hello
END
```

### `stats` fields

The cache as a whole:

- `segment_bytes`, `segments_total`, `segments_free`
- `item_capacity`
- `cleaner_passes`, `cleaner_bytes_relocated`, `cleaner_bytes_evicted`, `cleaner_seconds`

For each tenant, prefixed `app:<name>:`:

- `get_hits`, `get_misses`, `sets`, `shadow_hits`, `evictions`
- `actual_bytes`, `target_bytes`, `reserved_bytes`, `pooled_bytes`, `idle_bytes`
- `need`

## 16. The simulator

`memshare_sim` replays a trace through several cache configurations and reports hit rates and
cleaner costs. It uses the real `Cache` implementation in inline-cleaning mode, on a virtual clock
driven by trace timestamps. The cache is treated as a **lookaside** cache: a GET that misses is
followed by a SET of the item, as the application would do after reading the database.

### Configurations compared

| Name | What it is |
|---|---|
| `memcached` | Baseline: memcached's slab allocator partitioned per tenant, as at Memcachier (§2 of the paper). See below. |
| `static` | Memshare, 100% reserved: each tenant reserves its share of the item capacity |
| `pooled` | Memshare, `--reserved` (default 75%) reserved, the rest pooled |
| `pooled0` | Memshare with no reservations: everything pooled |
| `idletax` | Memshare, 100% reserved, idle tax at `--tax-rate` with `--idle-seconds` |

How the `memcached` baseline works:

- Each tenant gets a fixed number of 1 MB slabs.
- A slab is assigned to a size class (64 B·2ⁱ) when that class first needs space, and is never
  reassigned.
- Each class runs its own LRU.
- Items carry a 48-byte header.
- A store fails if its class has no slab and the tenant has no slabs left.

Every Memshare configuration gets the same total memory as the baseline. Memshare's reserve and
free headroom come out of that total.

### Options

| Option | Default | Meaning |
|---|---|---|
| `--trace FILE` | synthetic | CSV trace |
| `--app-mem NAME:MB` | — | memory per tenant; required for every tenant in a CSV trace |
| `--requests N` | 12,000,000 | synthetic trace length |
| `--seed N` | 1 | synthetic trace seed |
| `--dump FILE` | — | write the synthetic trace as CSV and exit |
| `--policies LIST` | all five | comma-separated subset |
| `--reserved F` | 0.75 | reserved fraction for `pooled` |
| `--credit KB` | 64 | credit size |
| `--tax-rate F` | 0.5 | |
| `--idle-seconds S` | 900 | |
| `--segment KB` | 1024 | |
| `--clean-segments N` | 40 | *n* |
| `--shadow MB` | 10 | shadow queue size |

### Trace format

```
# time,app,op,key,value_bytes
0.0012,web,get,user:17,420
0.0019,api,set,token:9,64
0.0025,web,delete,user:3,0
```

- `time` is in seconds and increasing.
- `op` is `get`, `set` or `delete`.
- `value_bytes` is the item's value size. For a GET it is the size to fill on a miss; 0 skips the
  fill.
- Tenants are numbered in the order of their `--app-mem` flags. A tenant missing from those flags
  is an error.

### Built-in synthetic scenario

Five tenants whose behaviours exercise different parts of Memshare. Key popularity is Zipfian.
Value sizes are log-uniform and fixed per key. 2% of requests are explicit updates.

| Tenant | Memory | Rate | Keys | Zipf | Values | Behaviour |
|---|---|---|---|---|---|---|
| steady | 48 MB | 300/s | 400k | 0.85 | 200–2000 B | working set larger than its memory |
| overprov | 48 MB | 250/s | 30k | 1.0 | 100–1500 B | small, hot working set; over-provisioned |
| bursty | 24 MB | 60/s | 50k | 0.9 | 300–3000 B | every hour, a 15-minute burst at 8× rate, 80% of it to 150k new keys |
| tiny | 16 MB | 200/s | 300k | 0.95 | 10–80 B | tiny items; the slab allocator wastes the most on these |
| fading | 32 MB | 150/s | 60k | 0.8 | 400–4000 B | stops sending requests after one hour but keeps its memory |

### Output

The simulator prints:

- hit rate per tenant and combined, for each configuration;
- for each configuration: combined hit rate, miss reduction relative to the first configuration
  listed, cleaner bandwidth (MB relocated per second of trace time), number of passes, and wall
  time;
- each tenant's memory used and target at the end of the trace;
- each tenant's shadow-queue hits and evictions (Memshare configurations only).

### Results

Default run: 12M requests, about 3.5 simulated hours, `./build/memshare_sim`.

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

Reading the results:

- **The log allocator alone** (`static` against `memcached`, with the same partitions) removes
  about a quarter of all misses. Two things contribute:
  - no slab fragmentation, which matters most for `tiny` and for large items;
  - tenants can use memory others leave unused, such as the 30+ MB `overprov` never touches.
- **Pooling** adds more by moving memory toward `steady`, `bursty` and `fading`, whose
  shadow-queue hits show they gain the most from it.
  - With 75% reserved, `overprov` keeps its 99% hit rate.
  - With nothing reserved (`pooled0`), it gives up some hit rate for a higher combined total. This
    is the isolation-versus-efficiency trade-off of the paper's Table 3.
- **The idle tax** reclaims `fading`'s memory once it goes quiet, while leaving active tenants'
  reservations alone.
- **Pooled equilibrium.** Under the pooled policy, credits flow until tenants' shadow-hit rates are
  roughly equal. A tenant that is already below its target can keep collecting credits it does not
  use. That is harmless: eviction follows need *relative* to other tenants, so the unused target
  does not hurt anyone.

These numbers come from a synthetic workload. The paper's week-long Memcachier trace is not public,
so they are not comparable to the paper's figures. The paper reports 39.7% fewer misses than
memcached for the pooled policy, and 1% gained from the allocator alone. The direction and ordering
of the results match.

## 17. Tests

`./build/memshare_tests [filter]` runs every test whose name contains `filter`. The exit status is
nonzero on failure.

| Test | What it checks |
|---|---|
| `basic_ops` | get/set/delete, flags, separate key spaces per tenant, overwrite accounting, expiry, oversize rejection, `findApp` |
| `shadow_queue` | byte-bounded LRU trimming; hits consume entries |
| `choose_survivors` | Algorithm 1 in isolation: need-proportional survivors (4:2 for targets 600:300), each tenant's highest-ranked items kept |
| `eviction_by_rank` | one tenant writing 60k items through an 8 MB cache: a re-read hot set never misses, recent items survive intact, old cold items are gone |
| `static_isolation` | a tenant flooding the cache cannot evict another tenant that is under its reservation |
| `pooled_sharing` | the tenant with shadow hits gains pooled memory; the other stays at or above its reservation; targets sum to capacity; hit rate improves over time |
| `idle_tax` | a fully idle tenant's target drops to `reserved × (1 − taxRate)`, and an active tenant then squeezes it |
| `lfu` | frequently read items survive a flood of new ones under LFU |
| `concurrent_stress` | 8 threads, 3 tenants and 2 background cleaners doing mixed get/set/delete: every value read is intact and belongs to the requested key, no allocation fails, accounting stays within capacity |

Values in the tests encode their key and a version plus a checkable pattern, so torn reads,
misdirected reads and use-after-reuse all show up as failures. The suite passes under
ThreadSanitizer and AddressSanitizer/UBSan.

## 18. Differences from the paper

The paper's implementation modifies memcached 1.4.24. This one is standalone. Where the paper
leaves details open or where behaviour differs:

- **Copy order.** Algorithm 1's keep/evict decisions are exactly as described. Survivors are then
  copied one source segment at a time, not in rank order. Each source segment can be freed and
  reused as soon as its survivors are out, so a pass needs about two spare segments instead of
  *n − 1*. This matters: with n = 100 and 1 MB segments, *n − 1* would be 99 MB held back. Packing
  overflow is resolved by dropping the least important survivors (§9.3).
- **Rank metadata** (last access, count) is kept in the hash table rather than in items, so the log
  stays immutable and GETs never write to it.
- **Segment scoring.** The paper says only "most data from applications with the lowest need". Here
  that is `Σ live × (1 / need)`, with dead bytes weighted above everything else, using per-segment,
  per-application live-byte counters.
- **Shadow hits consume the entry**, and credits are never taken from the requesting application
  itself. Algorithm 2's candidate set as written can include the requester.
- **The idle tax at `taxRate = 1`** uses the formula's limit: a tenant with any idle memory gets
  target 0, and a fully active one keeps its reservation.
- **Cleaner threads** are a fixed pool. The paper starts more threads when the cleaner falls behind.
- **Not implemented:** evicting the last output segment of a pass when it is under 50% full (end of
  the paper's §3.2).
- **Server.**
  - Text protocol only: no binary protocol, no SASL, no CAS.
  - Tenants are selected with the `app` command rather than by authentication.
  - `add` and `replace` are check-then-store, so they are not atomic.
  - One thread per connection instead of memcached's event loop.

## 19. Limitations

- **Fixed at construction.** The set of tenants and the hash table size cannot change while the
  cache runs.
- **Shadow queue size.** Shadow queues use `std::list` and `std::unordered_map`, which take more
  memory per entry than the paper's 8 bytes.
- **Idle-tax scan cost.** The scan walks the whole hash table, one chunk of buckets at a time,
  every second. That is fine for millions of items; very large caches would want incremental
  tracking.
- **Scale limits.** At most 1,024 threads can use caches at once (epoch slots). Keys are limited
  to 65,535 bytes in the library and 250 bytes in the server. Values are limited to one segment.
- **Wedged cache.** `NoMemory` is returned only after roughly 10,000 failed attempts to find space.
  It indicates a misconfigured, far-too-small cache rather than normal operation.

## Citation

```
@inproceedings{cidon2017memshare,
  title     = {Memshare: a Dynamic Multi-tenant Key-value Cache},
  author    = {Cidon, Asaf and Rushton, Daniel and Rumble, Stephen M. and Stutsman, Ryan},
  booktitle = {2017 USENIX Annual Technical Conference (USENIX ATC 17)},
  year      = {2017}
}
```
