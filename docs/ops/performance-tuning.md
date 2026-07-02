# Performance Guidelines

This document provides best practices for maximizing throughput and minimizing latency when using the Ledger v3 API. It covers data modeling (Numscript scripts, account addresses), operational tuning, and explains the internal design choices that make these recommendations effective.

> **Reference benchmark:** 106K tx/s sustained, p99 < 75 ms, 3-node Raft cluster (8 cores / 4 GiB per node). See [benchmarks](../sales/benchmarks.md) for full details.

---

## 1. Numscript: Use Variables, Never Templates

### The Rule

Write **one script with variables** instead of generating a unique script per transaction.

```numscript
// GOOD: single script, reused across all calls
vars {
  account $source
  account $destination
  monetary $amount
}

send $amount (
  source = $source
  destination = $destination
)
```

```numscript
// BAD: unique script per transaction (hardcoded values)
send [USD/2 1500] (
  source = @users:alice
  destination = @merchants:shop42
)
```

### Why It Matters

Parsed Numscript ASTs are cached using a blake3 hash of the script content. Every unique script text triggers a full parse (CPU-bound, allocations). The same text with different variable values is a cache hit.

| Pattern | Cache behavior | Parse cost |
|---------|---------------|------------|
| Variables (`$source`, `$amount`) | 1 parse, all subsequent calls hit cache | Amortized to ~0 |
| Hardcoded values | 1 parse **per unique script** | Linear with traffic |

The cache is bounded (LRU, default 1024 entries). If you generate more than 1024 unique scripts, older entries get evicted and must be re-parsed.

### Practical Guidelines

- Parameterize **all varying parts**: accounts, amounts, assets, metadata values
- A small set of script templates (transfer, payment-with-fees, escrow, funding) is ideal
- Use `$variables` for dynamic account addresses instead of string interpolation at the application level:

```numscript
// GOOD: dynamic address via account interpolation
vars {
  string $order_id
  monetary $amount
}

send $amount (
  source = @world
  destination = @escrow:$order_id
)
```

```numscript
// BAD: template-generated at application level
send [USD/2 500] (
  source = @world
  destination = @escrow:order-12345
)
```

---

## 2. Account Address Design

Account addresses directly impact cache efficiency, storage size, and key hashing performance. The address is encoded into every Pebble key and every cache lookup.

### 2.1. Use Monotonic Segments

Addresses are hierarchical (segments separated by `:`). When possible, make the **first segments stable and shared** and place the varying part at the end.

```
// GOOD: shared prefix, monotonic varying suffix
users:alice
users:bob
merchants:shop42
escrow:order-00001
escrow:order-00002

// BAD: fully random, no shared prefix
a3f8e2b1-9c4d-4e7a-b5f6-1234567890ab
7b2c9d4e-5f6a-8b3c-d4e5-abcdef012345
```

**Why:** Pebble is an LSM-tree. Keys with shared prefixes compact better and range scans (used for balance reconstruction and account listing) are more efficient. Sequential or monotonic suffixes keep related data physically close on disk.

If you use UUID-based addresses, prefer **UUIDv7** (time-ordered) over UUIDv4 (random) to preserve temporal locality.

### 2.2. Keep Segments Short

Every byte of the account address is stored in:
- Every Pebble key (volume entries, metadata entries)
- Every cache key (hashed, but the input size affects hash time)
- Every protobuf message (Raft entries, snapshots, gRPC responses)
- Every Numscript execution context

Short segments reduce memory pressure across all these layers.

| Long | Short | Savings |
|------|-------|---------|
| `merchants:shop42` | `mrs:shop42` | 6 bytes/key |
| `platform:fees` | `plt:fees` | 5 bytes/key |
| `treasury:main` | `trs:main` | 5 bytes/key |
| `users:alice:checking` | `usr:alice:chk` | 7 bytes/key |

At 100K tx/s with 2 postings per transaction, that's 200K key lookups/writes per second. A 6-byte saving translates to ~1.2 MB/s less data hashed, serialized, and stored.

**Recommendations:**
- 3-letter abbreviations for well-known segment types: `usr`, `mrs`, `plt`, `trs`, `esc`
- Avoid redundant segments: `@users:alice` is better than `@accounts:users:alice`
- Keep the total address under ~40 characters when practical

### 2.3. Minimize Account Cardinality per Numscript

Each distinct account in a Numscript execution requires:
1. Cache lookup (or Pebble preload if not cached)
2. Lock acquisition during admission (canonical ordering)
3. Volume tracking in the FSM

Fewer accounts per script = faster admission + faster apply.

```numscript
// GOOD: 2 accounts
send $amount (
  source = $source
  destination = $destination
)

// LESS IDEAL: 4 accounts (more locks, more preloads)
send $amount (
  source = {
    $primary
    $secondary
    $backup allowing overdraft up to $limit
  }
  destination = $destination
)
```

Use multi-source/multi-destination patterns when the business logic requires it, but be aware of the per-account cost.

---

## 3. Bulk Operations

### Use Bulk Mode for High Throughput

The bulk API (`POST /{ledger}/_bulk` or gRPC `Apply` with multiple actions) amortizes the Raft consensus overhead across many transactions.

| Approach | Raft entries | Network roundtrips |
|----------|--------------|--------------------|
| 50 individual requests | 50 | 50 |
| 1 bulk of 50 | 1 | 1 |

Our benchmarks use a bulk size of **50 transactions** with atomic mode enabled.

### Atomic vs Non-Atomic

- **Atomic** (`?atomic=true`): All-or-nothing. One business error (e.g., insufficient funds) rolls back the entire bulk. Best for cross-ledger transfers where consistency matters.
- **Non-atomic** (default): Each transaction is independent. Faster for ingestion workloads where partial success is acceptable.

### Recommended Bulk Size

Start with **50** and adjust based on your payload size and latency requirements. Larger bulks increase throughput but also increase per-request latency and memory usage.

---

## 4. Hot Account Patterns

### The Problem (v2)

In Ledger v2, a hot account (e.g., `bank`, `fees`, `treasury`) debited by many concurrent transactions created a PostgreSQL row-level lock bottleneck. Transactions queued sequentially waiting for the lock.

### The Solution (v3)

v3 uses **append-only volume entries** (last-write-wins). Each transaction appends a new absolute volume value keyed by Raft index. No read-modify-write, no locks, no contention.

```
Transaction A: bank/USD/log_1 = {input: 1000, output: 100}  (append)
Transaction B: bank/USD/log_2 = {input: 1000, output: 150}  (append, parallel)
Transaction C: bank/USD/log_3 = {input: 1000, output: 225}  (append, parallel)
```

Balance is the latest absolute value (last-write-wins).

### Recommendations

- Hot accounts (central wallets, fee collectors) are no longer a throughput bottleneck
- Use unbounded overdraft for accounts like `@world` or known-funded sources to avoid unnecessary balance checks:

```numscript
send $amount (
  source = $source allowing unbounded overdraft
  destination = $destination
)
```

- If balance checking is required, the account must be preloaded at admission. Hot accounts stay in the in-memory generation cache (`gen0/gen1`) and are served from RAM.

---

## 5. Operational Tuning

### 5.1. Storage: Fast Disk for WAL

The Raft WAL writes are **synchronous and on the critical path** of every write operation. Place the WAL directory on a fast NVMe/SSD. If possible, use **separate disks** for WAL and Pebble data to avoid I/O contention.

### 5.2. Pebble Configuration

The default configuration is tuned for write-heavy workloads:

| Parameter | Default | Purpose |
|-----------|---------|---------|
| MemTableSize | 256 MB | Larger memtables → fewer flushes |
| L0CompactionThreshold | 4 | Low threshold: Pebble auto-compacts aggressively, keeping L0 clean |
| L0StopWritesThreshold | 16 | ~4x ratio above compaction threshold |
| LBaseMaxBytes | 2 GB | Large L1 reduces write amplification |
| CacheSize | 1 GB | Block cache for read performance |
| MaxConcurrentCompactions | 2 | Parallel compaction threads |

**L0 compaction and cold starts:** The low `L0CompactionThreshold` (4) ensures Pebble keeps L0 clean natively, so L0 files never accumulate excessively. Combined with the extended block cache warmup covering `[0xF1, 0xFF)` on startup, cold start read latency is minimal without needing manual startup or periodic compaction.

Monitor these metrics for write stalls:
```promql
increase(pebble_write_stall_total[5m]) > 0
```

### 5.3. Generation Rotation Threshold

The `GenerationRotationThreshold` (`K`) controls how many Raft entries fit in one cache generation. Any account touched in the last `~2K` entries is guaranteed in RAM.

- **Larger K**: More accounts in RAM, fewer Pebble preloads. More memory usage.
- **Smaller K**: Less memory, but more preloads for moderately active accounts.

Default is appropriate for most workloads. Increase if admission preload metrics (`admission.preload.duration`) show high latency.

### 5.4. Bloom Filters

Application-level bloom filters sit in front of Pebble and short-circuit point lookups for keys that definitely don't exist. This is especially valuable when Pebble is on network-attached storage (e.g., Ceph RBD) where each miss costs ~1ms.

Each attribute type has its own filter with independent `expected-keys` and `fp-rate` settings. Types with `expected-keys=0` are disabled (no memory allocated).

**Default configuration:**

| Type | Expected Keys | FP Rate | Enabled |
|------|--------------|---------|---------|
| Volumes | 0 | — | No |
| Metadata | 0 | — | No |
| References | 0 | — | No |
| Ledgers | 0 | — | No |
| Boundaries | 0 | — | No |
| Transactions | 0 | — | No |
| Sink configs | 0 | — | No |
| Numscript versions | 0 | — | No |
| Numscript contents | 0 | — | No |
| Ledger metadata | 0 | — | No |

Bloom filters are disabled by default. Enable only the attribute types that are expected to avoid enough missing-key Pebble reads to justify the memory cost.

**Tuning guidelines:**

- Set `expected-keys` to the number of unique keys you expect for that attribute type. Over-estimating wastes memory; under-estimating increases false positives.
- A lower `fp-rate` reduces false positives but increases memory usage. The default 1% is a good starting point.
- Monitor `bloom.negatives` (Pebble Gets avoided) and `bloom.lookups` (total checks). A high negatives/lookups ratio means the filter is effective.
- Changing any bloom configuration triggers a full repopulation from Pebble on next startup.

See [CLI Reference](./cli.md#server-bloom-filter-flags) for all flags.

### 5.5 Hash Algorithm

The log hash chain uses BLAKE3 by default (cryptographic, tamper-resistant). For write-heavy workloads where throughput matters more than tamper-resistance (e.g., blockchain ingestion, bulk imports), switching to XXH3-128 reduces hash computation CPU by ~5-10x.

```bash
# Use XXH3-128 for faster hashing (non-cryptographic)
ledger run --hash-algorithm xxh3 [other flags...]

# Default: BLAKE3 (cryptographic)
ledger run --hash-algorithm blake3 [other flags...]
```

The setting is cluster-wide and replicated via Raft. Changing it takes effect on the next log produced — existing logs retain their original hash and remain verifiable via the `hash_version` field stored on each log. See [audit hash chain — Hash primitive](../technical/architecture/subsystems/checker/audit-chain.md#hash-primitive) for details.

### 5.6 Numscript Cache Size

Default: **1024** entries. Increase if your application uses more than 1024 distinct script texts (monitor `numscript.cache.size` gauge). In practice, most applications have fewer than 10 distinct scripts.

### 5.7. Admission Metrics

Admission metrics (histograms, counters) are **disabled by default** because OpenTelemetry histogram internals can cause contention under high concurrency. Enable them (`--admission-metrics`) only when profiling, not in steady-state production.

---

## 6. Understanding the Hot Path

The following diagram shows the critical write path. Each step is optimized to minimize allocations and I/O:

```
Client Request
    │
    ▼
Admission (leader, parallel)
    ├─ Numscript parse (cached via blake3 hash)
    ├─ Account lock (canonical order, fine-grained)
    ├─ Cache check: gen0 ∪ gen1 (RAM)
    ├─ Preload from Pebble (only for uncached accounts)
    ├─ Proposal marshal (vtprotobuf + sync.Pool buffer)
    │
    ▼
Raft Consensus (replicated to all nodes)
    │
    ▼
FSM Apply (all nodes, sequential, RAM-only)
    ├─ Proposal unmarshal (vtprotobuf)
    ├─ Balance check: base@boundary + overlay delta
    ├─ Amounts: uint256 stack variables (zero allocation)
    ├─ Volume diffs: append to Pebble batch (no sync)
    ├─ Order hash: reusable buffer (zero allocation)
    │
    ▼
Pebble Batch Commit (single commit per batch of entries)
```

### Key Design Decisions

| Decision | Impact |
|----------|--------|
| **RAM-only FSM Apply** | No Pebble reads during apply. All data comes from cache or preload. |
| **Uint256 wire format** | 4 x `uint64` assignments instead of `big.Int` heap allocations. Zero-alloc on hot path. |
| **vtprotobuf** | ~2-3x faster serialization than standard protobuf. Registered transparently server-side. |
| **64-shard concurrent map** | Cache-line padded shards with per-shard RWMutex. ~1.6% reader-writer collision probability. |
| **XXH3 for cache keys** | 13ns vs 205ns (BLAKE3). 16x faster for non-cryptographic hashing. |
| **Append-only volume entries** | No row-level locks. Hot accounts don't create contention. Last-write-wins semantics. |
| **Old entry cleanup at merge** | Runs in same Pebble batch as generation rotation. No background goroutine. |

---

## 7. Read Performance

### Local Reads

Read operations (`GetLedger`, `ListAccounts`, `GetTransaction`, `GetBalances`) are served **locally** on any node (leader or follower). By default they first use a Raft ReadIndex barrier to guarantee linearizability, then read from the local store. Requests that explicitly use stale consistency skip the barrier. This allows read scaling by adding follower nodes without routing every read to the leader.

### Balance Reconstruction

Current balance = `base + latest cumulative diff`. Pebble range scans are efficient for this pattern. Volume diffs are compacted at generation boundaries to keep the number of entries bounded (~2K per account/asset).

---

## 8. Monitoring Checklist

| Metric | What to watch | Action |
|--------|--------------|--------|
| `numscript.cache.size` | Approaching max (default 1024) | Increase cache size or reduce unique scripts |
| `admission.preload.duration` | High latency | Increase generation threshold K |
| `admission.preload.cache_hits` | Low hit rate | Review account access patterns |
| `raft.apply_entries.duration` p99 | > 50ms | Check disk I/O, compaction backlog |
| `pebble_write_stall_total` | Any increase | Add disk IOPS, tune compaction |
| `cache.rotations` | Frequency | Informational: correlates with K |
| `bloom.negatives` / `bloom.lookups` | Low ratio per type | Filter not effective for that type — consider disabling it |
| `bloom.ready` | 0 after startup | Filter still populating — preloads fall back to Pebble |
| Memory usage | Sustained growth | Check generation size, snapshot frequency |

---

## 9. Summary: Do / Don't

| Do | Don't |
|----|-------|
| Use variables in Numscript scripts | Generate unique script text per transaction |
| Use short, abbreviated account segments | Use verbose multi-word segments |
| Use monotonic/sequential suffixes | Use random UUIDv4 as addresses |
| Bulk transactions (50+) | Send individual requests in a loop |
| Place WAL on NVMe/SSD | Share disk between WAL and data |
| Use unbounded overdraft for known-funded sources | Check balance on `@world` or treasury accounts |
| Monitor `numscript.cache.size` | Assume cache is infinite |
| Keep admission metrics off in production | Leave admission histograms on under load |

---

## Related Documentation

- [Benchmark Results](../sales/benchmarks.md) - 106K tx/s benchmark details
- [Numscript Reference](../technical/contributing/numscript.md) - Full Numscript language documentation
- [Deterministic FSM Cache](../technical/architecture/subsystems/fsm/deterministic-fsm.md) - Cache and preload architecture
- [Uint256 Wire Format](../technical/architecture/primitives/uint256-wire-format.md) - Zero-allocation monetary amounts
- [Attribute Key Hashing](../technical/architecture/subsystems/attributes/key-hashing.md) - XXH3 vs BLAKE3 performance
- [Storage Drivers](../technical/architecture/subsystems/storage/storage-drivers.md) - Pebble configuration details
- [Metrics Reference](./monitoring.md) - Complete metrics catalog and alerting rules
- [Deployment Guide](./deployment.md) - Production deployment recommendations
- [V2 Problems Solved](../sales/v2-vs-v3.md) - Hot account contention eliminated
