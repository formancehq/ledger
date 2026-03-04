# Memory Management

This document describes every component that contributes to the memory footprint of a Ledger v3 node, the configuration parameters that control each component, and the trade-offs involved when tuning them.

At startup the server logs an estimated memory breakdown and warns if it exceeds `GOMEMLIMIT`. The estimate is intentionally conservative (rounds up).

---

## Memory Budget Overview

| Component | Default | CLI Flag(s) | Tunable? |
|-----------|---------|-------------|----------|
| [Pebble block cache](#pebble-block-cache) | 1 GiB | `--pebble-cache-size` | Yes |
| [Pebble memtables](#pebble-memtables) | 1.5 GiB | `--pebble-memtable-size`, `--pebble-memtable-stop-writes-threshold` | Yes |
| [bbolt read index mmap](#bbolt-read-index) | 1 GiB | `--read-index-initial-mmap-size` | Yes |
| [Raft transport buffers](#raft-transport-buffers) | 10 MiB/peer | `--raft-transport-buffer-size` | Yes |
| [FSM cache](#fsm-cache) | ~18 MiB | `--cache-rotation-threshold` | Yes |
| [Numscript cache](#numscript-cache) | ~5 MiB | `--numscript-cache-size` | Yes |
| [gRPC buffers](#grpc-buffers) | ~82 MiB/conn | -- | No (constants) |
| [Go runtime](#go-runtime) | ~200 MiB | `GOMEMLIMIT` | Partially |
| **Typical 3-node total** | **~3.8 GiB** | | |

---

## Pebble Block Cache

**Flag:** `--pebble-cache-size`
**Default:** `1073741824` (1 GiB)
**Type:** bytes

Shared LRU cache for decompressed SST data blocks. Every Pebble read (point lookup or range scan) checks this cache first.

**Impact of changing:**

| Direction | Effect |
|-----------|--------|
| Increase | More hot data served from RAM; fewer disk reads; directly increases RSS |
| Decrease | More disk I/O; higher read latency; saves memory |

**Recommendation:** This is usually the single biggest lever for trading memory for read performance. On read-heavy workloads, allocate as much as the memory budget allows. On write-only workloads (bulk ingest), it can be reduced safely.

---

## Pebble Memtables

**Flags:**
- `--pebble-memtable-size` (default: `268435456` / 256 MiB)
- `--pebble-memtable-stop-writes-threshold` (default: `6`)

**Worst-case memory:** `memtable-size * stop-writes-threshold` = 256 MiB * 6 = **1.5 GiB**

Memtables are in-memory write buffers. Pebble keeps up to `stop-writes-threshold` memtables alive simultaneously (one active + frozen ones waiting for flush). When all slots are occupied, writes stall until a flush completes.

**Impact of changing:**

| Parameter | Increase | Decrease |
|-----------|----------|----------|
| `memtable-size` | Fewer flushes, larger batches to disk, more memory per table | More frequent flushes, smaller write batches, less memory |
| `stop-writes-threshold` | More headroom before write stalls, more total memory | Lower memory, but write stalls appear sooner under sustained load |

**Recommendation:** The defaults (256 MiB * 6) are tuned for sustained write-heavy workloads. Reduce `memtable-size` to 128 MiB or 64 MiB on memory-constrained nodes, but expect more frequent write stalls under burst traffic.

### Related Pebble Parameters

These have indirect or minor memory impact:

| Flag | Default | Purpose |
|------|---------|---------|
| `--pebble-l0-compaction-threshold` | 4 | L0 files before triggering compaction. Lower = cleaner L0, more compaction CPU |
| `--pebble-l0-stop-writes-threshold` | 16 | L0 files before stalling writes. Higher = more tolerance, more read amplification |
| `--pebble-lbase-max-bytes` | 2 GiB | L1 size cap. Affects compaction scheduling, not resident memory |
| `--pebble-target-file-size` | 256 MiB | SST file size target. Larger = fewer files on disk |
| `--pebble-max-concurrent-compactions` | 2 | Parallel compaction goroutines. Each uses temporary memory for merge buffers |
| `--pebble-bytes-per-sync` | 1 MiB | Bytes written before fsync during flush/compaction |
| `--pebble-wal-bytes-per-sync` | 1 MiB | WAL bytes written before fsync |
| `--pebble-wal-min-sync-interval` | 0 | Min delay between WAL syncs (0 = immediate) |
| `--pebble-disable-wal` | false | Disables WAL entirely (**dangerous**: data loss on crash) |

---

## bbolt Read Index

**Flag:** `--read-index-initial-mmap-size`
**Default:** `1073741824` (1 GiB)
**Type:** bytes

The read index uses bbolt with a configurable `InitialMmapSize`. This is a **virtual address space reservation**, not physical RAM. The OS maps physical pages on demand as the database grows. In practice, RSS from bbolt tracks the actual database file size, but the virtual mapping appears in `VIRT`/`VSZ` metrics.

**Impact of changing:**

| Direction | Effect |
|-----------|--------|
| Increase | Larger pre-allocated virtual address space; avoids mmap grow stalls for very large databases |
| Decrease | Smaller virtual footprint; bbolt may need to remap as the database grows, causing brief stalls |

**Related flags:**

| Flag | Default | Purpose |
|------|---------|---------|
| `--read-index-initial-mmap-size` | 1 GiB | Pre-allocated virtual address space for bbolt mmap |
| `--read-index-no-freelist-sync` | false | Skip freelist serialization on commit. Faster bulk writes, slower reopen after crash |
| `--read-index-batch-size` | 1000 | Log entries per bbolt write transaction. Larger = fewer transactions, more memory per batch |
| `--read-index-freelist-sync-interval` | 5m | Periodic freelist sync when `no-freelist-sync` is enabled. Safety net for crash recovery |

---

## Raft Transport Buffers

**Flag:** `--raft-transport-buffer-size`
**Default:** `10485760` (10 MiB)
**Type:** bytes, per peer

Each peer connection allocates a send buffer of this size. Total transport memory scales linearly with the number of peers.

**Formula:** `transport-buffer-size * (number of peers - 1)`

| Cluster size | Default memory |
|-------------|----------------|
| 3 nodes | 20 MiB |
| 5 nodes | 40 MiB |

**Impact of changing:**

| Direction | Effect |
|-----------|--------|
| Increase | More messages buffered before backpressure; useful for bursty workloads or high-latency networks |
| Decrease | Earlier backpressure; lower memory; may drop messages under burst if peer is slow |

### Related transport flags

| Flag | Default | Purpose |
|------|---------|---------|
| `--raft-transport-reception-queues` | `10,512,512` | Per-priority receive queue capacities (high, medium, low) |
| `--raft-transport-send-queues` | `10,512,512` | Per-priority send queue capacities (high, medium, low) |

Queue capacities affect the number of in-flight message batches, not byte-level memory.

---

## FSM Cache

**Flag:** `--cache-rotation-threshold`
**Default:** `1000`

The FSM cache holds recently-written attribute data (volumes, account metadata, ledger info, etc.) in 9 `AttributeCache` instances, each with two generations (Gen0 = current, Gen1 = previous).

When the Raft applied index crosses a generation boundary (`index % threshold`), Gen1 is discarded and Gen0 becomes Gen1.

**Estimation formula:**

```
FSM cache ~= 2 generations * threshold * 30 keys/entry * 300 bytes/key
```

| Threshold | Estimated cache size |
|-----------|---------------------|
| 1,000 (default) | ~18 MiB |
| 5,000 | ~90 MiB |
| 10,000 | ~180 MiB |

The 9 caches and their approximate per-entry sizes:

| Cache | Value type | ~bytes/entry |
|-------|-----------|-------------|
| Volumes | 4x Uint256 | 232 |
| AccountMetadata | metadata map | 128 |
| Ledgers | LedgerInfo proto | 848 |
| Boundaries | LedgerBoundaries | 348 |
| IdempotencyKeys | key value | 65 |
| References | tx reference | 72 |
| SinkConfigs | sink config | 128 |
| NumscriptVersions | string | 64 |
| NumscriptEntries | bool | 33 |

**Impact of changing:**

| Direction | Effect |
|-----------|--------|
| Increase | More accounts served from RAM during admission (fewer Pebble preloads); higher memory; faster writes |
| Decrease | Less memory; more Pebble reads during admission; higher write latency for active accounts that fall out of cache |

**Recommendation:** Monitor `admission.preload.duration` and `admission.preload.cache_hits`. If preload latency is high, increase the threshold. The default of 1000 works well for most workloads.

---

## Numscript Cache

**Flag:** `--numscript-cache-size`
**Default:** `1024` (entries)

LRU cache of parsed Numscript ASTs, keyed by BLAKE3 hash of the script text. Each cached program is typically 1-10 KiB depending on complexity.

**Estimated memory:** `numscript-cache-size * ~5 KiB` = ~5 MiB at default.

**Impact of changing:**

| Direction | Effect |
|-----------|--------|
| Increase | More unique scripts cached; avoids re-parsing; marginal memory increase |
| Decrease | More frequent re-parsing under diverse script workloads |

Most applications use fewer than 10 distinct scripts, so the default of 1024 is generous.

---

## gRPC Buffers

**Not configurable** (constants in `internal/infra/transport/connection_pool.go`).

| Constant | Value | Purpose |
|----------|-------|---------|
| `GRPCInitialWindowSize` | 16 MiB | Per-stream flow control window |
| `GRPCInitialConnWindowSize` | 64 MiB | Per-connection aggregate flow control |
| `GRPCReadBufferSize` | 1 MiB | Per-connection read buffer |
| `GRPCWriteBufferSize` | 1 MiB | Per-connection write buffer |
| `GRPCMaxMsgSize` | 64 MiB | Max message size (snapshots use chunked streaming) |

These apply to both client (inter-node Raft transport) and server (gRPC service API) connections. Actual memory usage depends on the number of concurrent connections and in-flight data.

---

## Go Runtime

**Environment variable:** `GOMEMLIMIT`

Controls the Go garbage collector's target heap size. When set, the GC works harder to stay under the limit, reducing the chance of OOM but increasing CPU spent on GC.

**Recommendation:** Set `GOMEMLIMIT` to ~90% of the container's memory limit. For example, if the container has 4 GiB, set `GOMEMLIMIT=3600MiB`.

The server logs the current `GOMEMLIMIT` and `GOMAXPROCS` at startup. If estimated memory exceeds `GOMEMLIMIT`, a warning is emitted.

**Runtime overhead estimate:** ~200 MiB for GC metadata, goroutine stacks, and internal structures. This is a rough estimate and varies with workload.

---

## Startup Memory Estimate

At boot, the server logs a line like:

```
Memory estimate: pebbleCache=1024MiB memtables=1536MiB bboltMmap=1024MiB transport=20MiB fsmCache=18MiB goRuntime=200MiB total=3822MiB
```

If `GOMEMLIMIT` is set and the estimate exceeds it:

```
WARNING: estimated memory usage (3822MiB) exceeds GOMEMLIMIT (3072MiB) — risk of OOM. Consider increasing memory limits or reducing pebble-cache-size / pebble-memtable-size.
```

### Sizing for Kubernetes

| Container memory | Recommended `GOMEMLIMIT` | Suggested tuning |
|-----------------|--------------------------|------------------|
| 2 GiB | 1800MiB | `--pebble-cache-size=268435456` (256 MiB), `--pebble-memtable-size=67108864` (64 MiB) |
| 4 GiB | 3600MiB | Defaults work |
| 8 GiB | 7200MiB | Increase `--pebble-cache-size` to 4 GiB for better read perf |
| 16 GiB | 14400MiB | Increase both cache and threshold for large datasets |

---

## Reducing Memory Usage

If you need to fit in a smaller memory envelope, reduce these parameters in order of impact:

1. **`--pebble-cache-size`** — biggest single component (default 1 GiB). Reduce to 512 MiB or 256 MiB.
2. **`--pebble-memtable-size`** — reduces worst-case memtable memory. Reduce to 128 MiB or 64 MiB.
3. **`--pebble-memtable-stop-writes-threshold`** — reduce from 6 to 4. Increases write stall risk.
4. **`--cache-rotation-threshold`** — reduce from 1000 to 500. Increases Pebble preload frequency.

5. **`--read-index-initial-mmap-size`** — reduce to 512 MiB or 256 MiB (virtual, not RSS). May cause mmap grow stalls on large databases.

The Go runtime overhead (~200 MiB) cannot be reduced via configuration.

---

## Related Documentation

- [Performance Tuning](./performance-tuning.md) — write path optimization and operational tuning
- [Monitoring](./monitoring.md) — metrics reference and alerting rules
- [Deployment](./deployment.md) — production deployment recommendations
- [Disk Space](./disk-space.md) — disk usage and compaction
