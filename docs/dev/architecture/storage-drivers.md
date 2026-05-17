# Storage Drivers

This document describes the storage driver for the Store in the Ledger v3 POC.

## Overview

The Store is responsible for persisting:
- **System logs** - Immutable record of all system-wide operations (by global sequence)
- **Ledger info** - Ledger metadata (by numeric ledger ID)
- **Idempotency entries** - Track processed requests to prevent duplicates (direct prefix)
- **Transaction updates** - Per-ledger transaction state (init, revert, metadata changes)
- **Attributes** - Generation-cached key-value pairs for volumes, metadata, reversions, etc.
- **Audit entries** - Optional audit trail for every proposal outcome
- **Last applied index/timestamp** - Raft index and HLC timestamp for crash recovery

The storage backend is **Pebble**, a high-performance LSM-tree based storage engine from CockroachDB.

---

## Pebble

### Description

Uses CockroachDB's Pebble key-value store, a high-performance LSM-tree based storage engine. Designed for high-throughput workloads with excellent write performance.

### Library

```
github.com/cockroachdb/pebble
```

### Characteristics

| Property | Value |
|----------|-------|
| **CGO Required** | No |
| **Pure Go** | Yes |
| **Performance** | Excellent for writes |
| **Cross-compilation** | Easy |
| **Docker compatibility** | Works with scratch images |

### Pebble Settings

Optimized for ledger workloads with high write throughput:

```go
MemTableSize:                256 << 20  // 256MB memtable (absorb more writes before flush)
MemTableStopWritesThreshold: 6          // Reduce write stalls
L0CompactionThreshold:       4          // Low threshold: Pebble auto-compacts aggressively
L0StopWritesThreshold:       16         // ~4x ratio above compaction threshold
LBaseMaxBytes:               2 << 30    // 2GB base level
CacheSize:                   1024 << 20 // 1GB block cache
TargetFileSize:              256 << 20  // 256MB per SST file
BytesPerSync:                1 << 20    // 1MB sync interval
WALBytesPerSync:             1 << 20    // 1MB WAL sync interval
MaxConcurrentCompactions:    2          // Parallel compactions (balance CPU/IO)
```

### Key Schema

Every Pebble key starts with a **zone byte** that groups data by access pattern, followed by a **sub-prefix** and type-specific fields. Zone bytes are defined in `internal/storage/dal/store.go`.

#### Zone layout

| Zone | Byte | Description |
|------|------|-------------|
| Attributes | `0x01` | Hot-path attribute data (volumes, metadata, boundaries, etc.) |
| Cache | `0x02` | Generation-based cache for fast restart (0xFF zone) |
| Per-Ledger | `0x03` | Per-ledger state (reversions, pending cleanups, mirror cursors) |
| Cold | `0x04` | Archivable data (logs, audit entries) |
| Idempotency | `0x05` | Deduplication keys with TTL |
| Global | `0x06` | Singleton system state (applied index, ledger info, signing, periods, config) |

#### Attributes zone (`0x01`)

Key format: `[0x01][SubAttr][canonicalKey]`

Each attribute type has a single entry per canonical key (last-write-wins).

| Sub-prefix | Attribute | Canonical Key Format |
|------------|-----------|---------------------|
| `0x01` | Volumes | `ledger\x00account\x00asset` |
| `0x02` | Account Metadata | `ledger\x00account\x01key` |
| `0x03` | Transaction State | `ledger\x00txID` |
| `0x04` | Ledger Info | `ledger\x00` |
| `0x05` | Boundaries | `ledger\x00` |
| `0x06` | References | `ledger\x00reference` |
| `0x07` | Ledger Metadata | `ledger\x00key` |
| `0x08` | Sink Configs | `name` |
| `0x09` | Numscript Versions | `ledger\x00name` |
| `0x0A` | Numscript Contents | `ledger\x00name\x00version` |
| `0x0B` | Prepared Queries | `ledger\x00name` |

See [System Attributes](./attributes.md) and [Attribute Key Hashing](./attribute-key-hashing.md) for the caching model and U128 hash key system.

#### Cache zone (`0x02`)

Key format: `[0x02][genByte][SubAttr][16-byte U128]`

Mirrors attribute values in a lean format (`[8-byte tag][proto bytes]`) for fast restart without scanning the full attributes zone. The gen byte alternates between 0 and 1 on generation rotation.

Special keys:
- `[0x02][0xFF]` — global cache snapshot metadata (`CacheSnapshotMeta`)
- `[0x02][genByte][0x00]` — per-generation metadata (`CacheGenerationMeta`)

#### Per-Ledger zone (`0x03`)

Key format: `[0x03][SubPL][ledger\x00][...]`

| Sub-prefix | Data |
|------------|------|
| `0x01` | Reversion bitset words |
| `0x02` | Pending ledger cleanups |
| `0x03` | Prepared queries (per-ledger) |
| `0x04` | Mirror source head |
| `0x05` | Mirror cursor |
| `0x06` | Mirror status |

#### Cold zone (`0x04`)

Key format: `[0x04][SubCold][sequence (8 bytes BE)]`

| Sub-prefix | Data |
|------------|------|
| `0x01` | Transaction logs (protobuf `Log`) |
| `0x02` | Audit entries (protobuf `AuditEntry`) |

Cold zone data is archived to cold storage per period, then purged via `DeleteRange`.

#### Idempotency zone (`0x05`)

| Sub-prefix | Key Format | Data |
|------------|-----------|------|
| `0x01` | `[0x05][0x01][key_string]` | `IdempotencyKeyValue` protobuf |
| `0x02` | `[0x05][0x02][timestamp (8B BE)][key_string]` | Time index for TTL eviction |

#### Global zone (`0x06`)

Singleton keys for system-wide state:

| Sub-prefix | Data |
|------------|------|
| `0x01` | Last applied Raft index (`uint64 BE`) |
| `0x02` | Last applied HLC timestamp (`uint64 BE`) |
| `0x03` | Ledger info entries (keyed by `ledger\x00`) |
| `0x04` | Signing keys (Ed25519 public keys) |
| `0x05` | Signing config (require signatures flag) |
| `0x06` | Periods state (protobuf per period) |
| `0x07` | Next period ID counter |
| `0x08`-`0x0A` | Sink cursors, events config, sink status |
| `0x0B` | Maintenance mode flag |
| `0x0C` | Persisted config (node-id, cluster-id validation) |
| `0x0D` | Period schedule (cron expression) |
| `0x0E`-`0x10` | Query checkpoints, next checkpoint ID, checkpoint schedule |
| `0x11` | Cluster config (rotation threshold, bloom config) |
| `0x12` | Bloom filter persisted blocks |

### Balance Storage Model

Volumes use **last-write-wins** semantics with a single entry per canonical key:

```
Key:   [0x01][0x01][ledger\x00account\x00asset]
Value: VolumePair protobuf (Input + Output as Uint256)
```

Each write overwrites the previous value. The in-memory cache tracks two generations for eviction; see [Attribute Key Hashing](./attribute-key-hashing.md).

### Use Cases

- **High-throughput workloads** with many transactions
- **Write-heavy applications** where write performance is critical
- **Large ledgers** that benefit from LSM-tree compaction
- **Production environments** requiring pure Go builds

### Directory Structure

```
data/runtime/
├── live/                    # Active database directory
│   ├── 000001.sst
│   ├── 000002.sst
│   ├── MANIFEST-000001
│   ├── OPTIONS-000001
│   └── ...
├── checkpoints/             # Checkpoint directories for snapshots
│   ├── 0/                   # Initial checkpoint
│   └── N/                   # Subsequent checkpoints
└── CURRENT_CHECKPOINT       # File containing current checkpoint ID
```

### Startup and Checkpoint System

With incremental cache persistence (cache zone written in each Pebble batch), the `live/` directory is always up-to-date after each commit. On startup:

1. **Normal restart**: If `live/` exists, open it directly — no checkpoint restoration needed. Pebble's own WAL ensures crash safety.
2. **Fresh start**: If `live/` does not exist, create a new Pebble database.
3. **Follower sync**: Checkpoints are created on Raft snapshot and used by followers joining the cluster via `SynchronizeWithLeader`.
4. **Efficiency**: Checkpoints use hard links, so they don't duplicate data.

### L0 Compaction Management

The `L0CompactionThreshold` is set low (default 4) so that Pebble auto-compacts aggressively and L0 never accumulates excessively. This eliminates the need for manual startup or periodic compaction.

The key space is divided into zones with different compaction characteristics:

- **Cold zone** (`0x04`) — logs, audit. Immutable, sequential, write-once data. Compacting this zone only benefits after a period purge deletes data.
- **Attributes zone** (`0x01`) — volumes, metadata, etc. Last-write-wins entries are naturally compacted by Pebble.
- **Cache zone** (`0x02`) — `DeleteRange` tombstones from generation-rotation pruning are pushed down the LSM by Pebble's automatic compaction.
- **Global zone** (`0x06`) — tiny singleton keys, Pebble handles natively.

Two mechanisms keep L0 under control:

1. **Post-purge compaction — cold zone** (`smart_compactor.go`): When a `ConfirmArchivePeriod` is applied and period data is purged, the FSM signals the `SmartCompactor` via a channel. The compactor then runs `db.Compact` over the cold zone to push the purge tombstones down the LSM and reclaim space.

2. **Pebble automatic compaction**: Pebble's built-in compaction runs when L0 reaches the threshold (default 4). This handles steady-state write workloads and keeps L0 clean at all times.

| Mechanism | Zone | Trigger | Blocking | When |
|-----------|------|---------|----------|------|
| Post-purge compaction | Cold (`0x04`) | Period purge applied | No (background goroutine) | After period archival |
| Pebble automatic | Full range | L0 >= threshold (4) | No (background) | During sustained writes |

Source files: `internal/storage/dal/smart_compactor.go` (post-purge).

### Metrics

Pebble exposes detailed metrics accessible via the API through OpenTelemetry:

- Compaction metrics (count, duration, bytes)
- Flush metrics
- Write stall metrics
- Level statistics
- Cache hit rates

---

## Configuration

Pebble can be configured using command-line flags:

```bash
./ledger serve \
  --pebble-memtable-size=256Mi \
  --pebble-memtable-stop-writes-threshold=6 \
  --pebble-l0-compaction-threshold=4 \
  --pebble-l0-stop-writes-threshold=16 \
  --pebble-lbase-max-bytes=2Gi \
  --pebble-cache-size=1Gi \
  --pebble-target-file-size=256Mi \
  --pebble-bytes-per-sync=1Mi \
  --pebble-wal-bytes-per-sync=1Mi \
  --pebble-max-concurrent-compactions=2 \
  --pebble-wal-min-sync-interval=0 \
  --pebble-disable-wal=false
```

Or via environment variables:

```bash
PEBBLE_MEMTABLE_SIZE=268435456 ./ledger serve
```

---

## Creating a Ledger

Ledgers are created without specifying storage - Pebble is the only storage backend:

### HTTP API

```bash
curl -X POST http://localhost:9000/my-ledger \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": {
      "description": "My ledger"
    }
  }'
```

---

## Implementation Details

### Store and Batch

The `Store` (`internal/storage/dal/store.go`) manages the Pebble database lifecycle, checkpoints, and read operations. It provides:

- **Log operations**: `GetLogBySequence`, `ListTransactionIDs`
- **Idempotency**: `GetSequenceForIdempotencyKey`
- **Ledger queries**: `GetLedgerInfo`, `ListLedgers`
- **Transaction queries**: `GetTransactionByID` (reconstructs from updates)
- **Audit**: `ListAuditEntries`
- **Snapshots**: `CreateSnapshot`, `CreateBackup`

The `Batch` (`internal/storage/dal/batch.go`) provides atomic write operations:

- `SetAppliedIndex` / `SetLastAppliedTimestamp` — Raft progress tracking
- `AppendLogs` — System logs with idempotency index
- `SaveLedger` — Ledger info persistence
- `StoreTransactionUpdate` — Transaction state changes
- `AppendAuditEntries` — Audit trail entries
- `Set` / `DeleteRange` — Low-level operations used by the attribute system

### Source Files

| Component | Source File |
|-----------|-------------|
| Store | `internal/storage/dal/store.go` |
| Batch | `internal/storage/dal/batch.go` |
| Config | `internal/storage/dal/config.go` |
| Smart Compactor | `internal/storage/dal/smart_compactor.go` |
| Metrics | `internal/storage/dal/metrics.go` |
| Types (keys) | `internal/storage/dal/types.go` |
| Key builder | `internal/storage/dal/key_builder.go` |

---

## See Also

- [Storage and Persistence](./storage.md) - Overview of storage architecture
- [Architecture](./architecture.md) - System architecture overview
- [API Reference](./api.md) - API documentation
