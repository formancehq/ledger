# Storage Drivers

This document describes the storage driver for the Store in the Ledger v3 POC.

## Overview

The Store is responsible for persisting:
- **System logs** - Immutable record of all system-wide operations (by global sequence)
- **Ledger info** - Ledger metadata (by numeric ledger ID)
- **Idempotency entries** - Track processed requests to prevent duplicates (direct prefix)
- **Transaction updates** - Per-ledger transaction state (init, revert, metadata changes)
- **Attributes** - Generation-cached key-value pairs for volumes, metadata, reversions, etc.
- **Audit entries** - Audit trail for every proposal outcome
- **Last applied index/timestamp** - Raft index and HLC timestamp for crash recovery

The primary store uses **RocksDB** through `internal/storage/kv` and
`github.com/linxGnu/grocksdb`. The read index and usage projection are separate
RocksDB databases. The server requires CGO and a compatible RocksDB 11 library.
Ledger v3 has no stable Pebble storage format to support. Start with a fresh
RocksDB data directory; existing development Pebble directories are not opened
or migrated by the server.

## RocksDB settings

The main store maps the existing configuration fields to RocksDB write buffers,
L0 thresholds, base and target file sizes, per-level compression, block cache,
background compaction, and WAL sync pacing. Defaults remain 256 MiB per write
buffer, six write buffers, L0 trigger four, L0 stop threshold sixteen, 2 GiB
base level, 1 GiB block cache, 256 MiB target SST, and two background
compactions. A durable WAL fence precedes Raft WAL snapshot maintenance.
Published primary checkpoints flush before copying SST files; the read index
has WAL disabled and flushes before publishing its own checkpoints. A direct
read-index checkpoint without that flush deliberately omits memtable writes.

RocksDB has no qualified equivalents for the configured Pebble automatic WAL
failover, WAL minimum sync interval, or value separation policy. Startup
rejects those nondefault settings. The read index currently uses bounded
lexicographic scans without the old ledger-prefix bloom optimization.

### Key Schema

Every primary-store key starts with a **zone byte** that groups data by access pattern, followed by a **sub-prefix** and type-specific fields. Zone bytes are defined in `internal/storage/dal/store.go`.

#### Zone layout

| Zone | Byte | Description |
|------|------|-------------|
| Attributes | `0x01` | Hot-path attribute data (volumes, metadata, boundaries, etc.) |
| Cache | `0x02` | Generation-based cache for fast restart (0xFF zone) |
| Per-Ledger | `0x03` | Per-ledger state (reversions, pending cleanups, mirror source head and status) |
| History | `0x04` | Permanent history (logs, audit entries) |
| Idempotency | `0x05` | Deduplication keys with TTL |
| Global | `0x06` | Singleton system state (applied index, ledger info, signing, config) |

#### Attributes zone (`0x01`)

Key format: `[0x01][SubAttr][canonicalKey]`

Each attribute type has a single entry per canonical key (last-write-wins).

| Sub-prefix | Attribute | Canonical Key Format |
|------------|-----------|---------------------|
| `0x01` | Volumes | `[ledgerName 64B][account]\x00[color]\x00[asset_base][precision]` |
| `0x02` | Account Metadata | `[ledgerName 64B][account]\x01[key]` |
| `0x03` | Transaction State | `[ledgerName 64B]\x02[txID 8B]` |
| `0x04` | Ledger Info | `[ledger name]` |
| `0x05` | Boundaries | `[ledger name]` |
| `0x06` | References | `[ledgerName 64B][reference]` |
| `0x07` | Ledger Metadata | `[ledgerName 64B]\x01[key]` |
| `0x08` | Sink Configs | `[name]` |
| `0x09` | Numscript Versions | `[ledgerName 64B][name]` |
| `0x0A` | Numscript Contents | `[ledgerName 64B][name]\x00[version]` |
| `0x0B` | Prepared Queries | `[ledgerName 64B][name]` |

> **Note:** All ledger-scoped attribute keys are prefixed by the fixed-width **64-byte, zero-padded ledger name** (`LedgerScopedPrefix`), which gives uniform prefix-scan semantics (e.g. scanning by `(ledgerName, account)` returns every color of a volume). `LedgerKey` (Ledger Info and Boundaries) uses the bare ledger-name string, because those entries are looked up by name. For volumes the color is placed **between** account and asset (`[account]\x00[color]\x00[asset_base]`) so a `(ledgerName, account)` prefix scan still returns all colors of an account.

See [System Attributes](../attributes/attributes.md) and [Attribute Key Hashing](../attributes/key-hashing.md) for the caching model and U128 hash key system.

#### Cache zone (`0x02`)

Key format: `[0x02][genByte][SubAttr][16-byte U128]`

Mirrors attribute values in a lean format (`[8-byte tag][1-byte flag][proto bytes]`) for fast restart without scanning the full attributes zone. The flag byte at offset 8 is `0x00` for a live entry and `0x01` for a tombstone (no trailing bytes — tombstones are uniform 9-byte rows). The gen byte alternates between 0 and 1 on generation rotation.

The explicit flag byte is required because some attribute protos legitimately marshal to zero bytes (presence-only markers, all-default scalars, unset oneofs). Using `len(value) == 0` as the tombstone signal would silently resurrect such live entries as tombstones on restore.

Special keys:
- `[0x02][0xFF]` — global cache snapshot metadata (`CacheSnapshotMeta`)
- `[0x02][genByte][0x00]` — per-generation metadata (`CacheGenerationMeta`)

#### Per-Ledger zone (`0x03`)

Key format: `[0x03][SubPL][ledgerID BE 4B][...]`

| Sub-prefix | Data |
|------------|------|
| `0x01` | Reversion bitset words |
| `0x02` | Pending ledger cleanups |
| `0x03` | Prepared queries (per-ledger) |
| `0x04` | Mirror source head |
| `0x03` | Mirror source head |
| `0x04` | Mirror status |

#### History zone (`0x04`)

Key format: `[0x04][SubHistory][sequence (8 bytes BE)]`

| Sub-prefix | Data |
|------------|------|
| `0x01` | Transaction logs (protobuf `Log`) |
| `0x02` | Audit entries (protobuf `AuditEntry`) |
| `0x03` | Audit items (protobuf `AuditItem`) |
| `0x04` | Applied proposals (protobuf `AppliedProposal`) |

History zone data is permanent: it is never purged.

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
| `0x03` | Ledger info entries (keyed by ledger name string) |
| `0x04` | Signing keys (Ed25519 public keys) |
| `0x05` | Signing config (require signatures flag) |
| `0x06`-`0x08` | Sink cursors, events config, sink status |
| `0x09` | Maintenance mode flag |
| `0x0A` | Persisted config (node-id, cluster-id validation) |
| `0x0B`-`0x0D` | Query checkpoints, next checkpoint ID, checkpoint schedule |
| `0x0E` | Cluster config (rotation threshold, bloom config) |
| `0x0F` | Bloom filter persisted blocks |
| `0x13` | Next ledger ID counter (`uint32`) |

### Balance Storage Model

Volumes use **last-write-wins** semantics with a single entry per canonical key:

```
Key:   [0x01][0x01][ledgerName 64B][account]\x00[color]\x00[asset_base][precision]
Value: VolumePair protobuf (Input + Output as Uint256)
```

Each write overwrites the previous value. The in-memory cache tracks two generations for eviction; see [Attribute Key Hashing](../attributes/key-hashing.md).

### Use Cases

- **High-throughput workloads** with many transactions
- **Write-heavy applications** where write performance is critical
- **Large ledgers** that benefit from LSM-tree compaction
- **Linux deployments** with a compatible RocksDB library and CGO toolchain

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

With incremental cache persistence (cache zone written in each RocksDB batch), the `live/` directory is always up-to-date after each commit. On startup:

1. **Normal restart**: If `live/` exists, open it directly — no checkpoint restoration needed. RocksDB's own WAL ensures crash safety.
2. **Fresh start**: If `live/` does not exist, create a new RocksDB database.
3. **Follower sync**: Checkpoints are created on Raft snapshot and used by followers joining the cluster via `SynchronizeWithLeader`.
4. **Efficiency**: Checkpoint clones hard-link immutable SST/blob files and copy mutable metadata. Each database keeps its own lock file.

### L0 Compaction Management

The `L0CompactionThreshold` is set low (default 4) so that RocksDB auto-compacts aggressively and L0 never accumulates excessively. This eliminates the need for manual startup or periodic compaction.

The key space is divided into zones with different compaction characteristics:

- **History zone** (`0x04`) — logs, audit. Immutable, sequential, write-once data; RocksDB's automatic compaction is all it needs.
- **Attributes zone** (`0x01`) — volumes, metadata, etc. Last-write-wins entries are naturally compacted by RocksDB.
- **Cache zone** (`0x02`) — `DeleteRange` tombstones from generation-rotation pruning are pushed down the LSM by RocksDB's automatic compaction.
- **Global zone** (`0x06`) — tiny singleton keys, RocksDB handles natively.

**RocksDB automatic compaction** runs when L0 reaches the threshold (default 4). This handles steady-state write workloads and keeps L0 clean at all times. A synchronous full-keyspace compaction can be triggered on demand (`Store.CompactAll`, exposed via the `Compact` RPC).

Source files: `internal/storage/dal/compact.go`.

### Metrics

The DAL exports available RocksDB properties through OpenTelemetry, including
memtable size, pending flush and compaction, live SST size, block-cache use,
snapshot count, stopped writes, and background errors. Pebble-specific event
and VFS counters are unavailable. The service API retains the old metrics
protobuf envelope for compatibility, but only fields with a direct RocksDB
meaning are populated.

---

## Configuration

Use the `--rocksdb-*` flags to configure the primary store:

```bash
./ledger serve \
  --rocksdb-memtable-size=256Mi \
  --rocksdb-memtable-stop-writes-threshold=6 \
  --rocksdb-l0-compaction-threshold=4 \
  --rocksdb-l0-stop-writes-threshold=16 \
  --rocksdb-lbase-max-bytes=2Gi \
  --rocksdb-cache-size=1Gi \
  --rocksdb-target-file-size=256Mi \
  --rocksdb-bytes-per-sync=1Mi \
  --rocksdb-wal-bytes-per-sync=1Mi \
  --rocksdb-max-concurrent-compactions=2 \
  --rocksdb-wal-min-sync-interval=0 \
  --rocksdb-disable-wal=false
```

Or via environment variables:

```bash
ROCKSDB_MEMTABLE_SIZE=268435456 ./ledger serve
```

---

## Creating a Ledger

Ledgers are created without specifying storage; RocksDB is the storage backend:

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

The `Store` (`internal/storage/dal/store.go`) manages the RocksDB database lifecycle, checkpoints, and read operations. It provides:

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

### Write session ownership

`WriteSession` owns its RocksDB write batch until `Commit` or `Cancel`.
`Commit` applies the batch atomically with WAL enabled and no per-batch fsync,
then destroys the native batch and clears the reference. The DAL explicitly
syncs or flushes before a durable Raft WAL boundary or published checkpoint.
A failed commit leaves the session owned by its caller so it can be cancelled;
it does not imply rollback. A terminal session cannot be reused.

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

- [Storage and Persistence](storage.md) - Overview of storage architecture
- [Architecture](../../overview.md) - System architecture overview
- [API Reference](../api/http-api.md) - API documentation
