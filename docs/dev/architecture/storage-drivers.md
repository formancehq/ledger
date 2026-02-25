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
MemTableStopWritesThreshold: 4          // Reduce write stalls
L0CompactionThreshold:       8          // Trigger L0->L1 compactions earlier
L0StopWritesThreshold:       32         // Higher threshold for stop-the-world writes
LBaseMaxBytes:               512 << 20  // 512MB base level
TargetFileSize:              64 << 20   // 64MB per SST file
BytesPerSync:                1 << 20    // 1MB sync interval
WALBytesPerSync:             1 << 20    // 1MB WAL sync interval
MaxConcurrentCompactions:    2          // Parallel compactions (balance CPU/IO)
```

### Key Schema

Pebble uses single-byte prefixes for efficient key organization:

| Prefix | Data Type | Key Format | Value |
|--------|-----------|------------|-------|
| `0x00` | Last Applied Index | `[0x00]` | `uint64` (8 bytes, big-endian) — Raft index |
| `0x01` | System Logs | `[0x01][sequence]` | Protobuf-encoded `Log` message |
| `0x02` | Idempotency | `[0x02][key_string]` | `uint64` (8 bytes) — log sequence |
| `0x03` | Ledger Info | `[0x03][ledgerID]` | Protobuf-encoded `LedgerInfo` |
| `0x04` | Last Applied Timestamp | `[0x04]` | `uint64` (8 bytes) — HLC microseconds |
| `0x08` | Transaction Updates | `[ledgerPrefix][0x08][txID][byLog]` | Protobuf-encoded `TransactionUpdate` |
| `0x09` | Attributes | `[0x09][attrPrefix][canonicalKey][raftIndex][entryType]` | Varies by attribute type |
| `0x0A` | Audit Entries | `[0x0A][sequence]` | Protobuf-encoded `AuditEntry` |

**Key encoding details**:
- `[sequence]`, `[txID]`, `[byLog]`, `[raftIndex]` — 8 bytes, big-endian uint64
- `[ledgerID]` — 4 bytes, big-endian uint32
- `[ledgerPrefix]` — 4 bytes, big-endian uint32 (ledger numeric ID)
- `[attrPrefix]` — 1 byte ASCII letter (see [Attributes](./attributes.md))
- `[entryType]` — 1 byte: `0` = base, `1` = diff
- `[canonicalKey]` — variable-length, domain-specific (e.g., `[ledgerID][account]\x00[asset]` for volumes)

### Attribute Sub-Prefixes

Under the `0x09` attribute prefix, each attribute type uses an ASCII letter sub-prefix:

| Sub-prefix | Attribute | Canonical Key |
|------------|-----------|---------------|
| `'I'` (0x49) | Input Volumes | `[ledgerID][account]\x00[asset]` |
| `'O'` (0x4F) | Output Volumes | `[ledgerID][account]\x00[asset]` |
| `'M'` (0x4D) | Account Metadata | `[ledgerID][account]\x01[key]` |
| `'L'` (0x4C) | Ledger Metadata | `[ledgerID][key]` |
| `'R'` (0x52) | Reversions | `[ledgerID][txID]` |
| `'K'` (0x4B) | Idempotency Keys | `[key_string]` |
| `'F'` (0x46) | Transaction References | `[ledgerID][reference]` |
| `'G'` (0x47) | Ledgers | `[ledger_name]` |
| `'B'` (0x42) | Boundaries | `[ledgerID]` |

See [System Attributes](./attributes.md) for the complete attribute storage and caching model.

### Balance Storage Model

Pebble stores volumes using the **attribute base + cumulative diff** model:

- Each volume attribute has a **base** entry and **cumulative diff** entries keyed by Raft index
- Balance is computed as `base + latest_cumulative_diff`
- Only the latest diff is needed (diffs are cumulative, not incremental)
- Generation-rotation pruning and inline compaction keep entry count bounded

```
Key:   [0x09][ledgerID][account]\x00[asset]['V'][raftIndex][entryType]
Value: VolumePair protobuf (base value or cumulative diff)
```

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

### Checkpoint System

Pebble uses a checkpoint-based system for durability:

1. **On startup**: If `CURRENT_CHECKPOINT` exists, restore from that checkpoint using hard links to the `live/` directory
2. **On first run**: Create an initial checkpoint (ID: 0)
3. **On snapshot**: Create a new checkpoint with incremented ID using Pebble's built-in checkpoint feature
4. **Efficiency**: Checkpoints use hard links, so they don't duplicate data

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
  --pebble-memtable-size=268435456 \
  --pebble-memtable-stop-writes-threshold=6 \
  --pebble-l0-compaction-threshold=64 \
  --pebble-l0-stop-writes-threshold=256 \
  --pebble-lbase-max-bytes=2147483648 \
  --pebble-cache-size=1073741824 \
  --pebble-target-file-size=268435456 \
  --pebble-bytes-per-sync=1048576 \
  --pebble-wal-bytes-per-sync=1048576 \
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
| Metrics | `internal/storage/dal/metrics.go` |
| Types (keys) | `internal/storage/dal/types.go` |
| Key builder | `internal/storage/dal/key_builder.go` |

---

## See Also

- [Storage and Persistence](./storage.md) - Overview of storage architecture
- [Architecture](./architecture.md) - System architecture overview
- [API Reference](./api.md) - API documentation
