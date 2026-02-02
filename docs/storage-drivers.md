# Storage Drivers

This document describes the storage driver for the Store in the Ledger v3 POC.

## Overview

The Store is responsible for persisting:
- **Transaction logs** - Immutable record of all ledger operations
- **Balances** - Current balance for each account/asset combination
- **Account metadata** - Key-value metadata associated with accounts
- **Idempotency entries** - Track processed requests to prevent duplicates
- **Transaction ID mappings** - Map transaction IDs to log IDs
- **Reverted transaction IDs** - Track which transactions have been reverted

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

Pebble uses single-byte prefixes for efficient key organization. Keys are formatted as `{ledger}/{prefix}{data}`:

| Prefix | Data Type | Key Format | Value |
|--------|-----------|------------|-------|
| `0x00` | Last Applied Index | `{prefix}` | `uint64` (8 bytes, big-endian) - Raft index |
| `0x01` | Logs | `{ledger}/{prefix}{log_id}` | Protobuf-encoded `Log` message |
| `0x02` | Balance diffs | `{ledger}/{prefix}{account}{asset}{timestamp}` | `big.Int` bytes - balance delta |
| `0x03` | Account metadata | `{ledger}/{prefix}{account}{key}` | `string` - metadata value |
| `0x04` | Idempotency entries | `{ledger}/{prefix}{idempotency_key}` | `uint64` (8 bytes) - log ID |
| `0x05` | Transaction ID → Log ID | `{ledger}/{prefix}{transaction_id}` | `uint64` (8 bytes) - log ID |
| `0x06` | Reverted transaction IDs | `{ledger}/{prefix}{transaction_id}` | `[]byte{1}` - presence marker |

**Key encoding details**:
- `{ledger}` - ledger name as string
- `{log_id}`, `{transaction_id}` - 8 bytes, big-endian uint64
- `{timestamp}` - 8 bytes, big-endian int64 (nanoseconds since Unix epoch)
- `{account}`, `{asset}`, `{key}` - strings (concatenated without separator)

### Balance Storage Model

Pebble stores **balance diffs** (deltas):

- Each transaction creates new diff entries with a unique timestamp
- Balance is computed by summing all diffs for an account/asset pair
- No read-before-write needed for updates
- Excellent write throughput

```
Key:   {ledger}/0x02{account}{asset}{timestamp_int64}
Value: big.Int bytes representing the balance delta
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

### Common Interface

The Store implements the `Batch` interface defined in `internal/store/types.go`. The interface provides:

- **Log operations**: `AppendLogs`, `GetAllLogs`, `GetLogBySequence`
- **Runtime queries**: `GetBalanceDiffs`, `GetAccountMetadata`
- **Idempotency**: `GetSequenceForIdempotencyKey`
- **Transaction tracking**: `GetSequenceForTransactionID`, `IsTransactionReverted`
- **Lifecycle**: `Close`, `CreateSnapshot`, `GetLastAppliedIndex`

### Source Files

| Component | Source File |
|-----------|-------------|
| Store | `internal/store/pebble_store.go` |
| Batch | `internal/store/batch.go` |
| Config | `internal/store/config.go` |
| Metrics | `internal/store/metrics.go` |
| Interfaces | `internal/store/types.go` |

---

## See Also

- [Storage and Persistence](./storage.md) - Overview of storage architecture
- [Architecture](./architecture.md) - System architecture overview
- [API Reference](./api.md) - API documentation
