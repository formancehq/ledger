# Storage Drivers

This document describes the available storage drivers for the Store and how they are configured in the Ledger v3 POC.

## Overview

The Store is responsible for persisting:
- **Transaction logs** - Immutable record of all ledger operations
- **Balances** - Current balance for each account/asset combination
- **Account metadata** - Key-value metadata associated with accounts
- **Idempotency entries** - Track processed requests to prevent duplicates
- **Transaction ID mappings** - Map transaction IDs to log IDs
- **Reverted transaction IDs** - Track which transactions have been reverted

**Storage is configured at the server level** using the `--storage-type` flag. All ledgers on a node use the same storage driver.

## Configuration

Storage type is specified when starting the server:

```bash
# Using SQLite (mattn driver)
./ledger serve --storage-type sqlite-mattn

# Using SQLite (modern driver)  
./ledger serve --storage-type sqlite-modern

# Using Pebble
./ledger serve --storage-type pebble
```

Or via environment variable:

```bash
STORAGE_TYPE=pebble ./ledger serve
```

## Available Drivers

| Driver | Library | CGO Required | Best For |
|--------|---------|--------------|----------|
| `sqlite-mattn` | `github.com/mattn/go-sqlite3` | ✅ Yes | Production (better performance) |
| `sqlite-modern` | `modernc.org/sqlite` | ❌ No | Cross-compilation, Docker scratch images |
| `pebble` | `github.com/cockroachdb/pebble` | ❌ No | High-throughput workloads |

---

## SQLite Mattn (`sqlite-mattn`)

### Description

Uses the popular `github.com/mattn/go-sqlite3` driver, which is a CGO wrapper around the SQLite C library. This provides the best performance and full SQLite compatibility.

### Library

```
github.com/mattn/go-sqlite3
```

### Characteristics

| Property | Value |
|----------|-------|
| **CGO Required** | Yes |
| **Pure Go** | No |
| **Performance** | Excellent |
| **Cross-compilation** | Difficult (requires C compiler) |
| **Docker compatibility** | Requires glibc (no scratch images) |

### SQLite Settings

The driver is configured with optimized settings for write-heavy workloads:

```sql
_journal_mode=WAL          -- Write-Ahead Logging for better concurrency
_synchronous=NORMAL        -- Balanced durability/performance
_cache_size=-32768         -- 32MB cache
_temp_store=MEMORY         -- Keep temp tables in memory
_busy_timeout=5000         -- 5 second timeout for locked database
_txlock=immediate          -- Acquire write lock immediately
```

### Schema

```sql
-- Transaction logs (with idempotency tracking embedded)
CREATE TABLE logs (
    ledger TEXT NOT NULL,
    id INTEGER NOT NULL,
    data BLOB NOT NULL,          -- Protobuf-encoded log
    date TEXT,
    idempotency_key TEXT,
    idempotency_hash TEXT,
    PRIMARY KEY (ledger, id),
    UNIQUE(ledger, idempotency_key)
) WITHOUT ROWID;

CREATE INDEX idx_logs_ledger_idempotency_key ON logs(ledger, idempotency_key);

-- Account balances
CREATE TABLE balances (
    ledger TEXT NOT NULL,
    account TEXT NOT NULL,
    asset TEXT NOT NULL,
    balance TEXT NOT NULL DEFAULT '0',
    PRIMARY KEY (ledger, account, asset)
) WITHOUT ROWID;

-- Account metadata
CREATE TABLE account_metadata (
    ledger TEXT NOT NULL,
    account_address TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (ledger, account_address, key)
) WITHOUT ROWID;

-- Transaction ID to log ID mapping
CREATE TABLE transaction_ids (
    ledger TEXT NOT NULL,
    transaction_id INTEGER NOT NULL,
    log_id INTEGER NOT NULL,
    PRIMARY KEY (ledger, transaction_id)
) WITHOUT ROWID;

-- Reverted transactions tracking
CREATE TABLE reverted_transaction_ids (
    ledger TEXT NOT NULL,
    transaction_id INTEGER NOT NULL,
    PRIMARY KEY (ledger, transaction_id)
) WITHOUT ROWID;

-- Last applied Raft index tracking
CREATE TABLE last_applied_index (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    value INTEGER NOT NULL DEFAULT 0
);
```

### Use Cases

- **Production deployments** where performance is critical
- **Linux servers** with standard glibc
- **Environments** where CGO is available

### File Location

```
data/runtime.db
```

---

## SQLite Modern (`sqlite-modern`)

### Description

Uses the `modernc.org/sqlite` driver, which is a pure Go implementation of SQLite. No CGO required, making it ideal for cross-compilation and minimal Docker images.

### Library

```
modernc.org/sqlite
```

### Characteristics

| Property | Value |
|----------|-------|
| **CGO Required** | No |
| **Pure Go** | Yes |
| **Performance** | Good (slightly slower than mattn) |
| **Cross-compilation** | Easy |
| **Docker compatibility** | Works with scratch images |

### SQLite Settings

Similar optimized settings as sqlite-mattn:

```sql
journal_mode(WAL)          -- Write-Ahead Logging
synchronous(NORMAL)        -- Balanced durability/performance
busy_timeout(5000)         -- 5 second timeout
temp_store(MEMORY)         -- Keep temp tables in memory
cache_size(-32768)         -- 32MB cache
```

### Schema

Same schema as sqlite-mattn (see above).

### Use Cases

- **Cross-platform builds** where CGO is not available
- **Minimal Docker images** (scratch, distroless)
- **Development environments** without C compiler
- **CI/CD pipelines** for simpler builds

### File Location

```
data/runtime.db
```

---

## Pebble (`pebble`)

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

Unlike SQLite which stores the current balance, Pebble stores **balance diffs** (deltas):

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

## Comparison

### Performance

| Operation | sqlite-mattn | sqlite-modern | pebble |
|-----------|--------------|---------------|--------|
| Single insert | ⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ |
| Batch insert | ⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ |
| Balance read | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ |
| Range scan | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |

### Features

| Feature | sqlite-mattn | sqlite-modern | pebble |
|---------|--------------|---------------|--------|
| SQL queries | ✅ | ✅ | ❌ |
| Pure Go | ❌ | ✅ | ✅ |
| ACID transactions | ✅ | ✅ | ✅ |
| Compression | ❌ | ❌ | ✅ |
| Point-in-time recovery | ❌ | ❌ | ✅ (checkpoints) |
| Metrics | Basic | Basic | Detailed |

### Recommendations

| Scenario | Recommended Driver |
|----------|-------------------|
| Production (Linux) | `sqlite-mattn` or `pebble` |
| Development | `sqlite-modern` |
| Docker scratch images | `sqlite-modern` or `pebble` |
| High write throughput | `pebble` |
| Simple debugging | `sqlite-mattn` or `sqlite-modern` |
| Cross-compilation | `sqlite-modern` or `pebble` |

---

## Creating a Ledger

Ledgers are created without specifying storage - storage is determined by the server's `--storage-type` flag:

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

All drivers implement the `Store` interface defined in `internal/store/store.go`. The interface provides:

- **Log operations**: `AppendLogs`, `GetAllLogs`, `GetLogByID`
- **Runtime queries**: `GetBalances`, `GetAccountMetadata`
- **Idempotency**: `GetLogIDForIdempotencyKey`
- **Transaction tracking**: `GetLogIDForTransactionID`, `IsTransactionReverted`
- **Lifecycle**: `Close`, `CreateSnapshot`, `GetLastAppliedIndex`, `DeleteLedger`

### Source Files

| Driver | Source File |
|--------|-------------|
| sqlite-mattn | `internal/store/sqlite/store.go` |
| sqlite-modern | `internal/store/sqlite/store.go` |
| pebble | `internal/store/pebble/store.go` |
| Common SQLite | `internal/store/sqlite/db.go` |
| Interfaces | `internal/store/store.go` |

---

## See Also

- [Storage and Persistence](./storage.md) - Overview of storage architecture
- [Architecture](./architecture.md) - System architecture overview
- [API Reference](./api.md) - API documentation
