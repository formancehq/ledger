# Storage Drivers

This document describes the available storage drivers for the Runtime Store and how they are used in the Ledger v3 POC.

## Overview

The Runtime Store is responsible for persisting:
- **Transaction logs** - Immutable record of all ledger operations
- **Balances** - Current balance for each account/asset combination
- **Account metadata** - Key-value metadata associated with accounts
- **Idempotency entries** - Track processed requests to prevent duplicates
- **Transaction ID mappings** - Map transaction IDs to log IDs
- **Reverted transaction IDs** - Track which transactions have been reverted

Each ledger has its own Runtime Store instance, and the driver is specified when creating the ledger.

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

### Configuration

```json
{
  "storeDriver": "sqlite-mattn",
  "storeConfig": {}
}
```

The DSN (Data Source Name) is automatically generated based on the ledger name and data directory. No manual configuration required.

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
-- Transaction logs
CREATE TABLE logs (
    id INTEGER PRIMARY KEY,
    data BLOB NOT NULL,          -- Protobuf-encoded log
    date TEXT,
    idempotency_key TEXT,
    idempotency_hash TEXT,
    UNIQUE(idempotency_key)
) WITHOUT ROWID;

CREATE INDEX idx_logs_idempotency_key ON logs(idempotency_key);

-- Account balances
CREATE TABLE balances (
    id INTEGER PRIMARY KEY,
    account TEXT NOT NULL,
    asset TEXT NOT NULL,
    balance TEXT NOT NULL DEFAULT '0',
    UNIQUE (asset, account)
);

-- Account metadata
CREATE TABLE account_metadata (
    account_address TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (account_address, key)
);

-- Idempotency tracking
CREATE TABLE idempotency (
    key TEXT NOT NULL,
    hash BYTEA NOT NULL,
    log_id INTEGER NOT NULL,
    PRIMARY KEY (key)
);

-- Transaction ID to log ID mapping
CREATE TABLE transaction_ids (
    transaction_id INTEGER NOT NULL,
    log_id INTEGER NOT NULL,
    PRIMARY KEY (transaction_id)
) WITHOUT ROWID;

-- Reverted transactions tracking
CREATE TABLE reverted_transaction_ids (
    transaction_id INTEGER NOT NULL,
    PRIMARY KEY (transaction_id)
) WITHOUT ROWID;
```

### Use Cases

- **Production deployments** where performance is critical
- **Linux servers** with standard glibc
- **Environments** where CGO is available

### File Location

```
data/ledgers/{ledger-name}/{ledger-name}-runtime.db
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

### Configuration

```json
{
  "storeDriver": "sqlite-modern",
  "storeConfig": {}
}
```

The DSN is automatically generated. No manual configuration required.

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
data/ledgers/{ledger-name}/{ledger-name}-runtime.db
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

### Configuration

```json
{
  "storeDriver": "pebble",
  "storeConfig": {}
}
```

The data directory is automatically generated. No manual configuration required.

### Pebble Settings

Optimized for ledger workloads:

```go
MaxConcurrentCompactions: 3      // Parallel compactions
MemTableSize: 8MB                // 8MB memtable
```

### Key Schema

Pebble uses a key-value model with prefixed keys for different data types:

| Prefix | Data Type | Key Format |
|--------|-----------|------------|
| `l` | Logs | `l{log_id}` (8 bytes, big-endian) |
| `lid` | Log idempotency index | `lid{idempotency_key}` |
| `bal` | Balance diffs | `bal{account}:{asset}:{timestamp}` |
| `met` | Account metadata | `met{account}:{key}` |
| `idm` | Idempotency entries | `idm{key}` |
| `tid` | Transaction ID → Log ID | `tid{transaction_id}` (8 bytes) |
| `rtx` | Reverted transaction IDs | `rtx{transaction_id}` (8 bytes) |

### Balance Storage Model

Unlike SQLite which stores the current balance, Pebble stores **balance diffs** (deltas):

- Each transaction creates new diff entries
- Balance is computed by summing all diffs for an account/asset
- No read-before-write needed for updates
- Excellent write throughput

```
Key: bal{account}:{asset}:{nanosecond_timestamp}
Value: Balance diff as big.Int bytes
```

### Use Cases

- **High-throughput workloads** with many transactions
- **Write-heavy applications** where write performance is critical
- **Large ledgers** that benefit from LSM-tree compaction
- **Production environments** requiring pure Go builds

### Directory Structure

```
data/ledgers/{ledger-name}/
└── live/
    ├── 000001.sst
    ├── 000002.sst
    ├── MANIFEST-000001
    ├── OPTIONS-000001
    └── ...
```

### Metrics

Pebble exposes detailed metrics accessible via the API:

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

## Creating a Ledger with a Specific Driver

### HTTP API

```bash
curl -X POST http://localhost:9000/my-ledger \
  -H "Content-Type: application/json" \
  -d '{
    "storeDriver": "pebble",
    "storeConfig": {},
    "metadata": {
      "description": "My high-performance ledger"
    }
  }'
```

### Available Driver Values

- `sqlite-mattn` - SQLite with github.com/mattn/go-sqlite3
- `sqlite-modern` - SQLite with modernc.org/sqlite
- `pebble` - CockroachDB Pebble

---

## Implementation Details

### Common Interface

All drivers implement the `RuntimeStore` interface:

```go
type RuntimeStore interface {
    LogStore
    GetBalances(ctx context.Context, balanceQuery map[string][]string) (ledgerpb.Balances, error)
    GetAccountMetadata(ctx context.Context, accounts []string) (map[string]metadata.Metadata, error)
    GetLogForIdempotencyKey(ctx context.Context, idempotencyKey string) ([]byte, uint64, error)
    GetLogIDForTransactionID(ctx context.Context, transactionID uint64) (uint64, error)
    IsTransactionReverted(ctx context.Context, transactionID uint64) (bool, error)
    GetLastProcessedLogID(ctx context.Context) (uint64, error)
}
```

### Source Files

| Driver | Source File |
|--------|-------------|
| sqlite-mattn | `internal/service/store_runtime_sqlite_mattn.go` |
| sqlite-modern | `internal/service/store_runtime_sqlite_modern.go` |
| pebble | `internal/service/store_runtime_pebble.go` |
| Common SQLite | `internal/service/store_runtime_sqlite.go` |
| Common logic | `internal/service/store_sqlite_common.go` |

---

## See Also

- [Storage and Persistence](./storage.md) - Overview of storage architecture
- [Architecture](./architecture.md) - System architecture overview
- [API Reference](./api.md) - API documentation
