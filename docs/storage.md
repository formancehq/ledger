# Storage and Persistence

## Overview

The Ledger v3 POC system uses multiple storage layers to ensure data durability and recovery:

1. **WAL (Write-Ahead Log)**: Raft log for consensus
2. **Snapshots**: Periodic restoration points
3. **Runtime Store**: Logs + runtime state (balances, account metadata, idempotency)

All ledgers share a **single storage layer**, with data organized by ledger name prefixes.

## Storage Architecture

```mermaid
graph TB
    subgraph "Raft Storage"
        WAL[WAL<br/>Write-Ahead Log]
        HardState[HardState<br/>Term, Vote, Commit]
        Snapshot[Raft Snapshot<br/>FSM State]
    end
    
    subgraph "Application Storage"
        RuntimeStore[Runtime Store<br/>All Ledgers]
    end
    
    subgraph "Directory Structure"
        WalDir[wal/]
        WalSubdir[wal/]
        SpoolDir[spool/]
        DataDir[data/]
        RuntimeDB[runtime.db or runtime/]
    end
    
    WAL --> WalSubdir
    HardState --> WalSubdir
    Snapshot --> SpoolDir
    WalSubdir --> WalDir
    SpoolDir --> WalDir
    RuntimeStore --> RuntimeDB
    RuntimeDB --> DataDir
```

## WAL (Write-Ahead Log)

### Concept

The WAL is the main log used by Raft to guarantee entry durability. It uses the `etcd/wal` library which provides:

- **Durability**: All writes are synchronized on disk
- **Performance**: Sequential writes optimized
- **Recovery**: Automatic replay at startup

### WAL Structure

```
data/
├── raft/
│   ├── wal/
│   │   ├── 0000000000000000-0000000000000000.wal
│   │   ├── 0000000000000001-0000000000000001.wal
│   │   └── ...
│   ├── raft-hardstate.json
│   └── raft-snapshot.json
└── runtime.db (SQLite) or runtime/ (Pebble)
```

### WAL Operations

#### Write

When a new entry is proposed:

1. The entry is added to memory cache (`entries`)
2. The entry is written in the WAL
3. The WAL is synchronized on disk (fsync)
4. The entry is available for replication

#### Read

At startup, the WAL is replayed to rebuild the memory cache:

1. The last snapshot is loaded
2. WAL entries after the snapshot are replayed
3. The memory cache is rebuilt
4. The FSM state is restored

### WAL Management

The WAL grows indefinitely until a snapshot is created. After a snapshot:

- Entries before the snapshot index can be compacted
- The WAL is segmented to facilitate management
- Old segments can be deleted

## HardState

### Concept

The HardState contains the critical state of the Raft cluster:

- **Term**: Current term of the cluster
- **Vote**: Node ID for which this node voted
- **Commit**: Index of the last committed entry

### Persistence

The HardState is persisted in `raft-hardstate.json`:

```json
{
  "term": 5,
  "vote": 2,
  "commit": 1234
}
```

### Update

The HardState is updated when:
- A new election occurs (term and vote change)
- An entry is committed (commit changes)

## Snapshots

### Concept

Snapshots are restoration points that contain:
- The complete FSM state at a given index
- Necessary metadata to restore the state

### Snapshot Creation

Snapshots are created automatically when:

1. **Log threshold reached**: `SnapshotThreshold` entries from the last snapshot

### Snapshot Contents

The snapshot contains the complete FSM state:
- Map of all ledgers with their states
- Each ledger state includes:
  - Ledger metadata (name, creation date, etc.)
  - Next log ID
  - Next transaction ID
  - Last applied log ID

### Snapshot Format

Snapshots are serialized using Protocol Buffers:

```protobuf
message State {
  map<string, LedgerState> ledgers = 1;
}

message LedgerState {
  LedgerInfo ledger_info = 1;
  uint64 next_log_id = 2;
  uint64 next_transaction_id = 3;
  uint64 last_applied_log_id = 4;
}
```

### Restoration from Snapshot

When a node starts or recovers:

1. The most recent snapshot is loaded
2. The FSM state is restored from the snapshot
3. For each ledger with missing logs, logs are streamed from the leader using gRPC
4. Commands buffered during synchronization are replayed from the spool
5. The final state is reached

#### Spool: Command Buffer During Synchronization

When a node is synchronizing from a snapshot (e.g., after joining the cluster or recovering from a failure), it enters a "syncing" mode. During this mode:

- **Committed entries are not applied directly to the FSM**: Instead, they are written to a spool file
- **Spool purpose**: Buffers commands that arrive during synchronization, preventing them from being lost
- **After synchronization**: Commands from the spool are replayed sequentially to catch up

**File**: `internal/raft/spool.go`

**Spool Operations**:

```go
// Append commands to the spool during synchronization
func (s *spool) AppendCommittedEntries(ctx context.Context, commands ...Command) error

// Read the next command from the spool (iterator pattern)
func (s *spool) Next() (Command, error) // Returns io.EOF when no more commands

// Reset the spool after replay is complete
func (s *spool) Reset() error
```

**Spool File Format**:
- Each record contains a magic number (`0x53504F4C` = "SPOL")
- Record header: magic (4 bytes) + payload length (4 bytes) + CRC32 (4 bytes) + reserved (4 bytes)
- Record payload: Binary-encoded Command (protobuf)

**Spool Location**: `{dataDir}/spool`

#### Syncer: FSM Synchronization Manager

The syncer manages the synchronization process between the Raft log and the FSM:

**File**: `internal/raft/syncer.go`

**Responsibilities**:
- Manages the "syncing" state flag
- Buffers commands to the spool during synchronization
- Replays spool commands after snapshot restoration
- Provides a unified interface for snapshot creation and restoration

**Synchronization Flow**:

1. **Snapshot restoration starts**: `SyncSnapshot()` is called
2. **Syncing mode activated**: `syncing = true`
3. **FSM restored**: Snapshot data is applied to the FSM
4. **Logs synced**: For each ledger, missing logs are streamed from the leader
5. **Spool replay**: Commands from the spool are replayed
6. **Syncing mode deactivated**: `syncing = false` after replay completes
7. **Spool reset**: Spool file is cleared

**During Synchronization**:
- New committed entries are appended to the spool instead of being applied directly
- The node cannot serve writes until synchronization completes
- Reads may be blocked or delayed depending on implementation

**After Synchronization**:
- Normal operation resumes
- Committed entries are applied directly to the FSM
- The spool is empty and ready for the next synchronization cycle

## Runtime Store (logs + runtime state)

### Concept

The Runtime Store is responsible for persistent storage of transactions (logs) and derived runtime state (balances, account metadata, idempotency). It implements the `RuntimeStore` interface. **All ledgers share the same Runtime Store instance**, with data keyed by ledger name.

### Log Operations

**Write**:

```go
func (s *runtimeStore) InsertLogs(ctx context.Context, logs ...*ledgerpb.Log) error
```

- Persists logs (each log includes its ledger name)
- Updates balances and metadata in the same store
- Records idempotency entries

**Read**:

```go
func (s *runtimeStore) GetAllLogs(ctx context.Context, ledger string, from uint64, to uint64) (Cursor[*ledgerpb.Log], error)
```

- Reads logs for a specific ledger starting from sequence `from` (exclusive)
- Stops at sequence `to` (inclusive) if `to > 0`, otherwise reads until the end
- Returns a cursor for iteration

### Interface

```go
type RuntimeStore interface {
    LogStore
    GetBalances(ctx context.Context, ledger string, balanceQuery map[string][]string) (ledgerpb.Balances, error)
    GetAccountMetadata(ctx context.Context, ledger string, accounts []string) (map[string]metadata.Metadata, error)
    GetLogForIdempotencyKey(ctx context.Context, ledger string, idempotencyKey string) ([]byte, uint64, error)
    GetLogIDForTransactionID(ctx context.Context, ledger string, transactionID uint64) (uint64, error)
    IsTransactionReverted(ctx context.Context, ledger string, transactionID uint64) (bool, error)
    GetLastProcessedLogID(ctx context.Context, ledger string) (uint64, error)
}
```

The `RuntimeStore` interface combines runtime queries with log access, providing runtime data access and log storage for all ledgers.

### Implementation

#### SQLite

**Files**: `internal/store/sqlite/runtime.go`, `internal/store/sqlite/db.go`

**Characteristics**:
- Single SQLite database for all ledgers
- No external dependencies
- Ideal for development and small deployments
- Logs and runtime state stored together with ledger prefixes

**Schema**:
```sql
CREATE TABLE logs (
    ledger TEXT NOT NULL,
    id INTEGER NOT NULL,
    data BLOB NOT NULL,
    date TEXT,
    idempotency_key TEXT,
    idempotency_hash TEXT,
    PRIMARY KEY (ledger, id)
);

CREATE UNIQUE INDEX idx_logs_idempotency_key ON logs(ledger, idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE balances (
    ledger TEXT NOT NULL,
    account TEXT NOT NULL,
    asset TEXT NOT NULL,
    balance TEXT NOT NULL DEFAULT '0',
    PRIMARY KEY (ledger, account, asset)
);

CREATE TABLE account_metadata (
    ledger TEXT NOT NULL,
    account_address TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (ledger, account_address, key)
);
```

#### Pebble

**File**: `internal/store/pebble/runtime.go`

**Characteristics**:
- Single Pebble database for all ledgers
- High-performance LSM-tree based storage
- Data keyed with ledger name prefixes

### Key Set Locker

**File**: `internal/service/keysetlocker.go`

The `KeySetLocker` provides key-based locking for concurrent access to balance-related operations:

- **Purpose**: Ensures safe concurrent access to balances during transaction processing
- **Mechanism**: Uses mutexes keyed by `ledger:account:asset` combinations
- **Behavior**: Locks are acquired before reading balances and released after transaction processing
- **Cleanup**: Locks are removed from the internal map when no goroutine holds a reference
- **No caching**: Always reads from the underlying database store

**Note**: This is NOT a cache. It only provides locking - balances are always read from the database.

## Data Organization

### Directory Structure

```
data/
├── raft/                          # Raft data
│   ├── wal/                       # WAL segments
│   ├── raft-hardstate.json        # HardState
│   └── raft-snapshot.json         # Snapshot metadata
├── spool                          # Spool file for sync
└── runtime.db (SQLite)            # All ledgers data
    OR
└── runtime/ (Pebble)              # All ledgers data
    ├── 000001.sst
    ├── MANIFEST-000001
    └── ...
```

### Data Isolation

- **Raft data**: Unified in `data/raft/`
- **Ledgers**: All stored in shared Runtime Store with ledger name as key prefix
- **Logs**: Stored with `(ledger, id)` composite key

## Durability and Guarantees

### Write Durability

1. **WAL**: Synchronized on disk before commit
2. **RuntimeStore**: ACID transactions for SQLite, durable writes for Pebble
3. **Snapshots**: Created periodically for recovery

### Recovery after Failure

The system can recover completely from:

1. **Snapshot + WAL**: Rapid restoration from the last snapshot
2. **Complete WAL**: If no snapshot, complete replay of the WAL
3. **RuntimeStore**: Reconstruction of balances from the logs

### ACID Guarantees

- **Atomicity**: Complete transactions or nothing
- **Consistency**: Consistent state guaranteed by Raft
- **Isolation**: Locks per account for balances
- **Durability**: Writes synchronized on disk

## Performance and Optimizations

### Memory Cache

- **Raft Entries**: Cache in memory for fast access
- **FSM State**: All ledger states kept in memory
- **Balances**: Read from database (no cache, consistent reads)

### Compaction

- **WAL**: Compacted after snapshots
- **RuntimeStore**: Pebble performs automatic LSM compaction

### Indexing

- **Idempotency keys**: Index for fast verifications
- **Sequences**: Primary index for ordering
- **Log IDs**: Index for fast lookups

## Next Steps

To deepen your understanding:

1. [Storage Drivers](./storage-drivers.md) - Detailed documentation on each storage driver
2. [Consensus Raft](./raft-consensus.md) - How Raft uses storage
3. [Buckets and Ledgers](./buckets-ledgers.md) - Data organization
4. [Deployment](./deployment.md) - Storage configuration in production
