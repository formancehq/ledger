# Draft — Data Retention & Cold Storage

**Status**: Draft for team review
**Author**: Geoffrey + Claude
**Date**: 2025-02-12

---

## 1. Problem Statement

The ledger accumulates data indefinitely: transaction logs, volumes, metadata, audit entries. Over time this leads to:

- **Unbounded storage growth** on each Raft node (Pebble)
- **Increasing snapshot sizes** slowing down node recovery and cluster joins
- **Degraded compaction performance** as the LSM tree grows
- **No separation between operational and historical data**

Financial ledgers naturally operate in **periods** (month, quarter, year). Once a period is closed, its detailed data is rarely accessed and can be moved to cheaper storage.

## 2. Goals

1. **Period-based lifecycle**: introduce the concept of accounting periods with explicit close operations
2. **Cold storage archival**: move closed period data to external storage (S3, GCS, filesystem)
3. **Hot storage compaction**: after archival, purge detailed data from Pebble, keeping only balance snapshots
4. **Receipt-based reverts**: allow reverting archived transactions without accessing cold storage
5. **Hash chain continuity**: maintain cryptographic integrity across period boundaries
6. **Minimal operational impact**: archival should not block writes or degrade latency

## 3. Scope

### In scope
- Period model (open, closed, archived)
- Cold storage interface (write-only) and S3 implementation
- Period close and archive operations (manual + scheduled)
- Auto-purge after verified archival
- Receipt mechanism (JWT) for cross-period reverts
- Hash chain sealing at period boundaries
- Balance snapshot at close

### Out of scope
- Read-only querying of archived data from the ledger (Phase 3 — future)
- Cold storage cleanup/deletion (external tooling responsibility)
- Restore from cold storage (future)
- Per-ledger retention policies (use Raft sharding instead — see [Section 10.1](#101-per-bucket-vs-per-ledger-retention--decision-record))
- Migration of existing data predating period model (separate effort)

## 4. Design Overview

```
                    PERIOD LIFECYCLE

  ┌─────────┐    close     ┌─────────┐   archive    ┌──────────┐
  │  OPEN   │ ──────────►  │ CLOSED  │ ──────────►  │ ARCHIVED │
  │         │              │         │              │          │
  │ writes  │              │ read-   │              │ purged   │
  │ allowed │              │ only    │              │ from hot │
  └─────────┘              └─────────┘              └──────────┘
       │                        │                        │
       │ hot storage            │ hot storage             │ cold storage
       │ (Pebble)               │ (Pebble)                │ (S3/GCS/FS)
       │                        │                         │
       │ full data              │ full data               │ full data
       │                        │ + balance snapshot      │ + balance snapshot
       │                        │ + sealing hash          │
       │                        │                         │
       │                        │                    hot storage:
       │                        │                    balance snapshot only
       │                        │                    + sealing hash
       │                        │                    + reversion bitset
```

## 5. Period Model

### 5.1 Definition

A **period** is a time-bounded interval applied at the **bucket level** (i.e., the entire Raft group). All ledgers within the bucket share the same period boundaries.

```protobuf
message Period {
  uint64 id = 1;                    // Auto-incrementing period ID
  google.protobuf.Timestamp start = 2;
  google.protobuf.Timestamp end = 3;
  PeriodStatus status = 4;
  uint64 close_sequence = 5;       // Global log sequence at close
  bytes sealing_hash = 6;          // Hash chain anchor (set after snapshot completes)
  uint64 archive_sequence = 7;     // Sequence when archival completed
}

enum PeriodStatus {
  PERIOD_OPEN = 0;
  PERIOD_CLOSING = 1;              // Boundary marked, snapshot in progress
  PERIOD_CLOSED = 2;               // Snapshot complete, sealed
  PERIOD_ARCHIVED = 3;
}
```

### 5.2 Rules

- At most **one open period** at any time
- Closing is a **two-step process** (see [Section 5.3](#53-two-step-close-process))
- Transactions are assigned to a period based on their **insertion timestamp** (not the user-supplied timestamp)
- Once the boundary is marked (CLOSING), no new transactions can be assigned to that period

### 5.3 Two-Step Close Process

Closing a period cannot be done in a single synchronous Raft command because:
- The **in-memory FSM cache only contains hot keys** (recently accessed accounts)
- **Cold accounts** (not accessed within the last two generations) exist only in Pebble
- Computing a complete balance snapshot requires a **full scan of Pebble** (all volume attributes)
- A synchronous scan would block the Raft consensus loop, degrading latency

The close process is therefore split into two steps:

```
Step 1: ClosePeriod         Step 2: SealPeriod
(Raft command, instant)     (background scan, then Raft command)

  ┌─────────┐                ┌──────────┐               ┌─────────┐
  │  OPEN   │──ClosePeriod──►│ CLOSING  │──SealPeriod──►│ CLOSED  │
  │         │                │          │               │         │
  └─────────┘                └──────────┘               └─────────┘
                              │                          │
                              │ boundary marked          │ snapshot complete
                              │ new period opened        │ sealing hash set
                              │ new txs go to new period │ ready for archival
```

#### Step 1: ClosePeriod (Raft command — instant)

- Marks the current period's `close_sequence` = current global sequence
- Sets the period status to `CLOSING`
- Sets the period `end` timestamp
- Opens the next period (id+1, status=OPEN, start=end of previous)
- **Does NOT compute the balance snapshot** — returns immediately

This is a lightweight Raft command that executes in the FSM hot path without blocking.

#### Step 2: SealPeriod (background + Raft command)

Runs asynchronously on the leader after the ClosePeriod command is applied:

1. **Scan Pebble** for all volume attributes (Input `'I'` + Output `'O'` prefixes under `0x09`)
2. For each canonical key, call `ComputeValue()` at the `close_sequence` boundary to get the consolidated value (base + diffs)
3. Compute the **balance snapshot hash** from all consolidated volumes
4. Compute the **sealing hash**: `BLAKE3(period_id || close_sequence || last_log_hash || balance_snapshot_hash)`
5. Propose a **SealPeriod** Raft command containing the sealing hash and balance snapshot
6. Upon application, the period transitions from CLOSING to CLOSED

This reuses the same pattern as the existing `Compactor` (`internal/service/state/compactor.go`) which already scans Pebble in the background and consolidates volume attributes.

#### Failure handling

If the leader crashes between Step 1 and Step 2:
- The period remains in `CLOSING` state
- The new leader detects the `CLOSING` period on startup and re-triggers Step 2
- The scan is idempotent — re-scanning produces the same snapshot

#### Constraints during CLOSING

While a period is in `CLOSING` state:
- New transactions are written to the **new** period (the one opened in Step 1)
- The `CLOSING` period cannot be archived (must be `CLOSED` first)
- A new close cannot be initiated until the current one is sealed

### 5.4 Balance Snapshot

The balance snapshot captures the **complete state** of all accounts at the close boundary:

- **Per-account, per-asset volumes** (input, output) as of `close_sequence`
- Computed by scanning all volume attributes in Pebble (not just the in-memory cache)
- Uses `ComputeValue()` which consolidates base + diffs at the boundary index

The balance snapshot enables the system to operate correctly after the detailed data is purged:
- Balance queries work from the snapshot + current period's diffs
- No need to replay historical transactions

### 5.5 Sealing Hash

Once the snapshot is complete, a **sealing hash** anchors the period:

```
sealing_hash = BLAKE3(period_id || close_sequence || last_log_hash || balance_snapshot_hash)
```

This anchors the hash chain at the period boundary. After archival and purge, the sealing hash serves as proof of integrity for the purged data. The archived data in cold storage can be independently verified against this hash.

## 6. Cold Storage

### 6.1 Interface

```go
// ColdStorage defines the interface for archiving period data.
// Cold storage is write-only: the system archives data but does not manage
// its lifecycle (no deletion, no restoration). Cleanup of old archives
// is the responsibility of external tooling or infrastructure policies
// (e.g., S3 lifecycle rules).
type ColdStorage interface {
    // Archive writes a period's data to cold storage.
    // The data is a self-contained archive (tar.gz) that includes:
    // - All logs for the period (with hash chain)
    // - Balance snapshot at period close
    // - Audit entries for the period
    // - Period metadata (sealing hash, boundaries)
    Archive(ctx context.Context, bucketID string, period PeriodArchive) error

    // Exists checks if a period archive exists in cold storage.
    // Used to verify archival succeeded before purging hot data.
    Exists(ctx context.Context, bucketID string, periodID uint64) (bool, error)
}
```

### 6.2 Archive Format

The archive is a **tar.gz** containing:

```
period-{id}/
├── metadata.json         # Period metadata, sealing hash, boundaries
├── logs.pb               # Protobuf-encoded logs (sequential)
├── balance-snapshot.pb   # Account balances at close
├── audit.pb              # Audit entries for the period
└── checksum.sha256       # SHA-256 of all files for integrity
```

Archives are **gzip-compressed**. This format is self-contained: an archive can be verified independently.

### 6.3 S3 Implementation

First implementation targets S3-compatible storage (AWS S3, MinIO, GCS via S3 API):

```
s3://{bucket}/{bucket-id}/periods/{period-id}/archive.tar.gz
```

Configuration:
```yaml
cold-storage:
  driver: s3           # s3 | filesystem
  s3:
    bucket: my-archives
    region: eu-west-1
    prefix: ledger/
    # Standard AWS SDK credential chain (env, IAM role, config file)
```

### 6.4 Filesystem Implementation

For development and testing:

```
{base-path}/{bucket-id}/periods/{period-id}/archive.tar.gz
```

## 7. Receipt Mechanism

### 7.1 Problem

After a period is archived and purged from hot storage, the detailed transaction data (postings) is no longer available locally. However, clients may need to **revert** transactions from previous periods.

Options considered:
- **Fetch from cold storage**: adds latency and creates a dependency on cold storage availability for write operations — rejected
- **Keep revert index in hot storage**: defeats the purpose of archival if it grows unboundedly — rejected
- **Receipt-based revert**: client provides proof of the original transaction — **selected**

### 7.2 Design

When a transaction is created, the service emits a **receipt** as a signed **JWT** alongside the transaction response. Using JWT means the client can also decode and inspect the receipt contents without needing to call the service.

#### JWT Payload (claims)

```json
{
  "ledger": "main",
  "txId": 42,
  "postings": [
    {"source": "users:alice", "destination": "users:bob", "amount": 100, "asset": "USD/2"}
  ],
  "timestamp": "2025-03-01T00:00:00Z",
  "periodId": 3,
  "iat": 1740787200
}
```

The receipt is:
1. **Self-contained**: includes all data needed to construct the revert transaction
2. **Signed**: JWT signature proves it was issued by the service
3. **Tamper-proof**: any modification invalidates the signature
4. **Client-readable**: standard JWT format, decodable by any JWT library

### 7.3 Signature

The JWT is signed with **HMAC-SHA256** (HS256). The signing key is a cluster-level secret configured at startup (CLI flag or environment variable).

All nodes in the cluster share the same signing key, ensuring any node can verify receipts issued by any other node.

If external receipt verification becomes a requirement (e.g., third-party auditors), the signing algorithm can be upgraded to **RS256** or **EdDSA** (asymmetric) — the JWT format supports this transparently via the `alg` header.

### 7.4 Revert Flow

```
Client                          Service
  │                                │
  │  RevertTransaction             │
  │  { tx_id, receipt }            │
  │ ─────────────────────────────► │
  │                                │── 1. Verify receipt signature
  │                                │── 2. Check tx_id not already reverted
  │                                │      (reversion attribute or bitset)
  │                                │── 3. Build reverse postings from receipt
  │                                │── 4. Apply revert as new transaction
  │                                │      in CURRENT period
  │  RevertResponse                │
  │  { revert_tx_id, receipt }     │
  │ ◄───────────────────────────── │
```

### 7.5 Double-Revert Prevention

After archival, the detailed transaction data is purged, but the **reversion status** must survive. Two mechanisms:

1. **Reversion attribute** (current mechanism): already tracks `tx_id → reverted` as an attribute. During archival, these are compacted into the balance snapshot.
2. **Reversion bitset**: a compact bitset of reverted transaction IDs, kept in hot storage. For 1M transactions, this is ~125KB.

The reversion attribute is already part of the generation system. During period close, reverted transaction IDs from the closed period are consolidated into a compact set that persists across archival.

### 7.6 Receipts for Current Period

Receipts are emitted for **all** transactions, not just those in periods that will be archived. This ensures:
- Clients don't need to change behavior based on period status
- Receipts are available before archival happens
- The revert path is uniform regardless of whether the data is in hot or cold storage

For transactions still in hot storage, the service can revert using either the receipt or the local data (receipt is optional in this case). Once the period is archived, the receipt becomes **required**.

## 8. Operations

### 8.1 Close Period (Manual)

```
ledgerctl periods close [--at <timestamp>]
```

- Creates a `ClosePeriod` Raft command
- Computes balance snapshot for all accounts across all ledgers
- Computes sealing hash
- Opens the next period automatically

### 8.2 Close Period (Scheduled)

Configuration:
```yaml
retention:
  auto-close:
    enabled: true
    schedule: "0 0 1 * *"    # Cron expression — configurable and modifiable at runtime
```

The **period granularity is configurable** and can be changed at any time. The schedule defines when the auto-close triggers, not the period structure. Changing from monthly (`0 0 1 * *`) to quarterly (`0 0 1 1,4,7,10 *`) mid-flight simply means the next period will be longer than the previous ones — existing closed periods are unaffected since they just have `start/end` timestamps.

The scheduler runs on the Raft leader and proposes a `ClosePeriod` command at the scheduled time.

### 8.3 Archive Period

```
ledgerctl periods archive <period-id>
```

- Verifies the period is closed
- Exports logs, audit entries, and balance snapshot to cold storage (gzip compressed)
- Verifies the archive integrity (checksum validation via `Exists`)
- Marks the period as archived
- **Automatically purges** detailed data from Pebble (range delete on log sequences)

Archival and purge are a single atomic operation from the operator's perspective. There is no manual confirmation step between archive and purge — the system verifies the archive exists in cold storage before purging.

### 8.4 List Periods

```
ledgerctl periods list
```

Displays all periods with their status, boundaries, and storage location.

## 9. Raft Integration

### 9.1 New FSM Commands

Three new top-level Order types (bucket-level, not per-ledger):

```protobuf
message Order {
  oneof type {
    // ... existing types ...
    ClosePeriodOrder close_period = 5;     // Step 1: mark boundary
    SealPeriodOrder seal_period = 6;       // Step 2: finalize with snapshot
    ArchivePeriodOrder archive_period = 7; // Phase 2: archive + purge
  }
}

message ClosePeriodOrder {}  // No fields — closes the current open period

message SealPeriodOrder {
  uint64 period_id = 1;
  bytes sealing_hash = 2;         // Computed from balance snapshot
  bytes balance_snapshot = 3;     // Serialized snapshot (protobuf)
}

message ArchivePeriodOrder {
  uint64 period_id = 1;
}
```

### 9.2 Coordination

- **ClosePeriod** (Step 1): instant Raft command. Marks the boundary, opens new period. All nodes transition the period to `CLOSING`.
- **SealPeriod** (Step 2): proposed by the leader after background Pebble scan. Contains the balance snapshot and sealing hash. All nodes transition the period to `CLOSED`. Followers don't need to re-scan — they trust the snapshot from the leader (verified by Raft consensus).
- **Archive** (Phase 2): the leader exports to cold storage, then proposes `ArchivePeriod`. All nodes purge the data upon applying the command (deterministic range delete on Pebble).

### 9.3 Snapshot Impact

Raft snapshots only contain data from non-archived periods. This directly reduces snapshot size as periods are archived, improving:
- Node join time
- Recovery time
- Memory footprint of the FSM in-memory state

## 10. Options / Future Considerations

### 10.1 Per-Bucket vs Per-Ledger Retention — Decision Record

**Decision**: Periods operate at the **bucket level** (entire Raft group). All ledgers within a bucket share the same period boundaries and retention policy.

**For different retention policies per ledger**, the recommended approach is to **shard ledgers into separate Raft groups** (buckets), each with its own retention configuration.

#### Why per-bucket is the right choice

| Argument | Detail |
|---|---|
| **Log stream is global** | The Raft log uses a single global sequence. Cutting at a global sequence naturally captures all ledgers. Per-ledger would require filtering interleaved logs, turning a simple range delete into a scatter-gather operation. |
| **Attribute generations are global** | The two-generation compaction system tracks Raft index (global), not per-ledger sequences. Per-ledger periods would break generation boundary alignment, requiring a complete redesign of the compaction logic. |
| **Idempotency keys are bucket-level** | IK keys are stored globally (`0x02` prefix). Their retention aligns naturally with bucket-wide periods. Per-ledger would require partitioning IK keys, adding complexity for no functional benefit. |
| **Pebble range delete is trivial per-bucket** | Logs are keyed `[0x01][global_sequence]`. Purging a period is a single `DeleteRange(seqStart, seqEnd)`. Per-ledger purge would require iterating all logs, checking ledger membership, and deleting individually — orders of magnitude slower. |
| **Single FSM state** | One period state to track in the FSM (current period ID, status). Per-ledger would require N period states, one per ledger, all maintained in the FSM hot path. |
| **Sealing hash is straightforward** | One hash chain, one sealing point. Per-ledger would require N independent sealing hashes, computed at different points in the global log. |
| **Raft snapshot size** | After archival, the snapshot shrinks uniformly. Per-ledger archival would leave a mixed snapshot (some ledgers archived, others not), complicating snapshot serialization and restoration. |

#### Why per-ledger is NOT worth the complexity

Per-ledger periods within a single bucket would require:
- **Partial log purges**: scanning the global log to identify and skip entries belonging to non-archived ledgers (O(n) scan instead of O(1) range delete)
- **Mixed attribute states**: some ledgers with compacted attributes, others with full history — breaks generation rotation assumptions
- **Complex snapshot format**: snapshot must distinguish archived vs. active ledgers, complicating both serialization and node sync
- **Split-brain retention**: a single Raft group managing N different retention timelines is an operational nightmare (monitoring, alerting, debugging)
- **No practical benefit**: if two ledgers need different retention policies, they represent different business domains and should be in separate buckets anyway

#### The correct alternative for per-ledger retention

Shard ledgers with different retention needs into **separate Raft groups (buckets)**. Each bucket has its own:
- Period configuration (schedule, granularity)
- Cold storage settings
- Retention duration
- Independent compaction and archival lifecycle

This is operationally cleaner, architecturally simpler, and aligns with the single-responsibility principle: one bucket = one retention policy.

### 10.2 Read-Only Access to Archives (Phase 2)

Phase 1 is write-only (export). Phase 2 could add transparent read-only querying:
- The service detects that a query targets an archived period
- Fetches and caches the archive locally (temporary)
- Serves the query from the cached archive
- Evicts the cache after a configurable TTL

### 10.3 Receipt Key Rotation

The signing key should be rotatable. Receipts include a `key_id` field to support verification with older keys during rotation.

## 11. Implementation Plan (Suggested Phases)

### Phase 1 — Foundation
- Period model (proto, FSM commands, storage)
- Close period with balance snapshot and sealing hash
- Receipt emission (JWT/HS256) on transaction creation
- Receipt-based revert
- CLI commands: `periods close`, `periods list`

### Phase 2 — Cold Storage
- Cold storage interface
- S3 implementation
- Filesystem implementation (dev/test)
- Archive + auto-purge operations
- CLI commands: `periods archive`
- Scheduled auto-close (configurable cron)

### Phase 3 — Read-Only Access (Future)
- Transparent read-only queries on archived periods
- Local archive cache with TTL
- API support for querying historical data

---

## 12. Decisions Record

| Topic | Decision | Rationale |
|---|---|---|
| **Period scope** | Per-bucket | Aligns with global log, global generations, trivial range delete (see [Section 10.1](#101-per-bucket-vs-per-ledger-retention--decision-record)) |
| **Receipt format** | JWT (HS256) | Client-readable, standard format, upgradable to RS256/EdDSA later |
| **Signing key** | Cluster-level secret (CLI flag / env var) | Simple provisioning, shared across all nodes |
| **Archive compression** | gzip (tar.gz) | Standard, widely supported, good enough compression |
| **Purge strategy** | Auto-purge after verified archive | No manual confirmation step; system verifies archive exists before purging |
| **Period granularity** | Configurable cron schedule, modifiable at runtime | Changing granularity only affects future periods, existing ones are immutable |
| **Cold storage cleanup** | Not the system's responsibility | Delegated to external tooling (S3 lifecycle rules, infrastructure policies) |
| **Per-ledger retention** | Via Raft sharding (separate buckets) | Avoids complexity of mixed retention within a single Raft group |
| **Close process** | Two-step (ClosePeriod + SealPeriod) | In-memory cache only has hot keys; cold accounts are only in Pebble. A full scan is needed but cannot block the Raft consensus loop. Background scan reuses the Compactor pattern. |

## 13. Open Questions for Team

1. **Signing key provisioning**: CLI flag? Environment variable? External KMS integration later?
2. **Receipt key rotation**: include a `kid` (key ID) in the JWT header to support rotation — confirm approach
