# Draft — Data Retention & Cold Storage

**Status**: Draft for team review
**Author**: Geoffrey + Claude
**Date**: 2025-02-12 (updated 2026-02-18)

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
3. **Hot storage compaction**: after archival, purge logs and transaction updates from Pebble, keeping attributes (volumes, metadata) in hot storage
4. **Receipt-based reverts**: allow reverting archived transactions without accessing cold storage
5. **Hash chain continuity**: maintain cryptographic integrity across period boundaries
6. **Minimal operational impact**: archival should not block writes or degrade latency

## 3. Scope

### In scope
- Period model (open, closed, archived)
- Cold storage interface (write-only) and S3 implementation
- Period close and archive operations (manual + scheduled)
- Auto-purge of logs and transaction updates after verified archival
- Receipt mechanism (JWT) for cross-period reverts
- Hash chain sealing at period boundaries

### Out of scope
- Read-only querying of archived data from the ledger (Phase 3 — future)
- Cold storage cleanup/deletion (external tooling responsibility)
- Restore from cold storage (future)
- Per-ledger retention policies (use Raft sharding instead — see [Section 10.1](#101-per-bucket-vs-per-ledger-retention--decision-record))
- Migration of existing data predating period model (separate effort)

## 4. Design Overview

```
                    PERIOD LIFECYCLE

  ┌─────────┐   close    ┌─────────┐   seal     ┌─────────┐   archive   ┌───────────┐   confirm   ┌──────────┐
  │  OPEN   │ ────────►  │ CLOSING │ ────────►  │ CLOSED  │ ────────►  │ ARCHIVING │ ────────►  │ ARCHIVED │
  │         │            │         │            │         │            │           │            │          │
  │ writes  │            │ sealing │            │ sealed  │            │ exporting │            │ logs     │
  │ allowed │            │ in prog │            │ ready   │            │ to cold   │            │ purged   │
  └─────────┘            └─────────┘            └─────────┘            └───────────┘            └──────────┘
       │                      │                      │                      │                        │
       │ hot storage          │ hot storage           │ hot storage          │ hot storage             │ cold storage
       │ (Pebble)             │ (Pebble)              │ (Pebble)             │ (Pebble)                │ (S3/FS)
       │                      │                       │                      │                         │
       │ full data            │ full data              │ full data            │ full data               │ logs + audit
       │                      │ + checkpoint           │ + sealing hash       │ + sealing hash          │
       │                      │                        │                      │                    hot storage:
       │                      │                        │                      │                    attributes (volumes,
       │                      │                        │                      │                      metadata, reverts)
       │                      │                        │                      │                    + sealing hash
```

**Key design decision**: attributes (volumes, metadata, reversion status) are **kept in hot storage** after archival. Only logs and transaction updates are purged. This avoids the need for a separate balance snapshot format — attributes ARE the compact, derived state. They grow with the number of unique accounts/assets, not with the number of transactions, so they remain small.

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
  PERIOD_ARCHIVING = 4;            // Archive export in progress (leader-only)
}
```

### 5.2 Rules

- At most **one open period** at any time
- Closing is a **two-step process** (see [Section 5.3](#53-two-step-close-process))
- Transactions are assigned to a period based on their **insertion timestamp** (not the user-supplied timestamp)
- Once the boundary is marked (CLOSING), no new transactions can be assigned to that period

### 5.3 Two-Step Close Process

Closing a period cannot be done in a single synchronous Raft command because:
- Computing the sealing hash requires creating a **Pebble checkpoint** and iterating all attribute entries
- A synchronous checkpoint + hash computation would block the Raft consensus loop, degrading latency

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

This is a lightweight Raft command that executes in the FSM hot path without blocking.

#### Step 2: SealPeriod (background + Raft command)

Runs asynchronously on the leader after the ClosePeriod command is applied:

1. **Create a Pebble checkpoint** — a frozen point-in-time snapshot of the entire Pebble DB at the exact ClosePeriod boundary. Remaining Raft entries (received after ClosePeriod) are spooled and replayed afterward.
2. **Open the checkpoint read-only** and iterate all attribute entries in `[0x09, 0x0A)` to compute the **state hash**: `BLAKE3(all key+value pairs)`
3. Compute the **sealing hash**: `BLAKE3(period_id || close_sequence || last_log_hash || state_hash)`
4. **Remove the checkpoint** from disk (no longer needed)
5. Propose a **SealPeriod** Raft command containing the sealing hash
6. Upon application, the period transitions from CLOSING to CLOSED

The checkpoint approach is simpler and more correct than per-key scanning: the checkpoint captures the exact Pebble state at the ClosePeriod boundary, so no filtering or index-based lookups are needed. Determinism is guaranteed because Pebble iteration order is deterministic and compaction is 100% deterministic via Raft.

#### Failure handling

Two crash windows exist between ClosePeriod and SealPeriod:

**Window 1: Crash after ClosePeriod batch commit but before checkpoint creation**
- Pebble is at the exact ClosePeriod boundary (spooled entries not yet replayed)
- On restart, `NewNode()` detects a CLOSING period with no seal checkpoint on disk
- It creates the checkpoint **before** spool replay (critical: replay would advance Pebble past the boundary)
- Sends the seal request to the Sealer channel

**Window 2: Crash after checkpoint creation but before SealPeriod proposal**
- The seal checkpoint exists on disk (only removed on success)
- On restart, `Sealer.Start()` detects a CLOSING period with an existing checkpoint
- It sends the seal request to re-trigger sealing

**Transient seal failures** (e.g., disk I/O error opening the checkpoint):
- The Sealer retries with exponential backoff (100ms → 10s max)
- The checkpoint remains on disk until sealing succeeds
- Retrying is safe because the checkpoint is immutable

#### Constraints during CLOSING

While a period is in `CLOSING` state:
- New transactions are written to the **new** period (the one opened in Step 1)
- The `CLOSING` period cannot be archived (must be `CLOSED` first)
- A new close cannot be initiated until the current one is sealed

### 5.4 State Hash (Checkpoint-based)

The state hash captures a cryptographic digest of the **entire Pebble attribute store** at the close boundary:

- Computed by iterating all attribute entries in `[0x09, 0x0A)` inside a frozen Pebble checkpoint
- Each key+value pair is fed into BLAKE3: `state_hash = BLAKE3(key₁ || value₁ || key₂ || value₂ || ...)`
- The checkpoint is a point-in-time snapshot created at the exact ClosePeriod boundary, before any subsequent entries modify Pebble
- Deterministic: Pebble iteration order is deterministic, and compaction is fully deterministic via Raft

This approach is simpler and more comprehensive than per-key volume scanning — it covers all attribute types (volumes, metadata, ledger info, boundaries, etc.) in a single pass.

### 5.5 Sealing Hash

Once the state hash is computed, a **sealing hash** anchors the period:

```
sealing_hash = BLAKE3(period_id || close_sequence || last_log_hash || state_hash)
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
├── logs.pb               # Protobuf-encoded logs (sequential, with hash chain)
├── audit.pb              # Audit entries for the period
└── checksum.sha256       # SHA-256 of all files for integrity
```

Archives are **gzip-compressed**. This format is self-contained: an archive can be verified independently.

> **Note**: No balance snapshot is included in the archive. Attributes (volumes, metadata) remain in hot storage after archival — they are the compact, derived state and do not need to be archived or restored.

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

After archival, logs and transaction updates are purged, but **attributes remain in hot storage**. The reversion status (`tx_id → reverted`) is tracked as an attribute and naturally survives archival — no separate mechanism is needed.

The reversion attribute is already part of the generation system and compacted alongside other attributes (volumes, metadata). This means double-revert prevention works identically whether the period is open, closed, or archived.

### 7.6 Receipts for Current Period

Receipts are emitted for **all** transactions, not just those in periods that will be archived. This ensures:
- Clients don't need to change behavior based on period status
- Receipts are available before archival happens
- The revert path is uniform regardless of whether the data is in hot or cold storage

For transactions still in hot storage, the service can revert using either the receipt or the local data (receipt is optional in this case). Once the period is archived, the receipt becomes **required**.

## 8. Operations

### 8.1 Close Period (Manual)

```
ledgerctl periods close
```

- Creates a `ClosePeriod` Raft command (instant, marks boundary)
- Background sealer computes sealing hash from Pebble checkpoint
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
- Exports all cold-storable KV pairs (logs, audit entries, transaction updates) to cold storage as a raw binary tar.gz archive
- Verifies the archive integrity (checksum validation via `Exists`)
- Marks the period as archived
- **Purges cold-storable data** from Pebble (range delete for sequence-keyed prefixes, filtered delete for transaction updates)
- **Keeps attributes** in hot storage (volumes, metadata, reversion status)

Archival and purge are a single atomic operation from the operator's perspective. There is no manual confirmation step between archive and purge — the system verifies the archive exists in cold storage before purging.

After archival, reads (`GetAccount`, `ListAccounts`, volumes) continue to work normally from attributes. Only detailed transaction history (log replay) requires fetching from cold storage.

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
  bytes sealing_hash = 2;         // Computed from Pebble checkpoint state hash
}

message ArchivePeriodOrder {
  uint64 period_id = 1;
}
```

### 9.2 Coordination

- **ClosePeriod** (Step 1): instant Raft command. Marks the boundary, opens new period. All nodes transition the period to `CLOSING`.
- **SealPeriod** (Step 2): proposed by the leader after checkpoint-based hash computation. Contains the sealing hash. All nodes transition the period to `CLOSED`. Followers don't need to recompute — they trust the hash from the leader (verified by Raft consensus).
- **ArchivePeriod** (Phase 2): transitions the period from CLOSED to ARCHIVING on all nodes. Only the **leader** dispatches the archive request to the background Archiver (avoiding N redundant uploads). The Archiver exports logs and audit entries to cold storage (S3 or filesystem), then proposes `ConfirmArchivePeriod`. All nodes purge logs and transaction updates upon applying the confirm command (deterministic range delete on Pebble). Attributes (volumes, metadata, reversion status) are kept. On leadership gain, the new leader scans for ARCHIVING periods and retries.

### 9.3 Snapshot Impact

After archival, Raft snapshots no longer contain logs and transaction updates from archived periods. Attributes remain but are compact (proportional to unique accounts/assets, not transaction count). This reduces snapshot size as periods are archived, improving:
- Node join time
- Recovery time
- Pebble LSM tree depth and compaction performance

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

### Phase 1 — Foundation ✅ (implemented)
- Period model (proto, FSM commands, storage)
- Two-step close with sealing hash (ClosePeriod + SealPeriod)
- Crash recovery for both failure windows
- Receipt emission (JWT/HS256) with period_id on transaction creation
- Receipt available on GetTransaction response
- Receipt-based revert (admission layer verifies JWT, extracts postings)
- CLI commands: `periods close`, `periods list`
- CLI: `--receipt` flag on `transactions revert`, receipt display on `transactions get`

### Phase 2 — Cold Storage ✅ (implemented)
- Cold storage interface (`ColdStorage` with `Archive` and `Exists` methods)
- Filesystem implementation (dev/test)
- S3 implementation (production, S3-compatible including MinIO)
- `ARCHIVING` intermediate state for deterministic crash recovery
- Leader-only archive dispatch (avoids N redundant uploads in N-node cluster)
- Two-step archive: `ArchivePeriod` (CLOSED → ARCHIVING) → leader-only background export → `ConfirmArchivePeriod` (ARCHIVING → ARCHIVED)
- Background Archiver (follows Sealer pattern: channel-based, exponential backoff retry)
- Auto-purge of logs and audit entries via Pebble `DeleteRange` after verified archival
- Attributes (volumes, metadata, reversion status) remain in hot storage
- Crash recovery on leadership gain: scan for ARCHIVING periods and retry
- CLI command: `periods archive <period-id>`
- Server flags: `--cold-storage-driver`, `--cold-storage-path`, `--cold-storage-bucket-id`, `--cold-storage-s3-bucket`, `--cold-storage-s3-region`, `--cold-storage-s3-endpoint`
- Scheduled auto-close (deferred — configurable cron)

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
| **Close process** | Two-step (ClosePeriod + SealPeriod) | In-memory cache only has hot keys; cold accounts are only in Pebble. A full scan is needed but cannot block the Raft consensus loop. |
| **No balance snapshot** | Keep attributes in hot storage, purge only logs | Attributes (volumes, metadata, reverts) ARE the compact derived state. They grow with unique accounts/assets, not transaction count. No separate snapshot format needed. Reads continue to work after archival. |

## 13. Open Questions for Team

1. ~~**Signing key provisioning**: CLI flag? Environment variable? External KMS integration later?~~ **Resolved**: CLI flag `--receipt-signing-key` (auto-bound to `RECEIPT_SIGNING_KEY` env var). KMS integration deferred.
2. **Receipt key rotation**: include a `kid` (key ID) in the JWT header to support verification with older keys during rotation — confirm approach
