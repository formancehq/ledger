# Log Integrity and Correctness

This document describes the mechanisms ensuring the integrity and immutability of the ledger's log chain.

## Audit Hash Chaining

Integrity verification uses a **batch-level hash chain on audit entries** rather than per-log hashing. Each audit entry (one per Raft proposal) is cryptographically chained to its predecessor, providing tamper detection and ordering proof at the proposal level.

### Hash Formula

```
audit_hash = H(key, header_payload || concat(per_item_payload) || previous_audit_hash)
```

Where:
- `H` is the configured keyed hash function (BLAKE3 or XXH3-128).
- `key` is derived from the immutable `ClusterID` via domain-separated BLAKE3.
- `header_payload` is the canonical binary encoding of EVERY AuditEntry field except `hash`: sequence, timestamp, proposal_id, outcome (success with log-range bounds, or failure with all sub-fields including the context map), order_count, ledgers, hash_version, caller_snapshot, idempotency key, signature.
- `per_item_payload` is the canonical binary encoding of each AuditItem (order_index, log_sequence, serialized_order) — one payload per item, concatenated in order_index order.
- `previous_audit_hash` is the hash of the immediately preceding audit entry (empty for the first entry).

The verifier rebuilds `header_payload` and each `per_item_payload` from the stored typed fields via the spec below, so any tampering with any bound field changes the rebuilt bytes and breaks the chain hash. Detection scope: every field of `AuditEntry` and `AuditItem` except `AuditEntry.hash` itself.

### Bound vs unbound fields

| Field | Bound by | Tampering detection |
|---|---|---|
| `AuditEntry.sequence` | `header_payload` | ✓ |
| `AuditEntry.timestamp` | `header_payload` | ✓ |
| `AuditEntry.proposal_id` | `header_payload` | ✓ |
| `AuditEntry.outcome` (success or failure) | `header_payload` | ✓ |
| `AuditEntry.order_count` | `header_payload` | ✓ |
| `AuditEntry.items` | NOT stored in entry value — see `per_item_payload` below | ✓ |
| `AuditEntry.ledgers` | `header_payload` (sorted) | ✓ |
| `AuditEntry.hash` | output of chain — not in pre-image by construction | — |
| `AuditEntry.hash_version` | `header_payload` | ✓ |
| `AuditEntry.caller_snapshot` | `header_payload` (identity, source oneof, god, sorted scopes) | ✓ |
| `AuditEntry.idempotency` | `header_payload` (key string) | ✓ |
| `AuditEntry.signature` | `header_payload` (key_id, signature, payload) | ✓ |
| `AuditItem.order_index` | `per_item_payload` | ✓ |
| `AuditItem.serialized_order` | `per_item_payload` | ✓ |
| `AuditItem.log_sequence` | `per_item_payload` | ✓ |

### Header payload binary spec

All integers are big-endian. All variable-length blobs (strings, sub-payloads, repeated entries) are prefixed by their length as `u32 BE`. Maps and `repeated string` fields are iterated in lexicographic key order.

```
HashedHeaderPayload {
    u64 sequence
    u64 timestamp_data
    u64 proposal_id
    u32 order_count
    u32 hash_version
    u32 ledgers_count
    repeated { u32 ledger_len ; bytes ledger }     // sorted lexicographically
    u8  outcome_tag                                  // 0 = success, 1 = failure
    u32 outcome_payload_len
    bytes outcome_payload                            // shape selon outcome_tag
    u32 caller_snapshot_len                          // 0 when CallerSnapshot is absent
    bytes caller_snapshot_payload
    u32 idempotency_key_len ; bytes idempotency_key  // 0 when the batch is unkeyed
    u32 signature_len                                // 0 when the batch is unsigned
    bytes signature_payload
}

SignaturePayload {                                  // empty buffer when unsigned
    u32 key_id_len ; bytes key_id
    u32 signature_len ; bytes signature
    u32 payload_len ; bytes payload
}

OutcomeSuccessPayload {
    u64 min_log_sequence
    u64 max_log_sequence
}

OutcomeFailurePayload {
    u32 reason                                          // common.ErrorReason enum value
    u32 message_len ; bytes message
    u32 context_count
    repeated { u32 key_len ; bytes key ; u32 value_len ; bytes value }  // sorted by key
}

CallerSnapshotPayload {                              // empty buffer when snapshot is nil
    u32 subject_len ; bytes subject
    u8  source_tag                                   // 0 = none, 1 = issuer, 2 = key_id
    u32 source_value_len ; bytes source_value
    u8  god                                          // 0 or 1
    u32 scopes_count
    repeated { u32 scope_len ; bytes scope }         // sorted
}
```

### Per-item payload binary spec

```
PerItemPayload {
    u32 order_index
    u64 log_sequence
    u32 serialized_order_len
    bytes serialized_order
}
```

`serialized_order` is the exact `Order.MarshalDeterministicVT()` output captured at apply time and persisted in `AuditItem.serialized_order` — verifiers re-hash those bytes directly and never re-marshal an `Order` proto, so the chain is immune to vtprotobuf or `Order` schema evolution.

The Go reference implementation lives in `internal/infra/state/audit_envelope.go` (`BuildHashedHeaderPayload` and `BuildPerItemPayload`); cross-language verifiers re-implement the spec above. A golden test (`internal/infra/state/audit_envelope_test.go`) recomputes the hash by hand for a fixed fixture and trips on any drift between the spec and the Go implementation.

### Why Batch-Level (Not Per-Log)

Per-log hashing creates a strict sequential dependency in the FSM hot path: each log's hash depends on the previous log's hash, preventing any parallelism in `ProcessOrders`. Moving the hash chain to audit entries means:

1. The hash is computed **once per batch** (after all business logic completes), not per log
2. `ProcessOrders` is free of sequential hash dependencies
3. Failure entries are included in the chain, preventing silent removal of error records

### Hash Algorithm Selection

The hash algorithm is a cluster-wide setting configured via `--hash-algorithm` and replicated through Raft (stored in `ClusterConfig`). Each audit entry records which algorithm was used in its `hash_version` field.

| `hash_version` | Algorithm | Output Size | Properties |
|----------------|-----------|-------------|------------|
| `0` (default) | BLAKE3 | 32 bytes | Cryptographic — detects both corruption and intentional tampering |
| `1` | XXH3-128 | 16 bytes | Non-cryptographic — detects corruption only, ~5-10x faster |

### Implementation Details

- **BLAKE3**: `github.com/zeebo/blake3` — stateless `blake3.Sum256`, zero shared state
- **XXH3-128**: `github.com/zeebo/xxh3` — stateless, zero allocation
- **Hash field**: Stored as `bytes hash` (field 9) in the `AuditEntry` protobuf message
- **Version field**: Stored as `uint32 hash_version` (field 10) in the `AuditEntry` protobuf message
- **Computation**: Performed synchronously in `applyProposal()` on the FSM goroutine, after `ProcessOrders`

### State Persistence

The `lastAuditHash` is maintained as volatile state in the `Machine` (FSM). It is:
- **Recovered** on startup from the last `AuditEntry` in Pebble
- **Updated** after each audit entry is written (success or failure)

The chain is **not broken** by Raft snapshots: the snapshot contains all Pebble data including audit entries, and recovery reads the last entry's hash.

### Failure Isolation

If a proposal fails (e.g., insufficient funds, ledger not found), a failure audit entry is created. The hash binds the failure outcome (reason, message, context) alongside the orders, so a failure cannot silently be relabelled as a success (or vice versa) without breaking the chain. The `WriteSet` state is discarded without being merged into the `Machine`, but the audit chain advances. This ensures failed proposals are tamper-evident.

### Idempotent Responses

When an idempotent request matches a previously processed order, the processor returns a **reference** to the existing log rather than creating a new one. References do not produce new logs, but the audit entry still covers the batch.

## Double-Entry Invariant Check

A defensive check is performed at `Merge()` time in the `WriteSet` layer to verify the fundamental accounting invariant: **the sum of all input deltas must equal the sum of all output deltas**.

### Rationale

Every posting moves the same amount from a source account (increases output) to a destination account (increases input). If these totals ever diverge, it means money was created or destroyed — a critical bug in the processing logic.

### How It Works

After merging both Input and Output volume updates, the check:

1. Computes the **net delta** for each volume update by comparing `New` vs `Old` values from the `DerivedKeyStore`
2. Sums all input deltas and all output deltas independently
3. Returns an error if the sums differ

The delta computation handles absolute `Known` values by subtracting the old value from the new value.

### When It Triggers

This check runs on every successful proposal merge, before any state is committed to storage. A violation would prevent the batch from being committed, stopping the node rather than persisting corrupted data.

## Store Integrity Check

The `store check` command performs a comprehensive offline verification of the entire store. It replays all logs from the beginning and compares the derived state against the actual stored values.

### Verification Process

The check runs in three passes:

**Pass 1 — Log replay and state verification:**

For each log from sequence 1 to the last sequence:

1. **Sequence continuity**: Verifies that no log sequence numbers are missing (gaps)
2. **State replay**: Replays each log payload to build expected state:
   - `CreateLedger` logs register ledger name-to-ID mappings
   - `CreatedTransaction` logs accumulate expected input/output volumes per account/asset, and track account metadata set during the transaction
   - `RevertedTransaction` logs accumulate reversed posting volumes
   - `SavedMetadata` logs track expected metadata values per account/key
   - `DeletedMetadata` logs mark metadata as deleted

**Pass 2 — Volume comparison:**

For each account/asset pair encountered during replay, the checker compares the expected cumulative input and output volumes against the actual values computed from the attribute store. Any mismatch is reported as a `VOLUME_MISMATCH` error.

**Pass 3 — Metadata comparison:**

For each account metadata key/value encountered during replay (excluding deleted keys), the checker compares the expected value against the actual value from the attribute store. Any mismatch is reported as a `METADATA_MISMATCH` error.

### Error Types

| Error Type | Description |
|------------|-------------|
| `HASH_MISMATCH` | Audit entry hash does not match recomputed hash (corruption or tampering) |
| `SEQUENCE_GAP` | A log sequence number is missing |
| `VOLUME_MISMATCH` | Account input or output volume does not match log replay |
| `METADATA_MISMATCH` | Account metadata value does not match log replay |
| `UNKNOWN_LEDGER` | A log references a ledger not created by any prior log |
| `TRANSACTION_UPDATE_MISMATCH` | Transaction updates in Pebble do not match log replay |

### CLI Usage

```bash
# Interactive check with progress spinner
ledgerctl store check

# Machine-readable JSON output
ledgerctl store check --json
```

### gRPC API

The check is exposed as a server-streaming RPC:

```protobuf
rpc CheckStore(CheckStoreRequest) returns (stream CheckStoreEvent);
```

Events are streamed in real-time:
- `CheckStoreProgress` events report the number of logs checked vs total
- `CheckStoreError` events report each integrity error as it is found

### Implementation

The checker (`internal/application/check/checker.go`) takes a `*data.Store` and `*attributes.Attributes` as input. It reads logs sequentially from the store, replays them to build expected state in memory, then queries the attribute stores for actual values.

Progress events are emitted every 100 logs and at the end of the replay.

## Hybrid Logical Clock (HLC)

A Hybrid Logical Clock ensures that all timestamps produced by the FSM are **strictly monotonically increasing**, regardless of clock skew between Raft nodes. This is critical because the leader can change at any time, and the new leader's physical clock may be behind the previous leader's clock.

### Problem

Without an HLC, when the leader changes:

1. Leader A proposes entries with timestamp `T=1000`
2. Leader B takes over, but its clock reads `T=950`
3. Leader B's proposals would get timestamps **earlier** than Leader A's, breaking monotonicity

This can lead to logs and transactions with out-of-order timestamps, violating the expected temporal ordering.

### Algorithm

The HLC is computed in the FSM during `applyProposal()`:

```
effectiveDate = max(proposalDate, lastAppliedTimestamp + 1)
```

Since timestamps are stored as `uint64` microseconds since epoch, `+1` represents a 1-microsecond increment.

More precisely:

```go
if proposalDate > lastAppliedTimestamp {
    lastAppliedTimestamp = proposalDate
} else {
    lastAppliedTimestamp++
}
```

This guarantees `timestamp(entry N+1) > timestamp(entry N)` for all entries.

### Why at the FSM Level

The HLC is implemented in the FSM (not in the admission layer) for several reasons:

1. **Deterministic**: All nodes (leader + followers) compute the same effective timestamp for each entry
2. **No leader-change handling needed**: The FSM state persists the last applied timestamp; a new leader automatically benefits from the HLC state
3. **Minimal surface area**: Only the FSM and persistence layer are affected; admission and processor remain untouched

### State Persistence

The `lastAppliedTimestamp` is maintained as volatile state in the `Machine` (FSM). It is:

- **Persisted to disk** via `SetLastAppliedTimestamp()` in every `ApplyEntries()` batch, alongside the Raft applied index
- **Loaded at startup** from the data store via `GetLastAppliedTimestamp()`
- **Included in Raft snapshots** via the `last_applied_timestamp` field in `MemorySnapshot`
- **Restored** when a node installs a snapshot from the leader

See [Hybrid Logical Clock Architecture](../technical/architecture/subsystems/consensus/hybrid-logical-clock.md) for a detailed technical description.

### Clock Skew Check

As a complementary safety mechanism, the `HealthChecker` periodically queries each peer's physical clock via the `GetNodeTime` gRPC RPC. If the absolute skew between any two nodes exceeds the configured threshold (`--health-clock-skew-threshold`, default 500ms), the cluster is marked unhealthy and the admission layer rejects new write operations. This ensures operators are alerted to NTP issues before the HLC has to compensate excessively, which would degrade timestamp quality.

See [Clock Skew Check](../technical/architecture/subsystems/consensus/hybrid-logical-clock.md#clock-skew-check) for details.

## Cold Archive Integrity

When a chapter is closed, its logs are exported as a Pebble SST to cold storage (filesystem or S3) and the leader proposes `ConfirmArchiveChapter`. Confirming an archive eventually leads to purging the source data, so the leader **must not propose** until it has cryptographic evidence that the archived bytes are intact.

### What is persisted

Every archive carries an attached SHA-256 checksum, written atomically with the data:

| Backend | Data | Checksum |
|---|---|---|
| Filesystem | `<bucketID>/chapters/<chapterID>/archive.sst` | sidecar `archive.sst.sha256` (32 bytes) |
| S3 | object body | user metadata key `sha256` (hex-encoded) |

The filesystem backend writes each file via a `<name>.tmp` → fsync → atomic rename → fsync directory sequence, with the data file fully committed before the sidecar is touched. The S3 backend relies on `PutObject` to apply the body and the `Metadata` map atomically.

### Verification flow

After upload (`Archive` returns), the leader recomputes the remote SHA-256 and compares it with the local checksum. Mismatch aborts the upload — no `ConfirmArchiveChapter` is proposed.

On boot, the archiver retry loop re-evaluates every still-open archive request:

- **Archive missing or checksum sidecar/metadata missing** (`Exists` returns false): treated as "not yet committed". The leader rebuilds and re-uploads. Pre-PR archives that never had a sidecar end up here too — they are silently re-uploaded on first contact with the new binary.
- **Archive complete**: the leader reads the persisted `ExpectedChecksum`, recomputes `Checksum` over the current bytes, and proposes `ConfirmArchiveChapter` only on equality. A mismatch is reported as a hard error with both hex digests in the log; the chapter stays unconfirmed so no purge runs.

The crash-recovery path is what catches truncation or post-upload bit rot: an SST that is "readable" still produces a digest, but only the comparison against the reference value detects that the bytes diverge from what was originally committed.

### Determinism of archive content

The SST produced by `buildSSTArchive` is a deterministic function of the chapter being archived: identical chapters produce byte-identical SSTs. `chapterMetadata` carries only `chapterId`, `startSequence`, `closeSequence`, `startAuditSequence`, and `closeAuditSequence` — no timestamps, no nonces. The two audit-sequence fields were added in #312 so the SST is scoped to BOTH the log range AND the (independent) audit-sequence range; otherwise audit entries would silently miss the archive and then be purged. This contract is enforced by `TestArchiver_BuildSSTIsDeterministic`. Adding a non-deterministic field would invalidate the checksum reference on re-upload and is treated as a regression.

## Integrity Guarantees

1. **Tamper detection**: With BLAKE3 (default), modifying any historical audit entry or its covered logs invalidates all subsequent hashes and an attacker cannot forge a valid chain. With XXH3, corruption is detected but a motivated attacker with direct storage access could theoretically construct collisions
2. **Ordering proof**: The audit hash chain proves the exact ordering of all proposals (and their logs)
3. **Determinism**: Given the same initial state and the same sequence of operations, the exact same audit hash chain is produced
4. **Crash recovery**: The audit hash chain survives node restarts — recovered from the last audit entry in Pebble
5. **Double-entry balance**: Every merge verifies that the sum of inputs equals the sum of outputs, catching any accounting inconsistency before it is persisted
6. **Full store verification**: The `store check` command validates the entire log sequence and all derived data (volumes, metadata) against the source of truth (logs)
7. **Timestamp monotonicity**: The Hybrid Logical Clock guarantees that every log has a strictly increasing timestamp, even across leader changes and clock skew
8. **Archive integrity at confirm time**: A cold archive is only confirmed after its SHA-256 matches the value persisted alongside the data. Truncated or bit-rotted archives surface as integrity errors and never trigger source-data purge
