# Log Integrity and Correctness

This document describes the mechanisms ensuring the integrity and immutability of the ledger's log chain.

## Log Hash Chaining

Every log produced by the ledger is cryptographically chained to its predecessor using BLAKE3 hashing. This creates a verifiable chain where any tampering with a historical log invalidates all subsequent hashes.

### Hash Formula

```
hash = blake3(lastLogHash || deterministicSerialize(logWithoutHash))
```

Where:
- `lastLogHash` is the hash of the immediately preceding log (empty for the genesis log)
- `deterministicSerialize(logWithoutHash)` is the Protocol Buffers deterministic serialization of the log **before** the hash field is set
- `||` denotes concatenation

### Genesis Case

The first log in the chain has no predecessor. In this case, `lastLogHash` is empty (zero-length), so the hash is simply:

```
hash = blake3(deterministicSerialize(log))
```

### Implementation Details

- **Hash algorithm**: BLAKE3 via `github.com/zeebo/blake3`
- **Serialization**: `proto.MarshalOptions{Deterministic: true}` ensures stable byte output across runs
- **Hasher reuse**: A shared `blake3.Hasher` is reused (with `Reset()`) across log hash computations to avoid allocation overhead
- **Hash field**: Stored as `bytes hash` (field 4) in the `Log` protobuf message

### State Persistence

The `lastLogHash` is maintained as volatile state in the `Machine` (FSM). It is:
- **Persisted** in Raft snapshots via the `last_log_hash` field in `MemorySnapshot`
- **Restored** when a node installs a snapshot from the leader
- **Propagated** through the `Buffered` layer during proposal processing

The chain is **not broken** by Raft snapshots: the snapshot captures the latest hash, and new logs computed after snapshot restoration continue the chain correctly.

### Failure Isolation

If a proposal fails (e.g., insufficient funds, ledger not found), the `Buffered` state is discarded without being merged into the `Machine`. This means `lastLogHash` remains unchanged, preserving the chain's integrity.

### Idempotent Responses

When an idempotent request matches a previously processed order, the processor returns a **reference** to the existing log rather than creating a new one. References do not advance the hash chain.

## Double-Entry Invariant Check

A defensive check is performed at `Merge()` time in the `Buffered` layer to verify the fundamental accounting invariant: **the sum of all input deltas must equal the sum of all output deltas**.

### Rationale

Every posting moves the same amount from a source account (increases output) to a destination account (increases input). If these totals ever diverge, it means money was created or destroyed — a critical bug in the processing logic.

### How It Works

After merging both Input and Output volume updates, the check:

1. Computes the **net delta** for each volume update by comparing `New` vs `Old` values from the `DerivedKeyStore`
2. Sums all input deltas and all output deltas independently
3. Returns an error if the sums differ

The delta computation handles both absolute values (`Known`) and relative values (`DiffSinceBaseIndex`) by subtracting the old value from the new value in each case.

### When It Triggers

This check runs on every successful proposal merge, before any state is committed to storage. A violation would prevent the batch from being committed, stopping the node rather than persisting corrupted data.

## Store Integrity Check

The `store check` command performs a comprehensive offline verification of the entire store. It replays all logs from the beginning and compares the derived state against the actual stored values.

### Verification Process

The check runs in three passes:

**Pass 1 — Log replay and hash chain verification:**

For each log from sequence 1 to the last sequence:

1. **Sequence continuity**: Verifies that no log sequence numbers are missing (gaps)
2. **Hash chain integrity**: Recomputes the expected BLAKE3 hash for each log and compares it to the stored hash. The hash is computed over the deterministic protobuf serialization of the log (without the hash field), chained to the previous log's hash
3. **State replay**: Replays each log payload to build expected state:
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
| `HASH_MISMATCH` | Log hash does not match recomputed hash (corruption or tampering) |
| `SEQUENCE_GAP` | A log sequence number is missing |
| `VOLUME_MISMATCH` | Account input or output volume does not match log replay |
| `METADATA_MISMATCH` | Account metadata value does not match log replay |
| `UNKNOWN_LEDGER` | A log references a ledger not created by any prior log |

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

The checker (`internal/service/check/checker.go`) takes a `*data.Store` and `*attributes.Attributes` as input. It reads logs sequentially from the store, replays them to build expected state in memory, then queries the attribute stores for actual values.

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

See [Hybrid Logical Clock Architecture](./architecture/hybrid-logical-clock.md) for a detailed technical description.

### Clock Skew Check

As a complementary safety mechanism, the `HealthChecker` periodically queries each peer's physical clock via the `GetNodeTime` gRPC RPC. If the absolute skew between any two nodes exceeds the configured threshold (`--health-clock-skew-threshold`, default 500ms), the cluster is marked unhealthy and the admission layer rejects new write operations. This ensures operators are alerted to NTP issues before the HLC has to compensate excessively, which would degrade timestamp quality.

See [Clock Skew Check](./architecture/hybrid-logical-clock.md#clock-skew-check) for details.

## Integrity Guarantees

1. **Tamper detection**: Modifying any historical log invalidates all subsequent hashes in the chain
2. **Ordering proof**: The hash chain proves the exact ordering of all logs
3. **Determinism**: Given the same initial state and the same sequence of operations, the exact same hash chain is produced (verified by tests)
4. **Crash recovery**: The hash chain survives node restarts via Raft snapshot persistence
5. **Double-entry balance**: Every merge verifies that the sum of inputs equals the sum of outputs, catching any accounting inconsistency before it is persisted
6. **Full store verification**: The `store check` command validates the entire log chain and all derived data (volumes, metadata) against the source of truth (logs)
7. **Timestamp monotonicity**: The Hybrid Logical Clock guarantees that every log has a strictly increasing timestamp, even across leader changes and clock skew
