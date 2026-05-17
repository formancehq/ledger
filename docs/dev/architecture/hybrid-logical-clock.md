# Hybrid Logical Clock (HLC)

## Overview

The ledger uses a Hybrid Logical Clock (HLC) to guarantee that all timestamps in the system are **strictly monotonically increasing**. This is essential for a distributed system built on Raft consensus, where the leader can change at any time and different nodes may have different physical clocks.

## Problem Statement

In a Raft-based system with a single Raft group:

1. The **leader** node proposes entries. Each proposal includes a `date` field set from the leader's physical clock (`time.Now()`).
2. When a **leader election** occurs, the new leader may have a physical clock that is **behind** the previous leader's clock.
3. Without correction, the new leader would assign timestamps earlier than entries already applied by the old leader.

This violates the expected invariant: `timestamp(entry N+1) > timestamp(entry N)`.

### Concrete Example

```
Timeline:
  Leader A (clock accurate): proposes at T=1000000, T=1000001, T=1000002
  Leader A crashes, Leader B takes over
  Leader B (clock 50ms behind): proposes at T=999950, T=999951, ...
```

Without an HLC, the log would contain:

```
Sequence 1: T=1000000 (Leader A)
Sequence 2: T=1000001 (Leader A)
Sequence 3: T=1000002 (Leader A)
Sequence 4: T=999950  (Leader B) <-- VIOLATION: earlier than sequence 3
```

## Solution: HLC at the FSM Level

The HLC is implemented inside the FSM's `applyProposal()` method. This is the point where every node (leader and followers alike) processes each committed Raft entry.

### Algorithm

```go
func (fsm *Machine) hlcTimestamp(proposalDate *commonpb.Timestamp) *commonpb.Timestamp {
    if proposalDate.Data > fsm.lastAppliedTimestamp {
        fsm.lastAppliedTimestamp = proposalDate.Data
    } else {
        fsm.lastAppliedTimestamp++
    }
    return &commonpb.Timestamp{Data: fsm.lastAppliedTimestamp}
}
```

In plain English:

- If the proposal's date is **ahead** of the last applied timestamp, use the proposal's date (the physical clock is advancing normally).
- If the proposal's date is **behind or equal** to the last applied timestamp, increment the last applied timestamp by 1 microsecond.

This guarantees that the returned timestamp is always strictly greater than any previously returned timestamp.

### Timestamp Resolution

Timestamps are stored as `uint64` values representing **microseconds since the Unix epoch**. The minimum increment is therefore 1 microsecond (1 unit). This provides:

- Sufficient precision for financial transaction ordering
- Room for ~584,542 years of operation before overflow
- Natural ordering via simple integer comparison

## Architecture Design

### Why FSM Level (Not Admission Layer)

The HLC could theoretically be implemented in the admission layer (leader-only) or in the FSM. We chose the FSM for these reasons:

| Aspect | Admission Layer | FSM Level |
|--------|----------------|-----------|
| **Determinism** | Only the leader computes it; followers trust the value | All nodes compute the same result independently |
| **Leader changes** | Requires explicit state transfer or recovery logic | Automatic: FSM state persists across leaders |
| **Implementation complexity** | Needs coordination between admission and FSM | Self-contained in a single method |
| **Failure recovery** | Must handle partial state during leader transitions | State is part of the normal FSM snapshot/restore |

### Data Flow

```
Proposal created (admission layer)
    |
    v
proposal.Date = time.Now()  (leader's physical clock, in microseconds)
    |
    v
Raft consensus (proposal committed to log)
    |
    v
FSM.applyProposal()
    |
    +-- effectiveDate = hlcTimestamp(proposal.Date)
    |       |
    |       +-- if proposal.Date > lastAppliedTimestamp: use proposal.Date
    |       +-- else: lastAppliedTimestamp + 1
    |
    +-- writeSet = NewWriteSet(fsm)
    |       |
    |       +-- All logs created by the processor use effectiveDate
    |
    v
Logs and state persisted with monotonic timestamps
```

### Persistence

The `lastAppliedTimestamp` is persisted through three mechanisms:

1. **Per-batch persistence**: Written to PebbleDB in the same atomic batch as the applied index. Key: `[0x06][0x02]` (`ZoneGlobal` + `SubGlobLastAppliedTimestamp`). This ensures the timestamp survives node restarts.

2. **Snapshot inclusion**: Included in the `PreviewRestoreResponse` protobuf (field `last_applied_timestamp = 2` in `misc/proto/restore.proto`). This ensures the timestamp is correctly transferred during Raft snapshot installation.

3. **Startup recovery**: Loaded from PebbleDB during `NewMachine()` initialization, alongside the last applied index.

### Storage Format

```
Key:   [0x06][0x02]              (2-byte prefix: ZoneGlobal + SubGlobLastAppliedTimestamp)
Value: [uint64 big-endian]       (8 bytes, microseconds since epoch)
```

## Correctness Properties

### Strict Monotonicity

For any two proposals P1 and P2 where P1 is applied before P2:

```
effectiveDate(P2) > effectiveDate(P1)
```

This holds regardless of the physical clock values in P1 and P2.

**Proof**: After processing P1, `lastAppliedTimestamp >= effectiveDate(P1)`. When processing P2:
- If `P2.Date > lastAppliedTimestamp`: `effectiveDate(P2) = P2.Date > lastAppliedTimestamp >= effectiveDate(P1)`
- If `P2.Date <= lastAppliedTimestamp`: `effectiveDate(P2) = lastAppliedTimestamp + 1 > lastAppliedTimestamp >= effectiveDate(P1)`

### Determinism

All nodes in the Raft group compute the same `effectiveDate` for the same entry, because:
1. All nodes apply the same entries in the same order (Raft guarantee)
2. The HLC computation is purely a function of `proposal.Date` and `lastAppliedTimestamp`
3. `lastAppliedTimestamp` is part of the deterministic FSM state

### Crash Recovery

After a crash and restart:
1. The node loads `lastAppliedTimestamp` from PebbleDB
2. This is the timestamp from the last committed batch
3. New entries will have timestamps strictly greater than this value

If the node installs a Raft snapshot:
1. `lastAppliedTimestamp` is restored from the `MemorySnapshot`
2. The node continues with correct HLC state from the snapshot point

### Clock Skew Tolerance

The HLC tolerates **unbounded** clock skew between nodes. Even if a new leader's clock is hours behind, the HLC will simply increment by 1 microsecond per proposal until the physical clock catches up.

In practice, with NTP-synchronized clocks (typical skew < 100ms), the HLC will almost always use the proposal's physical clock value directly.

## Clock Skew Check

While the HLC guarantees correctness regardless of clock skew, large drift between nodes degrades timestamp quality: the HLC falls back to incrementing by 1 microsecond per proposal instead of using the physical clock. This means timestamps no longer reflect wall-clock time, which can confuse operators and monitoring tools.

To detect this proactively, the `HealthChecker` periodically queries each peer's physical clock via the `GetNodeTime` gRPC RPC and compares it to the local clock. If the absolute skew exceeds the configured threshold (`--health-clock-skew-threshold`, default 500ms), the cluster is marked unhealthy and the admission layer rejects new write operations.

### How It Works

1. The leader's `HealthChecker` runs periodically (configured by `--health-check-interval`)
2. For each peer, it calls `GetNodeTime` which returns the peer's `time.Now()` in microseconds
3. The skew is computed using the midpoint of the RPC call as the local reference time (to account for network RTT)
4. If `abs(localTime - remoteTime) > clockSkewThreshold`, the node logs a warning and the cluster is marked unhealthy
5. When unhealthy, the admission layer returns `ErrUnhealthy` for all write requests

### Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--health-clock-skew-threshold` | `500ms` | Maximum allowed clock skew between nodes. Set to `0` to disable the check. |

### Relationship to HLC

The clock skew check is a **complementary safety mechanism**, not a replacement for the HLC:

- The **HLC** guarantees correctness (monotonic timestamps) regardless of clock skew
- The **clock skew check** ensures timestamp quality by alerting operators before the HLC has to compensate excessively

## Testing

Tests are located in `internal/infra/state/machine_hlc_test.go`:

- **Unit tests** (`TestHLCTimestamp`): Test the `hlcTimestamp()` method in isolation with various scenarios (ahead, behind, equal, monotonicity across sequences).
- **Integration tests** (`TestHLCTimestampIntegration`): Test the full pipeline including `ApplyEntries()`, persistence to PebbleDB, and snapshot round-trips.

## Files

| File | Role |
|------|------|
| `internal/infra/state/machine.go` | HLC field, `hlcTimestamp()` method, integration in `applyProposal()` and `ApplyEntries()`, snapshot handling |
| `internal/query/config.go` | `ReadLastAppliedTimestamp()` for reading from PebbleDB |
| `internal/infra/state/batch.go` | `SetLastAppliedTimestamp()` for writing to PebbleDB |
| `misc/proto/restore.proto` | `last_applied_timestamp` field in `PreviewRestoreResponse` |
| `internal/infra/state/machine_hlc_test.go` | Unit and integration tests |
