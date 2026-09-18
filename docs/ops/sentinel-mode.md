# Sentinel Mode

Sentinel mode enables **runtime volume consistency assertions** that verify the correctness of the ledger's volume tracking at every Raft apply. When enabled, four independent checks run in the critical path to detect bugs, cache/storage divergence, and accounting invariant violations as early as possible.

This mode is designed for **testing environments** (e.g., Antithesis chaos testing) and **staging deployments** where catching bugs early is more important than raw throughput. It adds overhead to every write operation.

## Quick Start

```bash
# Enable sentinel mode at startup
ledger run --sentinel-mode [other flags...]
```

Environment variable: `SENTINEL_MODE=true`

## Checks Performed

Sentinel mode runs two checks during proposal preparation and two after the batch commits:

### 1. Volume Update Monotonicity

Verifies that volumes **never decrease** (input and output can only grow). A shrinking volume indicates a stale base value was used during processing.

- **Where**: `WriteSet.Merge()`, before Pebble commit
- **Catches**: Stale preloads, cache eviction bugs, concurrent processing errors

### 2. Delta / Posting Cross-Check

After merge, computes the expected volume deltas from the postings in the committed logs and compares them to the actual volume deltas produced by the processing pipeline.

- **Where**: `applyProposal()`, after `Merge()`
- **Catches**: Wrong amount applied, wrong account credited/debited, missed posting

### 3. Post-Commit Volume Verification

After the Pebble batch commits, `CommitPreparedBatch()` reads a snapshot pinned
to that commit and compares the surviving volume rows with values captured by
`Merge()`. Repeated updates use the last value for each canonical key. Ephemeral
purges invalidate that key's earlier updates; a successful ledger deletion
invalidates every update for that ledger at or before the deletion, including
updates in the same proposal. Other ledgers remain checked, and unexpected
missing rows or different values still fail.

The deletion list is captured from the successful `WriteSet`, with independent
slice ownership before the next proposal resets it. Rejected deletion orders do
not invalidate expectations. Same-name ledger recreation remains rejected by
the ledger tombstone contract.

### 4. Aggregated Volume Balance

On the same post-commit snapshot, every touched ledger's aggregate volumes must
satisfy the **double-entry invariant**: total inputs equal total outputs for
each asset. This detects imbalances even outside the individual updated rows.

Both post-commit checks run for live apply, follower catch-up, and WAL replay.
Preparation has already mutated the in-memory FSM and staged its writes; the
Pebble commit happens before verification. A failed check therefore returns an
error after those writes are durable; it does not roll back the committed batch.

## Antithesis Integration

Some invariant branches also call the Antithesis SDK's `assert.Unreachable()`. All check failures propagate as errors; the missing-volume post-commit branch, for example, returns an error without a dedicated assertion.

## Performance Impact

Sentinel mode adds measurable overhead:

- **Monotonicity check**: O(n) over volume updates — negligible
- **Delta/posting cross-check**: O(n) over volume updates + log postings — negligible
- **Aggregated volume balance**: Full Pebble scan per touched ledger — **significant for large ledgers**
- **Post-commit verification**: One Pebble read per volume update — moderate

**Recommendation**: Enable in testing/staging. Disable in production unless actively investigating a suspected volume corruption issue.

## Key Files

| File | Role |
|------|------|
| `internal/infra/state/sentinel.go` | All four verification functions and helper utilities |
| `internal/infra/state/machine.go` | Guard conditions (`sentinelMode`) at apply and post-commit |
| `internal/infra/state/write_set.go` | Guard condition for monotonicity check at merge time |
| `internal/bootstrap/config.go` | `SentinelMode` configuration field |
| `cmd/server/server.go` | `--sentinel-mode` flag definition |
