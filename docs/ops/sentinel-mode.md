# Sentinel Mode

Sentinel mode enables **runtime volume consistency assertions** that verify the correctness of the ledger's volume tracking at every Raft apply. When enabled, four independent checks run in the critical path to detect bugs, cache/storage divergence, and accounting invariant violations as early as possible.

This mode is designed for **testing environments** (e.g., Antithesis chaos testing) and **staging deployments** where catching bugs early is more important than raw throughput. It adds overhead to every write operation.

## Quick Start

```bash
# Enable sentinel mode at startup
ledger-v3-poc run --sentinel-mode [other flags...]
```

Environment variable: `SENTINEL_MODE=true`

## Checks Performed

Sentinel mode runs four checks, in order, during every `applyProposal()`:

### 1. Volume Update Monotonicity

Verifies that volumes **never decrease** (input and output can only grow). A shrinking volume indicates a stale base value was used during processing.

- **Where**: `Buffered.Merge()`, before Pebble commit
- **Catches**: Stale preloads, cache eviction bugs, concurrent processing errors

### 2. Delta / Posting Cross-Check

After merge, computes the expected volume deltas from the postings in the committed logs and compares them to the actual volume deltas produced by the processing pipeline.

- **Where**: `applyProposal()`, after `Merge()`
- **Catches**: Wrong amount applied, wrong account credited/debited, missed posting

### 3. Aggregated Volume Balance

For every ledger touched by the proposal, reads back the full aggregated volumes from Pebble and verifies the **double-entry invariant**: for each asset, the global sum of inputs must equal the global sum of outputs.

- **Where**: `applyProposal()`, after delta cross-check
- **Catches**: Any form of volume corruption, regardless of root cause

### 4. Post-Commit Cache/Pebble Verification

After the Pebble batch is committed, reads back the volume values from Pebble and compares them to the expected values written during `Merge()`. When multiple entries in the same `ApplyEntries` batch touch the same volume key, only the last entry's value is verified (earlier values are overwritten).

- **Where**: `ApplyEntries()`, after Pebble batch commit
- **Catches**: Pebble write failures, cache/storage divergence, snapshot inconsistencies

## Antithesis Integration

All checks use the Antithesis SDK's `assert.Unreachable()` to report detected invariant violations. This allows Antithesis to flag these as property violations during chaos testing, even if the node subsequently crashes or recovers.

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
| `internal/infra/state/buffer.go` | Guard condition for monotonicity check at merge time |
| `internal/bootstrap/config.go` | `SentinelMode` configuration field |
| `cmd/server/server.go` | `--sentinel-mode` flag definition |
