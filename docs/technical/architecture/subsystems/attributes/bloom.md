# Bloom Filters

## Overview

A bloom filter sits in front of each attribute cache. Its job is to **short-circuit "key absent" lookups** during preload: when `MayContain(k) == false`, the loader knows the key is *certainly* missing and can skip the Pebble `Get` entirely. When `MayContain(k) == true`, the key *might* be present and the loader proceeds normally — a false-positive costs one Pebble read.

The bloom filter is not a correctness primitive — it does not gate the FSM apply path and the system is correct without it. It is a **preload-time performance optimisation** that materially reduces the load on Pebble for workloads dominated by writes to fresh accounts (the common case).

Source: `internal/infra/bloom/bloom.go`.

## Per-attribute filters

There are **12 independent filters**, one per attribute kind — the same kinds the [coverage gate](../fsm/coverage-gate.md) enumerates:

```
Volume, Metadata, Reference, Ledger, Boundary, Transaction,
SinkConfig, NumscriptVersion, NumscriptContent, LedgerMetadata,
PreparedQuery, Index
```

Adding a new attribute kind requires adding both a coverage-gate entry and a bloom-filter slot (`filterSnapshot.filterForAttrType` dispatch in `bloom.go:244` and the `bloomTypes()` descriptor at `bloom.go:633`). The two pieces were historically forgotten in isolation; landing them together is the convention now (the wire-up of `PreparedQuery` in `bloom.go:237, 488` is the most recent example — added by EN-1321).

## Structure — blocked filter

Each filter is a **blocked** bloom filter (`internal/infra/bloom/filter.go:72-98`). Memory is laid out as fixed-size 512-bit blocks (one cache-line each), with `k` hash functions chosen via the standard `c · ln(2)` formula against the target false-positive rate.

For each key:

1. Compute two 64-bit hashes (a `double-hash` to avoid running `k` distinct hash functions).
2. Pick a block via `reducerange(h1, numBlocks)` — Lemire's range reduction, faster than modulo.
3. Set `k` bits inside that block via further reductions.

A `MayContain` lookup is the same shape, with atomic loads on the bit words.

`optimize()` (`filter.go:201-252`) computes `nbits` from the target false-positive rate and the expected key count, then derives `nhashes`. Concrete sizing is per-attribute.

## Concurrency model

The filter is **lock-free on read, single-writer on write**:

- **Readers** (preload loaders calling `MayContain`) use `atomic.LoadUint32` on the bit words. Any number of them can run concurrently.
- **Writer** (the FSM `Add` path, `bloom.go:86-91`) uses `atomic.OrUint32` to set bits. The FSM is the single writer by construction — it runs as a single goroutine.
- **Snapshot swap** (replacing one `filterSnapshot` with another after a rebuild) uses `atomic.Pointer[filterSnapshot]` plus a `readyMu` mutex (`bloom.go:292`) to close the TOCTOU window during the swap.

There is no per-block lock. The blocked layout means contention between writers is non-existent (one writer) and contention between a writer and a reader is on a single 512-bit block (a cache line), which atomic ops handle without coordination.

## Lifecycle

### Adds happen on the FSM apply path

When the FSM commits a batch, it calls `AddCanonicalKeys(snap.<kind>, updates.<kind>)` for every attribute kind touched (`bloom.go:488`, called from `machine.go:1527`). The updates were collected by the `WriteSet.Merge` step. `Add` itself is a tight loop of atomic ORs; for batches, `addBatch()` (`bloom.go:96`) amortises the OTel counter increments.

There is **no preload-time `Add`**. `MirrorPreload` writes to the cache and Pebble but does not touch the bloom — the bloom learns about a key only when the FSM has actually committed something for it. This is intentional: preload-time adds would pollute the filter with values that ended up not being written if the proposal was rejected.

### Persistence

Dirty blocks are flushed to Pebble at every batch commit:

- Key layout: `[ZoneGlobal][SubGlobBloom][attrCode][blockIdx LE8]`.
- `PersistDirtyBlocks()` (`bloom.go:113-131`) iterates dirty blocks and queues writes into the FSM batch — so the bloom flush is atomic with the rest of the batch's effects.
- On boot, `RestoreFromStore()` (`bloom.go:165-211`) reloads the persisted blocks, merging via OR. Because OR is monotone, partial restorations are safe.

### Boot path — populate from the attribute store

`PopulateFromStore()` (`bloom.go:536`) scans the attribute zones at boot and re-adds every key to the relevant filter. This handles two cases:

1. The node has never run before (no persisted bloom rows).
2. A snapshot was installed — the bloom rows from the donor node may not match this node's view.

The populate runs **asynchronously** after boot (`StartAsyncBloomPopulate` in `cache_snapshotter.go:602-667`). Until it completes, the bloom is in a "not-ready" state and `MayContain` conservatively returns `true` for every key — meaning preload behaves as if the bloom didn't exist (every lookup hits Pebble). `SetReadyIfEpoch()` (`bloom.go:414`) closes the window via an epoch number, preventing a stale rebuild from publishing over a fresh one.

### Cache rotation interaction

The attribute cache rotates Gen0 → Gen1 → discard at every `--cache-rotation-threshold` Raft indices. The bloom must stay coherent across the rotation because the cache-prediction primitive depends on it.

The mechanism:

1. **Before commit**: `PersistDirtyBlocks()` runs in the same batch as the FSM writes. Any add made during this Raft index is persisted before the batch commits.
2. **After rotation**: `replayBloomFromCache()` (`cache_snapshotter.go:706-727`) walks Gen0 + Gen1 and re-adds every live key to the bloom. This guarantees the filter is a superset of the cache — `MayContain == false` implies the key is truly absent.

The "bloom must follow rotations strictly" rule appears in [CLAUDE.md invariant #5](../../../../../AGENTS.md): never delete cache entries outside of rotations. Adding a one-off `Remove` operation to the bloom would break the prediction primitive (false negatives are not recoverable in a standard bloom).

## Metrics

All four counters are OTel `Int64Counter`s registered in `bloom.go:641-659`, each labelled with `type=<attribute kind>` via `withAttr()` (`bloom.go:716`):

| Counter | When it increments | Operator signal |
|---------|--------------------|-----------------|
| `bloom.lookups` | Every `MayContain` call. | Total work the bloom is doing. |
| `bloom.negatives` | `MayContain` returned `false`. | Pebble reads the bloom saved. |
| `bloom.adds` | A key was inserted via `Add`. | Growth rate per attribute kind. |
| `bloom.false_positives` | `MayContain` said `true` but the loader's Pebble `Get` returned `NotFound`. | Filter saturation indicator — a rising rate means more keys than the filter was sized for. |

A rising `false_positives / lookups` ratio (above ~1%) is the usual operator trigger to bump filter sizing.

## What the bloom does not do

- **It is not a tombstone source.** `MayContain == false` means "never inserted", which for a monotone bloom is equivalent to "key has never been written". It cannot represent "key was deleted" because the filter doesn't support deletion.
- **It does not gate the FSM.** The coverage gate (`Scope.GetX`) is the structural guarantee — the bloom only optimises the preload-time loader. A bug in the bloom can never cause divergence between replicas; it can only cause one of them to be slower than the others.
- **It is not consulted on the apply path.** The FSM holds a `*dal.WriteSession` with no read methods — see [invariant #3](../../../../../AGENTS.md). Bloom is preload-only.
