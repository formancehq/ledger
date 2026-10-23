# Admission Cache Horizon

**Status:** Implemented
**Scope:** admission-side rejection of proposals whose cache prediction cannot be honored at FSM apply
**Related:** [../fsm/cache-layers.md](../fsm/cache-layers.md), [../fsm/deterministic-fsm.md](../fsm/deterministic-fsm.md)

## What it is

When admission builds a proposal's preload plan, it queries the dual-generation
cache (`AttributeCache.CheckCache(at, key)`) to decide, per key, whether to
emit a coverage-only `AttributeCoverage` (value nil) or a seed
`AttributeCoverage` (value set). The decision compares the predicted
apply-time generation `Gen(at, threshold)` against the FSM's current prepared
generation and the last index durably committed to Pebble:

| `Gen(at) − currentGen` | `CheckCache` result | Plan emitted |
|---|---|---|
| 0 | `CacheHit` / `CacheMiss` | coverage-only (`value = nil`) / seed (`value` set) |
| 1 | `CacheHit` (Gen0-hit only) / `CacheMiss` | coverage-only / seed |
| **≥ 2** | **`CacheUnreachable`** | **proposal rejected at admission** |

The ≥ 2 case is the "cache horizon exceeded" condition this document is about.
Admission also rejects when `Gen(at) − Gen(lastPersistedIndex) ≥ 2`, even if
`Gen(at) − currentGen` is zero. Preparation rotates the in-memory cache before
its Pebble batch commits. Two prepared rotations can evict a recent write from
both cache generations while Pebble still contains its older value. In that
window, a cache miss must not seed the proposal from Pebble. The durable cursor
advances only after `batch.Commit()` returns and is restored from Pebble on
startup and follower synchronization.

## Threshold-change reset barrier

Changing the rotation threshold clears both cache generations. Without a
commit barrier, the applier could prepare that reset while the preceding
batch's writes exist only in memory: admission would see a cache miss and read
their older values from Pebble under the new epoch. The generation-distance
guard cannot detect this when the new, larger threshold places both the
durable and predicted indexes in one generation.

The applier therefore splits a `ClusterConfig` entry into its own FSM batch
and waits for the preceding Pebble commit **before** preparing that entry.
Replay uses the same split and commits each prefix synchronously. Admission
builds that began before the reset are still checked by the cache epoch;
builds that begin after it can read the now-durable primary-store values.
The split scans the already-decoded entries once, and the extra commit wait
occurs only for a cluster-config proposal, not for ordinary business batches.

## Why we reject instead of preload

In the ≥ 2 regime, two cache rotations are predicted to fire on the FSM side
between this proposal's propose-time and its apply-time. Each rotation does
the canonical `gen0 → gen1, drop old gen1, gen0 := ∅` move. So any value
loaded *now* and emitted in a `MirrorPreload` plan would land in `gen0` and
then:

1. First rotation: `gen0` (with the preloaded value) becomes `gen1`.
2. Second rotation: `gen1` (still holding the preloaded value) is **dropped**.

By the time the order applies, the cache has no record of the preload —
reads would miss the value the proposer guaranteed. The FSM-side coverage
gate (`Scope.CheckCoverage`) would still admit the read, but the underlying
`KeyStore.Get` would return `ErrNotFound` for a key admission promised would
be available. This is the class of bug invariant #1 ("cache is the source of
authority") is designed to make impossible.

Returning `CacheUnreachable` and rejecting the proposal at admission keeps
the contract honest: the proposal never enters Raft, no audit trail is owed,
and the client retries against a fresh admission snapshot. By the time the
retry lands, the FSM has caught up and the predicted horizon is within reach.

## Why this is not a domain error

The rejection is **not** a business outcome — the user-submitted operation
might be perfectly valid. It is an *operational* signal that admission's
view is too far behind FSM apply for the system to safely admit any new
proposal whose preload would target keys this stale.

Consequently the error lives in `internal/infra/plan/` as
`plan.ErrCacheHorizonExceeded`, **not** in `internal/domain/`. The gRPC
adapter (`internal/adapter/grpc/server.go`) maps it to `codes.Unavailable`
so existing client-side retry interceptors handle it transparently. No
`AuditFailure` is recorded — the system explicitly declined to take
responsibility for this proposal.

## When can it fire?

Under a correctly tuned rotation threshold (`--cache-rotation-threshold`,
default 1000) and an FSM apply rate that keeps up with proposals, the
≥ 2-generation horizon should be rare. It can occur with two prepared
rotations ahead of Pebble, including when a pending batch crosses a
generation boundary. The exact index gap depends on where the durable cursor
lies within its generation.

Recurring `ErrCacheHorizonExceeded` is therefore an *operational* signal:

- The rotation threshold is too low for the workload (raise it).
- FSM apply is falling behind admission (overload — check apply latency,
  disk, GC).
- A transient backlog after a leadership transfer, snapshot restore, or
  bloom rebuild (self-resolves; clients retry).

Use `lifecycle.SendEvent` / metric counters at the rejection site (TODO
follow-up) to make this observable to SREs.

## Implementation

- `internal/infra/cache/cache.go` — `CacheUnreachable` constant, the
  generation and durable-cursor horizon checks in `CheckCache` and
  `ObserveAdmission`.
- `internal/infra/state/machine.go` — publishes the durable cursor to the
  cache together with `lastPersistedIndex` after commit or recovery.
- `internal/infra/plan/errors.go` — `ErrCacheHorizonExceeded` sentinel.
- `internal/infra/plan/resolve.go` — short-circuit on
  `CacheUnreachable`, return `ErrCacheHorizonExceeded`.
- `internal/adapter/grpc/server.go` — `convertToGRPCError` maps
  `ErrCacheHorizonExceeded` to `codes.Unavailable`.
- `internal/application/admission/admission.go` — already wraps the
  builder error and returns it to the gRPC layer (no new code).

## Tests

- `cache_test.go` / `TestAttributeCache_IsGuaranteedInCache_TwoGenerationsAhead`
  — `CheckCache` returns `CacheUnreachable` for the default case (both with
  and without a stored value).
- `builder_test.go` / `TestBuildPreloads_RejectsCacheHorizonExceeded` —
  end-to-end resolver short-circuits with `ErrCacheHorizonExceeded` when
  the tracker is pinned past the second generation boundary.
