# Admission Cache Horizon

**Status:** Implemented
**Scope:** admission-side rejection of proposals whose cache prediction cannot be honored at FSM apply
**Related:** [../fsm/cache-layers.md](../fsm/cache-layers.md), [../fsm/deterministic-fsm.md](../fsm/deterministic-fsm.md)

## What it is

When admission builds a proposal's preload plan, it queries the dual-generation
cache (`AttributeCache.CheckCache(at, key)`) to decide, per key, whether to
emit a `Declare`, a `Touch`, or a `Preload`. The decision compares the
predicted apply-time generation `Gen(at, threshold)` against the FSM's current
applied generation:

| `Gen(at) − currentGen` | `CheckCache` result | Plan emitted |
|---|---|---|
| 0 | `CacheGuaranteed` / `CacheNeedsTouch` / `CacheMiss` | `Declare` / `Touch` / `Preload` |
| 1 | `CacheGuaranteed` / `CacheMiss` | `Declare` / `Preload` |
| **≥ 2** | **`CacheUnreachable`** | **proposal rejected at admission** |

The ≥ 2 case is the "cache horizon exceeded" condition this document is about.

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
gate (`Plan.CheckCoverage`) would still admit the read, but the underlying
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
≥ 2-generation horizon should never be reached: admission's `nextIndex`
would have to sit at least `2 × threshold` indices past the FSM's
last-applied index. In a healthy single-leader cluster that means
**2000+ in-flight proposals** between admit and apply — well outside
the normal operating envelope.

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
  default branch of `CheckCache`.
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
