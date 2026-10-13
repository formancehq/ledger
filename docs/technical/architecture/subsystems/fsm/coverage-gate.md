# Coverage Gate

## Overview

The coverage gate is the FSM-side guarantee that **the apply path can only read what admission declared it would read**. It enforces the contract built by [preload](preload.md): every `Scope.GetX(...)` call against the in-memory attribute cache must hit a key that the originating proposal's `plan.Coverage` had pre-declared.

A read that slips past the gate — for example by iterating the parent `KeyStore` directly — silently sees keys that admission did not preload and did not stamp into the proposal's coverage. That breaks the symmetry that makes the FSM deterministic across replicas: one node's cache might happen to hold an extra key the others don't, and now the same input produces different outputs.

[CLAUDE.md invariant #9](../../../../../AGENTS.md) states the rule: **never bypass the FSM coverage gate.** This page is the longer explanation.

## How the gate is wired

The FSM apply path receives a `Scope`, not a raw cache handle. Every accessor returned by `Scope` is a `gatedAccessor` that consults coverage bits before delegating.

```
Scope (gatedScope)
├── Ledgers()        → gatedAccessor[ledger]
├── Boundaries()     → gatedAccessor[boundary]
├── Volumes()        → gatedAccessor[volume]
├── AccountMetadata()→ gatedAccessor[metadata]
├── References()     → gatedAccessor[reference]
├── Transactions()   → gatedAccessor[transaction]
├── SinkConfigs()    → gatedAccessor[sink_config]
├── NumscriptVersions() / NumscriptContents()
├── PreparedQueries()
├── LedgerMetadata()
└── Indexes()
```

Source: `internal/infra/state/scope.go:79-113` (struct), `internal/infra/state/accessor.go:99-107` (`gatedAccessor.Get`).

`gatedAccessor.Get(key)` checks `CheckCoverage(kind, key.Bytes())` first, then delegates to the underlying `rawAccessor` if the key is admitted. On miss, it returns `ErrCoverageMiss{Attribute, CanonicalHex, IDHex, RaftIndex}` (`scope.go:34-54`) — a `domain.Describable` business error, not an FSM invariant break.

`CheckCoverage` (`scope.go:341-353`) does the actual lookup:

```go
slot := coverageSlotIndex[kind]
return slices.Contains(g.coverage[slot], id)
```

A dense `[len(cacheAttrKinds)][]U128` array (`scope.go:430-439`) holds the allowed IDs per kind. Linear scan beats a hash map at the typical size (a handful of IDs per order); the data structure is reused across phases of the same proposal to avoid allocation.

## What is gated

The full enumeration of gated kinds (`cacheAttrKinds` in `internal/infra/state/scope.go`) — 12 in total, one per `dal.SubAttr*` code in the resolver registry:

```
Volume, Metadata, Reference, Ledger, Boundary,
SinkConfig, NumscriptVersion, Transaction, NumscriptContent,
PreparedQuery, LedgerMetadata, Index
```

If a new attribute kind is added to the `attrCode` resolver registry (`internal/infra/plan/attribute_resolvers.go`), a matching entry must land in `cacheAttrKinds` here, and a new `gatedAccessor` must be wired into `Scope`. The two registries live in **different packages** — declaration in `internal/infra/plan`, enforcement in `internal/infra/state` — so nothing structural stops them drifting; only review does. Keep them in lock-step.

## Per-order vs proposal-wide scope

A proposal may carry several orders, each with its own coverage. The FSM applies them sequentially, **switching scope between orders**:

| Constructor | Coverage |
|-------------|----------|
| `NewScope(bits)` (`scope.go:318-336`) | Narrowed to `bits` — used for per-order apply. |
| `NewProposalScope()` (same file) | Admits every declared plan in the proposal — used for proposal-wide validations (e.g. transient-volume bookkeeping). |
| `NewScope(nil)` | Admits **nothing** — useful for tests of the gate itself. |

`applyPlans` (`scope.go:176-224`) and `applyAllPlans` (`scope.go:233-260`) translate the proposal's plans into the per-order vs proposal-wide coverage layouts. Both reuse the backing arrays to avoid allocations on the hot path.

## What violates the gate

> Reading the underlying `Registry.X.KeyStore().M` — or any other parent-cache iterator — directly **bypasses the gate**.

The result is a value the apply path was never authorized to consult. Two failure modes follow:

1. **Cross-node divergence.** Replica A holds the key in cache (because some earlier proposal preloaded it incidentally); replica B does not. The same proposal applies differently on the two nodes. The audit chain catches this eventually via the checker (`compareVolumes` and friends — see [checker.md](../checker/checker.md)), but only on the next `Check()` run.
2. **Non-deterministic ordering.** Iterating a map in Go has non-deterministic key order. Even if both replicas hold the same set of keys, they might process them in different orders, and any path-dependent output diverges.

Both are catastrophic for an FSM that is supposed to be a pure function of its declared inputs.

### What is *not* a gate violation

A stale admission observation is not one, and must never reach the gate. When a producer derives coverage from a read rather than from request data — a revert's volume keys come from the stored target, not from the request — the declaration is only as good as what the producer saw. Admission reads the local store with no read barrier, so a target that is committed but not yet applied there reads as absent and the order declares no volume keys, while apply reads the real postings.

Left alone that trips the gate, which would report a legitimate revert as a server defect. The producer therefore binds its observation (`OrderTechnical.revert_target_digest`) and the handler re-derives it, from state it is already authorized to read, *before* performing the dependent reads: `processRevertTransaction` compares digests before building the reversed postings. See [preload.md](preload.md#coverage-derived-from-a-read-must-bind-that-read) and the [revert-target observation](../admission/README.md#revert-target-observation).

A mismatch that reaches that comparison is classified by whether re-admission can converge — `STALE_INPUTS_RESOLUTION` when the target predates the batch, `REVERT_TARGET_CREATED_IN_BATCH` when the batch creates it. The comparison sits behind the handler's existing checks on the target, so an unknown, already-reverted or inconsistent target keeps its own reason; see [the revert-target observation](../admission/README.md#revert-target-observation). Neither classification is a softened coverage miss: an order whose digest *matches* and still reads an undeclared key reaches the gate and stays fatal.

## How a violation surfaces

A coverage miss means admission omitted a key the FSM tried to read. The gate
refuses the access and returns `ErrCoverageMiss` with the attribute, canonical
key, hashed identifier, and Raft index for operator diagnostics. The node logs
and counts the miss, and the Antithesis contract assertion reports it. The
public API returns reason `COVERAGE_MISS` with a redacted message and no key
metadata.

The FSM rejects the proposal before the audit writer. Earlier writes staged in
the proposal's `WriteSet` are discarded; no `AuditFailure`, log, or frozen
idempotency outcome records this admission bug. Once admission declares the
missing key, the same business request can be retried. The error implements
`UncommittedFailure` to carry this disposition from its producer to apply.
Other preparation, storage, and invariant failures use the same disposition;
only definitive business and governance rejections follow the audited path.

`domain.StoreFailure` preserves a nested coverage miss rather than relabelling
it as `STORAGE_OPERATION_FAILED`. The domain helper matches its stable reason
because importing the state-owned concrete type from processing would create
an import cycle. It walks ordinary and multi-error unwrap chains in stable
order. The same handling applies to `gatedAccessor.Delete` when an overlay
flushes a staged tombstone. Keeping the concrete miss intact lets the FSM
recognise the uncommitted failure and lets operators retain its diagnostics.

`INVALID_EXECUTION_PLAN` is a separate plan error. It is also uncommitted:
an admission/FSM coverage contract violation is not a business rejection.
Technical-update and preload failures already return before the audit path.

## The cascade-on-delete edge case

Some operations naturally want to scan: "delete every metadata row attached to this account", "purge every volume belonging to a deleted ledger", etc. These are the cases where a naive implementation reaches for `Registry.X.KeyStore().M` — and where the rule has historically been challenged.

The accepted solutions, in order of preference:

1. **Declare the relevant `plan.Coverage` upfront.** If admission can enumerate the set of keys at propose time, it does, and the apply path becomes a normal gated read.
2. **Defer to a lifecycle path.** `batch.deleteLedgerData` queues a Pebble `DeleteRange` over the ledger's key range and `MarkLedgerForCleanup` updates `LedgerInfo.DeletedAt`. The FSM itself never iterates the cache for the doomed ledger; read paths consult `DeletedAt` and short-circuit. See `internal/domain/processing/processor_ledger.go:99-113`.
3. **Reject the design.** If neither of the above fits, the operation does not belong in the FSM hot path.

There is **no documented exception** — every other path either declares its needs or defers. The temptation to wrap a raw `KeyStore` scan in a "convenience method" on `WriteSet` (and pretend it's gate-equivalent) is exactly what the rule guards against; helpers like that are the violation, not the resolution.

## Metrics

The gate counts misses (`scope.go:367-369`):

```
g.miss.Add(ctx, 1, metric.WithAttributes(kindAttr(kind)))
```

This is an OTel counter labelled by the attribute kind. A non-zero rate is a smoke signal that a producer's `plan.Coverage` declaration is incomplete — the FSM is asking for something admission did not preload, and the proposal will fail until the producer is fixed.

## Why this matters

The gate is what binds **admission's declared key set** to **the FSM's legitimate read horizon**. Without it:

- The preload contract becomes advisory ("please declare your needs, but you can also just iterate the cache if you want").
- The audit chain can no longer ground a guarantee that "all reads were observed and tracked".
- Adding a new persisted dataset becomes risky because the checker has no firm boundary on what the FSM might have consumed.

The gate is therefore strictly more than a performance optimisation. It is the only structural guarantee that the FSM is a pure function of its declared inputs — which is, in turn, the only reason the system is replicable in the first place.
