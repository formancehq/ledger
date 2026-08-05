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

## How a violation surfaces

A coverage miss is an admission bug, not an infrastructure fault, and it is labelled that way on every surface it reaches:

| Surface | Value |
|---------|-------|
| Hash-chained `AuditFailure.Reason` | `ERROR_REASON_COVERAGE_MISS` |
| `AuditFailure.Context` | `attribute`, `canonicalHex`, `idHex`, `raftIndex` |
| Frozen idempotency outcome | **not applicable** — `KindInternal` failures are deliberately never frozen (`IsFreezableFailure`, `internal/domain/errors.go:152-158`), so a coverage miss leaves no idempotency record and the request stays retryable |
| gRPC / HTTP `ErrorInfo.reason` | `COVERAGE_MISS` (status `INTERNAL` — both reasons are `KindInternal`) |
| Node-local log + OTel counter | `scope.go:374-378` (see [Metrics](#metrics)) |

The `Metadata()` keys are camelCase, matching every other `Describable` and the repo-wide wire convention. The structured log emitted alongside keeps snake_case field names — that is a log, not a wire payload.

The idempotency row is worth stating explicitly because the non-freezing is a design point, not an oversight: `recordIdempotencyFailure` returns early (`machine.go:1729`) for any non-freezable kind, and `KindForReason` classifies `ERROR_REASON_COVERAGE_MISS` as `KindInternal` (`internal/domain/reason.go:124`). Freezing would pin a server bug against the caller's key for the whole TTL. This was equally true before EN-1379 — `ERROR_REASON_STORAGE_OPERATION_FAILED` sits in the same `KindInternal` arm — so the idempotency outcome is not one of the surfaces the fix repairs.

### Upgrade note: the reason is hash-bound

`buildAuditFailurePayload` (`internal/infra/state/audit_envelope.go:176-198`) folds the reason, the message and every sorted context key **and value** into the audit hash pre-image. Relabelling an FSM-emitted failure is therefore *not* hash-neutral: for one and the same Raft entry, a node on the old build hashes `{STORAGE_OPERATION_FAILED, "storage operation failed: loading ledger", {operation}}` while a node on the new build hashes `{COVERAGE_MISS, "preload coverage miss (…)", {attribute, canonicalHex, idHex, raftIndex}}`.

During a rolling upgrade both builds apply the same replicated log, so a coverage miss landing inside the mixed-version window writes **different hashes for the same index** — invariant #2 across the version boundary, after which `checker.verifyAuditHashChain` fails on whichever node disagrees with the persisted chain. Nothing absorbs this: `HashVersion` comes from `HashGenerator.Algorithm()` (`machine.go:1427`) and identifies the hash *algorithm* only, carrying no notion of failure-projection semantics, so a node cannot recognise "this entry was produced under the old classification".

Consequences to hold in mind:

- Coverage misses are correctly and consistently classified only once **every** node runs the new build. A miss inside the window can diverge the chain.
- The window is narrow in practice: the trigger is itself an admission bug that should never fire. This is a low-probability path, not a routine one.
- Only the read sites EN-1379 **converted** are affected. The numscript re-resolution path already returned the bare miss, so its reason and context are unchanged; only its message shifts, and only because `Error()` was aligned to the camelCase `raftIndex` spelling.
- **This hazard is general.** It attaches to any change to an FSM-emitted error's reason, message or metadata — not to EN-1379 specifically. Treat "relabelling an FSM error is observable in the hash chain" as the standing rule when reviewing such a change.

Making this structurally safe would mean carrying a failure-projection semantics version alongside `HashVersion` so old entries keep reproducing their original bytes. That is a deliberate design decision, not something EN-1379 took on.

The operational consequence is declared where operators look for supported upgrade paths: [deployment.md — Upgrading across an FSM error-identity change](../../../../ops/deployment.md#upgrading-across-an-fsm-error-identity-change) states the standing rule — mixed-binary rolling upgrades are not supported across any such change, and no data wipe is required because no persisted layout changes — and lists EN-1379 alongside the earlier changes it covers.

This holds because FSM read sites build their failure through `domain.StoreFailure(operation, err)` instead of constructing a `domain.ErrStorageOperation` directly:

```go
// internal/domain/coverage.go
func StoreFailure(operation string, err error) Describable {
	if violation := CoverageContractViolation(err); violation != nil {
		return violation // propagated verbatim — reason and metadata intact
	}

	return &ErrStorageOperation{Operation: operation, Cause: err}
}
```

The distinction is load-bearing. `buildAuditFailure` (`internal/infra/state/audit.go:19-29`) reads `Reason()` and `Metadata()` off the **outermost** `Describable` and never unwraps:

```go
failure.Reason = domain.ReasonCode(d.Reason())
maps.Copy(failure.GetContext(), d.Metadata())
```

So wrapping a `*ErrCoverageMiss` would permanently record an admission bug as `STORAGE_OPERATION_FAILED`, with the context stripped to `{operation: "..."}` — in a chain that is immutable by construction. `businessErrorToGRPCStatus` (`internal/adapter/grpc/errors.go:86`) performs the same outermost-only read, so the client sees the relabelled reason too. (`recordIdempotencyFailure` reads the outermost `Describable` the same way, but never reaches a coverage miss at all — see the idempotency row above.)

`CoverageContractViolation` matches on the stable domain `Reason()` string rather than the concrete type, because `internal/domain/processing` cannot import `internal/infra/state` — `state` imports `processing`, so a type-based `errors.As` would be an import cycle. It recognises both `COVERAGE_MISS` and `INVALID_EXECUTION_PLAN`, walking the `Unwrap` chain so a violation nested behind a numscript `QueryBalanceError` is still found. It also descends into multi-error nodes (`errors.Join`, or `fmt.Errorf` with several `%w`), which `errors.Unwrap` cannot follow, visiting members in slice order so the result stays deterministic as the apply path requires. The `forbidigo` rule below cannot see a future `Join` — it is a plain call, not a forbidden type reference — so handling it in the walk is what keeps the guard whole.

A `forbidigo` rule in `.golangci.yaml` forbids bare `ErrStorageOperation` construction, so a new read site cannot silently reintroduce the flattening.

Technical-update handlers take a different route to the same place: they return plain `fmt.Errorf("...: %w", err)` and `applyTechnicalUpdates` extracts the violation via `planInvariantDescribable` (`machine.go:1133`), which uses `errors.As` because the `state` package *can* name the concrete type. Both routes end at the same reason code.

Note that a violation is not only produced by reads. `gatedAccessor.Delete` (`internal/infra/state/accessor.go:113-118`) also calls `CheckCoverage`, so a staged tombstone flushed by `orderOverlayScope.Commit()` surfaces a coverage miss too — which is why `ProcessOrders` routes that error through `StoreFailure` rather than dropping it (`internal/domain/processing/processor.go:341-350`).

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
