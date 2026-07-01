# FSM Cache Layering

**Status:** Implemented
**Scope:** the FSM-side read/write path during apply — how the `Scope` facade (interface), the gate decorator, the `WriteSet` engine, `Plan`, `KeyStore`, and `AttributeCache` compose, and which paths are allowed to mutate the cache
**Non-goals:** admission-side Preload building, Pebble layout, snapshot/restore (covered in [deterministic-fsm.md](deterministic-fsm.md) and [storage.md](../storage/storage.md))

## Why this exists

The FSM apply path holds two hard invariants:

1. **No Pebble reads on the hot path.** Every read during apply must come from the in-memory cache or from the proposal payload itself. This is compiler-enforced: the FSM only holds `*dal.WriteSession`, a type that exposes no `Get`/`NewIter`.
2. **Reads are scoped by what the proposer declared.** A read of a key the proposer never declared surfaces as `*ErrCoverageMiss`, propagated to the order handler as a normal error and turned into a business-level rejection (failure audit entry + `BusinessError` on `ApplyResult`). The cache stays in lockstep with Pebble because the `gatedScope` decorator refuses to forward the read before any mutation lands.

These invariants are upheld by a layered stack between the order processor and the in-memory cache.

## Read path

```
 ┌──────────────────────────────────────────────────────────────────────┐
 │   Order processor (processing.RequestProcessor)                       │
 │   Technical-update handlers (applyMirrorSyncUpdate,                   │
 │     applyMetadataConversionBatch, applyMetadataConversionCompletion,  │
 │     applyIndexReady, applyConvertEntry…)                              │
 │                                                                       │
 │   All receive a `processing.Scope` (interface). The engine is hidden  │
 │   — handlers cannot reach into Derived / view / Registry directly.    │
 └─────────────────────────────────┬────────────────────────────────────┘
                                   │ scope.GetLedger / PutLedger / …
                                   ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │   processing.Scope (interface, internal/domain/processing/store.go)   │
 │   ~50 methods covering reads, writes, counters, per-order ops.        │
 │   Two impls compose in production:                                    │
 │     • state.gatedScope (decorator: coverage gate on every read)       │
 │     • state.WriteSet   (engine: overlay + Merge — no gate)            │
 └─────────────────────────────────┬────────────────────────────────────┘
                                   │ gatedScope.GetXxx
                                   ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │   state.gatedScope  (decorator, internal/infra/state/scope.go)        │
 │   • *WriteSet (embedded — implicit forward for everything)            │
 │   • coverage [256]map[U128]struct{}   ← immutable, built once         │
 │                                                                       │
 │   Override only the ~13 cache-attribute Get* methods:                 │
 │     coverage[kind] map lookup                                         │
 │       miss → return *ErrCoverageMiss                                  │
 │       hit  → forward to embedded *WriteSet.GetXxx                     │
 │                                                                       │
 │   Writes, counters, chapter ops, signing, etc. forward implicitly      │
 │   via the embedded *WriteSet — no gate logic.                         │
 └─────────────────────────────────┬────────────────────────────────────┘
                                   │ inner.GetXxx / inner.PutXxx
                                   ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │   state.WriteSet  (engine, internal/infra/state/write_set.go)         │
 │   • Derived  *DerivedRegistry   ← in-batch overlay (writes queued)    │
 │   • Date / NextSequenceID / …   ← FSM counters scoped to batch        │
 │                                                                       │
 │   Get*  : Derived.<Kind>.Get          (no gate — engine is raw)       │
 │   Put*  : Derived.<Kind>.Put          (queued for Merge)              │
 │   Merge : drains Derived → cache + Pebble (batched 0xF1+0xFF)         │
 └─────────────────────────────────┬────────────────────────────────────┘
                                   │ overlay miss → parent.Get
                                   ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │   DerivedKeyStore[K,T]  (overlay in-batch, 1 per kind)                │
 │   • values     map[K]T                                                │
 │   • deletions  map[K]struct{}                                         │
 │   • parent     *KeyStore[K,T]   ← bound directly to the engine store  │
 └─────────────────────────────────┬────────────────────────────────────┘
                                   │ overlay miss → KeyStore.Get
                                   ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │   KeyStore[K,T]                                                       │
 │   • wraps  kv.KV[U128, Entry[T]]                                      │
 │   • Entry = {Tag, Data, Deleted}                                      │
 └─────────────────────────────────┬────────────────────────────────────┘
                                   │ shard lookup
                                   ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │   AttributeCache[T]  (in-memory, 1 per kind, owned by StateRegistry)  │
 │   • Gen0  ShardedMap[U128, Entry[T]]                                  │
 │   • Gen1  ShardedMap[U128, Entry[T]]                                  │
 │   Get = Gen0 → fallback Gen1                                          │
 └──────────────────────────────────────────────────────────────────────┘
```

The coverage state lives on `gatedScope` as `coverage coverageSlots` — a dense `[len(cacheAttrKinds)][]attributes.U128` array, indexed by the precomputed `coverageSlotIndex[byte]` lookup (sub-attribute byte → slot or `-1`). `CheckCoverage(kind, canonical)` is one `int8` table read followed by a linear scan over the slot's slice — cache-friendly for the ≤ 10 ids per slot the typical proposal carries.

A single `gatedScope` is allocated per proposal: `gatedScope` doubles as its own factory (the `processing.ScopeFactory` interface is implemented on it), and each `NewScope(coverage_bits)` truncates the slots in place and re-applies the selected plans. The FSM apply path is strictly sequential — `applyTechnicalUpdates` → `ProcessOrders` → `ValidateTransientVolumes` never overlap — so reusing one instance is safe and avoids the `*gatedScope` + 11 backing-array allocations per order the original design paid.

Empty `coverage_bits` on `NewScope` admit no plan (any `CheckCoverage` misses). Proposal-wide coverage — needed by `ValidateTransientVolumes` to reach into every ledger the proposal touched — uses the separate `NewProposalScope()` method, so an order whose proto `CoverageBits` is unset cannot silently inherit coverage from another order in the same batch.

## TechnicalUpdate envelope (per-update coverage on tech-side writes)

`Proposal.technical_updates` is a `repeated TechnicalUpdate`. Each `TechnicalUpdate` wraps one of the seven non-order payloads (mirror sync, events-sink cursor, idempotency eviction, cluster config, metadata batch, metadata completion, index ready) inside a `oneof kind` and carries its own `coverage_bits` — symmetric to `Order.coverage_bits`. The FSM apply loop builds one `Scope` per `TechnicalUpdate`, narrowed by that update's bits, before dispatching to the handler:

```go
for _, tu := range proposal.GetTechnicalUpdates() {
    scope, err := scopeFactory.NewScope(tu.GetCoverageBits())
    if err != nil { /* business rejection */ }
    switch kind := tu.GetKind().(type) {
    case *raftcmdpb.TechnicalUpdate_MirrorSync:    ...
    case *raftcmdpb.TechnicalUpdate_MetadataBatch: ...
    ...
    }
}
```

Two consequences fall out of the envelope:

1. **Per-update isolation, like orders.** A `MirrorSyncUpdate` on ledger `A` whose bits flag only `A` cannot read `Ledgers[B]` even if `B` is declared elsewhere in the same proposal's `ExecutionPlan`. The proposal-wide loophole the old asymmetry left open is structurally gone — proposers must declare what each update reads.

2. **Uniform shape for non-reading payloads.** `EventsSinkUpdate`, `IdempotencyEviction`, `ClusterConfig` ship with empty `coverage_bits` because their handlers consult no cache state; the scope they get back has every slot empty, so any cache read would miss — and these handlers consult no cache state, so the gate is never invoked. No special-case branch in the loop.

Coverage bits for technical updates are computed by the runner's per-`WriteOperation` `bitsForNeeds` (same lifecycle hook as for orders) — the `ExecutionPlan` is finalized only after `Build`, and may be swapped on rebuild, so bit positions are re-resolved at marshal time.

### Layer-by-layer

- **`processing.Scope`** (`internal/domain/processing/store.go`). The handler-facing interface. Defined in the domain package so handlers (both order processors in `processing/` and technical-update handlers in `state/`) depend only on a domain contract — never on the concrete engine.
- **`state.gatedScope`** (`internal/infra/state/scope.go`). The decorator that adds the coverage gate. One instance per proposal, reused across `NewScope` calls (the FSM apply path is strictly sequential). It embeds `*WriteSet` so every method except the ~13 gated `GetXxx`/`GetXxxEntry` reads forwards implicitly to the inner engine. The gated overrides open with `coverageSlotIndex[kind]` + a linear scan over the slot's `[]U128`; on a miss they log, increment the OTel counter and return `*ErrCoverageMiss`. The decorator owns *only* the gate concern — no overlay logic, no Merge.
- **`processing.ScopeFactory`** is implemented directly by `gatedScope`: each `NewScope(bits)` truncates the slots and re-applies the selected plans, returning the same `*gatedScope` instance. `NewProposalScope()` is the separate entry point used by `ValidateTransientVolumes` to admit every declared plan. Both return `*domain.ErrInvalidExecutionPlan` when the bits flag positions past the plans slice, when a plan declares an unknown attr_code, or when an AttributeID is missing / not 16 bytes / has no intent — surfaced as a business rejection in `applyProposal` (`planInvariantDescribable`), no cache mutation lands.
- **`state.WriteSet`** (`internal/infra/state/write_set.go`). The engine — the inner that `gatedScope` wraps. Raw overlay/merge: read methods read directly from `Derived` with no inline coverage check; write methods queue in `Derived` for the eventual `Merge()`.
- **`DerivedKeyStore[K,T]`** (`internal/infra/attributes/key_store.go`). The in-batch overlay used by every handler. Reads check `deletions` first (`ErrNotFound` for an in-batch delete), then `values` (in-batch Put), then fall through to the parent `KeyStore`. This is what lets handler *N* read a value that handler *N-1* wrote within the same proposal. Coverage is gated upstream by `gatedScope`, so the overlay never serves a value the reader didn't declare.
- **`KeyStore[K,T]`** (`internal/infra/attributes/key_store.go`). Wraps a `kv.KV[U128, Entry[T]]`. The `Entry.Tag` field carries the xxh3 collision tag; `Entry.Deleted` carries the tombstone bit. For the FSM hot path, the underlying KV *is* the AttributeCache's gen0+gen1, so a KeyStore read is a cache read.
- **`AttributeCache[T]`** (`internal/infra/cache/cache.go`). The terminal layer. Two `ShardedMap[U128, Entry[T]]` instances, Gen0 and Gen1. `Get` checks Gen0 first, falls back to Gen1. Rotation is driven by Raft index thresholds — see [deterministic-fsm.md](deterministic-fsm.md).

A read at the handler level therefore travels: `Scope` (gate) → overlay → underlying KeyStore → cache, with **no Pebble Get** anywhere on the path.

## Write paths

The cache and Pebble are mutated by two paths during apply, both honoring the same alignment guarantee — `AttributeCache.Gen0` is updated in-memory; Pebble 0xF1 and 0xFF rows are batched and persisted at `batch.Commit()`:

```
 Preload phase                Commit phase
 (before processing)          (after all handlers — tech + orders)
 ┌──────────────────────┐    ┌──────────────────────────────────────────┐
 │ CacheSnapshotter     │    │ WriteSet.Merge                           │
 │ • MirrorTouch        │    │ • derived.Merge() → writer.Put →         │
 │   Gen1→Gen0          │    │   KeyStore.Put → Gen0 (immediate)        │
 │ • MirrorPreload      │    │ • mergeSimpleWithCache → batch 0xF1+0xFF │
 │   raw→Gen0+Gen1      │    │ • SaveLedger / chapter writes / …         │
 │                      │    │                                          │
 │                      │    │ One Merge drains BOTH tech-update writes │
 │                      │    │ AND order writes — single atomic batch.  │
 └──────────┬───────────┘    └──────────┬───────────────────────────────┘
            │                           │
            ▼                           ▼
        AttributeCache (Gen0 / Gen1)  ◄────── shared ──────► Pebble (write-only
                                                              via WriteSession)
                       0xFF mirror is the restart-restore source for the cache
                       — see CacheSnapshotter.RestoreFromStore.
```

Pebble is reachable only through `*dal.WriteSession`, a write-only handle. The hot path holds nothing else. Pebble reads happen exclusively on the recovery paths (`state.Recovery`, `state.Synchronizer`) — see [deterministic-fsm.md](deterministic-fsm.md).

## Why this layering matters

Each layer enforces exactly one invariant that the layer above doesn't have to think about:

| Layer | Invariant it enforces |
|-------|----------------------|
| `processing.Scope` (interface) | Single handler-facing API. Hides the engine — a handler can call only what the interface exposes, never reach into `Derived`/`view` directly. |
| `state.gatedScope` (decorator) | Coverage enforcement. `CheckCoverage(kind, canonical)` runs once at the top of every cache-attribute read. The engine below stays ignorant of the gate. |
| `state.WriteSet` (engine) | One proposal = one read/write context. Overlay/Merge mechanics, counters, chapter ops. No gate logic — pure engine. |
| `DerivedKeyStore` | Same-batch read-your-own-writes. Handler *N* sees what handler *N-1* wrote without going through the view. |
| `Plan` | Holds the coverage state and emits `*ErrCoverageMiss` on undeclared reads. One coverage map per kind, indexed by sub-attribute byte. |
| `KeyStore` | Collision safety + tombstone visibility. `Entry.Tag` distinguishes U128 collisions; `Entry.Deleted` propagates deletes. |
| `AttributeCache` | Generation isolation. Gen0 = current epoch, Gen1 = previous epoch retained for in-flight proposals. |

If you find yourself wanting to bypass a layer (e.g. read from Pebble inside a handler, or skip Scope "just for this one read"), the answer is always to declare the key in the ExecutionPlan upstream and let the existing layering carry the read. The layers are not optional — every one of them is load-bearing for at least one safety property.

## Related

- [deterministic-fsm.md](deterministic-fsm.md) — generation rotation, Preload building (admission side), AttributeLoader concurrency
- [admission-cache-horizon.md](admission-cache-horizon.md) — admission-side rejection when ≥ 2 generations are predicted between propose and apply
- [attributes.md](../storage/attributes.md) — Pebble layout (0xF1 attribute zone, 0xFF cache mirror)
- [attribute-key-hashing.md](../storage/attribute-key-hashing.md) — U128 derivation and collision tag scheme
