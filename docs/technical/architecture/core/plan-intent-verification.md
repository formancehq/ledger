# Plan intent verification

**Status**: in force since EN-1242.
**Owning code**: `internal/infra/plan/`, `internal/infra/state/machine.go` (Preload path), `internal/infra/state/cache_snapshotter.go`, `internal/infra/cache/cache.go` (`AttributeCache.Get` / `AttributeCache.Del`).
**Related invariants**: [#1 (cache is authority)](../../../CLAUDE.md), [#3 (no Pebble reads on hot path)](../../../CLAUDE.md), [#6 (every FSM read has a preload)](../../../CLAUDE.md), [#7 (never silently skip)](../../../CLAUDE.md), [#9 (never bypass the coverage gate)](../../../CLAUDE.md).

## TL;DR

`AttributeCoverage.value` is optional:

- **`value` set** — seeding action. Admission's Pebble scan found a value; the FSM's `MirrorPreload` writes it into Gen0+Gen1 with gen1-wins semantics.
- **`value` nil** — coverage-only entry. Admission declares "this key is in scope for the emitting order" so the per-order `coverage_bits` (invariant #9) admit reads and deletes against it. **Preload skips coverage-only entries**: no promote pass runs at all. Reads rely on `AttributeCache.Get`'s gen0→gen1 fallback; deletes rely on `AttributeCache.Del`'s lazy Gen0-tombstone fabrication from Gen1's tag.

Correctness is anchored by two out-of-band guardrails: the **admission-side `CacheUnreachable` verdict** bounds the propose→apply window to at most one cache rotation, and the **coverage gate (invariant #9)** ensures the FSM apply path only reads keys admission actually declared.

## The problem

On the pre-fix code (release/v3.0), a `DeleteMetadata` on a key that had migrated to Gen1 via rotation left the cache and disk out of sync — a violation of invariant #1 (cache is the source of authority; every node must see the same cache state for a given applied index):

```
T0  Admission builds plan for Delete(X.k)
      CheckCache(nextIndex, id) → CacheMiss (Gen0∅, Gen1∅, Pebble∅)
      → resolver emits a coverage-only entry (no cache seed)

T1  Concurrent Save(X.k, v) applies at raftIndex M
      → Gen0[id] = v (via KeyStore.Put)

T2  Rotation fires (raftIndex crosses cache-rotation-threshold)
      → old Gen0 becomes new Gen1
      → new Gen0 = ∅
      → net state: Gen1[id] = v (live), Gen0[id] = ∅

T3  Delete(X.k) applies at raftIndex N > M
      KeyStore.Delete → s.M.Get(id) hits Gen1 via fallback → passes tag check
                     → s.M.Put(id, entry{Deleted=true}) writes to Gen0 only
      Mem net:  Gen0[id] = tombstone (fabricated via Put),
                Gen1[id] = v         (live, untouched)
      Disk net: writeCacheTombstone stamps a tombstone at BOTH gen bytes.
      → Mem Gen1 = live, Disk Gen1 byte = tombstone. Cache-mirror invariant
        broken until the next rotation or restart re-hydrates Gen1.
```

Between T3 and the next rotation the follower and leader can diverge on any read of `X.k` if one has been through a restore-from-store (sees the disk tombstone) and the other has not (still holds the live Gen1 row) — a live-vs-tombstone split on a hash-chained order.

## The fix has two parts

**1. Admission bounds the race window.** `CheckCache` returns `CacheUnreachable` when 2+ generation rotations are predicted between propose-time and apply-time. Admission rejects the proposal with `plan.ErrCacheHorizonExceeded` (gRPC `Unavailable`, HTTP 503 + `Retry-After: 1`) so the client re-admits against a fresh snapshot. This is what makes the propose→apply window a bounded race — at most one rotation, never two.

**2. `AttributeCache.Get` and `AttributeCache.Del` handle the bounded race in place.** With the race bounded to a single rotation, everything that could exist for a declared key is still somewhere in Gen0 or Gen1. Two primitives absorb the concern without a Preload-time promote pass:

- `AttributeCache.Get` falls back Gen0 → Gen1 when Gen0 misses. Reads on a rotated entry surface it directly.
- `AttributeCache.Del` first tries to tombstone in place in Gen0; on Gen0 miss it *fabricates* a Gen0 tombstone borrowing Gen1's tag. `writeCacheTombstone` writes a single row to the Gen0 byte — the in-memory tombstone and the on-disk tombstone stay byte-equivalent for the same applied index (invariant #1).

Applied to the race above, T3 now becomes:

```
T3  Delete(X.k) applies at raftIndex N > M
      FSM Preload: coverage-only entry → skipped (no cache mutation).
      processDeleteMetadata:
        s.AccountMetadata().Get(metaKey) → v (Gen0 miss, Gen1 fallback)
        s.AccountMetadata().Delete(metaKey)
          → KeyStore.Delete → AttributeCache.Del(id)
          → Gen0 miss, Gen1 hit → fabricate Gen0 tombstone (borrow Gen1's tag)
          → writeCacheTombstone(gen0Byte, ...) mirrors to Pebble
```

Gen1's live row is intentionally left untouched: the Gen0 tombstone shadows it on every read (Get returns the tombstone via the gen0→gen1 fallback — Gen0 hits first, and a tombstone surfaces as `ErrNotFound`), and rotation purges the stale Gen1 row on the next generation flip.

## The two guardrails and why they matter

### `CacheUnreachable` (admission-side)

The fix relies on the concurrent Save's value still being *somewhere* in cache when the delete runs. If two rotations fire between propose and apply, the value is dropped entirely (moved to Gen1 by rotation 1, discarded by rotation 2), and Del would find nothing to fabricate a tombstone from. `CacheUnreachable` prevents this: it rejects the proposal with `plan.ErrCacheHorizonExceeded` when 2+ rotations are predicted, forcing a retry against a fresh admission snapshot. Under a correctly tuned rotation threshold and a healthy apply rate, this should not fire — recurring occurrences indicate either a too-low threshold or FSM apply falling behind admission.

See `internal/infra/cache/cache.go` `CheckCache` / `internal/infra/plan/planerr/errors.go` `ErrCacheHorizonExceeded`.

### Coverage gate (invariant #9)

Every cache-attribute read on the FSM hot path goes through `Scope.GetX(...)` so the per-order `coverage_bits` admit it. The gate enforces that admission's declared preload set is the FSM's only legitimate read horizon. This is what keeps the coverage-only + lazy-Get/Del model honest: an order that reads a key admission didn't declare is rejected at the gate, not silently surfaced by the Gen1 fallback. Combined with the boundedness of the race window, coverage guarantees that "declared but not seeded" is a safe state — either the cache had the value all along and `Get`/`Del` surface it, or the key is genuinely absent and the handler gets a clean `ErrNotFound`.

## Read semantics — gen0→gen1 fallback

`AttributeCache.Get` returns Gen0 if present, otherwise falls back to Gen1. The fallback is safe under the coverage gate (invariant #9): the FSM apply path only reaches Get through `Scope.GetX`, and the gate rejects reads on keys the admission-declared coverage_bits didn't authorize. The fallback can only surface keys the proposer explicitly declared.

Callers that need to distinguish Gen0 from Gen1 explicitly (`MirrorPreload`'s gen1-wins seed decision, snapshot persistence, cache-restore diagnostics) use the `Gen0()` / `Gen1()` accessors.

## `value` is pure seeding

`value` is the only case where an AttributeCoverage carries a payload. Admission's Pebble scan resolved a fresh value, so the FSM seeds Gen0+Gen1 via `MirrorPreload`. Gen1-wins semantics apply: if a concurrent Save committed and populated Gen1 with a fresher value between admission's scan and apply, `MirrorPreload` skips seeding Gen0 (gen1 wins), and the FSM apply path reads the fresher value through the gen0→gen1 fallback.

Verifying `value` at Preload would require either a Pebble re-read (violates invariant #3) or a hash comparison against the seed (adds cost without a known bug to fix). The gen1-wins path is enough.

## Delete-like handlers

Delete cascades route through `KeyStore.Delete → AttributeCache.Del`. Admission declares coverage with `p.Add(dal.SubAttrX, key.Bytes())`; `AttributeCache.Del` handles the lazy Gen0-tombstone fabrication from Gen1's tag when needed. The relevant sites are enumerated as a checklist so new deletes don't drift:

| Order / cascade | Cache | Admission emission site |
|---|---|---|
| `DeleteMetadata` (Account target) | AccountMetadata | `admission.go` `LedgerApplyOrder_DeleteMetadata` |
| `DeleteLedgerMetadata` | LedgerMetadata | `admission.go` `LedgerScopedOrder_DeleteLedgerMetadata` |
| `MirrorIngest.DeletedMetadata` (Account) | AccountMetadata | `admission.go` + `mirror/worker.go` |
| `DeletePreparedQuery` | PreparedQueries | `admission.go` `LedgerScopedOrder_DeletePreparedQuery` |
| `DropIndex` | Indexes | `admission.go` `LedgerApplyOrder_DropIndex` |
| `RemoveMetadataFieldType` (index cascade) | Indexes | `admission.go` `LedgerApplyOrder_RemoveMetadataFieldType` |
| `DeleteLedger` (Boundaries cascade) | Boundaries | `admission.go` `LedgerScopedOrder_DeleteLedger` |
| `RemoveEventsSink` (SinkConfigs cascade) | SinkConfigs | `admission.go` `SystemScopedOrder_RemoveEventsSink` |

Any new Del site MUST declare coverage for the deleted key. Coverage alone is sufficient — Del's lazy fabrication and Get's fallback handle the race safety.

## Cross-references

- Proto definitions: `misc/proto/raft_cmd.proto` — `AttributeCoverage` (optional `value` field)
- Emission: `internal/infra/plan/resolve.go` — `resolveCoverage` maps `CheckCache` verdicts to seed / coverage-only entries
- Verification: `internal/infra/state/machine.go` `Preload` — one dispatch per plan entry (skip when `value` is nil)
- Cache primitives: `internal/infra/cache/cache.go` — `AttributeCache.Get` (gen0→gen1 fallback), `AttributeCache.Del` (in-place tombstone + lazy Gen0 fabrication), `CheckCache` (returns `CacheUnreachable` for 2+ rotation prediction)
- Admission guard: `internal/infra/plan/planerr/errors.go` `ErrCacheHorizonExceeded` — the admission rejection sentinel
- Regression harness: `tests/antithesis/run_model_test.sh` (singleton_driver_model exercises the delete-after-rotation flow through `CheckCache` + `AttributeCache.Del` under fault injection)
- Adapter mappings: `internal/adapter/grpc/server.go` (`codes.Unavailable`), `internal/adapter/http/error_handler.go` (503 + `Retry-After: 1`)
