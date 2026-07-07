# 0001 — vtprotobuf `unmarshal_unsafe` on the FSM apply path

**Status:** Rejected (2026-07-07). Revisit if vtprotobuf gains a
`SanitizeVT`-style primitive, if the FSM apply-path stops caching proto
values by reference, or if a workload emerges where string allocs
dominate more clearly.

## Context

Pyroscope on the `perf-world-to-bank` workload flagged
`state.PrepareDecodedEntries` as accounting for **25% of all
`alloc_objects`** during steady-state apply (1.70 Bil of 6.73 Bil).
That path decodes a `raftcmdpb.Proposal` per committed raft entry via
`UnmarshalVT`. Every `string` and `bytes` field in the wire message
lands on the Go heap as a fresh allocation — dozens of small allocs per
transaction just to materialize `posting.Source`, `Destination`,
`Asset`, metadata keys and values, references, ledger names.

vtprotobuf ships an opt-in feature
[`unmarshal_unsafe`](https://github.com/planetscale/vtprotobuf) that
generates an `UnmarshalVTUnsafe([]byte) error` method next to
`UnmarshalVT`. Instead of `string(byteSlice)` (copy) it emits an
`unsafe.Pointer`-backed conversion that shares the input buffer with
the message. **Precondition:** the input buffer must remain
untouched for the lifetime of the message and everything derived from
it.

## Isolated benchmark

Enabled `features=marshal+unmarshal+unmarshal_unsafe+size+clone+equal+pool`
and measured a representative `Proposal` payload (2 postings, one
reference, 2 metadata k/v, one ledger name):

```
BenchmarkProposal_UnmarshalVT       ~460 ns/op   1264 B/op   31 allocs/op
BenchmarkProposal_UnmarshalVTUnsafe ~360 ns/op   1136 B/op   19 allocs/op
```

Delta: **-12 allocs**, **~-20% wall time**. The 12 saved allocs match
the 12 string fields in the payload exactly — one heap alloc collapsed
per string.

Scaled naively to the flamegraph:

    1.70 Bil × (12/31) ≈ 660 Mil string allocs on the apply path
    → up to 10% of all program allocs saved if 100% of strings stay zero-copy.

That "if" is the whole ADR.

## Escape audit

`UnmarshalVTUnsafe` is safe *while the message is transient*.
It becomes a use-after-free the moment a string escapes into state that
outlives the raft entry buffer — chiefly the FSM cache
(`internal/infra/attributes.KeyStore.Put`).

Tracing `processCreateTransaction` (the perf-world-to-bank hot path):

| Field | Escape path | Cached? |
|---|---|---|
| `posting.Source/Destination/Asset` | `txState.Postings` → `TransactionStates().Put` | **yes** |
| `order.Metadata` keys + string values | `txState.Metadata` → `TransactionStates().Put` | **yes** |
| `order.AccountMetadata` values | `AccountMetadata().Put` | **yes** |
| `order.Reference` | `TransactionReferenceKey.Bytes()` → hashed; also written to log via `MarshalVT` before returning | no |
| `LedgerScopedOrder.Ledger` | `LedgerKey.Bytes()` → hashed; error messages; log payload | no |
| Log payload strings in general | `MarshalVT` synchronous inside apply cycle, before raft buffer release | no |

Roughly 10 of the 12 strings in a typical proposal reach the cache;
2 stay transient. `KeyStore.Put(canonical []byte, value T)` copies
the *canonical bytes* into a hash — so keys are safe. Only **values**
that carry string fields (`*TransactionState`, `*MetadataValue`)
retain unsafe pointers.

## Options considered

### Option A — `CloneVT()` at every cache `Put`

Blanket deep clone of the value passed in.

For our reference `TransactionState` (2 postings + 2 metadata):

| Alloc | Count |
|---|---|
| TransactionState struct | 1 |
| Postings slice | 1 |
| Posting structs | 2 |
| Posting strings | 6 |
| Uint256 (Amount) | 2 |
| Metadata map | 1 |
| MetadataValue structs | 2 |
| MetadataValue inner strings | 2 |
| **Total** | **17** |

vs. baseline `UnmarshalVT` + direct pointer share at `Put` (0 alloc at
Put, 12 string allocs at unmarshal): **regression of +5 allocs per
proposal**. `CloneVT` re-allocates the sub-message structs that
`UnmarshalVT` already allocates — the win from unsafe unmarshal is
erased *and* we pay extra for the fresh sub-message structs at Put.

Rejected.

### Option B — Selective string clone at cache boundaries

Instead of deep-cloning the whole message, `strings.Clone` **only** the
string fields that would dangle — leaving the sub-message struct
sharing intact. A helper per proto type (`sanitizePostings`,
`sanitizeMetadataValue`, ...) called from each cache-Put boundary.

Per proposal in perf-world-to-bank:

| Path | Clone count |
|---|---|
| Postings strings (6) | 6 |
| Metadata keys (2) + string values (2) + new map for cloned keys (1) | 5 |
| **Total at Put** | **11** |

Bilan: `0 unmarshal + 11 Put = 11` vs. baseline `12 + 0 = 12`.
**Net gain: -1 alloc per proposal.**

Scaled to the flamegraph: ~55 Mil allocs saved out of 6.73 Bil, or
**~0.8% of all program allocs**.

Rejected on cost/benefit grounds: modest gain (<1% of total allocs),
open-ended audit surface (every new processor must remember to
sanitize), and easy-to-miss correctness bug (a forgotten `sanitize` at
a new cache-Put site is a silent dangling-string use-after-free).

### Option C — Rebuild the cache values from scratch instead of sharing

The FSM would allocate fresh, heap-safe values before Put rather than
sharing pointers with the incoming Proposal. Functionally equivalent
to Option B — the fresh allocs shift from `UnmarshalVT` to the
processor. Also rejected.

## Why the win is much smaller than the isolated bench suggests

The isolated bench measures the alloc delta of `UnmarshalVT` vs
`UnmarshalVTUnsafe` on a single-shot decode without any consumer. On
the real hot path most of those strings must survive the apply
cycle, which means they eventually need a heap-allocated backing
store. Whether we allocate them at `UnmarshalVT` time or at cache
`Put` time, the total allocation count is the same.

The only true win comes from strings that are read-then-discarded
inside the apply cycle: passed through `.Bytes()`-then-hashed, used
in `MarshalVT` synchronously, or fed into an error message and
dropped. In the observed workload these are a minority (~2 of ~12
strings per transaction).

## Decision

Do **not** enable `unmarshal_unsafe` in the codegen and do **not**
switch any call site to `UnmarshalVTUnsafe`.

Keep this ADR as the record so the question doesn't get re-opened
without new evidence. If the situation changes:

- vtprotobuf grows a `SanitizeVT`-style primitive that clones just the
  string/bytes fields cheaply, tilting the alloc math;
- the FSM apply path changes to stop caching proto pointers (e.g.,
  serialising to bytes before caching), turning strings into transient
  hot-path data;
- a workload emerges where strings clearly dominate (much higher
  string-to-struct ratio than perf-world-to-bank).

Revisit and supersede this ADR.

## Where to look for the real allocation wins on the apply path

`PrepareDecodedEntries` is 25% of all allocs, but strings are only
part of it. The rest is sub-message struct allocation
(`Order`, `LedgerScopedOrder`, `LedgerApplyOrder`,
`CreateTransactionOrder`, `Posting`, `MetadataValue`, ...) plus the
`Proposal` itself. Levers worth exploring in a future PR:

- Wider vtprotobuf pool coverage — currently pooled: `Proposal`,
  `ExecutionPlan`, `AttributeValue`, `Order`, `TechnicalUpdate`,
  `EventsSinkUpdate`, `MirrorSyncUpdate`, `AuditEntry`,
  `AppliedProposal`. `Posting`, `MetadataValue`, and the various
  `*Order` payloads are not pooled and are allocated fresh per
  proposal.
- Cache-hit paths: when `TransactionStates().Put` is a repeated key,
  the previously-stored `*TransactionState` becomes garbage. Reusing
  it via a pool tied to the KeyStore would recover both the struct
  and its sub-structures.
- Schema compaction — some proposal shapes carry deeply nested
  wrappers (Order → LedgerScoped → LedgerApply → CreateTransaction)
  each of which is a fresh alloc. Flattening the schema for the
  hot-path variants would remove several allocs per proposal at the
  cost of a wire-format break.

None of these are covered by `unmarshal_unsafe`.
