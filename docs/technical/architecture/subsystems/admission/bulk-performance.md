# Bulk Performance

The HTTP bulk path combines three expensive operations: Numscript dependency
discovery during admission, authoritative execution in the FSM, and JSON
encoding of one result per order. Two request-local optimizations reduce work
that was redundant for repeated static programs without changing the accepted
orders, the audit payload, or the FSM execution.

## Numscript discovery memoization

`Admission.resolveScriptsAndEnrichNeeds` must discover every scripted order's
read/write set before proposing it. Discovery also executes the script on a
best-effort basis to fold its predicted balance and metadata effects into the
intra-batch overlay. This is required so a later state-dependent script sees the
same predecessor effects that the sequential FSM will see.

The `world-to-bank` workload sends 50 copies of the same variable-free program
in one atomic bulk. Its unbounded `@world` source and its destination are writes,
not reads, so resolving and executing the program 50 times produced identical
dependencies and effects.

Admission now follows this sequence:

1. Resolve the first occurrence normally against the current intra-batch view.
2. Consider it reusable only when the order has no variables and the resulting
   `ReadVolumes` and `ReadMetadata` sets are both empty.
3. Store the result in a request-local map keyed by ledger, script text, and
   `force` mode.
4. For a later matching order, reuse the immutable dependency/effect result,
   but still add every key to that order's coverage and merge its effects into
   the evolving batch overlay.

The memo is intentionally scoped to one request. It needs no synchronization,
does not retain caller scripts globally, and cannot cross a ledger or force-mode
boundary. Variable-bearing scripts keep the full path because their resolved
accounts, assets, amounts, or metadata can differ by variable values.

### Why an empty input hash is not enough

`InputsHash == nil` means no value influenced *which dependencies were
resolved*. It does not mean execution read no state. A literal bounded source,
for example, is a balance read even though its current value does not change the
dependency set. Reusing the first order's successful effects after a preceding
order depleted that source would create phantom intra-batch effects.

The reuse gate therefore checks the resolved read sets themselves. Numscript's
`ResolveDependenciesCoversRuntime` property requires every balance or metadata
query made by a successful execution to appear in those sets. Empty read sets
are the proof that the result cannot observe the evolving overlay. The maps and
`big.Int` values in the memo are not mutated: coverage only iterates the maps,
and `batchEffects.mergeDiscovery` copies a delta before accumulating it.

## Streaming transaction JSON views

Successful bulk responses contain one transaction per order. Passing each proto
transaction to the outer Sonic encoder invoked `Transaction.MarshalJSON`, which
started another encoder and allocated intermediate byte slices. Nested postings,
post-commit snapshots, and volume entries repeated that pattern.

`Transaction.JSONView` now builds the same camel-case REST representation from
plain Go values. The bulk response gives those views directly to its existing
streaming encoder, so a single encoder writes the complete response. The view
preserves the established distinctions and shapes:

- nil post-commit snapshots are omitted, while present empty snapshots emit
  `{}`;
- post-commit volumes stay flattened as
  `account -> [{asset, color, input, output}]`;
- uncolored postings and volume entries still emit `"color":""`;
- monetary amounts remain unquoted decimal JSON numbers;
- timestamps and optional revert fields keep their existing representation.

This changes no HTTP schema, protobuf message, audit serialization, storage
format, or FSM behavior.

## Impact by execution path

The two optimizations have deliberately different scopes. The table below is
also the checklist to use when evaluating a workload: “no discovery reuse”
means the original per-order admission path remains active, not that the order
is skipped or trusted without validation.

| Execution path | Numscript discovery | HTTP JSON encoding | Expected impact |
|----------------|---------------------|--------------------|-----------------|
| Bulk with repeated, variable-free script; same ledger, text, and `force`; empty volume/metadata read sets | First occurrence runs fully, later matches reuse it | Direct view for every successful HTTP result | Largest gain; both optimizations apply on HTTP |
| Same eligible bulk over gRPC | Reused | Unchanged | Admission gain only |
| HTTP bulk with unique scripts | Full path for every order | Direct view | JSON gain only |
| HTTP bulk with external variables | Full path for every order | Direct view | JSON gain only; variable bindings never share discovery |
| HTTP bulk with non-empty `ReadVolumes` / `ReadMetadata` (for example `balance()`, `meta()`, or a bounded balance-checked source) | Full path for every order against the evolving overlay | Direct view | JSON gain only; no discovery reuse |
| Same state-reading or variable-bearing bulk over gRPC | Full path for every order | Unchanged | No direct effect from this change |
| Different ledger, script text, or `force` value | Separate first discovery for each key | Direct view on HTTP bulk | No reuse across keys; later exact matches can still reuse |
| Unitary HTTP request | Full path (there is no later match) | `Transaction.MarshalJSON` uses the same plain view, but without the bulk direct-handoff saving | No discovery gain; smaller, unmeasured encoding benefit is possible |
| Unitary gRPC request | Full path (there is no later match) | Unchanged | No direct effect from this change |
| FSM, Raft, Pebble writes, audit, or storage | Unchanged | Not applicable | No effect |

Changing the ledger, script text, or `force` mode creates a different memo key.
Even on the accelerated path, preload coverage and predicted effects are still
added for every order, and the FSM still executes every accepted order.

## Measurements

Measurements were taken on 2026-08-06 on Linux/arm64 with Go 1.26.5. The A/B
comparison used the same VM and configuration for the base commit `ab36444a1`
and the optimized working tree:

- single-node Raft;
- `GOMAXPROCS=2`;
- 100 k6 virtual users for 10 seconds;
- atomic bulks of 50 transactions;
- `tests/perf/scripts/world_to_bank.js` with `USE_NUMSCRIPT=true`;
- identical Pebble, Raft, and cache settings;
- one run on a fresh store, then one continued-store run for each binary.

| Trial | Base tx/s | Optimized tx/s | Throughput gain | Base p95 | Optimized p95 | Errors |
|-------|----------:|---------------:|----------------:|---------:|--------------:|-------:|
| Fresh store | 13,570 | 16,486 | +21.5% | 458 ms | 382 ms | 0% / 0% |
| Continued store | 13,152 | 16,320 | +24.1% | 483 ms | 388 ms | 0% / 0% |
| Mean | 13,361 | 16,403 | **+22.8%** | 470 ms | 385 ms | 0% / 0% |

The admission microbenchmark isolates the same 50-order preparation shape:

```shell
go test ./internal/application/admission \
  -run '^$' \
  -bench '^BenchmarkResolveScriptsWorldToBankBulk50$' \
  -benchmem -benchtime=1s -count=12
```

| Metric (median of 12) | Base | Optimized | Change |
|-----------------------|-----:|----------:|-------:|
| Time per bulk | 1,106,562 ns | 288,750 ns | -73.9% |
| Bytes per bulk | 420,265 B | 129,769 B | -69.1% |
| Allocations per bulk | 5,172 | 1,009 | -80.5% |

The throughput gain is workload-specific. Bulks containing unique,
variable-bearing, or state-reading scripts deliberately receive no discovery
reuse. The direct JSON-view handoff applies to successful HTTP bulk responses;
other HTTP transaction responses still call `Transaction.MarshalJSON`, now
backed by the same plain view, while gRPC encoding is unchanged.
