# Numscript Bulk Admission Performance

The bulk path performs Numscript dependency discovery during admission before
the authoritative execution in the FSM. Request-local memoization reduces work
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

## Impact by execution path

“No discovery reuse” below means the original per-order admission path remains
active, not that the order is skipped or trusted without validation.

| Execution path | Numscript discovery | Expected impact |
|----------------|---------------------|-----------------|
| Bulk with repeated, variable-free script; same ledger, text, and `force`; empty volume/metadata read sets | First occurrence runs fully, later matches reuse it | Largest gain |
| Bulk with unique scripts | Full path for every order | No discovery saving; variable-free static scripts pay a small memo lookup/insertion cost |
| Bulk with external variables | Full path for every order | No direct effect; variable bindings never share discovery |
| Bulk with non-empty `ReadVolumes` / `ReadMetadata` (for example `balance()`, `meta()`, or a bounded balance-checked source) | Full path for every order against the evolving overlay | No direct effect; state-dependent scripts are never reused |
| Different ledger, script text, or `force` value | Separate first discovery for each key | Later exact matches can still reuse |
| Unitary request | Full path (there is no later match) | No direct effect |
| FSM, Raft, Pebble writes, audit, storage, or response encoding | Unchanged | No effect |

Changing the ledger, script text, or `force` mode creates a different memo key.
Even on the accelerated path, preload coverage and predicted effects are still
added for every order, and the FSM still executes every accepted order.

## Measurements

The admission microbenchmarks isolate two 50-order preparation shapes on
Darwin/arm64 (Apple M5 Pro) with Go 1.26.5: repeated static text exercises the
memo-hit path, while semantically equivalent scripts with distinct trailing
newlines exercise the no-hit path. Each iteration resets `Order.Technical`
outside the timed region so the measured Numscript phase receives fresh order
state, as it does in production.

```shell
go test ./internal/application/admission \
  -run '^$' \
  -bench '^BenchmarkResolveScriptsWorldToBank(Bulk50|UniqueBulk50)$' \
  -benchmem -benchtime=1s -count=12
```

| Workload (median of 12) | Metric | Base | Optimized | Change |
|-------------------------|--------|-----:|----------:|-------:|
| Repeated static script | Time per bulk | 211,291 ns | 65,235 ns | -69.1% |
| Repeated static script | Bytes per bulk | 425,130 B | 134,596 B | -68.3% |
| Repeated static script | Allocations per bulk | 5,222 | 1,059 | -79.7% |
| Unique static scripts | Time per bulk | 219,839 ns | 228,050 ns | +3.7% |
| Unique static scripts | Bytes per bulk | 425,223 B | 431,685 B | +1.5% |
| Unique static scripts | Allocations per bulk | 5,223 | 5,232 | +0.2% |

The gain is workload-specific. Bulks containing unique, variable-bearing, or
state-reading scripts deliberately receive no discovery reuse. The unique
static benchmark quantifies the bounded request-local memo bookkeeping paid
when static scripts do not repeat.
