# Structured workload generation

Every worker iteration opens one `generation.RunOnce` episode through
`runGeneration`. The episode includes the selected read or generated bulk and
its observation. A `*generation.Source` passes explicitly through the action,
read, query, filter, checkpoint and lifecycle generators. It is never stored in
the checker or shared with another goroutine. The processor and candidate-state
oracle keep their existing contracts.

Initial schemas use separate bounded episodes. Maintenance recovery, restore
jitter and index-readiness jitter each open an independent episode in their own
goroutine. They retain their existing schedules. The predicted-checkpoint probe
receives its already-selected node and needs no source.

`RunOnce` uses the URandom backend, generate phase and no database. Its callback
guard prevents Hegel failure replay from repeating RPC effects. Engine errors
emit the SDK Unreachable assertion and panic. Expected server rejections remain
ordinary model observations; they are not reported as Hegel failures. Existing
SDK conformance and coverage assertions remain in place.

Choice pools retain their repeated entries and weights. Primitive bounded draws
use explicit typed ranges. Full-width amounts retain zero and the entire uint64
domain. Color generation uses explicit equal-probability boolean decisions for
its geometric distribution and retains all 65 buckets. Counting trailing zeros
in a biased Hegel uint64 would distort its collision frequencies. Other integer
and float generators can be biased; pool entries do not promise exact
frequencies. Ratios and percentages in integer selector comments describe
nominal weights, conditional on reaching that draw, not coverage guarantees. Validate coverage through the SDK observations.

The only retained `internal.Rand` calls generate technical identities: the run
ID, fresh idempotency keys, unique transaction references and absent ledger
names. They retain the original full-width domain and collision behavior.

Pure generator tests use local `hegel.Test` callbacks. Parallel subtests own
separate callbacks. Concurrent generation and RPC regression tests open guarded
episodes inside each generating goroutine, preserving independent concurrency.
