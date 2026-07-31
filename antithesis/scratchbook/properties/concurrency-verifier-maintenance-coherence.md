# concurrency-verifier-maintenance-coherence — Verification remains coherent and progresses during maintenance

## Catalog entry

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Safety with liveness/reachability companion |
| **Property** | Every completed sampled or full verifier pass is evaluated against one pinned manifest/source boundary and cannot falsely certify or quarantine a mixed maintenance view; with bounded data and healed dependencies, rotating cold samples and a full authoritative replay both complete while builder and maintenance activity have been observed. |
| **Invariant** | SUT `AlwaysOrUnreachable(completedPass => onePinnedManifest && successIsInternallyCoherent && quarantineHasConcreteMismatch, "pit: verifier pass is coherent with its pinned history view")`, plus workload/SUT `Sometimes(sampledColdPassCompleted && fullReplayCompleted && concurrentMaintenanceObserved)`. The liveness predicate is a conjunction, never an implication. |
| **Antithesis Angle** | Accelerate the verifier to two-second passes and full replay every four passes; interleave publication, compaction, tiering, cold fetch, remote GC, cancellation and process restart, then heal faults and require both pass kinds to finish on the same process epoch. |
| **Why It Matters** | A mixed verification view can falsely quarantine valid history; a starved guard or cursor can silently remove the integrity backstop while PIT continues serving. |
| **Confidence** | High for pinned-store mechanics and serialized verifier ownership; medium until SDK reachability and bounded-progress assertions exist. |

**Open Questions:**

- None. Use SUT assertions for physical pass/cursor/full-replay phases and keep
  public monetary queries as a separate serving oracle.

## Evidence

- `internal/application/balancehistory/verifier.go:74-101,383-429` serializes
  periodic, on-demand and certification passes through one guard and records
  successful-pass progress only after verification returns cleanly.
- `internal/application/balancehistory/verifier.go:328-335,430-506` schedules a
  deterministic full replay every configured N passes; sampled physical
  verification advances its cursor only after a successful bounded pass.
- `internal/application/balancehistory/verifier.go:535-646` opens one pinned
  history verification view, validates the authoritative head, replays into an
  isolated scratch store and compares logical/semantic state before success.
- `internal/storage/balancehistorystore/verify.go:32-136` verifies against one
  Pebble snapshot and rotates over bounded hot/cold targets without mutating the
  projection being checked.
- `internal/application/balancehistory/verifier.go:276-330` cancels and drains
  the periodic pass during shutdown; maintenance and builder remain separate
  goroutines, so real overlap is scheduler-dependent.
- `existing-assertions.md` records no PIT verifier SDK assertion. The proposed
  Antithesis profile makes the missing path reachable with a two-second
  interval and full replay every four passes.

## Required instrumentation and workload

- Emit `Reachable("pit: verifier sampled cold history during maintenance")`
  after a successful sampled pass that checked at least one archive part and
  whose process epoch observed a concurrent manifest publication or
  maintenance phase.
- Emit `Reachable("pit: verifier full replay matched pinned history")` only
  after authoritative replay, reducer state and semantic digest all match the
  pinned view. Include process epoch and manifest/source watermarks, not file
  paths or object-store secrets.
- At every completed pass emit the `AlwaysOrUnreachable` predicate above. A
  cancellation is inconclusive; a persisted failure/quarantine must carry the
  exact missing/corrupt/digest mismatch that justified it.
- After faults and writers stop, require a sampled cold pass and a full replay
  to complete within the normal ten-minute command budget on the same process
  epoch. Restart resets the attempt instead of satisfying progress.

### Investigation Log

#### Can public queries alone prove verifier coherence and progress?

- **Examined:** verifier guard/lifecycle, sampled cursor updates, pinned replay,
  store snapshots, existing metrics and the Antithesis assertion inventory.
- **Found:** public exact PIT proves serving correctness but cannot distinguish a
  verifier that never runs, a cursor that never advances or a false quarantine
  cause. The verifier already has exact internal completion boundaries and
  bounded pass counters suitable for low-cardinality SDK assertions.
- **Not found:** an existing workload-visible verifier status containing the
  pinned manifest/source pair or current sampled cursor.
- **Conclusion:** keep the product API unchanged. Add SUT-side safety and
  reachability signals, then use public PIT only as the independent serving
  oracle after the verifier campaign.
