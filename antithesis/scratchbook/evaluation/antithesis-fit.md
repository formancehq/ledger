---
sut_path: /Users/flemzord/Developer/Formance/ledger
commit: fb3f9f8334c7fbbcc27c2c211c77da36762e6aaf
updated: 2026-07-31
external_references: []
---

# Lens 1 — Antithesis Fit

## Summary

The portfolio has a strong concurrency and recovery core, but its P0 queue does
not currently distinguish Antithesis search value from financial impact. Of 33
cataloged properties, 28 are P0. Several of those are deterministic transport,
format, parser, or already-confirmed control-flow checks that should be settled
by unit/integration tests before consuming fault-search budget.

The best Antithesis properties are the ones that combine independent processes
or durability domains: primary/history snapshot binding, pinned views during
maintenance, compaction/tiering/remote-GC races, follower and scale-up
reconciliation, crash-safe repair, quiescent multi-replica convergence,
backup/PVC restore, and S3 cancellation during shutdown.

The largest reachability risks are compound `Sometimes` predicates requiring a
very narrow crash or response window. They need a preceding SUT-side
`Reachable` checkpoint or a deterministic pause seam. One property additionally
depends on storage semantics that the repository cannot establish; its current
`Sometimes` may be impossible rather than merely rare.

## Findings

### 1. Deterministic protocol checks are overrepresented as P0 Antithesis properties

- **Properties:** `protocol-pit-view-provenance`,
  `protocol-pit-error-contract`, `protocol-leader-forwarding-provenance`
- **Concern:** Most of each invariant is a fixed serialization or mapping
  matrix. The evidence already cites focused gRPC, HTTP, CLI, and three-node E2E
  coverage. The forwarding oracle discards every sample with a leader/term
  change or different view token; once those gates hold, the remaining body and
  trailer equality is an ordinary integration check. The error property's
  `Sometimes(observedUnsupportedFilter)` is especially poor Antithesis fit: a
  fixed request shape reaches it deterministically, without a useful schedule.
- **Scope:** Property-specific, with portfolio priority impact.
- **Evidence:** The SUT analysis explicitly says deterministic coverage already
  includes PIT E2E and forwarding/convergence. The evidence files enumerate
  exact transport and error-mapping tests. `HISTORY_EXPIRED` is correctly kept
  out of reachability because format v1 cannot construct it.
- **Suggested action:** Keep the full field/status/header matrices in
  deterministic tests. Remove `Sometimes(observedUnsupportedFilter)`. Retain at
  most one low-cost Antithesis success-structure check in the main monetary
  driver, and let `concurrency-api-primary-history-boundary`, cold-source
  failures, and multi-replica properties own runtime fail-closed behavior.
  Demote these standalone entries from P0 or remove them from the scheduled
  portfolio after the deterministic suite is pinned.

### 2. A confirmed same-process repair defect should be a red regression test before an Antithesis campaign

- **Property:** `recovery-source-missing-heals-same-process`
- **Concern:** The evidence confirms a direct control-flow defect: a verifier-
  or query-originated persistent marker does not update the builder atomics, so
  the already-ready builder can take its caught-up early return forever. That
  outcome does not require schedule exploration to reproduce and the current
  property is expected to fail before faults add information.
- **Scope:** Property-specific.
- **Evidence:** `sut-analysis.md` labels this a confirmed code-level liveness
  gap. The property traces the missing store-to-builder notification and notes
  that restart is the existing control path.
- **Suggested action:** First add and commit a deterministic failing
  integration test, then fix the handoff. After the fix, retain a P1 Antithesis
  regression that combines real cold-object loss/restoration, verifier/query
  marker origin, continued process identity, and independent S3 faults. Do not
  spend P0 search budget rediscovering the known pre-fix outcome.

### 3. The out-of-order chapter safety half is integration-test territory; the recovery half is the Antithesis property

- **Properties:** `wildcard-out-of-order-chapters-fail-closed`,
  `wildcard-out-of-order-chapters-recover`
- **Concern:** Constructing chapter N hot and N+1 archived, then checking that
  `HotColdSource` rejects the topology, is a deterministic cross-component
  integration test. The safety property permits the exact fail-closed outcome
  the code is already known to produce. Antithesis becomes valuable only after
  N is archived too, because replicas must reset/rebuild/certify across process
  restarts and MinIO faults.
- **Scope:** Property-specific pair.
- **Evidence:** The SUT analysis calls the FSM/source mismatch confirmed but not
  runtime-reproduced. The safety evidence names the two deterministic branches;
  the liveness evidence spans builder repair and every replica.
- **Suggested action:** Add the mixed-topology regression as an integration
  test and keep its exact-or-fail-closed assertion as a companion guard in the
  recovery workload. Schedule `wildcard-out-of-order-chapters-recover` as the
  standalone Antithesis property; consider promoting it above P1 once the real
  archiver fixture is reliable.

### 4. Several compound `Sometimes` predicates need a reachable precondition checkpoint

- **Properties:** `idempotency-keyed-apply-changes-pit-once`,
  `replay-remote-delete-ack-is-idempotent`,
  `resources-s3-stall-does-not-block-shutdown`
- **Concern:** Each desired outcome intersects a narrow event with later
  recovery: commit-before-response loss, delete-before-durable-ack, or shutdown
  cancellation while a real S3 verb is active. Repeated random calls or pod
  deletion may never hit those windows. An `Always` guarded only by
  `reconciledKeyedOutcome` is also vacuous if no ambiguous committed retry is
  observed.
- **Scope:** Repeated assertion-pattern refinement.
- **Evidence:** Every evidence file already says the exact checkpoint is
  missing. Remote delete acknowledges immediately after the remote call;
  shutdown currently has no old-process/S3-verb correlation; the idempotency
  file leaves its post-commit timeout checkpoint open.
- **Suggested action:** Use two stages for each property:
  `Reachable(windowEntered)` at the SUT boundary, followed by a conditional
  safety assertion and `Sometimes(windowEntered && recoveredOutcome)`. Add a
  deterministic pause/block seam when the event is too short for the workload
  to coordinate. Do not count generic retries, object absence, replacement-pod
  readiness, or a pre-commit timeout as reachability of the intended window.

### 5. Unsynced-suffix loss may be impossible in the planned fault model

- **Properties:** `recovery-unsynced-suffix-replays` and, to a lesser extent,
  `integrity-atomic-publication-restart`
- **Concern:** `Sometimes(lostSuffixObserved && replayed...)` is invalid as a
  required portfolio property until the environment can actually discard
  non-fsynced PVC bytes. Grace-zero pod deletion kills a process but may retain
  the host page cache and all NoSync writes. Repeating an impossible branch is
  not state-space exploration.
- **Scope:** Property-specific and environment-dependent.
- **Evidence:** The recovery evidence requires environment-owner confirmation
  after an exhaustive repository search found no node-power, disk-reset, page-cache
  drop, or persistent-volume rollback fault. The deterministic hardening test
  reaches the state by replacing the live DB with an older synced checkpoint.
- **Suggested action:** Gate the `Sometimes` on confirmed Antithesis disk/node
  fault semantics or add a deterministic fault seam that restores the last
  synced history checkpoint. Until then, keep only
  `AlwaysOrUnreachable(lostPrefixObserved => replayIsExact)` plus ordinary
  process-crash atomicity. A hard pod restart that recovers the full prefix must
  not satisfy the lost-suffix property.

### 6. Follower snapshot gate priority is overestimated for the honest protocol

- **Property:** `lifecycle-follower-snapshot-install-fails-closed`
- **Concern:** The evidence now shows that an honest same-cluster checkpoint is
  forward-only and the controller independently raises the required history
  watermark to the replacement primary head. A wrong successful result needs
  an ahead or same-watermark-divergent manifest, a precondition not produced by
  ordinary follower synchronization. The missing synchronous readiness handoff
  is real, but the P0 public-safety impact is currently defense in depth.
- **Scope:** Property-specific priority/refinement.
- **Evidence:** `PrepareSnapshot.minAppliedIndex` excludes rollback-shaped
  follower checkpoints; the property itself rates externally wrong success as
  low confidence and accepts the watermark gate as a secondary guard.
- **Suggested action:** Add a deterministic synchronizer/builder epoch test and
  the narrow SUT assertion. Demote the standalone Antithesis property to P1 or
  merge its black-box probe into `coordination-scale-up-no-premature-local-pit`.
  Keep rollback/ahead replacement in the backup/restore campaign, where that
  state is actually constructible.

### 7. Cold corruption and owner containment each mix deterministic matrices with valuable lifecycle races

- **Properties:** `integrity-cold-content-verified`,
  `security-pit-remote-gc-owner-containment`
- **Concern:** Truncated/reordered/checksum-mismatched codecs, malformed names,
  traversal components, and adjacent-prefix filtering are fixed-input tests.
  They do not benefit from Antithesis scheduling. In contrast, cache
  publication during restart, ordinal/PVC reuse, current-root capture,
  delete-before-ack, and concurrent same-digest tiering cross real durability
  and ownership domains.
- **Scope:** Property-specific splits.
- **Evidence:** Both evidence files cite broad deterministic test matrices. The
  deployment topology also excludes custom object corruption from the first
  campaign, while operator scale-down/up and MinIO are real separate services.
- **Suggested action:** Keep codec/parser/foreign-prefix matrices in unit and
  integration tests. Narrow the Antithesis properties to (a) partial transport
  or restart during verified cold-cache admission, and (b) same-ordinal fresh
  PVC reuse plus concurrent remote GC/root changes. Do not require a custom
  object mutator in the core campaign.

### 8. Deterministic coverage matrices should not be modeled as liveness properties

- **Properties:**
  `wildcard-agent-unfiltered-fast-path-equals-account-fold`,
  `protocol-pit-error-contract`, and the semantic-mode portions of
  `integrity-dual-axis-reversal-exactness`
- **Concern:** `Sometimes(allFourOptionModesPaired)`,
  `Sometimes(observedUnsupportedFilter)`, and an exhaustive before/at/after
  timestamp matrix are test-completeness goals, not emergent liveness states.
  Encoding them as `Sometimes` gives the search engine credit for deterministic
  driver enumeration and can create false portfolio failures when a focused
  include intentionally omits a mode.
- **Scope:** Catalog-wide assertion-style refinement.
- **Evidence:** The workload design already has `first_` and `eventually_`
  phases that can enumerate bounded cases after quiescence. The real
  Antithesis value in these properties is equality while layouts, replicas, and
  publication timing vary.
- **Suggested action:** Enumerate fixed modes in table-driven unit/integration
  tests or one deterministic final command. Keep `AlwaysOrUnreachable` for
  every comparable success and reserve `Sometimes` for a genuinely
  schedule-dependent state, such as a same-view comparison spanning a manifest
  rewrite or a reversal observed after a leadership/restart interleaving.

### 9. Backup/PVC restore has higher Antithesis value than its P1 ranking suggests

- **Property:** `lifecycle-backup-restore-rebuilds-history`
- **Concern:** This is one of the few properties that crosses every relevant
  durability boundary: primary backup, incremental export, authoritative
  chapter archives, full Ledger PVC teardown, restore-only process, normal
  restart, and peer-projection rebuild. Deterministic tests can validate each
  component but not the full faulted choreography.
- **Scope:** Property-specific underestimation.
- **Evidence:** The evidence identifies a dedicated existing model template,
  independent monetary oracle, verified PVC deletion, and missing but concrete
  MinIO/PIT prerequisites. It also correctly isolates the campaign from
  concurrent post-backup writes.
- **Suggested action:** Once durable MinIO and PIT enablement are wired, treat
  this as P0 within the isolated restore campaign. Keep it out of the core
  `main` template so its long setup does not dilute unrelated search.

### 10. The optional-dependency liveness premise must be observable or workload-controlled

- **Property:** `wildcard-live-path-survives-pit-dependency-fault`
- **Concern:** `Sometimes(minioIsolation && liveWriteAndReadSucceeded)` is a
  good partial-failure property only if `minioIsolation` is an authoritative
  predicate. The current workload does not receive Antithesis fault state, and
  the launch recipe does not encode target-link exclusions. Without a
  controlled window, the assertion can pass on a healthy write before any
  MinIO fault or remain permanently unreachable despite useful generic faults.
- **Scope:** Property-specific uncertainty and assertion refinement.
- **Evidence:** The deployment topology lists targeted asymmetric faults as an
  assumption/open question and excludes workload faults, but provides no fault
  controller or link-state signal.
- **Suggested action:** Drive the dependency failure through a controllable
  proxy/test endpoint, or gate the workload `Sometimes` on a SUT-side
  `Reachable` showing a PIT maintenance/cold operation failed or blocked against
  MinIO while Raft quorum and client connectivity remained healthy. Until one
  exists, keep the property conditional rather than a mandatory P0 signal.

## Per-property fit matrix

| Property | Fit | Evaluation |
|---|---|---|
| `concurrency-api-primary-history-boundary` | Strong | Real TOCTOU across primary snapshot, async history, recreate, routing, and lag. Keep P0. |
| `integrity-dual-axis-reversal-exactness` | Strong with deterministic baseline | Keep the faulted oracle; move exhaustive timestamp/mode enumeration to deterministic tests. |
| `protocol-current-metadata-historical-money` | Strong but expensive | Three independently advancing views make this useful; keep only after exact per-replica index gating. |
| `integrity-ledger-incarnation-isolation` | Strong | Delete/recreate plus lag, restore, and compaction is a genuine distributed state space. |
| `protocol-pit-view-provenance` | Deterministic | Field/header/trailer validation is unit/integration territory; forwarding owns the only extra boundary. |
| `protocol-pit-error-contract` | Split | Error mapping and unsupported filters are deterministic; runtime no-fallback under cold/lag faults remains useful. |
| `protocol-leader-forwarding-provenance` | Mostly integration | Stable-term/equal-token gating removes the fault interleavings; deterministic cluster E2E is the natural home. |
| `wildcard-agent-unfiltered-fast-path-equals-account-fold` | Useful supplemental oracle | Cheap public metamorphic check across layout faults; P1 is more proportionate than P0. |
| `integrity-authoritative-prefix-only` | Strong black-box, weak duplicate SUT check | Keep the independent workload replay; avoid reasserting deterministic validator branches after they already returned success. |
| `integrity-atomic-publication-restart` | Strong if crash branch is reachable | Process-kill atomicity fits; actual NoSync suffix loss is conditional on storage semantics. |
| `integrity-layout-independent-semantics` | Strong workload property | Different replica layouts and crash points fit well; avoid an unbounded production-side full digest comparison. |
| `concurrency-pinned-view-maintenance-stability` | Strong | Long view versus publication/compaction/tiering/GC is core Antithesis territory. |
| `concurrency-compaction-cas-preserves-suffix` | Strong | Outside-lock preparation plus concurrent publication/reset and crash is a high-value race. |
| `concurrency-tier-lease-precedes-local-delete` | Strong | View lease, remote verification, two-phase local deletion, and kill span independent systems. |
| `integrity-cold-content-verified` | Split | Codec/corruption matrix is deterministic; network/cache/restart admission race fits Antithesis. |
| `concurrency-builder-source-snapshot-archive-purge` | Strong | Primary snapshot, real archiver purge, and cold-reader lease form a meaningful interleaving. |
| `concurrency-remote-gc-live-roots-protected` | Strong | Candidate aging, active views, reset/version ABA, and external delete are a sweet spot. |
| `concurrency-remote-gc-inventory-upload-linearization` | Strong | Paginated inventory versus upload/reset epoch and crash is exactly the desired state space. |
| `security-pit-remote-gc-owner-containment` | Split | Namespace/parser matrix is deterministic; ordinal reuse plus concurrent root/GC state is strong. |
| `recovery-unsynced-suffix-replays` | Conditional | Do not require `Sometimes` until a supported fault can lose non-fsynced PVC bytes. |
| `recovery-repair-gate-survives-crashes` | Strong | Repeated crashes across durable markers, replay, barrier, certification, and clear are high value. |
| `recovery-source-missing-heals-same-process` | Deterministic first | Known control-flow gap needs a red integration test/fix; retain post-fix fault regression only. |
| `lifecycle-follower-snapshot-install-fails-closed` | Overestimated | Honest sync is forward-only with a second watermark gate; demote/merge after adding deterministic epoch coverage. |
| `wildcard-out-of-order-chapters-fail-closed` | Deterministic | Fixed accepted/rejected topology is a cross-component integration regression. |
| `wildcard-out-of-order-chapters-recover` | Strong | Repair/certification across replicas, archiver, MinIO, and restarts benefits from search. |
| `coordination-quiescent-pit-convergence` | Strong | Stable full-membership denominator after arbitrary faults is an excellent eventual property. |
| `coordination-scale-up-no-premature-local-pit` | Strong | Direct ordinal access through join/snapshot/backfill/promotion is a valuable lifecycle state space. |
| `lifecycle-backup-restore-rebuilds-history` | Strong and under-ranked | Full backup/PVC/archive/rebuild choreography merits P0 in its isolated template. |
| `idempotency-keyed-apply-changes-pit-once` | Strong after reachability fix | Ambiguous cross-leader retry plus independent builders fits, but needs a post-commit-response-loss anchor. |
| `replay-remote-delete-ack-is-idempotent` | Strong after reachability fix | External side effect plus local durable acknowledgement is high value; add an exact pre-ack checkpoint. |
| `wildcard-live-path-survives-pit-dependency-fault` | Strong if fault premise is controlled | Asymmetric optional-dependency failure is ideal, but the current premise is not observable. |
| `resources-logical-run-debt-reconverges` | Good P1 fit | Reaching debt/stall/restart needs Antithesis; the pass-count arithmetic itself is deterministic. |
| `resources-s3-stall-does-not-block-shutdown` | Strong after reachability fix | Real transport cancellation during Fx/Kubernetes lifecycle is high value; active-at-cancel must be proven. |

## Passes

- Safety properties generally use `Always` or `AlwaysOrUnreachable`, while
  quiet-period recovery uses `Sometimes`; that basic split is sound.
- `protocol-pit-error-contract` correctly refuses to add an impossible
  `Sometimes(HISTORY_EXPIRED)` under format v1.
- `coordination-quiescent-pit-convergence` uses a stable leader-reported full
  membership denominator and does not silently omit a stuck learner. This is a
  strong non-vacuous liveness formulation.
- `coordination-scale-up-no-premature-local-pit` does not require observing a
  transient `HISTORY_BUILDING`; every success is constrained, and eventual
  success is checked separately.
- The remote-GC root and inventory properties use SUT-side assertions at the
  destructive/internal decision and public cold-read or destination checks as
  independent consequences. That is an appropriate assertion split.
- `recovery-repair-gate-survives-crashes` treats synchronous marker deletion as
  the final durability boundary and does not mistake process-local readiness
  for persistent authority.
- `resources-logical-run-debt-reconverges` uses completed maintenance passes,
  not synthetic wall-clock latency, so it remains a correctness/progress test
  rather than a benchmark.
- The catalog consistently keeps successful monetary exactness as an `Always`
  companion to liveness. It does not permit eventual convergence to excuse an
  earlier partial success.

## Uncertainties

- **Storage faults:** Repository evidence cannot determine whether the target
  Antithesis tenant can discard non-fsynced PVC bytes. This blocks the mandatory
  reachability form of `recovery-unsynced-suffix-replays`.
- **Fault targeting:** The checked-in launch path does not prove that one
  Ledger-to-MinIO link can be isolated while Raft and workload gRPC remain
  intact. This keeps `wildcard-live-path-survives-pit-dependency-fault`
  conditional.
- **Precise pause support:** Repository evidence does not show a production
  pause after remote delete, after keyed commit/before response, or while a
  maintenance S3 verb is active at shutdown. SUT checkpoints may be sufficient
  for Antithesis search, but a deterministic seam may still be required.
- **Cold-object mutation:** The first topology explicitly avoids arbitrary
  object mutation. Missing/checksum matrices should remain deterministic unless
  a scoped helper is deliberately added in a separate campaign.
- **Campaign economics:** The isolated restore property has excellent fit but a
  long setup. Whether it is P0 in total portfolio scheduling or P0 only inside a
  dedicated restore run is a human budget decision.

## Assumptions

- This evaluation ranks Antithesis search value, not solely customer impact.
- The SUT is the repository at
  `fb3f9f8334c7fbbcc27c2c211c77da36762e6aaf`; scratchbook artifacts are
  working evidence derived from that commit.
- No external fault semantics, tenant capabilities, or product commitments are
  assumed beyond repository evidence.

## Open Questions

- Can the target Antithesis environment lose non-fsynced PVC bytes through a
  supported node/disk fault?
- Can the campaign target or authoritatively observe an asymmetric
  Ledger-to-MinIO fault window?
- Are deterministic pause seams acceptable for the three narrow compound
  windows identified above?
- Should priority represent customer severity, Antithesis search value, or two
  separate scores?
