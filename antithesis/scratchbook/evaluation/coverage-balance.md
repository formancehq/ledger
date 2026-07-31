---
sut_path: /Users/flemzord/Developer/Formance/ledger
commit: fb3f9f8334c7fbbcc27c2c211c77da36762e6aaf
updated: 2026-07-31
external_references: []
---

# Coverage Balance Evaluation

## Summary

The 33-property portfolio is strong on public monetary correctness, atomic
projection publication, physical-layout safety, repair gating, and destructive
remote-GC safety. It has a reasonable top-level safety/liveness split (24 safety,
9 liveness), and 29 evidence files propose `Reachable`/`Unreachable` guidance,
so assertion-type diversity is present even though no PIT SDK assertions exist
yet.

The principal imbalance is within subsystems rather than between assertion
types. Remote GC has several P0 safety properties but no end-to-end inventory,
grace, and queue-drain liveness obligation. The verifier and cold cache are
stateful production components in the SUT/topology but appear mostly as
supporting actors in other properties. Several evidence files also delegate
critical guarantees to slugs absent from the catalog, creating real coverage
holes at quiescent equality, boot readiness, and restore-ahead rollback.

## Coverage matrix

| SUT/topology area | Existing property coverage | Balance assessment |
|---|---|---|
| Public monetary semantics | `integrity-dual-axis-reversal-exactness`, `protocol-current-metadata-historical-money`, `integrity-ledger-incarnation-isolation`, `wildcard-agent-unfiltered-fast-path-equals-account-fold` | Strong. Both axes, reversals, current metadata, incarnation, precision/color transforms, and redundant aggregation paths are represented. |
| API boundary and provenance | `concurrency-api-primary-history-boundary`, `protocol-pit-view-provenance`, `protocol-pit-error-contract`, `protocol-leader-forwarding-provenance` | Strong for successful and typed-failure responses. Default linearizable behavior during a leader partition remains uncovered. |
| Builder/source/reducer | `integrity-authoritative-prefix-only`, `integrity-atomic-publication-restart`, `idempotency-keyed-apply-changes-pit-once`, both out-of-order-chapter properties | Strong for ordinary Apply-derived source streams, archive handoff, and restart atomicity. Mirror-originated source ordering is not explicit. |
| Pinned views and maintenance | `concurrency-pinned-view-maintenance-stability`, `concurrency-compaction-cas-preserves-suffix`, `concurrency-tier-lease-precedes-local-delete`, `integrity-layout-independent-semantics` | Strong for logical correctness. Cache singleflight, cancellation, lease release, and bounded overage are blind spots. |
| Verifier and repair | `recovery-repair-gate-survives-crashes`, `recovery-source-missing-heals-same-process`, `integrity-cold-content-verified` | Strong after a failure marker exists. No property owns periodic verifier progress or coherence while builder/maintenance mutate adjacent state. |
| Remote GC | `concurrency-remote-gc-live-roots-protected`, `concurrency-remote-gc-inventory-upload-linearization`, `security-pit-remote-gc-owner-containment`, `replay-remote-delete-ack-is-idempotent` | Safety-heavy. Missing end-to-end queue convergence and restart/clock preservation of cursor and grace evidence. |
| Raft/routing/replica lifecycle | `lifecycle-follower-snapshot-install-fails-closed`, `coordination-scale-up-no-premature-local-pit`, `coordination-quiescent-pit-convergence`, forwarding property | Good coverage of join, forward snapshot, stale-local reads, and final convergence. Evidence delegates equality to an absent property, and default linearizable partition semantics are missing. |
| Backup/restore | `lifecycle-backup-restore-rebuilds-history` | Covers only fresh-PVC restore. Retained/ahead peer-store rollback and generic boot gating are delegated to absent slugs. |
| Resource/operational liveness | `wildcard-live-path-survives-pit-dependency-fault`, `resources-logical-run-debt-reconverges`, `resources-s3-stall-does-not-block-shutdown` | Thin relative to the SUT analysis. MinIO isolation, compaction debt, and shutdown are covered; cache resources and shared-PVC disk pressure are not. |
| MinIO | Source replay, cold reads, tiering, remote inventory, missing/corrupt objects, restart, and shutdown appear across the catalog | Strong, provided a PVC and the required fault/helper capabilities are supplied. |
| Operator | Scale-up, ordinal reuse inside owner containment, rolling restart, and restore use operator-controlled transitions | Partial by design. Operator failure itself is explicitly a separate campaign. |
| NATS and harness control plane | No PIT properties | Correct exclusion: the topology identifies them as outside the PIT data path. |

## Findings

### 1. Critical guarantees are delegated to properties absent from the catalog

- **Properties:** `coordination-quiescent-pit-convergence`,
  `lifecycle-backup-restore-rebuilds-history`; missing candidates
  `coordination-same-prefix-replicas-agree`,
  `lifecycle-boot-readiness-reconciled-prefix`, and
  `recovery-restore-ahead-fails-closed`.
- **Concern:** The quiescent-convergence evidence says monetary equality belongs
  to `coordination-same-prefix-replicas-agree`, but that slug has neither a
  catalog entry nor an evidence file. The restore evidence similarly assigns
  generic boot gating and retained/ahead projection rollback to
  `lifecycle-boot-readiness-reconciled-prefix` and
  `integrity-rollback-cannot-serve-ahead-history`, neither of which is
  cataloged. The catalog entry for quiescent convergence still claims equality,
  so catalog and implementation handoff disagree about assertion ownership.
- **Scope:** Catalog-wide coverage discontinuity.
- **Evidence:** The SUT analysis identifies replica-local divergence and the
  post-restore ahead window as high-value timing risks. The follower snapshot
  property explicitly excludes rollback-shaped replacement, and the restore
  property explicitly scopes itself to fresh PVCs.
- **Suggested action:** Add the three concrete properties above, or fold each
  delegated invariant back into an existing cataloged property and remove the
  dangling references. Prefer `recovery-restore-ahead-fails-closed` as the
  canonical retained-store name so it is not confused with ordinary forward
  follower snapshot installation.

### 2. Remote GC is safety-rich but lacks portfolio-level convergence

- **Properties:** Existing remote-GC quartet plus
  `replay-remote-delete-ack-is-idempotent`; missing candidates
  `resources-remote-gc-queue-converges` and
  `recovery-gc-cursor-and-grace-survive-restart`.
- **Concern:** Existing properties prove that a reached delete is scoped,
  unrooted, epoch-valid, and retryable. None requires paginated inventory to
  complete after faults stop, candidates to survive/recover across restart,
  the two-observation/grace evidence to remain conservative under clock faults,
  or the durable queue/oldest age to drain. A collector can therefore remain
  permanently stuck without violating any cataloged remote-GC property.
- **Scope:** Cross-cutting liveness gap inside an otherwise heavily covered
  subsystem.
- **Evidence:** `remoteGCState` persists cursor, scan epoch, completed inventory
  epoch, cycle, queue counts, and oldest observation; each collection call has
  bounded scan/delete budgets. Metrics already expose queue objects/bytes,
  oldest age, failures, and last completed inventory. The deployment profile
  accelerates GC to one second and explicitly lists restart and clock faults.
- **Suggested action:** Add one quiet-period queue-drain property guarded by
  prior observed debt, plus one restart/clock safety property for cursor and
  grace evidence. Do not collapse these into delete-before-ack: that property
  begins only after the collector has already reached an eligible delete.

### 3. The verifier has no independent concurrency or progress obligation

- **Properties:** Indirectly affects `integrity-layout-independent-semantics`,
  `integrity-cold-content-verified`, `recovery-repair-gate-survives-crashes`, and
  `recovery-source-missing-heals-same-process`; missing candidate
  `concurrency-verifier-maintenance-coherence`.
- **Concern:** The catalog tests repair after marker persistence and semantic
  equality after physical rewrites, but no property requires a periodic sampled
  or full verifier pass to finish against a coherent pinned source/manifest
  while builder, compaction, tiering, and cold faults run. A permanently starved
  verifier, a sample cursor that never advances, or a false quarantine caused by
  a mixed verification view can evade the current portfolio.
- **Scope:** Component blind spot and missing reachability/liveness pairing.
- **Evidence:** The SUT dedicates one guarded verifier goroutine, a rotating cold
  sample cursor, and periodic full semantic replay. The Antithesis profile sets
  a 2-second interval and full replay every four passes specifically to make
  these paths reachable. `existing-assertions.md` records no PIT verifier
  assertion.
- **Suggested action:** Add a safety property for every completed pass against
  its pinned inputs, paired with `Sometimes`/`Reachable` signals proving at
  least one sampled cold pass and one full replay complete under concurrent
  maintenance. Keep byte-corruption matrices in deterministic tests; the
  Antithesis value is the cross-goroutine and cross-service interleaving.

### 4. Cold-cache singleflight and resource release are uncovered

- **Properties:** Adjacent coverage in `integrity-cold-content-verified` and
  `concurrency-pinned-view-maintenance-stability`; missing candidates
  `concurrency-cold-cache-singleflight-leases`,
  `resources-cancelled-cold-reads-release-resources`, and
  `resources-cold-cache-overage-drains`.
- **Concern:** Correct cold bytes and immutable views do not prove correct cache
  coordination. Concurrent misses share an inflight load; cache entries have
  independent leases and shared indexes; active leases may exceed the byte
  budget; cancellation can occur for leaders or waiters. The catalog has no
  invariant that one digest publishes once, no check for lease/inflight leaks,
  and no liveness requirement that temporary over-budget state drains.
- **Scope:** Cross-subsystem concurrency and resource-liveness gap.
- **Evidence:** The cache owns `inflight`, per-entry lease counts, LRU eviction,
  invalidation, and temporary over-budget semantics. The deployment topology's
  fault map explicitly names cache singleflight and lease interleavings, and
  the test profile constrains the cache to 8 MiB to make eviction frequent.
- **Suggested action:** Add the three properties as one focused cache cluster:
  singleflight/lease safety, cancellation cleanup, and guarded post-lease
  overage convergence. Use bounded cache counters plus surgical reachability
  for joined inflight loads; process-wide goroutine counts alone are too noisy.

### 5. Default linearizable PIT behavior under partition is missing

- **Properties:** Adjacent coverage in
  `concurrency-api-primary-history-boundary` and
  `protocol-leader-forwarding-provenance`; missing candidate
  `coordination-linearizable-pit-partition-fails-closed`.
- **Concern:** The portfolio checks stale-local PIT coherence and successful
  explicit leader forwarding, but does not assert that a default linearizable
  request to an isolated follower or unconfirmed leader fails/forwards rather
  than silently reading the local store without a successful ReadIndex barrier.
  A locally exact result can still violate the requested cluster-consistency
  mode, so the source-watermark oracle alone does not cover this contract.
- **Scope:** Raft plus API-routing boundary gap.
- **Evidence:** `RoutedController.readCtrl` has three distinct paths: stale local,
  explicit leader, and default local ReadIndex with restricted leader fallback.
  The SUT analysis includes all three routing modes, while the catalog has a
  success-pair property only for explicit forwarding.
- **Suggested action:** Add the candidate safety property with route
  reachability signals for local ReadIndex success, follower fallback, and
  barrier failure. Under a proven partition, allow typed transport/quorum
  failure or a verified leader response, never an unbarriered local success.

### 6. Resource isolation stops at MinIO and omits the shared local volume

- **Properties:** `wildcard-live-path-survives-pit-dependency-fault` and
  `resources-logical-run-debt-reconverges`; conditional missing candidate
  `resources-pit-disk-pressure-is-isolated`.
- **Concern:** The product risk list includes PIT maintenance delaying live
  writes/reads, but the catalog's isolation property faults only the
  Ledger-to-MinIO link. The current operator places the primary store, PIT
  store, archive cache, and verifier scratch space on the same 2 GiB PVC. PIT
  ENOSPC or heavy scratch/cache I/O is therefore a shared failure mode, not
  covered by the MinIO dependency property.
- **Scope:** Topology-dependent resource bias.
- **Evidence:** Deployment topology explicitly warns not to interpret ENOSPC as
  isolated PIT degradation and says a dedicated history PVC is required before
  claiming disk-pressure isolation. The first campaign excludes ENOSPC.
- **Suggested action:** Do not add an unconditional isolation guarantee to the
  current topology. If dedicated PIT storage is a supported target, add the
  candidate property in that topology. Otherwise add a bounded fail-closed/no-
  corruption property for shared-volume exhaustion and document that primary
  write availability is outside the premise.

### 7. Mirror-originated monetary source behavior is only implicitly covered

- **Properties:** `integrity-authoritative-prefix-only`,
  `integrity-dual-axis-reversal-exactness`, and
  `idempotency-keyed-apply-changes-pit-once`; possible candidate
  `integrity-mirror-ingest-pit-prefix-exactness`.
- **Concern:** The SUT analysis says the reducer consumes logs after Numscript,
  mirror, and reversal resolution, but the workload portfolio describes
  ordinary Apply/retry fixtures. Mirror ingest has its own source ordering and
  identity behavior; if it can create proposal/log shapes not generated by
  ordinary Apply, the independent oracle may never exercise them.
- **Scope:** Lower-confidence cross-subsystem gap.
- **Evidence:** Mirror is named in the authoritative reduction path, while no
  catalog entry or workload design requirement mentions mirror ingestion.
  Deterministic reducer coverage includes a mirror-reverted transaction but
  does not supply real multi-process scheduling.
- **Suggested action:** First determine whether mirror-generated committed logs
  are observationally identical to the existing source fixtures. If not, add
  the candidate property; if yes, explicitly state that producer equivalence in
  the authoritative-prefix evidence rather than leaving the coverage implicit.

## Overinvestment assessment

- The public/API group (8 properties) is proportionate to monetary and protocol
  risk. Parsing-only behavior was correctly left to deterministic regression
  tests.
- Remote GC has five properties touching a comparatively narrow subsystem, but
  this is justified by its destructive authority and shared-bucket boundary.
  The problem is not excess safety coverage; it is the absence of matching
  progress coverage.
- Publication/replay pairs that appear similar are complementary rather than
  duplicates: `integrity-atomic-publication-restart` forbids torn state, while
  `recovery-unsynced-suffix-replays` requires eventual reconstruction.
- The fail-closed/recover pairs for out-of-order chapters and source repair are
  similarly balanced safety/liveness pairs and should remain separate.

## Assertion-type balance

- **Safety:** 24 catalog entries provide strong monetary and destructive-path
  protection.
- **Liveness:** 9 entries cover replay, repair, convergence, restore, compaction
  debt, and shutdown. Liveness is underrepresented specifically in verifier,
  cache, tier/remote-GC inventory, and retained-store restore paths.
- **Reachability:** No catalog entry uses Reachability as its sole type, but 29
  evidence files propose `Reachable` or `Unreachable` guidance. That is a sound
  pattern: internal phase signals guide search while workload safety/liveness
  remains authoritative. The implementation plan must preserve those paired
  signals because `existing-assertions.md` confirms the current PIT count is
  zero.

## Passes

- Every deployed Ledger replica role is represented: leader forwarding,
  follower-local reads/snapshot install, learners/scale-up, restart, and final
  all-member convergence.
- MinIO is exercised as authoritative chapter source, PIT-run destination,
  cold-read source, and remote-GC target, with both asymmetric faults and
  restart prerequisites distinguished from total object loss.
- The independent oracle rules correctly reconcile ambiguous writes and compare
  through returned log watermarks rather than client attempt counts or
  replica-local manifest IDs.
- The catalog correctly excludes NATS, workload faults, control-plane faults,
  parsing-only defects, and performance benchmarking from the core PIT
  campaign.
- Optional hot-only/cold-tier configuration behavior has deterministic config
  coverage, while the proposed Antithesis profile consistently enables the
  stateful cold paths that need fault scheduling.
- Restore is correctly isolated from the `main` template; the gap is retained-
  store coverage, not failure to recognize template isolation.

## Uncertainties

- Tenant support for process termination and clock faults determines whether
  unsynced-suffix, crash-window, and grace-period properties are reachable.
- MinIO needs a PVC before restart properties can distinguish service recovery
  from total authoritative object loss.
- Exact-object removal/corruption and thread-pause capabilities remain unknown;
  without them, some cold/verifier reachability must use SUT hooks or remain in
  deterministic tests.
- The supported restore product topology must clarify whether a PIT peer store
  can be retained independently of the primary PVC. The current model harness
  always exercises fresh storage.
- A dedicated PIT PVC is required before testing local disk-pressure isolation;
  with the current shared PVC, the stronger property is false by topology.
- It is unclear whether rolling changes to `BALANCE_HISTORY_ENABLED`, ledger
  allowlists, cold-tier settings, or store/archive format versions are supported
  operational scenarios. The uniform-config topology intentionally does not
  cover configuration skew or mixed-version upgrade. No new property should be
  required until that support contract is identified.
- Mirror source-shape equivalence to ordinary Apply remains unproven in the
  portfolio evidence and decides whether the mirror finding is a real gap.
