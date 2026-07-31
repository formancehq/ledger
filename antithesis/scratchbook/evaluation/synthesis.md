---
sut_path: /Users/flemzord/Developer/Formance/ledger
commit: fb3f9f8334c7fbbcc27c2c211c77da36762e6aaf
updated: 2026-07-31
external_references: []
---

# PIT property evaluation synthesis

## Summary

Four cold-review lenses evaluated all 33 initial properties. The portfolio is
strong on public monetary safety, publication, tiering and destructive remote
GC. It overused P0 for deterministic protocol checks, underrepresented several
lifecycle/liveness mechanisms, and assumed observability that the current
harness does not yet provide.

The actions below are incorporated before handoff: deterministic subchecks are
folded into the main oracle, four provenance-bearing slugs are renamed, missing
properties receive targeted discovery, and every conditional property states
the exact instrumentation or topology prerequisite.

## Refinements

### R1 — Protocol subchecks are not standalone search targets

- **Affected:** `protocol-pit-view-provenance`, `protocol-pit-error-contract`,
  `concurrency-api-primary-history-boundary`.
- **Finding:** Provenance completeness and exact error mapping are essential
  assertions, but mostly deterministic. Keeping them as separate P0 properties
  inflates the portfolio and Antithesis search priority.
- **Action:** Merge their predicates into the primary/history-boundary workload
  oracle and remove the two standalone catalog entries/evidence files. Preserve
  their resolved investigation evidence in the boundary file.

### R2 — Forwarding remains P1; follower snapshot becomes P1

- **Affected:** `protocol-leader-forwarding-provenance`,
  `lifecycle-follower-snapshot-install-fails-closed`.
- **Finding:** Forwarding has timing value around leadership movement but is
  substantially covered by deterministic routing tests. Normal follower
  snapshot install is forward-only and has multiple gates.
- **Action:** Keep both at P1. Reserve P0 for administrative restore-ahead and
  primary/history boundary safety.

### R3 — Backup/restore is promoted to P0

- **Affected:** `lifecycle-backup-restore-rebuilds-history`.
- **Finding:** Fresh-PVC restore crosses primary backup, object storage,
  operator lifecycle and a missing peer projection; this is an Antithesis sweet
  spot, not a secondary scenario.
- **Action:** Promote to P0 and run in the isolated `model` template with
  persistent MinIO and PIT cold tier disabled.

### R4 — Known source-missing liveness defect is staged correctly

- **Affected:** `recovery-source-missing-heals-same-process`.
- **Finding:** Code tracing confirms verifier/query-originated markers do not
  wake builder repair. A long chaos campaign would only rediscover a known
  deterministic failure.
- **Action:** First add a red deterministic regression and repair the handoff;
  then retain the Antithesis property to search variants around real MinIO and
  process schedules. Mark the property as known-failing until fixed.

### R5 — Narrow crash windows require reachability anchors

- **Affected:** `idempotency-keyed-apply-changes-pit-once`,
  `replay-remote-delete-ack-is-idempotent`,
  `resources-s3-stall-does-not-block-shutdown`,
  `recovery-unsynced-suffix-replays`.
- **Finding:** Black-box final states cannot prove the intended crash/interleave
  branch occurred.
- **Action:** Require correlated SUT-side `Reachable` signals or pause points for
  ambiguous commit, delete-before-ack, S3-in-flight shutdown and
  publication-before-barrier/recovered-behind.

### R6 — Canonical slugs describe behavior, not discovery provenance

- **Affected:** all `wildcard-*` slugs.
- **Action:** Rename to
  `unfiltered-fast-path-equals-account-fold`,
  `out-of-order-chapters-fail-closed`,
  `out-of-order-chapters-recover`, and
  `live-path-survives-pit-dependency-fault`.

### R7 — Campaign-specific topology replaces conflicting global assumptions

- **Affected:** `deployment-topology.md`, `sut-analysis.md`, restore and cold
  properties.
- **Finding:** Core cold testing may avoid MinIO termination, while restore
  requires persistent MinIO and disables the PIT cold tier. These are not one
  universal configuration.
- **Action:** Document separate `pit-core-cold` and `pit-restore` profiles.

## Gaps to fill

### G1 — Boot readiness reconciles persisted history

- **Missing risk:** After ordinary process restart, a persisted manifest must
  not serve before fresh source reconciliation even when no rebuild is needed.
- **Targeted discovery:** Add a boot-readiness safety/liveness property distinct
  from hard unsynced-suffix loss.

### G2 — Administrative restore-ahead safety

- **Missing risk:** A locally ahead peer projection may briefly satisfy the
  restored lower primary watermark before the next builder tick.
- **Targeted discovery:** Add the restore-ahead fail-closed property, explicitly
  separate from forward-only follower snapshot install.

### G3 — Remote-GC queue/grace recovery

- **Missing risk:** Safety dominates the GC portfolio; cursor/candidate progress
  can wedge indefinitely after views, S3 failures and delete-before-ack.
- **Targeted discovery:** Add bounded-pass queue convergence after a fresh
  inventory, grace and view drain.

### G4 — Cancelled cold reads release leases and cache resources

- **Missing risk:** Pinned/cancelled readers can block local/remote GC and grow
  file descriptor/cache usage without returning wrong money.
- **Targeted discovery:** Add resource-drain liveness and singleflight safety.

### G5 — Linearizable PIT under partitions

- **Missing risk:** The catalog covers stale/local and final convergence but not
  the default `ReadIndex` behavior under quorum/leader partitions.
- **Targeted discovery:** Add exact-success-or-transient-fail-closed safety plus
  post-heal progress.

### G6 — Transient MinIO failure must not become sticky source loss

- **Missing risk:** A temporary checksum/read transport error may be converted
  into durable `SOURCE_MISSING`, while other paths return retryable
  `EXTERNAL_SERVICE_ERROR`.
- **Targeted discovery:** Refine source repair with a specific transient-fault
  property and exact error-state transition.

### G7 — Verifier coherence and progress under maintenance

- **Missing risk:** A sampled/full verifier pass can starve indefinitely or
  falsely quarantine a mixed maintenance view without changing public money
  before the quarantine transition.
- **Targeted discovery:** Add one pinned-pass safety predicate plus guarded
  reachability/liveness requiring both a cold sample and a full authoritative
  replay to complete under observed maintenance activity.

### Covered findings requiring no new property

- Same-prefix replica equality is covered by
  `integrity-layout-independent-semantics` plus
  `coordination-quiescent-pit-convergence`.
- Normal boot, restore-ahead and backup restore are deliberately split after
  G1/G2 rather than folded into one vague recovery property.

## Biases requiring human/environment judgment

### B1 — NoSync loss semantics

The repository proves grace-zero termination bypasses graceful shutdown, but
not whether the target Antithesis PVC model can discard non-fsynced bytes. The
unsynced-suffix property requires tenant/storage confirmation or a supported
node/disk fault.

### B2 — Selective fault targeting

Several independence properties require faulting Ledger-to-MinIO without also
breaking Raft or workload gRPC. The repository does not encode the K8s launch
webhook's fault-exclusion/link-selection capabilities.

### B3 — Disk-isolation campaign

The current operator co-locates PIT and primary data on one PVC. No property can
honestly claim isolated ENOSPC behavior until a dedicated history volume exists.
Keep shared-PVC pressure as an explicit topology risk, not an implemented
property.

### B4 — Mirror-ingest reachability

The reducer supports resolved mirror postings, but the existing targeted
workload plan does not prove a mirror-ingest path is reachable. Decide whether
mirror needs a separate campaign/dependency or whether resolved-posting
equivalence is sufficient for the first PIT launch.

## Passes

- Safety, liveness and reachability are all represented.
- Every destructive remote operation has a stronger internal predicate than a
  public-query-only oracle.
- The topology separates the three Raft replicas and MinIO, enabling the most
  valuable asymmetric failures.
- The public monetary oracle is watermark-aware and does not assume every client
  attempt committed.
- Open questions are investigated and logged; only the target storage/fault
  model remains explicitly human-dependent.

## Assumptions

- Gap-fill properties use the same repository-only evidence boundary.
- Refinements may reduce property count; completeness is measured against risk,
  not the original count.

## Open Questions

- The four bias decisions above remain handoff questions; they do not invalidate
  the rest of the portfolio.
