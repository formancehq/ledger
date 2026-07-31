---
sut_path: /Users/flemzord/Developer/Formance/ledger
commit: fb3f9f8334c7fbbcc27c2c211c77da36762e6aaf
updated: 2026-07-31
external_references: []
---

# Wildcard evaluation

## Lens boundary

This pass starts beyond the three assigned lenses: Antithesis Fit evaluates
state-space value versus deterministic tests; Coverage Balance evaluates
portfolio distribution and omissions; Implementability evaluates whether the
planned topology, workload and observations can execute each property. The
Wildcard pass instead questions shared premises, cross-property interactions
and catalog mechanics that can make otherwise sound individual evaluations
misleading.

## Findings

### 1. Resolved decisions have not propagated into one coherent campaign model

- **Property/Properties:** catalog-wide, especially
  `lifecycle-backup-restore-rebuilds-history`.
- **Concern:** the evidence and catalog entry resolve the restore campaign in
  favor of the existing `model` template, fresh ledger PVCs, durable MinIO and
  `BALANCE_HISTORY_COLD_TIER=false`. The topology still says a dedicated
  `pit_restore` template is simpler, publishes one global profile with the PIT
  cold tier enabled, and leaves template and MinIO choices open. `sut-analysis`
  likewise retains already-resolved questions about restore template choice,
  node-owner reincarnation and same-process source repair. Other lenses can
  therefore reach opposite conclusions depending on which top-level artifact
  they treat as authoritative.
- **Scope:** catalog-wide cross-artifact contradiction.
- **Evidence:** `property-catalog.md:445-461` selects the fresh-PVC `model`
  path. `properties/lifecycle-backup-restore-rebuilds-history.md:22-38,64-79`
  requires that template, durable MinIO and no PIT cold tier. In contrast,
  `deployment-topology.md:71-109` applies `BALANCE_HISTORY_COLD_TIER=true` as
  the common profile, `:190-207` still offers two restore templates and prefers
  the dedicated one, and `:288-295` keeps both decisions open.
  `sut-analysis.md:376-390` retains several questions that their evidence files
  now resolve. The catalog's final global restore-topology question at
  `property-catalog.md:578-580` also conflicts with its property-level answer.
- **Suggested action:** define explicit configuration profiles rather than one
  universal campaign: at minimum `main-cold` (cold tier enabled),
  `model-restore` (cold tier disabled, durable MinIO, fresh ledger PVCs), and an
  optional total-object-loss profile. Propagate resolved decisions through
  `sut-analysis`, `deployment-topology`, catalog assumptions/open questions and
  relationships before synthesis. Do not reopen a resolved property question
  as a global topology choice.

### 2. One transient MinIO fault has two incompatible PIT meanings

- **Property/Properties:** `protocol-pit-error-contract`,
  `recovery-source-missing-heals-same-process`, and
  `wildcard-live-path-survives-pit-dependency-fault`.
- **Concern:** a direct cold PIT read preserves an S3 transport/API failure as
  `EXTERNAL_SERVICE_ERROR`, which the existing workload classifies as transient.
  The full verifier instead persists nearly every non-cancellation source replay
  error as `SOURCE_MISSING`. With the proposed two-second verifier interval, one
  MinIO partition can therefore change later public behavior from a retryable
  infrastructure error into a durable repair gate without any object ever being
  absent. The catalog models direct error typing, optional-dependency isolation
  and missing-object repair separately, but never specifies or tests this
  transition.
- **Scope:** cross-property semantic gap.
- **Evidence:** `balancehistorystore.mapArchiveError` returns non-integrity,
  non-missing archive errors unchanged
  (`internal/storage/balancehistorystore/verify.go:723-738`), and the gRPC layer
  maps wrapped Smithy API/operation errors to `EXTERNAL_SERVICE_ERROR`
  (`internal/adapter/grpc/server.go:590-629`). The workload explicitly retries
  that reason (`tests/antithesis/workload/internal/client.go:407-410,462-482`).
  Conversely, `HistoryVerifier.markReplayFailure` sends every non-context,
  non-scratch replay error to `MarkSourceMissing`
  (`internal/application/balancehistory/verifier.go:868-886`). The exact table in
  `properties/protocol-pit-error-contract.md` has no external-service row, while
  `recovery-source-missing-heals-same-process` constructs actual object removal
  rather than a transport-only marker origin.
- **Suggested action:** extend `protocol-pit-error-contract` with the direct
  `EXTERNAL_SERVICE_ERROR` mapping and add a transport-only branch to
  `recovery-source-missing-heals-same-process`: partition MinIO until a verifier
  pass occurs, heal connectivity without changing objects or restarting Ledger,
  and require exact PIT recovery. Record the unresolved product decision—whether
  verifier transport errors should durably latch `SOURCE_MISSING`—without making
  the observable same-process recovery guarantee depend on that implementation
  choice.

### 3. Lease safety has no cancellation-drain liveness counterpart

- **Property/Properties:** `concurrency-pinned-view-maintenance-stability`,
  `concurrency-tier-lease-precedes-local-delete`,
  `concurrency-remote-gc-live-roots-protected`, and the resource-liveness group.
- **Concern:** the portfolio correctly requires maintenance to preserve every
  active view, but never requires canceled, deadline-expired or disconnected PIT
  requests to release their manifest, run and cold-cache leases. A leaked lease
  is monetary-safe—it prevents deletion—so all existing safety assertions can
  remain green while remote GC stays permanently blocked, local bytes cannot be
  retired and cache resources accumulate. This is not the same as compaction
  debt or a stuck S3 shutdown.
- **Scope:** missing cross-cutting lifecycle perspective.
- **Evidence:** opening a view acquires one manifest lease and one lease per run
  (`internal/storage/balancehistorystore/view.go:181-220,271-302`); lazily fetched
  cold-part leases live until `View.Close` (`:166-168,337-357`). Any active
  manifest lease blocks remote deletion because reset can reuse manifest
  versions (`internal/storage/balancehistorystore/remote_gc.go:740-776`), and run
  leases prevent tiering from removing local bytes
  (`internal/storage/balancehistorystore/tier.go:717-729`). The public controller
  does defer `HistoricalVolumeView.Close`
  (`internal/application/ctrl/controller_default.go:971-997`), but existing
  deterministic cancellation tests check error propagation, not lease counts
  plus subsequent maintenance progress. No catalog invariant states eventual
  lease drain.
- **Suggested action:** add a property such as
  `resources-canceled-pit-leases-drain`: repeatedly cancel long filtered/cold
  reads at different fetch/iteration points, keep the process alive, quiesce,
  then require lease counts to return to baseline and a previously blocked
  tier/remote-GC operation to progress. Pair the workload liveness condition
  with a small SUT reachability signal for cancellation followed by successful
  view close; underflow/double-close remains a separate safety failure.

### 4. Canonical property IDs expose discovery-agent provenance

- **Property/Properties:**
  `wildcard-agent-unfiltered-fast-path-equals-account-fold`,
  `wildcard-out-of-order-chapters-fail-closed`,
  `wildcard-out-of-order-chapters-recover`, and
  `wildcard-live-path-survives-pit-dependency-fault`.
- **Concern:** canonical slugs describe which discovery lens found a property,
  not the SUT behavior. `wildcard-agent-...` is especially unstable: rerunning
  discovery with another ensemble would assign a different identity to the same
  guarantee. These names will leak into assertion plans, reports and long-lived
  triage links.
- **Scope:** catalog mechanics and maintainability.
- **Evidence:** all four IDs appear as catalog headings, evidence filenames and
  relationship keys. The other 29 IDs use behavioral domains such as
  `integrity`, `concurrency`, `recovery`, `protocol`, `security`, `resources` or
  `lifecycle`; only these four encode discovery provenance.
- **Suggested action:** rename them coherently across catalog, evidence and
  relationships, for example
  `integrity-unfiltered-summary-equals-account-fold`,
  `integrity-out-of-order-chapters-fail-closed`,
  `recovery-out-of-order-chapters-reconverges`, and
  `lifecycle-live-path-survives-pit-dependency-fault`. Preserve assertion message
  uniqueness independently from slug names.

## Passes

- Catalog/evidence/relationship bookkeeping is otherwise exact: all 33 catalog
  slugs have one evidence file, no evidence file is orphaned, and every
  relationship property reference resolves.
- The portfolio consistently separates replica-local physical identity
  (manifest version/token/layout) from cross-replica logical equality. It does
  not incorrectly require identical tokens between independent replicas.
- The remote-GC chain—owner containment, epoch-bound inventory, root/lease
  revalidation, idempotent delete acknowledgement—is internally coherent and
  makes explicit that no downstream recovery property can compensate for an
  unsafe delete.
- The confirmed empty-HTTP-`pit` parsing gap is deliberately kept out of the
  Antithesis portfolio as deterministic regression territory. That is a sound
  framing decision, not a missing chaos property.
- Safety and liveness are separated for the out-of-order chapter topology and
  for repair/rebuild paths; fail-closed behavior is not treated as sufficient
  recovery.

## Uncertainties

- The repository does not decide whether a transient verifier transport error
  should persist `SOURCE_MISSING` or remain infrastructure-transient. The
  observable recovery requirement can be tested either way, but the desired
  intermediate error contract needs product/operations judgment.
- Tenant support for node termination, targeted/asymmetric network faults and
  clock faults remains external to this strict repository-only evaluation.
- A lease-drain property needs either a bounded internal lease diagnostic or
  an indirect maintenance-progress oracle. Whether test-only diagnostics are
  acceptable remains a catalog-wide human decision.
