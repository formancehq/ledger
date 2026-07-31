# lifecycle-backup-restore-rebuilds-history — Backup restore reconstructs PIT from restored authority

**Focus:** Lifecycle transitions

**Priority:** P0

**Type:** Liveness

**Confidence:** High. The checked-in `model` template, fresh-PVC restore
choreography and independent cold-archive contract define one unambiguous
campaign topology.

## Property

After a full-plus-incremental backup is restored into fresh ledger PVCs, all
authoritative chapter archives referenced by the restored primary state remain
available, and the server returns in normal mode with PIT enabled, history
eventually rebuilds from the restored hot+cold audit/log prefix and serves the
exact backup-boundary monetary state without relying on any pre-restore PIT
projection.

**Campaign prerequisites:**

- Extend the existing `model` template and its restore-orchestrator sidecar; do
  not create a third `pit_restore` template. The model template already owns the
  only full backup/teardown/restore timeline and the independent monetary oracle.
- Set `BALANCE_HISTORY_ENABLED=true`. Keep
  `BALANCE_HISTORY_COLD_TIER=false` for this property so every post-restore PIT
  result demonstrably comes from a rebuilt local projection, not a surviving
  replica-owned PIT-run object.
- Persist MinIO `/data` on a PVC that the ledger teardown does not delete. The
  `backups` bucket and the authoritative `archives` bucket must survive MinIO
  process/pod restarts; total object-store loss is outside the premise.
- Before the full backup, create a chapter containing modelled monetary effects,
  close it, request archival, and wait until the production archiver reports the
  chapter `ARCHIVED`. Do not issue `ConfirmArchiveChapter` from the workload.
  Verify a real cold read before taking the full backup, then produce additional
  hot writes so the incremental export is non-empty.

## Invariant and assertion rationale

Use `Sometimes(restoredPITExact)` named
`pit: backup restore rebuilt exact historical projection`, evaluated after the
existing restore-cycle completion anchor and before model workers resume.
`restoredPITExact` requires both:

1. an exact PIT result at a selector inside the genuinely archived chapter; and
2. an exact result at the quiesced incremental-backup boundary,

with ledger ID, audit/log watermark and monetary aggregates matching snapshots
captured from the independent model. Poll through fail-closed rebuilding states
with a no-retry PIT client; any successful but divergent response is a safety
failure, not convergence. `Sometimes` is appropriate because rebuild is eventual
and a green run must demonstrate successful post-restore service.

Distinct SUT outcome candidates are `pit: normal boot found no restored peer projection and started genesis rebuild` and `pit: restored audit prefix certified for pit reads`.

The generic fail-closed startup invariant is owned by
`boot-readiness-reconciles-persisted-history`; retained/ahead local history is
owned by `restore-ahead-history-fails-closed`. This property is the
fresh-PVC restore liveness branch only. Missing authoritative archives are
outside its prerequisite and belong to the source-missing safety scenario.

## Antithesis angle

Extend `singleton_driver_model` rather than adding a new template. Change the
orchestrator's current eager startup full backup into a phase reached only after
the driver has produced and verified the archived-chapter fixture. Take that
full checkpoint, resume modelled writes, then quiesce, snapshot the model/PIT
expectations, take the non-empty incremental export, and use the existing
verified teardown of every ledger PVC. Restore through the isolated restore
process, relaunch normally, emit the existing restore-completion anchor, and
keep workers paused while a no-retry PIT probe converges to both exact expected
results. Resume ordinary model traffic only after the assertion succeeds.

Inject object-store and ledger-process faults before backup-manifest
publication, during download, during `RebuildDelta`, and during the subsequent
hot+cold PIT backfill. MinIO process faults are valid only after `/data` is on a
durable PVC; deleting that PVC is a different total-storage-loss campaign.

## Impact

Operators reasonably expect disaster recovery to restore historical query behavior. Accidentally treating the replica-local projection as backup authority could resurrect state beyond the backup point or make PIT permanently unavailable after an otherwise valid restore.

## Code evidence

- `internal/infra/backup/manager.go:55-81` checkpoints the primary `*dal.Store` passed to the backup manager; the separate balance-history store is not included.
- `internal/bootstrap/balance_history.go:105-110` places the PIT peer store under a separate `balance-history` directory by default.
- `internal/infra/backup/restore.go:105-141` restores exports and rebuilds primary derived state from checkpoint boundaries.
- `internal/application/balancehistory/source_hotcold.go:24-47` reconstructs the history source from archived chapters plus the primary-store tail and fails closed when an encountered archive is unavailable.
- `internal/bootstrap/module_restore.go:22-31,55-80` runs only restore services
  and calls `dal.ValidateFreshRestoreTarget`; it does not construct Raft, normal
  APIs, or PIT. `internal/storage/dal/store.go:97-138` rejects live primary-store
  artifacts in the target directory.
- `cmd/server/server.go:325-355` chooses either `RestoreModule` or the normal `Module`, and gates cold-storage runtime wiring in restore mode.
- `docs/technical/architecture/subsystems/chapters/backup.md:139-142` and
  `docs/ops/backup-restore.md:134-147,580-589` explicitly exclude cold storage
  from the backup artifact and require both backup and chapter archives for a
  complete historical restore.
- `tests/antithesis/workload/Dockerfile:44-85` maps each command group directly
  to an Antithesis template; `singleton_driver_model/main.go:7-13` says the
  `model` template owns its whole timeline.
- `tests/antithesis/workload/bin/cmds/model/singleton_driver_model/restore.go:16-23,96-129`
  already quiesces the exact model around the restore and emits the completion
  anchor, but its post-restore checks do not include PIT.
- `tests/antithesis/workload/restore-orchestrator.sh:120-138,191-235` deletes
  and verifies absence of every ledger WAL/data/cold-cache PVC before restoring
  the full-plus-incremental backup. This necessarily removes the default
  `<data-dir>/balance-history` projection.
- `tests/antithesis/k8s/cluster.yaml:40-78` configures the authoritative MinIO
  backend but does not enable PIT; `cmd/server/balance_history.go:34-48` confirms
  PIT is opt-in and its local cold tier is independently opt-in.
- `tests/antithesis/k8s/minio.yaml:1-43` mounts no persistent volume at `/data`.
  The current Kubernetes harness therefore does not yet satisfy this property's
  archive-preservation prerequisite, unlike Compose's named `minio-data` volume
  at `tests/antithesis/config/docker-compose.yaml:95-106,220-222`.
- `internal/infra/state/archiver.go:125-244` verifies the cold object before the
  production archiver proposes `ConfirmArchiveChapter`; the existing main-
  template chapter driver manually confirms and must not be reused as the
  restore fixture.

## Existing assertion cross-reference

`singleton_driver_model: restore cycle completed` already proves orchestration reachability. `backup completed successfully` and incremental-backup assertions cover artifact production in the other template. `existing-assertions.md` explicitly notes that one Antithesis timeline runs only one template and that no PIT assertions exist. The proposed correctness assertions are **missing**.

## Open questions

None.

### Investigation Log

- **Template choice — examined:** workload image template construction, the
  `model` command layout, restore rendezvous and the Kubernetes sidecar wiring.
  **Found:** the existing `model` template is deliberately isolated, owns the
  exact oracle, and is the only template whose driver quiesces around the full
  restore cycle. **Conclusion:** extend `model`; a `pit_restore` template would
  duplicate both oracle and orchestration without adding a needed isolation
  boundary.
- **PVC lifecycle — examined:** restore-mode freshness validation, the Operator
  restore flag, the Antithesis teardown script, default/custom PIT directory
  placement, and the ahead-history rollback property. **Found:** the current
  harness always deletes and verifies absence of all ledger PVCs, including the
  data PVC containing the default PIT store. A separately configured retained
  PIT directory is possible outside this harness, but its fail-closed behavior
  is already the scope of `restore-ahead-history-fails-closed`.
  **Conclusion:** this liveness property tests only the fresh-PVC branch.
- **Archive contract — examined:** backup manager input, restore/rebuild path,
  hot+cold source, backup architecture and operator documentation, Kubernetes
  and Compose MinIO manifests. **Found:** the product contract explicitly does
  not copy cold storage into a backup; authoritative chapter archives are an
  independent DR prerequisite. The Kubernetes harness creates separate
  `archives` and `backups` buckets but its MinIO `/data` is currently ephemeral.
  **Conclusion:** add durable MinIO storage and preserve the archive PVC across
  ledger teardown; do not weaken the property into claiming backup includes it.
- **Not found:** checked-in PIT enablement for the Antithesis cluster, a durable
  Kubernetes MinIO volume, a production-archiver-generated cold fixture in the
  `model` template, or a model-side PIT probe. These are concrete implementation
  gaps listed in the prerequisites above, not unresolved product decisions.
