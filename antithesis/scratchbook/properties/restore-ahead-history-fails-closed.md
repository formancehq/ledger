# restore-ahead-history-fails-closed — Administrative restore cannot serve a retained ahead projection

## Catalog candidate

| | |
|---|---|
| **Priority** | P0 — a violation can resurrect monetary effects that the administrative restore deliberately removed. |
| **Type** | Safety (correctness). |
| **Property** | After an administrative restore activates a primary authority older than a separately retained local PIT peer store, every PIT request fails closed until the builder has detected the rollback, invalidated the ahead projection, rebuilt and certified the restored source prefix; no successful response may expose an audit/log watermark or monetary effect beyond the restored authority. |
| **Invariant** | Workload `assert.Always(!structuredPITResponse || (success ? exactRestoredView(response, restoredHead, restoredOracle) : exactRestoreWindowFailClosedReason(error)), "pit: administrative restore never serves retained history ahead of restored authority", details)`. `Always` is required because one successful ahead response is a monetary safety failure. Transport attempts that never reach a PIT handler are inconclusive, not passes. Pair it with SUT `AlwaysOrUnreachable(!builder.Ready(), "pit: restore rollback closes retained history gate before projection reset", details)` at the reached rollback/divergence branch, plus `Reachable`/`Sometimes` coverage signals; those prevent the targeted campaign from passing without constructing the ahead state but do not replace the workload oracle. |
| **Antithesis Angle** | Preserve an ahead peer-store PVC while restoring the primary data/WAL from a deliberately older backup. Reboot in normal mode and probe the exact replica from first gRPC reachability while faults delay source-head sampling, rollback marker persistence, reset, replay, WAL sync and semantic certification. Kill/restart at each repair boundary. |
| **Why It Matters** | Administrative restore intentionally moves financial authority backward. If the replica-local derived store remains readable for even one request, a client can observe transactions that no longer exist in the restored audit chain and receive provenance that falsely presents them as authoritative history. |
| **Confidence** | High for the invariant, current startup gate and rollback mechanism; medium for immediate workload implementation because the checked-in Kubernetes restore deletes the default peer store and exposes no pre-readiness phase handshake. |

**Open Questions:**

- None.

## Scope and separation from adjacent properties

This is the retained-store safety branch of administrative disaster recovery.
It is deliberately separate from:

- `lifecycle-backup-restore-rebuilds-history`, which restores into fresh PVCs
  and proves eventual reconstruction when no old peer projection survives;
- the boot-readiness property, which requires every ordinary restart to
  reconcile a persisted manifest even when source and manifest are already at
  the same prefix; and
- `lifecycle-follower-snapshot-install-fails-closed`, which replaces a
  follower's primary store in-process from the current cluster leader. That
  checkpoint is constrained to be at least the follower's received Raft
  snapshot index and is therefore forward-only in normal operation.

Administrative restore is different: it is an offline node-level DR operation,
not a Raft order. Restore mode starts no Raft, normal API, or balance-history
runtime, writes an explicitly selected backup into a fresh primary data
directory, and then requires a normal-process restart. The restored source can
legitimately be below the peer manifest that survived on a separately
configured `--balance-history-dir`.

The repository does not currently exhibit the unsafe response. A new process's
readiness atomic starts false, `Builder.Start` clears it before asynchronous
boot, and the provider refuses PIT while it is false. This property preserves
that guarantee across the real restore/operator/storage choreography and guards
against regressions or future restore modes; it does not promote the
source-grounded race hypothesis into a claimed current defect.

## Exact safety oracle

Capture the selected backup's authoritative boundary before adding any ahead
effects:

- restored numeric ledger ID;
- restored audit sequence and audit hash;
- restored log sequence;
- exact monetary oracle at the chosen effective and insertion selectors.

After normal-mode activation, keep all writers paused. Issue direct, no-retry,
stale-consistency PIT requests to the replica whose ahead peer store was
retained, passing `minLogSequence = restoredLogHead`. A selector after the
post-backup transaction's timestamps makes the removed effect observable if the
ahead manifest leaks.

For every request that reaches the PIT handler and returns a structured result:

```go
safe := !success || (
    view.LedgerID == restoredLedgerID &&
    view.AuditWatermark == restoredAuditHead &&
    view.LogWatermark == restoredLogHead &&
    resultEquals(restoredOracle, selector)
)

assert.Always(
    safe,
    "pit: administrative restore never serves retained history ahead of restored authority",
    details,
)
```

The real implementation must also classify a reached failure through its exact
`ErrorInfo.reason`; before reconciliation, `HISTORY_BUILDING` is the current
provider outcome. `HISTORY_SOURCE_MISSING` or `HISTORY_BEHIND` remain safe
closed outcomes if a future implementation exposes the durable repair state or
watermark wait more directly. Generic `Internal`, a missing/duplicate view
trailer on success, or any live fallback is not acceptable. Transport
unavailability and a request deadline before the handler responds are recorded
as inconclusive reachability attempts.

Use a companion progress assertion only to prove the fixture recovered:

```go
assert.Sometimes(
    postRestoreExact,
    "pit: retained ahead history reconciles to restored authority",
    details,
)
```

That `Sometimes` is coverage/progress for the targeted scenario. The primary
property remains the `Always` safety assertion on every post-restore response.

## Workload and topology construction

Use an isolated restore profile derived from the existing `model` template:

1. Enable PIT and disable the PIT cold tier so the dangerous state is visibly a
   retained local projection, not an independently surviving PIT-run object.
2. Set `BALANCE_HISTORY_DIR=/data/cold-cache/balance-history`. The operator
   already mounts `cold-cache` separately from the primary data and WAL PVCs;
   the server explicitly supports a dedicated history directory.
3. Modify only this restore-ahead profile's teardown to delete/verify the
   primary data and WAL PVCs while retaining the target replica's cold-cache
   PVC. The existing fresh-restore profile must continue deleting all three.
4. Pause and drain model workers. Create a reserved ledger and seed state, then
   take a full backup into an isolated backup bucket/prefix. Record the backup
   response's last audit/log sequences and the exact reserved-ledger oracle.
5. While workers remain paused, submit a property-owned transaction with a
   unique account/asset/amount and timestamps after the backup selector. Do not
   merge this deliberately disposable write into the model state that will
   survive restore. Wait until a direct PIT response from the target replica
   includes the effect and advertises audit/log watermarks strictly above the
   saved backup boundary.
6. Stop the normal cluster, preserve the target peer-store PVC, and restore the
   specifically isolated older backup into fresh primary storage through the
   production restore-mode download/validate/finalize path.
7. Add a two-phase restore rendezvous. The current sidecar reports only after
   all normal replicas are Kubernetes Ready, which can miss the window. It must
   first signal that the restored normal-mode pod is starting/reachable, allow
   the driver to run direct probes concurrently, and only then report final
   cluster readiness/reconciliation.
8. From first direct gRPC reachability through the successful reconciled
   control, continuously apply the safety oracle above. Do not resume unrelated
   writers until the exact restored result succeeds.

An isolated backup destination is load-bearing: the existing model restore
normally appends a current incremental immediately before teardown, which
restores the latest state and therefore cannot create an ahead peer manifest.
This property must restore the frozen earlier backup selected in step 4.

## Candidate SUT instrumentation

Existing PIT instrumentation status is **missing**.
`existing-assertions.md` records no assertion in the builder, peer store, or PIT
workload.

Add three unique, surgical signals:

1. In `Builder.resetIfRolledBack`, after computing `rolledBack || diverged` and
   before `MarkSourceMissing`, emit:

   ```go
   assert.AlwaysOrUnreachable(
       !b.Ready(),
       "pit: restore rollback closes retained history gate before projection reset",
       details,
   )
   ```

   Include manifest/source audit and log watermarks and `hashDiverged` in
   details. This checks the dangerous internal boundary without making SDK
   state part of recovery authority.

2. At the same branch, emit
   `Reachable("pit: administrative restore detected retained history ahead of source", details)`.
   The workload should correlate this with the frozen backup ID/boundary and
   target node; rollback can have other causes, so the SUT message alone does
   not prove the administrative fixture.

3. After forced WAL durability, semantic `Certify`, failure-marker clearing,
   exact manifest/source-head equality and `Ready=true`, emit
   `Reachable("pit: retained restore-ahead projection rebuilt and reopened", details)`
   only when the process-local repair episode began in the rollback branch.

Do not make the provider synchronously read the primary source just for an
assertion. The workload oracle checks the public boundary, while the builder
signals identify the otherwise invisible reset/certification phases.

## Code evidence

- `docs/technical/architecture/subsystems/chapters/backup.md:113-124` defines
  restore as an offline, node-level DR operation into a fresh Pebble directory,
  followed by normal boot; it is not an in-cluster Raft command.
- `internal/bootstrap/module_restore.go:22-31,55-80` wires only the restore
  services and validates a fresh primary data directory. It starts no Raft,
  normal Bucket API, or PIT runtime.
- `internal/storage/dal/store.go:97-138` rejects a restore target containing a
  live primary database or ordinary checkpoint history, preventing an in-place
  primary rollback.
- `cmd/server/balance_history.go:34-48` and
  `internal/bootstrap/balance_history.go:105-110` allow the peer store to live
  outside the primary data directory. The default remains
  `<data-dir>/balance-history`.
- `misc/operator/internal/controller/reconcile_statefulset.go:315-356` mounts
  data, WAL, and cold-cache as distinct volumes. The current operator has no
  dedicated history volume, but the retained cold-cache mount can host the
  explicit test profile without changing the restore target's freshness.
- `tests/antithesis/workload/restore-orchestrator.sh:120-138,162-181,215-235`
  currently deletes data, WAL, and cold-cache PVCs, restores into a fresh
  primary store, and reports only after normal replicas are Ready. It therefore
  cannot reach this property until the retained-store variant and phase
  rendezvous are added.
- `tests/antithesis/workload/bin/cmds/model/singleton_driver_model/restore.go:45-77,90-129`
  already pauses and drains the exact model around restore, but exposes only a
  one-shot completion trigger and has no PIT probe during normal reboot.
- `internal/application/balancehistory/builder.go:296-333,428-434` makes
  readiness process-local and clears it before every asynchronous boot.
- `internal/bootstrap/balance_history_provider.go:41-66` refuses PIT with
  `ErrBuilding` whenever that process has not completed builder reconciliation,
  even if the retained manifest is structurally readable.
- `internal/application/balancehistory/builder.go:465-541,782-847,937-991`
  samples the restored source while readiness is false, detects lower
  audit/log watermarks or a same-sequence hash divergence, persists a
  fail-closed marker, resets the ahead projection, and restarts from genesis.
- `internal/storage/balancehistorystore/store.go:303-375,413-446` persists
  `SOURCE_MISSING` synchronously, advances the generation to invalidate pinned
  views, and atomically drops derived state while preserving the repair marker.
- `internal/application/balancehistory/builder.go:705-779` requires forced
  durability, full semantic certification, synchronous marker clearing, exact
  manifest/source-head equality and no pending repair state before setting
  `Ready=true`.
- `internal/storage/balancehistorystore/view.go:181-253,360-369` prevents new
  views while the failure marker is present and invalidates already-pinned
  views when reset changes the generation.
- `internal/application/balancehistory/builder_test.go:442-487` proves lower-head
  and same-sequence divergent-hash rebuilds. Lines 490-579 deliberately hold
  source-head reconciliation after restart and prove readiness remains false
  while the ahead manifest is still persisted, then opens only after rebuilding
  to the lower source.
- `internal/infra/state/synchronizer.go:43-116` and
  `misc/proto/snapshot.proto:19-25` implement a different in-process follower
  checkpoint path whose requested minimum applied index makes normal sync
  forward-only; it cannot construct this administrative rollback premise.

## Failure scenario

1. Backup boundary is `(audit=R_a, log=R_l)` and contains balance `B`.
2. The live cluster later commits transaction `T`; the retained target peer
   manifest is `(A_a,A_l)` with `A_a > R_a`, `A_l > R_l`, and returns `B+T`.
3. The cluster stops. Administrative restore activates the frozen backup in a
   fresh primary data directory while the target's separately mounted peer
   store remains at `(A_a,A_l)`.
4. Normal APIs become reachable before asynchronous source reconciliation
   completes. A regression preserves or reopens readiness, so a PIT request
   whose minimum is only `R_l` opens the ahead manifest and returns `B+T`.
5. The response is structurally valid but unauthoritative: transaction `T` is
   absent from the restored audit chain. The workload `Always` fails on the
   ahead watermark and monetary oracle before later rollback repair can hide
   the transient leak.

## Existing coverage and non-duplication

Deterministic tests already validate the current local mechanism: readiness is
cleared before source response, lower/hash-divergent sources cause reset, and
the final manifest matches the restored head. They do not compose restore mode,
an older backup selection, PVC retention, normal API startup, process faults,
and concurrent direct reads.

The existing Antithesis restore assertion
`singleton_driver_model: restore cycle completed` proves only that the fresh-
PVC orchestration returned. No PIT assertion observes an ahead retained store,
the rollback branch, or any post-restore historical response.

### Investigation Log

#### Can administrative restore actually leave the PIT store ahead of primary authority?

- **Examined:** restore-mode module and fresh-target validation; normal server
  module selection; backup/restore architecture and operations docs;
  `--balance-history-dir`; operator volume mounts; and both Kubernetes restore
  teardown implementations.
- **Found:** restore cannot replace a live primary in place and always requires
  normal-process restart. With the default history directory, deleting the data
  PVC also deletes the peer store, so the checked-in restore campaign exercises
  only fresh history. A configured history directory may live on a distinct
  retained volume; the existing cold-cache PVC provides such a mount in the
  test topology if the restore-ahead teardown deliberately preserves it.
- **Not found:** a dedicated balance-history volume in the Operator CRD or a
  checked-in restore profile that preserves any peer store.
- **Conclusion:** resolved as a conditional but repository-supported topology,
  not a default restore state. The property requires the explicit retained-
  directory profile above and must not claim that the current fresh-PVC campaign
  already reaches it.

#### Is this the same risk as ordinary follower snapshot installation?

- **Examined:** follower synchronizer checkpoint request/activation, snapshot
  protocol minimum index, administrative restore docs/module, and the existing
  follower property.
- **Found:** follower synchronization is in-process and requests current-cluster
  authority at or beyond a minimum applied index. Administrative restore is
  offline, restarts the process, and may select an older backup. Only the latter
  naturally produces a primary head below a retained peer manifest.
- **Not found:** an honest same-cluster follower snapshot path that can select a
  lower or hash-divergent authority.
- **Conclusion:** resolved. Keep the properties separate: follower sync checks
  the activation handoff for a forward replacement; this property checks
  rollback invalidation and removed-effect non-disclosure after DR.

#### Does the repository show a current restore-ahead leak?

- **Examined:** builder construction/start/boot/tick, provider gating,
  rollback/reset/certification code, store generation invalidation, and all
  rollback/readiness tests.
- **Found:** current code appears fail closed. Readiness is process-local and
  cleared before boot; the provider refuses the readable retained manifest;
  rollback persists a marker, invalidates views, resets, certifies and requires
  exact head equality before reopening. A deterministic test explicitly blocks
  the source-head call and proves the ahead manifest remains unservable.
- **Not found:** a runtime reproduction, a code path persisting readiness across
  process restart, or an administrative restore mode that keeps normal APIs
  active in the same process.
- **Conclusion:** this is a P0 claimed-guarantee/regression property, not a
  confirmed defect. Antithesis is justified by the multi-process mode switch,
  retained volume and narrow scheduling window, not by duplicating the local
  unit assertion.

#### What workload evidence proves the dangerous window was exercised?

- **Examined:** model pause/drain and restore trigger, sidecar request/response
  protocol, operator restore choreography, readiness probe semantics, per-node
  client behavior, backup response boundaries, and existing PIT assertion
  inventory.
- **Found:** the driver can keep model workers paused and the backup result
  carries last audit/log sequences. Direct ordinal addressing and a no-retry
  stale client can attribute responses to the retained-store replica. The
  current sidecar responds only after all pods are Ready, and the existing
  per-node helper retries `UNAVAILABLE`, so both can skip the target window.
- **Not found:** an existing pre-readiness restore phase signal, a no-retry PIT
  probe in the model template, or a SUT rollback reachability assertion.
- **Conclusion:** add the two-phase sidecar/driver handshake and the three SUT
  signals above. Require proof that the peer manifest was ahead before teardown,
  the rollback branch was reached after restore, and an exact restored PIT
  response eventually succeeded; otherwise the safety assertion is vacuous.

#### Which failures are acceptable before reconciliation?

- **Examined:** provider gate ordering, store failure/read paths, gRPC error
  mapping, PIT retry interceptor behavior, and the controller's primary-head
  minimum.
- **Found:** the current provider returns `HISTORY_BUILDING` before it consults
  the retained store whenever builder readiness is false. The rollback marker
  and reset also fail closed, while controller watermark binding alone is
  insufficient because an ahead manifest already satisfies the restored lower
  minimum. The normal retry client can hide the building response.
- **Not found:** a public restore-specific error reason or a workload API that
  exposes builder reconciliation phase.
- **Conclusion:** use direct no-retry probes. Treat structured building/behind/
  source-missing outcomes as fail closed, transport non-response as
  inconclusive, and validate every success against the exact restored boundary
  and independent oracle.
