# Operator backup scheduling

The Kubernetes `Backup` controller schedules `BackupRun` resources; the
`BackupRun` controller executes them through Jobs. This is control-plane
operational state, outside Ledger's audited store and exported backup data.
Restoring Ledger data into another cluster does not restore these Kubernetes
objects or their scheduling cursors.

## Manual run association

`BackupRun.spec.backupRef` identifies the parent Backup in the same namespace.
It is authoritative for scheduling, result summaries, completion cursors,
history pruning, and sibling execution exclusion. Directly applied runs need
neither an association label nor a Backup owner reference. A missing or
contradictory label does not exclude a run from its referenced parent's history
or associate it with another parent. Existing terminal manual runs are included
without metadata repair. Scheduled runs retain their labels and owner references;
manual runs are not adopted, and Kubernetes garbage collection still follows
their actual owner references.

BackupRun create, update and delete events enqueue the referenced parent,
including unowned manual runs. API-server CEL transition validation makes
`spec.backupRef` immutable from creation, including Pending and terminal runs.
This prevents transferring an existing Job's reservation or result to another
Backup. Create a new run to select another parent. Metadata and status updates
remain supported. The event mapper defensively enqueues both observed references
on updates; admission rejects reference edits before they can produce such an
event. Existing Jobs keep their persisted configuration.
Parent lists remain direct API-server reads; sibling lists retain their existing
informer consistency. Both list runs in the namespace and filter by the spec,
so response size and filtering cost are O(N) in namespace BackupRuns, rather than
only label-selected children. A cache-only field index cannot replace the direct
parent list required to protect durable summaries from informer lag. History
limits reduce terminal history but do not bound active or externally retained
runs. Large namespace histories therefore increase reconciliation cost.

The association change does not provide atomic execution exclusion across
concurrent reconcilers; it preserves the existing Running-reservation policy.

### Upgrading parent admission

The rule freezes the currently stored reference on existing runs; it cannot
recover a reference changed before installation. Stop the operator before
updating the CRDs and inventory existing runs and their owned Jobs. Recover
any known original parent from execution evidence, correct the run reference
before installing the rule, and reconcile any parent summaries/cursors already
attributed to the wrong Backup. If identity is ambiguous, retain the run and
Job evidence and resolve it before resuming the operator; do not guess or delete
execution evidence. Install the matching base/Helm CRDs before restarting the
operator. Unchanged runs need no rewrite. This affects Kubernetes control-plane
state only; it changes no Ledger persisted format, backup data, service RPC or
protocol revision.

## Retention must not reset the schedule (EN-2000)

History limits control retained Kubernetes execution records. Deleting those
records must not cause an early execution or disable an incremental schedule.
Previously the scheduler derived its only execution cursor from retained
terminal children. Zero successful or failed retention erased that cursor,
so the next reconciliation, including after restart, ran immediately.

`Backup.status.lastFullRunCompletionTime` and
`Backup.status.lastIncrementalRunCompletionTime` retain the greatest observed
terminal completion time per type, for both successful and failed runs,
including manual runs. They never move backwards when the remaining history
contains only older completions. They advance even when a schedule is disabled.
The first execution is immediate when no completion has been observed; later
executions use the first cron occurrence strictly after the completion cursor.
A changed cron expression is evaluated against the same cursor.

`lastFullBackup` and `lastIncrementalBackup` remain successful-result summaries;
failed runs never manufacture those summaries. Incremental scheduling requires
a successful full summary, which survives pruning. `nextFullBackupTime` and
`nextIncrementalBackupTime` remain derived next-deadline information, cleared
when their schedule is absent (or the incremental prerequisite is missing).
They are not scheduling cursors: while a run is active they describe the next
cron occurrence after the current reconciliation time.

## Reconciliation and failure ordering

1. Read the Backup directly from the API server, validate the Cluster and cron
   expressions, then list child runs directly from the API server. A failed direct
   read stops reconciliation before any scheduling or pruning.
2. Refresh completion cursors monotonically and successful-result summaries in
   memory from the observed children.
3. Evaluate schedules and create due runs. A same-type Pending, Running, or
   not-yet-initialized child prevents another scheduled run. The BackupRun
   controller separately gates execution on Running siblings across both types.
4. Persist the complete Backup status, including cursors, summaries, next
   deadlines, phase and conditions.
5. Only after that write succeeds, prune terminal children exceeding the
   configured history limits.

If scheduling or the status write fails, no history is pruned. Already-created
runs remain in Kubernetes and block duplicate scheduling while active; completed
runs remain available to reconstruct the cursor on retry. If pruning fails or
the controller stops during pruning, the status is already durable and any
remaining excess children are retried on the next reconciliation. Restarting
a controller after successful pruning recovers the cursor from Backup status.
The uncached Backup read is required: the child informer may observe deletion
before the Backup informer observes the status write. Reading a stale nil cursor
from that cache would create a run before the eventual status conflict, even
though the cursor is durable in the API server. The child list also bypasses the
informer cache: an older successful run still visible there must not overwrite
a newer durable result summary read from the API server. These sequential reads
are not a cross-resource transaction; a concurrent parent status update still
causes the final status write to conflict and retry.

This ordering protects controller-managed history deletion; it does not make
external deletion of execution evidence transactional with reconciliation.

The CRD schema and generated deepcopy code must carry both cursor fields,
including the Helm CRD copy. This API change preceded `v3.0.0-rc.1` and had no
migration from earlier development formats. It changes no Ledger service RPC or
protocol revision.

## Regression evidence

`misc/operator/internal/controller/backup_scheduling_test.go` combines daily
scheduling, zero retention, successful and failed completion, and fresh
reconciler instances before and at the next deadline. Additional cases cover
write/delete failures, active runs, independent incremental progress, disabled
and changed schedules, older retained history, and a parent informer lagging
behind child deletion.

## Completed Job result recovery

A successful Job is execution evidence; the run becomes Succeeded only after its
summary is acquired, parsed, and persisted with the terminal status. A Pod-list
failure, absent Pod, or fallback log open/read failure leaves the existing Running
reservation intact. The reconciler writes a changed diagnostic and returns the
acquisition error for controller-runtime backoff. If that write fails, the returned
error retains both causes and the stored run remains nonterminal. Repeated unchanged
diagnostics do not rewrite status. A fresh reconciler retries acquisition from the
same owned Job, without rerunning the backup, and siblings remain Pending.

On recovery, the result, Succeeded phase, completion timestamp and cleared diagnostic
are persisted in one run status update. A failed update leaves the stored run
nonterminal and retries acquisition. The parent then captures the successful summary
and completion cursor before pruning as described above. Failed-history retention
zero cannot prune a run awaiting acquisition; successful-history retention zero can
prune it after the parent records the recovered result. Acquired malformed payloads
and failed Jobs remain definitive terminal failures. There is no automatic deadline
or resubmission for permanently unavailable result evidence, and historical Failed
runs are not reopened.

The result-recovery regressions in `backuprun_result_recovery_test.go` exercise
full and incremental runs, one-time Pod-list errors and missing Pods, fresh
reconcilers, sibling exclusion, parent accounting, incremental eligibility and zero
retention. `backuprun_result_logs_test.go` exercises fallback log opening and read
failures followed by complete valid summaries. These client/HTTP fixtures prove
controller decisions, not kubelet execution or Kubernetes garbage collection.
