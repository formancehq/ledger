# Operator backup scheduling

The Kubernetes `Backup` controller schedules `BackupRun` resources; the
`BackupRun` controller executes them through Jobs. This is control-plane
operational state, outside Ledger's audited store and checkpoint backup data.
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
The resource requires `spec` on creation and update, so removing the whole
object cannot bypass the nested transition rule and permit a different parent
on re-addition. This prevents transferring an existing Job's reservation or
result to another Backup. Create a new run to select another parent. Metadata
and status updates remain supported. The event mapper defensively enqueues both observed references
on updates; admission rejects reference edits before they can produce such an
event. Existing Jobs keep their persisted configuration.
Parent lists remain direct API-server reads; sibling lists retain their existing
informer consistency. Both use a same-namespace `spec.backupRef` field selector.
Both shipped CRDs declare `.spec.backupRef` in `selectableFields`, requiring
Kubernetes 1.32 or later; install the updated CRDs before starting the operator.
The manager registers the matching field index before starting the BackupRun
controller. A cache-only index cannot replace the direct parent list required
to protect durable summaries from informer lag.

Direct responses and controller processing are proportional to the selected
parent's runs, rather than all runs in the namespace. Server-side selection does
not guarantee an indexed storage lookup or O(1) API-server work. The informer
still watches namespace-wide runs and maintains the parent index; cached sibling
queries return only that parent's runs. History limits reduce terminal history
but do not bound active or externally retained runs, so large per-parent
histories still increase reconciliation cost.

The association change does not provide atomic execution exclusion across
concurrent reconcilers; it preserves the existing Running-reservation policy.

### Historical parent-admission upgrade

This procedure applied to the earlier RC parent-admission change. It is not an
alternative to the current checkpoint-only CRD migration, which requires
deleting old BackupRun and Backup resources before installing the new CRDs.
Follow [the current migration steps](../../../../ops/backup-restore.md#crd-migration-for-checkpoint-only-scheduling)
when upgrading to checkpoint-only scheduling.

The rule freezes the currently stored reference on existing runs; it cannot
recover a reference changed before installation. Stop the operator before
updating the CRDs and inventory existing runs and their owned Jobs. Recover
any known original parent from execution evidence, correct the run reference
before installing the rule, and reconcile any parent summaries/cursors already
attributed to the wrong Backup. Include spec-less runs admitted by older CRDs:
recover their original `spec.backupRef` from owned Jobs or other execution
evidence and restore the spec before installing the required-spec schema. Do
not assign a new parent merely to make validation pass. If identity is ambiguous,
retain the run and Job evidence and resolve it before resuming the operator; do not guess or delete
execution evidence. Install the matching base/Helm CRDs before restarting the
operator. Unchanged runs need no rewrite. This affects Kubernetes control-plane
state only; it changes no Ledger persisted format, backup data, service RPC or
protocol revision.

## Retention must not reset the schedule (EN-2000)

History limits control retained Kubernetes execution records. Deleting those
records must not cause an early execution or disable the checkpoint schedule.
Previously the scheduler derived its only execution cursor from retained
terminal children. Zero successful or failed retention erased that cursor,
so the next reconciliation, including after restart, ran immediately.

`Backup.status.lastRunCompletionTime` retains the greatest observed
terminal completion time for both successful and failed runs,
including manual runs. They never move backwards when the remaining history
contains only older completions. They advance even when a schedule is disabled.
The first execution is immediate when no completion has been observed; later
executions use the first cron occurrence strictly after the completion cursor.
A changed cron expression is evaluated against the same cursor.

`lastBackup` is the last successful checkpoint result; failed runs never
manufacture a summary. `BackupRun.status.result` holds the corresponding
per-run result. Successful results also persist the completion time in Unix
nanoseconds. Kubernetes serializes the public completion timestamp to seconds;
the nanosecond value lets the parent keep the newer success when two runs finish
in that second, including after the older run has been pruned. `BackupHealthy`
reports the latest observed terminal outcome,
including failures after an earlier success. The persisted completion cursor
prevents an older retained run from reversing that condition after history
pruning, even when either history limit is zero. A failure wins when completion
timestamps tie; an equal-time success cannot clear a recorded failure after
pruning. A later completion can restore healthy status. `spec.schedule` is the optional
checkpoint cron expression.
`nextBackupTime` is the derived next deadline. It is not a scheduling cursor:
while a run is active it describes the next
cron occurrence after the current reconciliation time.

## Reconciliation and failure ordering

1. Read the Backup directly from the API server, validate the Cluster and cron
   expression, then list child runs directly from the API server. A failed direct
   read stops reconciliation before any scheduling or pruning.
2. Refresh the successful-result summary and health condition against the
   persisted completion cursor, then advance that cursor monotonically from
   the observed children.
3. Evaluate the checkpoint schedule and create a due run. Any Pending, Running,
   or not-yet-initialized child prevents another scheduled run. The BackupRun
   controller separately gates execution on Running siblings.
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

The CRD schema and generated deepcopy code carry the single cursor and result
fields, including the Helm CRD copy. Earlier RC operator CRD shapes are
intentionally unsupported; see the migration steps in `docs/ops/backup-restore.md`.
This changes no Ledger service RPC or protocol revision.

## Regression evidence

`misc/operator/internal/controller/backup_scheduling_test.go` combines daily
scheduling, zero retention, successful and failed completion, and fresh
reconciler instances before and at the next deadline. Additional cases cover
write/delete failures, active runs, disabled
and changed schedules, older retained history, and a parent informer lagging
behind child deletion.

## BackupRun execution decisions

The controller derives the next action from the run and its deterministically
named Job on each pass. `Running` is the durable reservation for one
execution under a Backup. The decisions below are local to the controller;
they do not add CRD phases.

| Observation | Next action |
|---|---|
| A sibling has reserved execution | Keep this run pending and requeue. |
| The run is eligible and not yet reserved | Persist `Running` before looking up or creating the Job. |
| The reserved run has no visible Job | Try to create it under the same name. If the Create response is uncertain, keep the reservation and look up the same name again. A Job owned by another run does not count as execution evidence. |
| The owned Job has neither a recognized success nor a failure condition | Wait for the Job watch and clear any previous provisioning diagnostic. |
| The owned Job reports failure | Record the run's failure. |
| The owned Job reports completion or that its success criteria were met | Read the result and persist it together with the run's terminal status. Until that write succeeds, the stored run still holds the reservation. |

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
checkpoint runs, one-time Pod-list errors and missing Pods, fresh
reconcilers, sibling exclusion, parent accounting and zero
retention. `backuprun_result_logs_test.go` exercises fallback log opening and read
failures followed by complete valid summaries. The completion-write regression
restarts reconciliation after the terminal status update fails and verifies that
the existing Job supplies the result without a new Job being created. These client/HTTP fixtures prove
controller decisions, not kubelet execution or Kubernetes garbage collection.
