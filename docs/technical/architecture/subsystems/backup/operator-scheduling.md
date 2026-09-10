# Operator backup scheduling

The Kubernetes `Backup` controller schedules `BackupRun` resources; the
`BackupRun` controller executes them through Jobs. This is control-plane
operational state, outside Ledger's audited store and exported backup data.
Restoring Ledger data into another cluster does not restore these Kubernetes
objects or their scheduling cursors.

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

1. Read the Backup, validate the Cluster and cron expressions, then list child runs.
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
This ordering protects controller-managed history deletion; it does not make
external deletion of execution evidence transactional with reconciliation.

The CRD schema and generated deepcopy code must carry both cursor fields,
including the Helm CRD copy. This pre-release API change has no migration or
legacy-format fallback. It changes no Ledger service RPC or protocol revision.

## Regression evidence

`misc/operator/internal/controller/backup_scheduling_test.go` combines daily
scheduling, zero retention, successful and failed completion, and fresh
reconciler instances before and at the next deadline. Additional cases cover
write/delete failures, active runs, independent incremental progress, disabled
and changed schedules, and older retained history.
