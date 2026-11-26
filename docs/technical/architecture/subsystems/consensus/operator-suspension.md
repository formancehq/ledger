# Reversible operator suspension

## Requirement

Disabling a Stack that uses Ledger v3 must stop its compute without deleting
committed business data, audit history, Raft state or the Cluster identity.
Re-enabling must start the original voters on their original volumes. A failed
prerequisite must leave the Pods running with a visible blocked status.

## Ownership and state

`Cluster.spec.disabled` is intent. `Cluster.status.suspension` is the durable
operation checkpoint; the StatefulSet, Pods and PVCs are observed effects.
The Cluster CR and its `clusterID` remain owned by the Stack Ledger module.
The StatefulSet remains owned by the Cluster. `spec.replicas` remains the
original Raft voter count while `StatefulSet.spec.replicas` is temporarily zero.
The normal scale-down function is never called: it removes Raft members and
deletes PVCs.

`Quiescing` commits replicated maintenance mode before reading a leader commit
barrier. `Barrier` checks each original voter reports maintenance and a
durably persisted Raft index through the barrier. It confirms the complete
voter set from the current leader, since followers do not expose that list.
If `disabled` is cleared before scale-to-zero, `Barrier` still records and
verifies that durable index, then proceeds directly to `Verifying` with the
existing Pods. This lets the operator reopen write admission after an early
cancel without stopping healthy voters.
`Stopping` is persisted before the operator
detaches stale StatefulSet owner references from PVCs and requests zero
replicas with both retention policies set to `Retain`. The operator reports
`Suspended` only when the StatefulSet and Pod list both show zero. A failed
gate reports `SuspensionBlocked` and retries; before the zero update it leaves
all Pods in place.

`Starting` checks each original PVC is still bound, refreshes the retained
auth-key ConfigMap from current Credentials, then restores only the
original replica count on the pinned StatefulSet template. `Verifying` waits
for all replicas and the original voter set with an elected leader. It clears
maintenance only if this suspension enabled it; `Opening` confirms every
member has observed the reopened write gate before reporting `Running`. A new
suspend request during resume returns to quiescing. Image, replica and storage
configuration changes are deferred until `Running`.

The Raft WAL synchronizes entries before they are applied. Pebble's apply
batches use `NoSync`, but the Raft WAL is compacted only after Pebble's WAL is
synchronized. A power loss after the barrier therefore recovers from Pebble
or replays retained Raft entries. The operator never treats a Pod Ready probe
alone as proof that the cluster has elected a leader.

## Boundaries

hostPath and PVC `Delete` retention policies are rejected. Running BackupRuns
must complete before stopping; pending and scheduled runs wait for resume.
EventSink, declarative Ledger and volume-expansion controllers also wait.
BackupRun admission records a reservation in Cluster status. The reservation
and suspension intent use the same resource version, so a pending run cannot
pass the stop barrier after it has been closed. An admitted run may finish
before scale-to-zero; terminal or deleted runs cannot start a new Job.
Ledger v2 uses its existing Stack disable behavior. A Ledger v3 preview beside
v2 follows this suspension protocol for its own Cluster.

Rollout order is CRD, suspension-aware Ledger operator, then Stack operator.
Rollback to an old operator while a Cluster is suspended is unsafe: an old
Ledger operator can scale up without the gate, and an old Stack operator can
delete the Cluster. Resume fully before such a rollback.
