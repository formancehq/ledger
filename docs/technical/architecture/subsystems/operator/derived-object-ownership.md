# Derived object ownership

Every Kubernetes object the operator creates for a `Cluster` has a name derived
from the Cluster name: `ledger-<name>` for the StatefulSet, the ClusterIP
Service, the Ingress and the NetworkPolicy, and `ledger-<name>-<suffix>` for
the headless and gRPC Services, the gRPC Ingress, the auth-keys ConfigMap, the
cluster Secret and the DNSEndpoints. Pods are `ledger-<name>-<ordinal>`.

These names are not unique by construction. The Service `ledger-a-headless` is
both the headless Service of Cluster `a` and the ClusterIP Service of Cluster
`a-headless`. Any other workload in the namespace can also hold a derived name.
A Cluster
recreated under the same name finds the previous Cluster's objects, still
controlled by the old UID, until garbage collection removes them.

**A name match is never proof of ownership.** An object belongs to a Cluster
only when its controller reference carries that Cluster's UID
(`metav1.IsControlledBy`). The operator never adopts, mutates, deletes, reports
or execs into a derived object that the Cluster does not control.

## Where the rule is enforced

The helpers live in `misc/operator/internal/controller/derived_object_ownership.go`.

| Path | Check | Outcome when the object is not controlled by the Cluster |
|---|---|---|
| Every `CreateOrUpdate` mutate function | `setOwnedControllerReference`: an existing object (non-empty `resourceVersion`) must already be controlled by the Cluster | `errResourceNameConflict`; `CreateOrUpdate` writes nothing |
| Reads that drive decisions before any write: the RC upgrade check, TLS state and cluster Secret (`fetchExistingStatefulSet`), and the selector-drift guard | Ownership of the StatefulSet and Services read | `errResourceNameConflict`; another owner's image, TLS mode or selector is never treated as this Cluster's |
| Name-based prunes of disabled objects (`deleteIfExists`, DNSEndpoint prune) | Ownership on the read, then a UID-preconditioned delete (`deleteInspected`) | Object left in place; logged |
| Auth-keys ConfigMap and cluster Secret cleanup | Same as above | Object left in place; logged |
| Reading the previous auth-keys ConfigMap for key carry-forward | Ownership on the read | `errResourceNameConflict`; foreign keys are never carried into the trust set |
| VolumeClaimTemplate orphan-and-recreate delete of the StatefulSet | Ownership on the read, then `deleteInspected` with orphan propagation | `errResourceNameConflict`; nothing deleted |
| Status readiness (`updateStatus`) | Ownership of the StatefulSet | Treated as absent; readiness of another runtime is never reported |
| Ledger, ledger-index and EventSink `ledgerctl` execs | `fetchClusterStatefulSet` (StatefulSet controlled by the Cluster UID), then `ownedPodExec` (pod-0 controlled by that StatefulSet UID) | `errClusterRuntimeAbsent`, `errResourceNameConflict` or `errPodNotOwned`; nothing executed |
| Annotated drain (`ledger.formance.com/drain`) preflight, barrier and release | `fetchClusterStatefulSet` for the live replica count, gRPC port and TLS mode, then `ownedPodExec` per member pod | `errClusterRuntimeAbsent` requeues; `errResourceNameConflict` or `errPodNotOwned` fail the step; no marker is proposed or released |
| Raft scale-down execs | `ownedPodExec` on the StatefulSet just written by the guarded mutate function | `errPodNotOwned`; replicas are not reduced |
| Volume-expansion disk probe | StatefulSet ownership, then `ownedPodExec` per pod | Expansion refused (`ResourceNameConflict` reason) or the measurement fails for that pod |
| BackupRun Job creation | StatefulSet ownership (TLS mode) and ClusterIP Service ownership (`requireClusterService`, an uncached read of the address the Job dials) | No Job created; the run retries |

Ownership is checked per object, not inherited. A StatefulSet the Cluster
controls does not make every `ledger-<name>-<ordinal>` pod its own: during
garbage collection of a deleted same-name Cluster, pod-0 can still belong to
the old StatefulSet, and after the orphan-and-recreate path a pod has no
controller until the new StatefulSet adopts it.

## Reporting a conflict

`ClusterReconciler.Reconcile` turns any `errResourceNameConflict` returned by a
reconcile step into:

- `ResourceNamesAvailable=False`, reason `NameConflict`, with the offending
  object and its controller in the message;
- phase `Error` and a `ResourceNameConflict` warning event;
- a requeue after one minute. Nothing else enqueues the Cluster, because it
  does not own the conflicting object.

A pass that reconciles every derived object, StatefulSet included, sets
`ResourceNamesAvailable=True`, reason `Owned`. A pass that stops earlier (an
invalid spec, the auth-keys fail-safe) leaves the condition as it was. A
conflicting StatefulSet or primary Service is detected by the reads at the start
of the pass, before any write. Other objects are detected where they are
written, so steps that ran earlier in the same pass have already been applied:
for example, a foreign cluster Secret is reported after the Services were
reconciled, and the StatefulSet is not reconciled in that pass.

Cross-CRD controllers report the refusal on their own resource: the Ledger
`Synced` or `IndexesSynced` condition carries the error, an EventSink reports
`Synced=False` with reason `ResourceNameConflict`, and volume expansion reports
the reason on its per-volume condition.

A deleting Ledger or EventSink whose Cluster has no StatefulSet of its own
releases its finalizer without exec, as it does when the Cluster is missing.
Ledger deletion remains best-effort: the external ledger may remain. This now
also applies while pod-0 has no controller, in the short window after the
VolumeClaimTemplate orphan-and-recreate path and before the new StatefulSet
adopts it; the StatefulSet-absent window just before it already behaved this
way.

## Accepted races

- **Delete identity only.** `deleteInspected` pins the delete to the UID read
  during the ownership check. A replacement created under the name between the
  read and the delete makes the delete fail with `Conflict`; the next pass
  re-inspects ownership. The precondition does not pin the
  `resourceVersion`, so an ownerReference change on the same object between
  the read and the delete is not detected. A `resourceVersion` precondition
  would turn every status write by another controller into a spurious
  conflict.
- **Exec target.** The exec API has no UID precondition. A pod replaced between
  `ownedPodExec`'s uncached read and the exec stream is not detected; the window
  is one API round trip.
- **Backup Service.** `requireClusterService` reads the Service uncached, so a
  replacement that happened before Job creation is refused even when the
  informer cache still holds the old object. A Service replaced after that
  read and before the Job dials it is not detected.

## Behaviour changes for existing deployments

Every object a released operator created already carries the Cluster's
controller reference, so upgrading the operator does not change how existing
Clusters reconcile. Two user-driven situations that used to be silently
adopted now stop with `ResourceNamesAvailable=False`:

- A Cluster recreated after `kubectl delete cluster --cascade=orphan` finds the
  orphaned objects without a controller reference. Delete them (keep the PVCs)
  or restore their owner references to the new Cluster UID.
- A pre-existing ServiceAccount named `ledger-<name>` while
  `spec.serviceAccount.create` is unset or `true`. Set
  `serviceAccount.create: false` and `serviceAccount.name` to use it.

Persisted state, Raft entries, snapshots, backups and audit records are
unchanged. The only API addition is the `ResourceNamesAvailable` status
condition.
