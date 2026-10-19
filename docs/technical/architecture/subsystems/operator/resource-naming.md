# Resource naming and ownership

Every Kubernetes object the operator derives from a `Cluster` shares one base
name. `clusterBaseName` in `misc/operator/internal/controller/names.go` is the
single source of truth: `spec.resourceName` when set, otherwise
`ledger-<metadata.name>` (`prefixedName`, EN-1319). Operator-facing names and
the resulting objects are listed in
[`docs/ops/deployment.md`](../../../../ops/deployment.md#resource-names).

## Derivation

All derived-name helpers (`headlessServiceName`, `grpcServiceName`,
`grpcIngressName`, `authKeysConfigMapName`, `clusterSecretName`,
`dnsEndpointName`, `podName`, `pvcName`, `serviceAccountName`) take the
`*Cluster`, never a CR name string, so the compiler forces every caller —
including in-pod addresses (Raft advertise/bootstrap, `ledgerctl --server`,
disk-usage probes), backup Job targets and status endpoints — through the
override. `prefixedName` remains only for objects that are not derived from a
Cluster base name (Credentials Secrets, backup Jobs).

Cross-CRD controllers hold only a Cluster reference name, so they load the
Cluster before deriving anything:

- the Ledger controller reads it through the dynamic client
  (`resolveEndpoint`/`resolveCluster`) and converts it to the typed object;
- the EventSink and BackupRun controllers read it through the typed client;
- an EventSink whose Cluster is already deleted cannot recompute the base name,
  so it finds the lingering StatefulSet by owner reference (GC window) or, once
  orphaned, by the operator's `managed-by` and `instance` labels together with
  the default name `ledger-<clusterRef>`, and only while it has no controller
  reference. The `instance` label alone is not trusted because
  `spec.additionalLabels` can override it; an orphaned StatefulSet of a Cluster
  that used `spec.resourceName` therefore does not hold the sink finalizer, and
  neither does another Cluster's StatefulSet that took the default name through
  `spec.resourceName` and the deleted Cluster's `instance` label.

The kubectl plugin mirrors the rule in `cmdutil.ClusterBaseName`.

`spec.resourceName` is create-time only. The CRD enforces a DNS-1035 pattern,
`maxLength: 52`, a field-level `self == oldSelf` rule and a
`ClusterSpec`-level `has(self.resourceName) == has(oldSelf.resourceName)`
transition rule, because a field rule alone does not run when the field is
added or removed. The root schema requires `spec` (`Cluster.Spec` has no
`omitempty`): a transition rule only runs when both the old and the new object
have the field it sits on, so an optional `spec` could be removed and re-added
with another `resourceName`, `clusterID`, `bindAddr`, `walDir` or `dataDir`.

The 52-character cap (`statefulSetNameMaxLength`) comes from the StatefulSet
controller, not DNS: it labels every pod with
`controller-revision-hash=<statefulset>-<hash>` (hash up to 10 characters), and
a label value is capped at 63 characters, so a longer StatefulSet name prevents
pod creation (kubernetes#64023). It is the tightest bound on any derived name
(`<base>-headless` is then at most 61 characters). `validateSpec` enforces it on
`clusterBaseName` for both the override and the default, which limits a
`metadata.name` without override to 45 characters (`52 - len("ledger-")`). The field is in the spec hash only when set, so Clusters
without it keep their pod-template hash.

`metadata.name` stays the `app.kubernetes.io/instance` label value
(`selectorLabels`), so `validateSpec` also caps it at 63 characters whatever the
override. This bound is not in the CRD: a root CEL rule on `metadata.name` also
runs on every update, status writes included, so a longer-named Cluster
admitted by `v3.0.0-rc.1` could no longer even report `ConfigValid=False`.
Making the rule create-only needs `optionalOldSelf`, which depends on the API
server's CRD validation ratcheting support.

Clusters admitted by `v3.0.0-rc.1` with a 46- or 47-character name (a 53- or
54-character StatefulSet) can be healthy: the revision hash in
`controller-revision-hash` is a decimal `uint32` of 1 to 10 digits, short enough
for about 23% of revisions at 53 characters and 2% at 54. They fail
`validateSpec` after the upgrade and are no longer reconciled; the upgrade note
in [`docs/ops/deployment.md`](../../../../ops/deployment.md#resource-names)
gives the preflight and the backup/restore path.

## Ownership guard

Two Clusters of one namespace can resolve to the same base name, and a base
name such as `ledger` can collide with unrelated objects. Every object the
operator creates receives its controller reference in the same
`CreateOrUpdate` mutate call that creates it, so an existing object under a
derived name that is not controlled by the Cluster's UID (`notControlledBy`:
no controller reference, or another one) was not created by that Cluster.

Ownership is enforced at three layers:

- **Adoption point.** Every Cluster reconciler mutate function (the three
  Services, both Ingresses, the NetworkPolicy, the auth-keys ConfigMap, the
  cluster Secret, DNSEndpoints, the ServiceAccount and both StatefulSet writes)
  calls `setOwnedControllerReference` instead of a bare
  `SetControllerReference`. When `CreateOrUpdate` hands it an existing object
  (one with a `resourceVersion`) that the Cluster does not control, it returns
  `errResourceNameConflict` before anything is written. This holds even if an
  object appears after the up-front check, or a new derived object is missing
  from it. The auth-keys carry-forward read refuses a foreign ConfigMap the same
  way, so its keys never enter the Cluster's trust set.
- **Deletes.** `deleteIfExists` (disabled optional Services, Ingresses and
  NetworkPolicy, `pruneDisabledOptionalServices`), the auth-keys ConfigMap and
  cluster Secret removals, the StatefulSet orphan-recreate path and the
  DNSEndpoint prune all skip or refuse objects the Cluster does not control.
  Each of these deletes goes through `deleteInspected`, which pins the delete
  to the UID of the object whose ownership was just read
  (`Preconditions.UID`). Delete addresses objects by name, so without the
  precondition an owned object replaced by a foreign one between the read and
  the delete, or a stale cached read, would delete the replacement. The
  guarantee is identity only: the delete hits the object that was read. The
  precondition pins the UID, not the `resourceVersion`, so a change to that
  same object between the read and the delete is not detected, including an
  ownerReference change (someone orphaning it, or handing it to another
  controller). That race is accepted because a `resourceVersion` precondition
  would turn every concurrent status write into a spurious conflict. A failed
  precondition (Conflict) is returned as an error, so the reconcile retries and
  re-inspects ownership; NotFound is not an error.
- **Scale-down PVCs.** `deleteScaledDownPVCs` reads each removed ordinal's
  claim `<volume>-<base>-<ordinal>` and deletes it only when
  `scaledDownPVCMismatch` finds that the Cluster's live StatefulSet created it:
  the claim carries every label of the StatefulSet's (immutable)
  `spec.selector.matchLabels`, which the StatefulSet controller copies onto
  each claim it creates, and its only ownerReferences are that StatefulSet's
  UID or the ordinal's Pod (the references `persistentVolumeClaimRetentionPolicy`
  `Delete` adds). The delete carries the claim's UID precondition. A claim that
  does not match, or that is replaced before the delete (Conflict), is kept
  and logged. A retained claim reused by a recreated Cluster with the same
  name and selector labels matches; one retained from a Cluster with another
  `metadata.name` under the same `resourceName` is mounted but never deleted
  by scale-down.

  The removed range is persisted, not derived: the update that lowers
  `spec.replicas` also records the previous count in the StatefulSet
  annotation `ledger.formance.com/scaled-down-from`. Every StatefulSet pass
  (`reconcileScaledDownPVCs`) re-evaluates `[spec.replicas, scaled-down-from)`:
  it deletes matching claims, continues past per-claim API errors (returned
  joined, so the pass is retried) and sets `ScaleDownPVCsRetained=True`
  (reason `PVCsRetained`) for the kept ones, with a `ScaleDownPVCNotDeleted`
  event on each change. The annotation and the condition are removed once no
  claim of the range is left. Deriving the range from whatever claims exist
  instead would delete PVCs retained from a deleted Cluster that a recreated,
  smaller Cluster never used. Before raising `spec.replicas`,
  `scaleUpBlockingClaims` refuses to bring back an ordinal of that range whose
  claim still exists (owned or not): the StatefulSet keeps its live replica
  count, the condition reason becomes `ScaleUpBlocked` with a
  `ScaleUpBlocked` event and an error log, the deletion-protection pass labels
  only the live ordinals (the user must stay able to delete the kept claim),
  and the Cluster is re-checked every minute. The StatefulSet
  orphan-and-recreate path for a VolumeClaimTemplate change drops the
  annotation: claims still pending then are neither deleted nor reported.
- **Up-front check.** `validateResourceNameOwnership` runs after `validateSpec`
  and before any create, update or delete, and checks only names this pass will
  create or update: the StatefulSet, the ClusterIP Service, the headless Service
  unless disabled, the gRPC Service and Ingress when gRPC ingress is enabled,
  the Ingress and NetworkPolicy when enabled, the operator-created
  ServiceAccount (not a user-managed one, `serviceAccount.create=false`), and
  DNSEndpoint entries that are enabled and have endpoints. A foreign object
  under a name the Cluster does not use does not block it. The auth-keys
  ConfigMap (exists only while Credentials match) and the cluster Secret (only
  while TLS is active) are left to the adoption point.

Any `errResourceNameConflict` — from the up-front check or an adoption point —
goes through `reportResourceNameConflict`: `ResourceNamesAvailable=False`
(reason `NameConflict`), phase `Error`, a `ResourceNameConflict` warning event,
and a one-minute requeue. `updateStatus` never reports the readiness of a
StatefulSet the Cluster does not control.

A user-named ServiceAccount that already exists (for example
`spec.serviceAccount.name: default`) needs `serviceAccount.create: false`;
with `create` left at its default it is a conflict, not an adoption.

Cross-CRD exec and dial paths go through `fetchClusterStatefulSet` (or
`fetchTLSMode`, which wraps it), which returns `errClusterRuntimeAbsent` when
the Cluster has no StatefulSet and `errResourceNameConflict` when the
StatefulSet under its base name is not controlled by it. Neither case execs or
dials: a pod named `<base>-0` can belong to a deleted Cluster's orphaned
runtime.

An owned StatefulSet does not make every `<base>-<ordinal>` pod its own either:
while the garbage collector removes a deleted same-base-name Cluster's runtime,
a pod under that name can still be controlled by the old StatefulSet UID, and
after the VolumeClaimTemplate orphan-and-recreate path a pod has no controller
until the new StatefulSet adopts it. Every
exec into a Cluster pod therefore goes through `ownedPodExec`, which re-reads
the target pod through the uncached clientset and runs nothing unless
`metav1.IsControlledBy(pod, sts)` holds for the StatefulSet just inspected; a
missing or foreign pod is `errPodNotOwned`. The pod read is bounded by
`ledgerExecTimeout`, like the exec. This covers the Ledger and
ledger-index execs, the EventSink and cluster-sink execs
(`ClusterReconciler.ledgerctlExecOutput`), Raft scale-down and the
volume-expansion disk-usage probe. Callers react as to any failed exec. The
exec API has no UID precondition, so a pod replaced between that read and the
exec stream is not detected.

- Ledger: create, promotion and index reconciliation report the error in
  conditions; deletion skips `ledgers delete` and releases the finalizer, the
  same as for a missing Cluster.
- EventSink: it reads the StatefulSet itself; a deleting sink releases its
  finalizer when the StatefulSet is missing or not controlled by the Cluster,
  otherwise it reports `Synced=False` with reason `ResourceNameConflict`.
- BackupRun: a conflict fails the run permanently without creating a Job; a
  missing StatefulSet records the reason and retries without creating a Job.
  The Job dials the ClusterIP Service `<base>` (`backupServerAddr`), so
  `requireClusterService` applies the same rule to that Service: not
  controlled by the Cluster fails the run permanently, missing
  (`errClusterServiceAbsent`) waits. Checking the Service, rather than dialing
  a verified pod address, keeps the Service's routing to any pod (the leader
  proxies the backup) and the certificate SAN the Job verifies. A Service
  replaced after the check, while the Job starts, is not detected.
- Volume expansion: it refuses a StatefulSet the Cluster does not control and
  reports `ResourceNameConflict` on the volume conditions.

Consequence for the default base name: an object left without a controller
reference (for example after `kubectl delete cluster --cascade=orphan`) is not
re-adopted by a recreated Cluster. Delete the orphaned objects (keeping the
PVCs), or restore their controller reference, before recreating. Objects still
owned by a deleted Cluster's old UID block a same-name replacement until
garbage collection removes them. Deletion of a kind the Cluster controller
`Owns()` (StatefulSet, Service, ServiceAccount, ConfigMap, Ingress,
NetworkPolicy) re-enqueues the new Cluster by owner name; a leftover Secret or
DNSEndpoint is only re-checked by the one-minute requeue.
