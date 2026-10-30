# Ledger Operator

The Ledger v3 Operator owns the Kubernetes lifecycle of a Raft cluster: the
`Cluster` contract, StatefulSet, Services, credentials, PVC lifecycle,
deletion protection, and operational controllers that must remain outside the
deterministic Ledger FSM.

Scale-down records the previous replica count on the StatefulSet before Raft
membership removal and replica reduction. Removed-ordinal PVC cleanup reads
that durable marker and the claim templates on each reconcile, so a failed
delete or operator restart cannot strand old claims before a later scale-up.
The marker is removed only after the deletion requests succeed. See the
[operator scale-down contract](../../../../../misc/operator/README.md#overview).

Ledger CR target identity is fixed at creation by API-server CEL transition
rules on `spec.name` and `spec.clusterRef`. This prevents a Pending creation,
promotion, index update or finalization from being redirected by a spec edit.
The controller continues to use the admitted target and keeps best-effort
external deletion semantics. Existing drift must be recovered with reconcilers
stopped before installing the rules; the applied-spec hash does not retain the
original target. See [Ledger target identity](../../../../../misc/operator/README.md#ledger-target-identity)
for the upgrade procedure and same-name Cluster replacement boundary.

Objects derived from a Cluster name are owned only through a controller
reference carrying the Cluster UID. The operator never adopts, mutates,
deletes or execs into a same-name object it does not control, and reports the
conflict as `ResourceNamesAvailable=False`.

## Documents

- [Derived object ownership](derived-object-ownership.md) — ownership checks on
  writes, prunes, status and execs, conflict reporting, and accepted races.
- [Automatic PVC expansion](volume-auto-expansion.md) — opt-in capacity policy,
  measurement path, convergence, cooldown, failure modes, and observability.
