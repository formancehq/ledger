# Ledger Operator

The Ledger v3 Operator owns the Kubernetes lifecycle of a Raft cluster: the
`Cluster` contract, StatefulSet, Services, credentials, PVC lifecycle,
deletion protection, and operational controllers that must remain outside the
deterministic Ledger FSM.

Explicit `whenScaled: Delete` cleanup records the previous replica count and
an inventory of authorized PVC/PV identities on the StatefulSet before Raft
membership removal and replica reduction. Each retry uses that inventory,
revalidates ownership and binding, and sends UID/resourceVersion-preconditioned
PVC deletes. A replacement with the same name is never adopted as a target.
Both markers remain until accepted deletion requests complete; `Retain` cancels
remaining Operator cleanup. See the
[operator scale-down contract](../../../../../misc/operator/README.md#overview).

Ledger CR target identity is fixed at creation by API-server CEL transition
rules on `spec.name` and `spec.clusterRef`. This prevents a Pending creation,
promotion, index update or finalization from being redirected by a spec edit.
The controller continues to use the admitted target and keeps best-effort
external deletion semantics. Existing drift must be recovered with reconcilers
stopped before installing the rules; the applied-spec hash does not retain the
original target. See [Ledger target identity](../../../../../misc/operator/README.md#ledger-target-identity)
for the upgrade procedure and same-name Cluster replacement boundary.

## Documents

- [Declarative storage reconciliation](storage-reconciliation.md) — configured
  PVC minima/profiles, actual CSI status, durable claim-template recovery,
  scale-down retention and guarded layout deletion.
- [Automatic PVC expansion](volume-auto-expansion.md) — opt-in capacity policy,
  measurement path, convergence, cooldown, failure modes, and observability.
