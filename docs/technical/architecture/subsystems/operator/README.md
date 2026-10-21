# Ledger Operator

The Ledger v3 Operator owns the Kubernetes lifecycle of a Raft cluster: the
`Cluster` contract, StatefulSet, Services, credentials, PVC lifecycle,
deletion protection, and operational controllers that must remain outside the
deterministic Ledger FSM.

## Documents

- [Resource naming and ownership](resource-naming.md) — the Cluster base name,
  `spec.resourceName`, cross-CRD name resolution, and the guard that refuses to
  adopt objects a Cluster does not control.
- [Automatic PVC expansion](volume-auto-expansion.md) — opt-in capacity policy,
  measurement path, convergence, cooldown, failure modes, and observability.
