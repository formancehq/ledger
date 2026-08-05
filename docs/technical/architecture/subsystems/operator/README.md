# Ledger Operator

The Ledger v3 Operator owns the Kubernetes lifecycle of a Raft cluster: the
`Cluster` contract, StatefulSet, Services, credentials, PVC lifecycle,
deletion protection, and operational controllers that must remain outside the
deterministic Ledger FSM.

## Documents

- [Automatic PVC expansion](volume-auto-expansion.md) — opt-in capacity policy,
  measurement path, convergence, cooldown, failure modes, and observability.
