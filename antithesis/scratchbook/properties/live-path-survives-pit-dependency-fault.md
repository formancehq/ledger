# live-path-survives-pit-dependency-fault — Live Ledger operations remain independent of PIT dependency faults

## Candidate catalog entry

- **Type:** Liveness
- **Priority:** P0
- **Assertion:** `Sometimes(liveWriteAndLiveReadSucceededDuringMinIOIsolation)` —
  while quorum and workload-to-Ledger connectivity are intact but one or more
  Ledger-to-MinIO links are faulted, at least one ordinary write and live volume
  read succeeds.
- **Confidence:** High that the architecture intends this separation; only a
  real targeted network fault proves there is no hidden dependency.

## Evidence

PIT is a peer secondary store. The builder is ticker-only, and compaction,
tiering and remote GC run outside the FSM path. Global health also intentionally
does not depend on PIT readiness.

Relevant paths:

- `internal/bootstrap/balance_history.go:27-70`
- `internal/bootstrap/balance_history_maintenance.go:23-65`
- `docs/technical/architecture/subsystems/read-path/point-in-time-balances.md`

## Fault and oracle

Use an asymmetric Ledger-to-MinIO network fault without disrupting Raft quorum
or the workload's gRPC path. Continue ordinary writes and live reads on a
separate ledger while cold PIT operations fail or block. The `Sometimes`
condition is meaningful progress under a deliberately isolated optional
dependency; it does not claim that every write succeeds under unrelated Raft
faults.

PIT responses during the same interval are governed by the exact-or-fail-closed
safety properties.

## Instrumentation status

Existing SUT and workload assertions cover generic writes, reads and sentinel
survival, but none correlate their success with an active PIT/MinIO dependency
fault. A workload-side semantic condition is missing. No FSM instrumentation
should be added solely for PIT; absence of a new hot-path edge is also enforced
by static review and performance tests.

## Open questions

- None, provided the launch environment can target the Ledger-to-MinIO link
  without simultaneously partitioning Raft or workload gRPC.
