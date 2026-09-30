# Idempotency, retries, timeouts, and partial failures

The companion manifest owns logical-operation identity, commit/response
ambiguity, partial effects, and recovery through the documented key-retention
window. Unkeyed attempts and expired keys remain outside the single-effect
guarantee. Error presentation at API boundaries is owned by
`api-boundary-contracts`; a transport code alone never establishes non-commit.

## Peer closure followed by maintenance (EN-2212)

The Antithesis `NewGRPCConn` factory retries in interceptors after EN-1627; a
native gRPC retry fixture alone does not validate that configuration. A real
transaction can commit, lose its response during peer closure, then encounter
maintenance on its retry. The exact bare forwarded close status is an ambiguous
category alongside `DeadlineExceeded`. Its wire representation does not prove
physical origin; `grpcerr.Conn.Invoke` observes local shutdown after invocation,
so an identical peer-authored status racing that shutdown is indistinguishable.
Do not broaden this into all `Unavailable`, `Canceled`, `Unknown`, or statuses
carrying structured details.

A maintenance rejection is not a definitive outcome for that retry and the
workload does not treat it as one. The gate sits at admission, ahead of the
FSM's idempotency replay, so it can refuse a write that already committed;
reporting the refusal loses the write. The retry rides the window out instead.
That terminates because a batch of nothing but `SetMaintenanceMode` is exempt
from the gate, so an enable whose own response was lost still reaches the
success that schedules its disable, and the business retry then replays the
frozen outcome under the same key.

`tests/antithesis/workload/internal/client_transport_test.go` exercises the
actual factory and real Ledger admission gate: commit, lost response,
maintenance rejection observed at the forwarding boundary, gate disable, then
the original key/payload surfacing the frozen success. It verifies the frozen
result, replay trailer, request and attempt counts, one transaction, and one
balance effect. It also asserts that the actual forwarded close error is
recognized by `IsAmbiguousCommit`, guarding producer/consumer drift. Preserve
the separate native retry control, default and forever modes, disabled-retry
manual recovery, caller cancellation, and terminal server-status controls.

## Atomic creation metadata (EN-2686)

Initial metadata shares the CreateLedger operation identity and proposal atomicity. A keyed replay returns the original creation result without an additional audit record, metadata save or ledger allocation.

After a keyed creation carrying metadata, does replay preserve the original metadata and audit sequence? Does an invalid metadata map leave neither a ledger nor metadata rows or a consumed ledger ID?

See [the creation contract](../architecture/subsystems/api/atomic-ledger-creation.md) for the authorized semantics and regression evidence. Treat HTTP response/forwarding, actual FSM readback, keyed replay, nonempty checkpoint-plus-delta restore, and primary-projection tampering as separate evidence oracles; a helper-only test does not prove every boundary.
