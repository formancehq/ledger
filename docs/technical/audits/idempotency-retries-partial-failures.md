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
maintenance on its retry. The exact bare forwarded close status is therefore
an ambiguous category alongside `DeadlineExceeded`. Its wire representation
does not prove physical origin; `grpcerr.Conn.Invoke` observes local shutdown
after invocation, so an identical peer-authored status racing that shutdown is
indistinguishable. Do not broaden this into all `Unavailable`, `Canceled`,
`Unknown`, or statuses carrying structured details.

`tests/antithesis/workload/internal/client_transport_test.go` exercises the
actual factory and real Ledger admission gate: commit, lost response, maintenance
rejection, gate disable, then recovery with the original key/payload. It verifies
the frozen result, replay trailer, request and attempt counts, one transaction,
and one balance effect. It also asserts that the actual forwarded close error
is recognized by `IsAmbiguousCommit`, guarding producer/consumer drift. Preserve
the separate native retry control, default and
forever modes, disabled-retry manual recovery, caller cancellation, and terminal
server-status controls. A first maintenance rejection and the existing
`no leader` then maintenance control remain definitive.
