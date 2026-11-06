# Idempotency, retries, timeouts, and partial failures

The companion manifest owns logical-operation identity, commit/response
ambiguity, partial effects, and recovery through the documented key-retention
window. Unkeyed attempts and expired keys remain outside the single-effect
guarantee. Error presentation at API boundaries is owned by
`api-boundary-contracts`; a transport code alone never establishes non-commit.

The service error boundary sends `grpc-retry-pushback-ms: -1` with a validated
`Unavailable` / `MAINTENANCE_MODE` rejection. That stops native gRPC retries;
it does not change the commit ambiguity of an earlier interrupted attempt.
Explicit application retry loops retain their policy. Preserve the actual
Antithesis factory's maintenance recovery regression below to prove that the
hint does not end retry-forever keyed recovery.

`UncommittedFailure` is a retryable technical interruption, not a frozen
failure. A keyed first attempt on this branch writes no audit entry or
idempotency result. An existing frozen outcome may still be replayed before
the branch is reached. Evidence must distinguish those cases and show that a
same-key retry after repair can establish one definitive outcome, while a
same-key retry of an earlier committed outcome does not execute it again.

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

A maintenance rejection is not a definitive outcome for that retry, and a
retry-forever client (`LEDGER_RETRY_FOREVER`, the model driver) does not treat
it as one. The gate sits at admission, ahead of the FSM's idempotency replay,
so it can refuse a write that already committed; reporting the refusal loses
the write. That client's retry rides the window out instead. That terminates
because a batch of nothing but `SetMaintenanceMode` is exempt from the gate, so
an enable whose own response was lost still reaches the success that schedules
its disable, and the business retry then replays the frozen outcome under the
same key. Every other client takes the refusal as definitive: the maintenance
driver awaits it before disabling maintenance itself.

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

## Technical-update submission boundary

Production submission through `plan.Builder.Run` rejects more than one technical
update and permits a technical update alongside orders only for MirrorSync.
Verify rejection before serialization/Raft submission, loader cleanup on both
preload paths, and unchanged standalone operational updates. Historical committed
compositions keep their interpreter; this admission guard must never become a
replay-time semantic change. Pair MirrorSync with real ingestion and a concurrent
ledger promotion: rejected ingestion must preserve the prior source head and
applied position through recovery; successful ingestion advances both.
See [the composition contract](../architecture/subsystems/fsm/technical-update-atomicity.md).

## Metadata policy tightening and retained recovery

Public admission preserves authentication, shape and safe preload construction,
then submits ordinary business proposals. Existing FSM handlers evaluate mutable
metadata policy after idempotency lookup. A matching retained success or failure
replays without new audit or business effects. A fresh definitive refusal and a
live content conflict are audited; missing, expired or evicted keys are new
attempts under the policy at apply. A retained refusal must survive later policy
loosening. Earlier preparation and readiness failures retain their precedence;
there is no general admission-error replay bypass.

Prove the original success/failure replay triggers, each metadata dimension,
fresh keyed and unkeyed failure audit items, no durable business effects,
conflict preservation of the original outcome, expiry/eviction, cancellation,
log-read failures, and maintenance/drain/audit-capacity refusal. A failure can
follow speculative handler writes, but the discarded WriteSet must leave no
business projection or log. Runtime gates remain shared with
`runtime-health-admission-contracts`; deduplicate by root cause.

Queue policy tightening and an original prepared under the older policy. Apply
only the policy, submit the retry, then hold both applies separately. Neither
proposal acceptance nor the original's apply completes the retry; after ordered
durable apply it returns the original frozen outcome without another audit.
Also consider a retry submitted before the original: it is then the first
attempt and must evaluate policy at that apply index.

The ordinary proposal replicates request bytes and preloads declared keys. Fresh
policy failures persist attempted orders in audit, while replay adds no audit
rows. Cost scales with request bytes/keys and script dependencies, without a new
historical scan or consensus-spanning lifecycle lock. Preserve cost-shape tests
and local benchmark evidence; do not claim network/saturation measurements.
No persisted projection, format, FSM interpreter or restore reconstruction changes.
See [the admission contract](../architecture/subsystems/admission/idempotency.md#replay-after-metadata-limits-tighten).

## Numscript library removal and retained recovery

A transaction referencing a library script by `latest` or exact semver must
replay its retained outcome after the library changes, including a ledger
deletion that purges it. Admission forwards an unresolved reference with the
coverage of the FSM's selector resolution and no script planning; the FSM
decides `NUMSCRIPT_NOT_FOUND` after idempotency lookup. Prove replay for both
selectors after deletion, an audited fresh-key refusal with its apply-time
reason, and audited keyed and unkeyed `NUMSCRIPT_NOT_FOUND` on a live ledger
whose failure facts match the client error. Hold a save proposed before the
reference, apply it first, and prove the reference is a retryable stale
rejection with no audit entry, frozen key or coverage miss, and that its retry
commits. The `OrderTechnical.numscript_reference_absent` marker relies on the
stopped RC.3 to RC.4 cutover. It is Raft-only execution metadata: audit items
keep the business order and the audit entry records the definitive failure.
Refused orders have no business effects, so restore reconstruction is unchanged.
See [the admission contract](../architecture/subsystems/admission/idempotency.md#replay-after-a-referenced-script-is-removed).
