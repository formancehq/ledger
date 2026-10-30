# Process boundary recovery: evidence and ownership

The [manifest](process-boundary-recovery.json) covers behavior that appears
only across a real process start, termination, death, and re-execution. An
in-process restart or a healthy peer response cannot prove that the new Ledger
process reopened its own state.

## Evidence contract

Record the exact binary, configuration, PID, endpoints, node identity, local
durable paths and committed sentinel before termination. For a graceful case,
deliver the supported signal to the production process, wait for its exit
status, and start a different PID on the same paths and endpoints. For a hard
death case, use process death that cannot invoke shutdown hooks. Verify the
sentinel directly on the replacement node with forwarding or peer repair
excluded where the test claims local recovery.

Distinguish process exit status and readiness from a wrapper's or operator's
status. For startup rejection, observe whether any endpoint became ready and
whether partially written local state affects the next execution. Capture
resource ownership and exact failure stage; an error inside a test helper is
not necessarily a production failure path.

## Startup registration termination

Exercise the production binary against a controlled bootstrap peer that answers
`GetPeers` and repeatedly returns `Unavailable` from `JoinAsLearner`. Observe
multiple registration calls before signaling the exact child PID. Successful
intentional termination requires completed rollback and signal-time logger
finalization; test cleanup that kills or reaps a stuck child cannot satisfy the
oracle. Verify resource closure and immediate re-exec on the exact paths and
endpoints separately from observing PID exit.

For a positive `total-stop-timeout`, measure the startup-unwind and deferred
logger deadline from signal receipt. Zero retains unlimited waiting. A queued
Fx notification must still stop an application whose startup succeeds, using
the existing separate normal-stop budget. Cover expiry and finalizer/rollback
failures as unsuccessful exits. Outer timeout bounds waiting and can skip or
outlive cleanup; it is not proof that every hook or worker closed.

Retain fatal registration and marker-write failures when they race a signal.
Cover a peer cancellation completed before local termination but not yet
classified by its caller. Read the identity marker without creating or repairing
it after each process exit; require a valid marker and identical bytes on re-exec.
Cancellation cannot undo remote membership committed before a reply was lost:
record remote membership and local `CLUSTER_JOINED` independently, then verify
restart follows the existing idempotent or stale-progress outcome. This process
oracle supplements the in-process cancellation, rollback-context and ownership
checks in `concurrency-lifecycle-shutdown`; neither substitutes for the other.

## Ownership boundaries

`configuration-startup-contracts` owns configuration values, validation and
constructor wiring. `concurrency-lifecycle-shutdown` owns normal goroutine
cancellation and join order. `persistence-restore-replay` owns durable replay
and repair transitions. `raft-membership-leadership` owns distributed quorum,
routing and leadership. This domain owns the OS process boundary, observable
exit/readiness sequencing, resource reuse and recovery after a new exec.
Attribute each shared symptom to the correction needed and avoid duplicate
findings. The proposed subprocess and container checks in the manifest are
ideas, not claims that they ran.
