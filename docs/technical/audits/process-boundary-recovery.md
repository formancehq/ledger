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
