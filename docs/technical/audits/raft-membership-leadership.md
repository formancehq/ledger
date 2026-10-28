# Raft membership and leadership evidence contract

The [manifest](raft-membership-leadership.json) defines a reusable audit of the
Ledger service-side consensus boundary. Its purpose is to establish whether the
current voter set is the only authority that can elect, certify reads, commit
normal changes, and transfer membership or leadership, and whether nodes expose
that authority only after the required local state has caught up. It adds no
product guarantee, runner, compatibility requirement, or authorization to run
the audit from this manifest change.

## Entry points and state model

For each hypothesis, identify the concrete entry point and carry these identities
through the complete transition:

- Raft node ID and persisted instance ID; every member must carry a valid 16-byte
  identity (bootstrap seed, joined member, removed incarnation, or fresh incarnation
  reusing an ordinal); identity-less phantom learners are no longer a reachable state;
- term, leader ID, entry index, committed index, locally applied/durable index,
  and the voter/learner `ConfState` that governs the operation;
- logical request identity, leader-local proposal/correlation identity, and Raft
  message identity. They are not interchangeable;
- discovered peer endpoint, outbound connection attempt, priority stream, and
  outstanding liveness probe identity and deadline;
- node status (`normal`, snapshotting, syncing/out-of-sync, stopped, or terminal)
  and the exact gate controlling proposal, ReadIndex, transfer, membership
  admission, and serving behavior; campaigning and voting depend on the Raft
  log alone and have no store gate;
- for snapshot catch-up, the snapshot term/index/ConfState, FSM snapshot horizon,
  spooled committed suffix, and main-store synchronization horizon.
- for controlled drain, the epoch, captured member IDs, marker index, cutoff
  index, local persisted index and release index on each member.

Supported entries include bootstrap and join, explicit learner add/promotion,
automatic promotion, normal removal, emergency force removal, leadership
election/transfer/loss, proposal and forwarding, ReadIndex, Raft stream
reconnection, retained-log catch-up, and snapshot send/install. A test helper or
admin-only RPC is evidence only if the production entry is reachable under its
documented preconditions.

## Proof and rejection standard

Every finding must establish all of the following from the audited SHA:

1. **Authority:** name the exact active voter configuration and term that should
   authorize the election, commit, ReadIndex, transfer, or ConfChange. For a
   force removal, state why normal quorum authorization is deliberately absent
   and list the operator-owned safety preconditions instead.
2. **Reachability:** trace the real caller, forwarding/admission checks, node
   status gates, Raft step/proposal path, transport path, and relevant callback
   or future. Show that the proposed interleaving survives existing term,
   pending-ConfChange, identity, synchronization, and cancellation guards.
3. **Ordering:** give a timeline using term and indexes. Distinguish proposal
   acceptance, quorum commit, ConfChange observation, local FSM preparation,
   durable apply, response delivery, and follower convergence. For snapshots or
   force removal, enumerate each durable write and the state left after failure.
4. **Violation:** identify the externally observable false success, conflicting
   committed history, stale authority, incorrect configuration, lost committed
   effect, unsafe rejoin, or sustained inability to recover after prerequisites
   return. A narrow timing window without a violated contract is not a finding.
5. **Falsification:** cite protections and tests that could disprove the claim,
   then provide a deterministic reproduction plan whose assertion uniquely
   identifies the alleged branch. Record attempt counts, exact indexes and
   final ConfState rather than relying on sleeps or role labels alone.

Reject a hypothesis when current quorum intersection or etcd/raft rules make the
state unreachable, a node-status/term/identity gate closes the path before an
external result, durable restart authority restores a valid configuration, or
the symptom is wholly caused by an adjacent domain and requires no consensus
correction. A missing test, comment, timeout, check, or transaction is not itself
a defect. An isolated former leader that still reports `leader` is not split
brain unless it can obtain a quorum-backed success or commit a conflicting
history. If authoritative intent is absent or conflicting, emit an audit
question rather than inventing a membership guarantee.

## Transition-specific evidence

### Normal membership changes

Prove which `ConfChangeV2` was admitted and which configuration commits it.
Inspect serialization against a pending ConfChange, learner/voter role checks,
`ConfChangeContext`, proposal correlation, membership-row mutation, observer
updates, the committed-index wait, and response/error mapping. Concurrent
operations against the same node must be distinguished by proposal identity and
expected change type; a late commit must not satisfy a replacement waiter.
Within one logical operation, retries must retain the same waiter. Delay WAL
ConfState persistence beyond the retry timer after consuming the correlated
waiter, then release the WAL and queued retry. Require the original committed
index, no second admission/proposal, and durable local FSM application before
success. Cancellation must release the caller without resolving a different
operation or reporting premature success.

For removal, every member carries a mandatory 16-byte instance_id (enforced at every admission, persistence, and promotion boundary). The replicated tombstone guarantee applies to every removal; identity-less phantom learners are no longer a supported member state. Distinguish a removal with a normal committed ConfChange from a force removal, which bypasses Raft consensus and relies on operator preconditions instead of quorum safety.
Prove rejoin and promotion behavior through every production entry rather than
assuming that one admission check covers direct add, auto-promotion, discovery,
or leadership change.

For retained-log catch-up, feed a real follower RawNode committed remove(A),
add(B), remove(B) at one ordinal in a single Ready; also delay cleanup across
separate Readies and compare with durable apply between cycles. Every removal
must remain an independent node/index event with immutable identity. Verify all
removed identities stay protected until their own durable index and tuple
tombstone, fresh identities remain eligible, and exact duplicates reuse events
while same-index contradictory identities fail. Cancel foreground waits, delay
old cleanup across newer events or replacement pointers, and recover the same
history through WAL replay. Require both tombstones and final ConfState parity.
Missing tombstones/read failures must retain protection. Check the lookup cost
partition and benchmark growing per-ordinal history separately from unrelated
nodes; no set lock may span apply, store reads or proposal waits.

### Leadership and quorum transitions

Use committed/applied indexes, not callback order or the locally reported Raft
role, as the oracle. On acquisition, prove the drain/barrier that precedes
leader-only admission, membership maintenance and ReadIndex ownership. On loss,
classify each pending operation by its actual boundary: rejected before Raft
acceptance, accepted but not known committed, committed but not durably applied
locally, durably applied with lost response, or completed.

For each voter-set change, establish the old, new, or joint quorum used by
election, commit, ReadIndex, and transfer. A partition experiment must record
which voters acknowledged the relevant term/index. Availability loss on a
minority side is expected CP behavior; success without the required majority is
the violation.

### Controlled drain membership fence

Trace capability preflight on every current member, the membership operation
lock, the Raft fence for already accepted changes, marker enqueue and apply,
and a concurrent add, promotion or normal removal. An old member that lacks
the drain RPC must make preflight fail closed during a rolling upgrade. After
preflight, a loss and regain of leadership must reject the old-term marker
before Raft append, even if the new leader changed membership in between.
The same term fence applies to a release preflight. After
enqueue, a timed-out caller cannot lift the pending membership fence until
the marker applies or Raft definitively drops it. The first applied marker
fixes the captured set and cutoff; retries with its epoch cannot move them,
and an already released epoch cannot be reused.

Require the operator's `Drained` certificate to match current membership and
each captured member's locally persisted cutoff. Leader `Match` is insufficient
to prove follower Pebble durability. A lost or replaced member prevents the
all-member certificate; emergency force removal can change the set and must
not silently satisfy the old certificate. Repeat with leader transfer, member
restart, retained-log catch-up and snapshot catch-up. The operator audit owns
phase persistence and retry decisions, and the persistence audit owns local
drain-state reconstruction and the cross-cluster restore discard rule.

### Force removal

Force removal is not a quorum-safe alternative to normal removal. Verify its
documented preconditions and the exact sequence:

1. leader-local live `ApplyConfChange`;
2. durable snapshot-file replacement and WAL snapshot-record update for the
   reduced `ConfState`;
3. peer deletion and, when identity exists, tombstone persistence in Pebble;
4. follower convergence through a later Raft snapshot.

At each injectable failure, compare live state, snapshot-file authority, WAL
authority, Pebble membership/tombstone state, queued command results, process
terminal state, and restart result. Because the live tracker transition cannot
be rolled back safely, any persistence-error path that continues serving is
material. The documented short window with reduced durable ConfState but stale
peer/tombstone data is not independently a defect unless the implementation
exceeds its stated operator-owned risk or fails to converge on retry/snapshot.

### Transport, forwarding, and ReadIndex

An interrupted cluster command on a stopped Raft run must surface as retryable
gRPC `Unavailable`, including a wrapped `raft.ErrStopped`. Verify the shared
unary/streaming conversion without classifying unrelated failures as transient;
those failures retain their sanitized error behavior.

Separate Raft message redelivery from logical request retry. etcd/raft may safely
receive duplicate or delayed protocol messages; that fact does not make an API
mutation idempotent. Trace stream authentication, peer/connection replacement,
term checks, unreachable reporting, proposal identity, forwarding metadata, and
the response path before claiming duplication or false acknowledgement.

For silent transport failure, record the old connection's last successful probe,
the next scheduled probe, its matching response or deadline, any blocked send,
the attempt cancellation, and the subsequent dial target. A one-way TCP
blackhole may leave the socket and gRPC stream open; the recovery claim requires
an observed replacement connection to the current endpoint and delivery of a
Raft message, not only a closed socket or a fired timer. Check that probing
still runs while priority queues stay nonempty and that a valid delayed pong
renews the deadline. Stale, duplicate, or mismatched pongs must not do so.
Only the high-priority stream carries these probes: its pong establishes that
stream's liveness, not independent progress on medium or low streams. The
regression in `internal/infra/node/transport_blackhole_test.go` exercises the
blackhole, redial, and message delivery path; its sustained-traffic and
delayed-pong cases guard against false recovery decisions. A full voter-quorum
recovery claim still needs a cluster-level partition and commit check.

For quorum-certified reads, this domain owns leader/quorum certification and the
wait until the returned index is locally applied. The read-projection audit owns
whether every projection and query surface correctly represents that certified
horizon. A forwarded fallback must not become an unbarriered local read merely
because leadership changed during resolution; explicit stale mode remains the
only expected bypass.

ReadIndex evidence must also track the complete request context across caller
process incarnations. Retain an outstanding leader-side read at `C`, reconstruct
the follower from retained storage with unchanged member identity, leader and
term, then commit a distinguishable write `D > C` through the other voters
while the restarted follower remains below `D`. Deliver the delayed heartbeat
confirmation and resulting old response: Raft may accept it in the unchanged
term, but production waiter resolution must reject it for the new invocation.
A fresh certificate must cover `D`, and serving must await local application of
that horizon. `internal/infra/node/read_index_restart_test.go` owns this
regression. Verify that legacy eight-byte and new UUID contexts are echoed
unchanged by the pinned Raft implementation; only upgraded callers gain the
restart protection, with no persisted-state or service-revision migration.

A committed `ConfChangeUpdateNode` with a peer-registration payload first
refreshes the membership cache plus the operational Raft and service connection
pools when `finishReady` observes the commit. The committed entry is submitted
to the asynchronous FSM applier afterward, and `WriteConfChange` then persists
the row in Pebble without a transport side effect. During that interval the
cache and pools contain the new endpoint while Pebble may still contain the old
row; a crash is repaired by committed-WAL replay followed by `Rehydrate`. Prove
that the next Raft dial uses the committed endpoint while the existing peer
identity, role, send loop, and queues remain intact, and that Pebble eventually
converges to the same registration.
A transient optional-TLS probe failure during endpoint refresh must preserve
the committed target address as the retry owner for the existing send loop and must
not strand the peer in a state where no active retry exists until the next leader restart.

### Snapshot installation and catch-up

Prove compatibility and order across Raft snapshot metadata/`ConfState`, WAL
installation, FSM replacement, spool replay, business-store synchronization,
status transition, and serving gates. Exercise interruption before and after
each durable boundary and preserve entries committed after the snapshot through
the retained log or spool. A receiver must reject transfer and fail or forward
quorum-dependent work until its applied and durable horizons meet the
documented requirement. It keeps voting and campaigning on its log: when the
voters holding the newest log are the resynchronizing ones, no other candidate
can win, so an out-of-sync voter may lead on its log while every
store-dependent path stays gated (admission, ReadIndex, event sinks and
mirrors, membership changes) until its checkpoint sync and spool replay
finish, and it must never fetch that checkpoint from itself. The regression is
`tests/e2e/cluster/out_of_sync_election_test.go`: the two newest-log voters
restart out of sync, elect among themselves, resynchronize from a peer, and
run a sink created inside the checkpoint. `TestSyncDonor`,
`TestAddLearnerWaitsForLeaderReady`, `TestCheckAndPromoteLearnersWaitsForLeaderReady`,
and `TestManagerLeadership` pin the donor rotation and the gates.

After real snapshot catch-up, `tests/e2e/cluster/raft_test.go` restarts the
recovered voter again and verifies retained business state through a
linearizable read. Accept a healthy `Follower` or `Leader` with a known leader:
a restarted voter may win an election, so a follower-only role assertion is not
a valid recovery oracle.

This domain owns the consensus-side selection, transfer, installation order and
authority gates. General checkpoint/restore content correctness belongs to
`persistence-restore-replay`; filesystem containment belongs to
`filesystem-confinement-contracts`.

## Antithesis witness and convergence evidence

The workload's operational sentinels capture the complete acknowledged
transaction and compare it after restart, scaling, configuration changes,
quorum recovery and leadership transfer. These witnesses have private ledgers
excluded from generic mutation. An ambiguous write is not an acknowledged
witness; a transient read does not prove survival. Transfer coverage requires
a fresh acknowledged business write as well as the transfer response.

`eventually_cross_node_identity` requires every current member, including
learners, to reach an exact durable horizon before comparing complete account
listings and audit identity. Treat inaccessible current members as incomplete
recovery evidence, not as replicas that can be excluded. Compare presence as
well as contents; require successful stream completion and valid pagination.
Read attribution depends on direct per-node addresses matching advertised
membership, not a load-balanced service. The routed `GetClusterState` identity
check validates the status response; it does not independently authenticate the
endpoint of a stale read.
Publish divergence only after final per-node/global/membership fences qualify
the observation, retrying changed horizons. Keep qualification failures separate
from actual unequal data. A one-member observation cannot prove replica
agreement. This oracle does not compare uncommitted Raft suffixes or establish
that every snapshot-install path was exercised.

The local SDK sensitivity fixtures prove that these observations detect their
target failures. They do not substitute for a remote campaign report or a
quorum-backed distributed transition. See the
[harness evidence contract](../../../tests/antithesis/README.md#raft-and-sentinel-evidence).

## Ownership and deduplication

One finding represents one root cause and required correction, even when it
appears during several terms, membership operations, or transport failures. Use
stable IDs `raft-membership-leadership/<root-cause-name>` without line numbers,
run IDs, severity, or a changing symptom count. Compare by mechanism and fix,
not title alone.

| Adjacent domain | Boundary |
| --- | --- |
| `fsm-determinism-cache-coverage` | Owns pure deterministic application, preload/coverage gates, cache-generation convergence, forbidden hot-path storage reads, impossible-state handling, and accepted-order immutability. This domain retains the consensus ordering, leadership barrier, ConfChange effect, and snapshot-install gate that supply FSM inputs. |
| `concurrency-lifecycle-shutdown` | Owns goroutine/task ownership, cancellation, shutdown joins, channel/lock safety, and deep leadership-generation lifecycle inside events, mirror, and backup workers. This domain retains whether the immediate leadership transition publishes or fences consensus/routing/membership authority in the right order. |
| `read-consistency-projections` | Owns projection/query correctness after a fixed applied-index horizon. This domain owns the ReadIndex leader/quorum certificate and wait to that horizon. |
| `persistence-restore-replay` | Owns snapshot/checkpoint/backup content durability and replay equivalence. This domain owns Raft snapshot selection/transport/installation, ConfState compatibility, committed-suffix preservation, and non-serving gates during catch-up. |
| `idempotency-retries-partial-failures` | Owns public logical retry identity and duplicate business effects after ambiguous outcomes. This domain owns proposal/ConfChange correlation and whether leadership/forwarding/transport returns a truthful consensus outcome. |
| `authentication-authorization-boundaries` | Owns caller permission and credential policy. This domain owns peer identity binding and Raft transport/join trust only where it can change consensus authority. |
| `operator-reconciliation-durability` | Owns Kubernetes replica/PVC/control-plane ordering and membership postcondition orchestration. This domain owns the service-side membership algorithm and Raft result. |
| `process-boundary-recovery` | Owns real process death/re-exec and generic durable reopen. This domain owns reconstructed Raft term/log/ConfState authority when that restart proves a consensus transition. |
| `test-reachability-enforcement` | Owns whether tests and model scenarios are collected and enforced. A missing test here supports a concrete consensus finding but is not a standalone coverage finding. |

Follow calls outside manifest globs only when necessary to prove or falsify a
consensus path. If the required correction lies wholly in an adjacent domain,
record the inspected boundary or residual risk instead of issuing a duplicate.

## Severity and residual risk

Use the native P0-P3 scale. P0-P2 require a concrete reachable transition and
observable impact. A demonstrated ability for disjoint configurations to commit
conflicting histories, or a force-removal persistence error that continues
serving an unpersisted reduced quorum, can justify P0/P1 depending on supported
preconditions and impact. Bounded membership unavailability, a false committed
outcome, or a stale-authority window with a real consumer may justify P1/P2.
Timing sensitivity alone does not raise severity.

List every invariant and major entry class actually inspected. Record model,
race, e2e, partition, crash-injection, multi-process, or external-environment
boundaries not exercised as residual risk. Green tests do not prove an invariant,
and optional dynamic checks do not authorize tracked edits or external mutation
during the read-only audit.

## Execution boundary

Review and merge this manifest change before an audit campaign. A later trusted
outer workflow may run:

```bash
bash scripts/ai-audit raft-membership-leadership
bash scripts/ai-audit-challenge <audit-result.json>
```

Both passes require the same exact clean `HEAD`; the providers remain read-only.
Do not launch either command while reviewing this manifest PR. Jira preview or
publication is a separate explicitly authorized action.

## Received snapshot Ready suffix evidence

For a Ready containing both a snapshot and subsequent entries, retain the
original entries/HardState durability order, then verify that snapshot
installation cannot erase the suffix from `LastIndex`, `Term`, `Entries` or
a reopened WAL before storage acknowledgement. Identify the residual
in-memory snapshot/cache and durable WAL prefix on any failure. The focused
regression is `process_ready_snapshot_test.go` under `internal/infra/node/`.
Snapshot-only processing retains its existing append count; a combined Ready
reappends only the suffix once, with no retained-log scan.

## Proposal payload cutover

The proposal codec is part of this contract. Production writers serialize the
execution and policy oneof directly after a full-stop upgrade. All source nodes
must stop before any target starts, and old binaries must never reopen a WAL
containing new payloads, including technical entries written during startup.
Historical and payload entries must have identical canonical audit intent,
hashes, coverage, HLC semantics, and checkpoint boundaries. Internal producers
own shape construction. Preserve historical decoding for replay and policy
restore parity with a nonempty incremental delta. Test a released store/WAL
opened by the candidate, retained legacy entry replay, new writes and restart.
See [proposal payloads](../architecture/subsystems/consensus/proposal-payloads.md).
