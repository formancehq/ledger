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

- Raft node ID and persisted instance ID, including whether the member is a
  bootstrap seed, joined member, phantom learner, removed incarnation, or fresh
  incarnation reusing an ordinal;
- term, leader ID, entry index, committed index, locally applied/durable index,
  and the voter/learner `ConfState` that governs the operation;
- logical request identity, leader-local proposal/correlation identity, and Raft
  message identity. They are not interchangeable;
- node status (`normal`, snapshotting, syncing/out-of-sync, stopped, or terminal)
  and the exact gate controlling proposal, ReadIndex, campaign, transfer, and
  serving behavior;
- for snapshot catch-up, the snapshot term/index/ConfState, FSM snapshot horizon,
  spooled committed suffix, and main-store synchronization horizon.

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

For removal, distinguish an identified joined member from a bootstrap seed or
phantom learner without an instance identity. The replicated tombstone guarantee
applies only where the authoritative consensus documentation says it does.
Prove rejoin and promotion behavior through every production entry rather than
assuming that one admission check covers direct add, auto-promotion, discovery,
or leadership change.

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

Separate Raft message redelivery from logical request retry. etcd/raft may safely
receive duplicate or delayed protocol messages; that fact does not make an API
mutation idempotent. Trace stream authentication, peer/connection replacement,
term checks, unreachable reporting, proposal identity, forwarding metadata, and
the response path before claiming duplication or false acknowledgement.

For quorum-certified reads, this domain owns leader/quorum certification and the
wait until the returned index is locally applied. The read-projection audit owns
whether every projection and query surface correctly represents that certified
horizon. A forwarded fallback must not become an unbarriered local read merely
because leadership changed during resolution; explicit stale mode remains the
only expected bypass.

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

### Snapshot installation and catch-up

Prove compatibility and order across Raft snapshot metadata/`ConfState`, WAL
installation, FSM replacement, spool replay, business-store synchronization,
status transition, and serving gates. Exercise interruption before and after
each durable boundary and preserve entries committed after the snapshot through
the retained log or spool. A receiver must reject transfer/campaign and fail or
forward quorum-dependent work until its applied and durable horizons meet the
documented requirement.

This domain owns the consensus-side selection, transfer, installation order and
authority gates. General checkpoint/restore content correctness belongs to
`persistence-restore-replay`; filesystem containment belongs to
`filesystem-confinement-contracts`.

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
