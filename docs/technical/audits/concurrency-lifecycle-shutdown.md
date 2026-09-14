# Concurrency, lifecycle and shutdown evidence contract

The [manifest](concurrency-lifecycle-shutdown.json) defines the reusable audit
for in-process concurrency ownership. It covers goroutines, callbacks and
mutable runtime resources while one Ledger process starts, changes leadership,
reconciles resources and stops. It does not launch an audit or add a product
guarantee.

## Ownership model

The manifest's first invariant is the canonical authority-token inventory. Do
not extend that inventory only in this explanatory table. Do not use
"generation" as a generic synonym for freshness; establish the specific token
that authorizes each operation:

| Token | Current examples | Required rejection or join boundary |
| --- | --- | --- |
| Fx process lifetime | bootstrap hooks, HTTP/gRPC servers, stores, metrics registrations, health workers | Failed start unwinds only acquired resources. Stop rejects new work, unregisters callbacks and joins owned goroutines before dependencies close. A timed-out outer Fx call is not proof that a hook or worker has exited. |
| Node run lifetime | `Node.Run`, `Applier`, transport, maintenance and FSM tasks | The persistent stop request is published once. Commit drain and child joins complete before node-owned storage or transport can close. |
| Ordered leadership generation | node observer, event and mirror managers, backup orchestrator, leader-only reconcilers | The transition is recorded synchronously in observer order. Work that captured generation N cannot install or remove generation N+1 resources; Stop advances a terminal generation. |
| Replicated resource identity | named sink configuration, mirror ledger incarnation/source, backup job and destination | Replacement invalidates old local ownership even if a human-readable name is reused. The successor resumes only from the replicated cursor/state promised by that subsystem. |
| Pending task identity | index backfill/rewrite version, bloom snapshot/epoch, cache snapshot invocation | Completion publishes only when the ledger/index/version or captured filter epoch is still current. Interrupt joins before replacement publication. |
| Durable cursor | indexbuilder, auditindexer, usagebuilder and tail workers | Process Stop joins the fold. Restart resumes from the subsystem's atomic durable cursor contract; leadership alone does not restart per-replica workers. |
| Request or session context | snapshot, restore, file stream, read lease and query checkpoint | Cancellation and completion converge on exactly-once release and cannot publish partial or superseded output. |

Cache rotation is an FSM/cache consistency mechanism, not authority to restart
unrelated workers. Likewise a leadership term alone is not a complete resource
identity when configuration, ledger incarnation or pending version can change
within the term.

## Evidence required for a finding

For each hypothesis, establish the following from the audited SHA:

1. **Owner and entry:** identify the public call, Fx hook, observer event,
   notification, timer or callback registration and its documented call
   cardinality.
2. **Captured authority:** name the exact context, channel, generation, resource
   identity, cursor or snapshot captured before the concurrent boundary.
3. **Interleaving:** give a deterministic sequence with a real blocking point;
   include which lock is held, which state is published and which goroutine can
   proceed. A possible scheduler delay without a harmful postcondition is not a
   defect.
4. **Superseding transition:** show the later Start, Stop, leadership change,
   replacement, restore swap, cancellation or completion that invalidates the
   captured authority.
5. **Observable violation:** demonstrate a stale mutation/proposal/publication,
   use after close, double close, lost join, leak, deadlock, partial output or
   incorrect final owner set. Distinguish permitted at-least-once event delivery
   from a lifecycle violation.
6. **Protection and falsification:** inspect rechecks, context propagation,
   close-once guards, lock ordering, unregistration, durable cursor recovery and
   existing tests. Provide a barrier/channel/synctest or race-enabled test plan
   that fails specifically at the claimed transition.

P0, P1 and P2 findings require a reachable production path and concrete harmful
postcondition. A missing generation counter, mutex, test or context is not on
its own a finding. If ownership, Stop cardinality or timeout semantics are not
defined by code and authoritative documentation, emit an audit question.

## Stale callback and global-state proof

Treat callbacks as asynchronous tasks even when registration is synchronous.
For node observers, notification handlers, retry timers, task completion and
OpenTelemetry callbacks, prove both sides of the lifetime:

- publication cannot occur before all captured state is initialized;
- a callback that has started either finishes before teardown, or revalidates
  its authority after every blocking call and before every side effect;
- unregistration plus the provider's callback contract establishes the needed
  happens-before edge before captured stores, snapshots or resources close;
- a restore or resource replacement cannot leave the callback reading the old
  object while reporting it as the current process state.

For package-level registries such as optional sink factories, establish whether
they are immutable after process initialization. If tests or repeated app
construction can register concurrently, require synchronization and idempotent
identity; do not classify immutable generated tables or sentinel errors as
lifecycle state merely because they are global variables.

## Concurrent transition scenarios

At minimum, attempt to falsify these sequences with deterministic barriers:

1. Start blocks after acquiring a listener, store, registration or source; Stop
   or startup rollback begins; the blocked operation resumes.
2. Leader generation N blocks in config read, source/sink construction, retry,
   upload or terminal proposal; loss and gain publish N+2; N resumes.
3. Stop advances the terminal generation while a notification or timer is
   queued; the callback begins after worker join or dependency close.
4. A sink/source config, ledger incarnation, index version, bloom snapshot or
   backing database is replaced while old completion work is ready to publish.
5. Request cancellation races successful snapshot/restore/stream/checkpoint
   completion and both paths attempt release or publication.
6. Two Stop callers observe an active worker or server while the deadline of one
   expires; verify whether the persistent shutdown request and the documented
   join still complete exactly once.

Record initial and final generations, identities, goroutine completion, resource
close counts, proposal/publication counts and durable cursor bytes. A test using
`time.Sleep` does not establish the required ordering.

## Ownership and deduplication

One finding represents one root cause and required correction. Use stable ids
`concurrency-lifecycle-shutdown/<root-cause-name>` and compare available prior
reports and active fixes by mechanism, not only title.

| Adjacent domain | Boundary |
| --- | --- |
| `process-boundary-recovery` | Owns OS signals, supervisor-visible exit, real process re-exec and reopening durable state. This domain owns the in-process join/cancel/unregister defect that can make those transitions unsafe. |
| `raft-membership-leadership` | Owns election, quorum, membership, forwarding, transport protocol and snapshot-install consensus ordering. This domain owns delivery order and lifetime of callbacks/work triggered by an established leadership transition. |
| `persistence-restore-replay` | Owns durable checkpoint, restore and replay equivalence. This domain owns concurrent session cleanup and stale in-memory work crossing a store/snapshot replacement. |
| `read-consistency-projections` | Owns projection contents, served horizons and checkpoint semantics. This domain owns worker/task lifetime and generation fencing; the projection domain owns a wrong result with otherwise-correct lifetime. |
| `event-delivery-contracts` / `mirror-ingestion-contracts` | Own delivery acknowledgement/cursor semantics and source transformation respectively. This domain owns leader/config generation, resource shutdown and stale callback effects. |
| `idempotency-retries-partial-failures` | Owns logical retry identity and ambiguous external outcomes. This domain owns the retry goroutine or callback outliving its authority token. |
| `runtime-health-admission-contracts` | Owns measurement-to-verdict and admission consumption. This domain owns health worker/callback start, stop, join and stale-owner behavior. |
| `test-reachability-enforcement` | Owns test collection and CI execution. A missing concurrency test supports a concrete finding here but is not a separate product defect. |

Follow callees outside the manifest only to prove the scoped lifecycle path. If
the root cause is wholly owned by an adjacent domain, record it in inspected
areas or residual risk rather than duplicating a finding.

## Validation and execution

Validate the JSON with the exact structural `jq` predicate in
`scripts/ai-audit`, verify every path glob matches a current path and every
related document exists, then run `bash scripts/agent-check` and
`AI_REVIEW_BASE_SHA=<exact-base-sha> bash scripts/agent-check-pr`. These checks
validate the manifest change, not runtime correctness.

After this manifest is reviewed and merged, a separate outer task may run:

```bash
bash scripts/ai-audit concurrency-lifecycle-shutdown
bash scripts/ai-audit-challenge <audit-result.json>
```

The same clean HEAD must be used for both passes. Manifest work must not launch
the product audit. Jira publication and fixes remain separately authorized.
