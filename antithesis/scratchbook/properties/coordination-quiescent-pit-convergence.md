# coordination-quiescent-pit-convergence — Quiescent replicas converge to one PIT result

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Liveness |
| **Property** | After writes, membership changes, and injected faults stop, every member in one stable leader-reported Raft membership snapshot (voter or learner) eventually reaches the same durable Raft index and serves the same exact monetary result through the common authoritative log head. |
| **Invariant** | Workload `assert.Sometimes(all_stable_raft_members_ready_at_common_head && all_results_equal_oracle, "pit: all quiescent raft members converge exactly", details)`. The predicate covers every node ID in the stable leader membership snapshot; an unreachable, syncing, unresolved, or monetarily divergent current member remains in the denominator until the 10-minute command deadline. |
| **Antithesis Angle** | Run sustained writes with elections, partitions, process kills, snapshot synchronization, and asymmetric object-store faults; then use an `eventually_` command after fault injection and other drivers stop. Search for replicas stuck in `BUILDING`/`BEHIND`, restore loops, or permanently divergent local projections. |
| **Why It Matters / Impact** | Replica-local lag is expected during faults, but permanent non-convergence leaves PIT availability dependent on which pod a client reaches. It can also mask a one-node semantic divergence behind leader routing. |
| **Confidence** | High. The existing workload establishes the quiescence and exact-index gate, the leader exposes the current Raft membership, and PIT readiness requires exact source-head reconciliation. The 10-minute bound is an Antithesis campaign budget, not a production latency guarantee. |

**Implementation:** Implemented. `first_default_ledger` seeds the pre-fault
half of a fixed two-bucket oracle; the dedicated `eventually_pit_convergence`
command appends an
idempotent post-fault marker and accepts only a stable, all-member, exact-index
sample whose direct stale PIT views match that oracle. The builder emits a
tagged reachability signal only after readiness reopens at the exact source
audit/log head.

## Code evidence

- `tests/antithesis/workload/bin/cmds/main/eventually_pit_convergence/main.go:18-43` gives the property an independent ten-minute `eventually_` context and owns the single `pit: all quiescent raft members converge exactly` liveness assertion.
- `tests/antithesis/workload/internal/quiescence.go:10-55` is the shared exact two-barrier proof used by both PIT convergence and the older cache oracle; two committed barriers exactly one index apart establish `B` without duplicating a weaker gate.
- `tests/antithesis/workload/internal/pit_convergence_coordination.go:38-203` commits the post-fault marker, establishes `B`, retains equal pre/post leader memberships, requires the full direct-member set, rechecks exact indexes after PIT, and accepts only a final `B+1` barrier.
- `tests/antithesis/workload/internal/pit_convergence_coordination.go:248-365` derives the complete sorted voter/learner denominator from leader state and resolves it all-or-nothing against direct addresses; lines 367-469 implement the 60-second exact-index phases and per-member monetary observations.
- `internal/infra/node/node.go:1955-2044` samples `LastPersistedIndex` with Raft status and, on the leader, builds the current node list directly from `status.Progress`, labeling every entry `Voter` or `Learner`.
- `tests/antithesis/workload/internal/pernode.go:58-94,135-173` shows that configured per-node addresses are only dial targets and that node IDs are resolved from the leader's node list. An unresolved or unreachable address is not evidence of membership, and a current member must not be discarded merely because resolution initially failed.
- `misc/operator/internal/controller/reconcile_statefulset.go:751-756` and `docs/ops/deployment.md:355-369` document that Kubernetes `/readyz` only means the local Raft loop started; it deliberately does not prove quorum, source synchronization, or PIT readiness.
- `internal/infra/node/node.go:2702-2739` promotes learners from Raft activity and match progress alone. PIT readiness is not a promotion prerequisite, so excluding a caught-up learner would leave a real stale-local serving target untested.
- `internal/application/balancehistory/builder.go:556-591` retries one build pass on its ticker, keeps readiness false while behind, and reopens only after caught-up repair/certification.
- `internal/application/balancehistory/builder.go:751-779` requires exact equality between manifest and sampled source audit/log heads before setting `ready=true`.
- `internal/bootstrap/balance_history_provider.go:59-66` turns non-ready local state into a fail-closed PIT response.
- `antithesis/scratchbook/deployment-topology.md:73-111` sets the campaign builder batch/yield to 32/1 ms and the verifier interval to 2 seconds. `eventually_pit_convergence/main.go:18-24` fixes the property budget at ten minutes, while `pit_convergence_coordination.go:14-20,367-421` fixes direct RPCs at five seconds and each all-member exact-index phase at 60 seconds.
- `internal/application/balancehistory/performance_evidence_test.go:795-806,1321-1343` bounds a 100,000-proposal default-cadence backfill at 90 seconds. This is useful headroom evidence, but it is not a cold-source SLA: the composite source may fetch archived primary chapters (`internal/bootstrap/balance_history.go:121-140`), so the campaign must also keep its generated source volume bounded.

## Proposed oracle sequence

1. Give the whole `eventually_` command a 10-minute context. Keep each direct PIT RPC short and no-configured-retry so one blocked replica or S3 call cannot consume the command budget.
2. Seed a fixed `2^64 + 1` bucket before faults, then idempotently append a distinct `2^128 + 1` bucket after faults and writers stop. Its acknowledged log sequence is the PIT floor and its two canonical buckets are the independent oracle.
3. Establish two consecutive barriers exactly one Raft index apart, producing exact durable target `B`.
4. Through `GetClusterState{NodeId: 0}`, take two equal leader snapshots of the complete `(node ID, suffrage, service address)` list. Include voters and learners. Ignore configured addresses and Kubernetes-ready pods absent from that list.
5. Resolve one direct connection for every snapshotted member and require each to report `sync_progress.status == normal` and `last_persisted_index == B`, using the existing 60-second phase bound. Unlike the older equality oracle, do not silently drop a syncing, unreachable or unresolved current member.
6. Poll direct stale PIT requests with the acknowledged marker as `minLogSequence`. Every success must authenticate the ledger incarnation and selector, cover the marker, contain no duplicate buckets, and equal both the independent oracle and every other member. Any error, including `HISTORY_SOURCE_MISSING` or `HISTORY_CORRUPT`, remains non-convergence.
7. Recheck every member at exact index `B`, take two equal post-read membership snapshots and issue the final barrier. If membership/suffrage/leader changed or the barrier is not exactly `B+1`, discard the sample and restart from quiescence.
8. Pass only when the stable snapshot is non-empty and every member satisfied the complete proof. A stable current member that remains unavailable or divergent at the 10-minute deadline fails this liveness property.

The 10-minute deadline is deliberately the repository's ordinary Antithesis command bound. The accelerated PIT cadence and the 90-second 100,000-proposal backfill test provide substantial hot-source headroom, while ten minutes leaves space for bounded archived-chapter fetches and restart recovery. Because cold replay time also depends on generated bytes and object-store behavior, do not derive the bound from ticker intervals alone or extend it indefinitely: cap the campaign's retained history/object volume, and treat a miss under that fixed profile as the counterexample.

## Instrumentation candidates and existing coverage

- **Implemented — quiescence:** `eventually_cross_node_identity` and `eventually_pit_convergence` both call the shared `WaitForQuiescentCommitIndex` helper. The PIT command owns an independent ten-minute context, so the older cache/account/audit oracle cannot consume its liveness budget.
- **Implemented — membership and learner scope:** equal pre/post leader snapshots carry every voter and learner. Resolution is all-or-nothing and each member is checked at `last_persisted_index == B` both before and after PIT reads.
- **Implemented — builder catch-up:** the tagged SUT emits `Reachable("pit: balance history reconciled to exact source head")` after `markReadyAfterReconciliation`, with manifest version and exact audit/log watermarks. The workload's `Sometimes` remains the liveness assertion and provides node identity through the stable direct-member map.
- **Implemented — follower restore completion to PIT readiness:** existing `sync_with_leader_complete` lifecycle events can be correlated in the same timeline with the builder-ready signal; the workload still requires the restored member's direct exact PIT result rather than accepting the internal event alone.
- **Implemented — PIT result equality:** the two-bucket pre/post-fault fixture is checked through the acknowledged post-fault log floor on every member. Canonical results must equal the independent oracle and one another; physical manifest versions and view tokens may differ.

## Open questions

- None.

### Investigation Log

#### Which nodes belong in the convergence denominator?

- Examined: the leader and targeted-node `GetClusterState` paths; Raft `status.Progress` conversion; per-node address discovery; Antithesis scaling helpers; operator StatefulSet readiness; and deployment health documentation.
- Found: only the current leader returns a complete node list, built directly from live Raft progress and labeled by suffrage. `LEDGER_PER_NODE_GRPC_ADDR` is a static superset covering possible ordinals, while `/readyz` is intentionally permissive and proves only that a local Raft loop started. Neither configured addresses nor Kubernetes readiness are membership authority.
- Not found: any code or documented contract making desired StatefulSet replicas, static workload addresses, or Ready pods the authoritative Raft membership set.
- Conclusion: resolve the denominator from one stable leader `ClusterState.Nodes` snapshot. Include every listed member and require a direct connection by node ID/address. A configured or Ready pod absent from Raft membership belongs to operator/membership testing; a listed member that remains unreachable belongs to this liveness failure.

#### What quiet/recovery budget should the property use?

- Examined: the existing quiescence oracle's 60-second exact-index and 5-minute overall bounds; the common 10-minute workload command context; 10- and 15-minute scaling recovery bounds; PIT builder batch/tick/yield behavior; the proposed accelerated Antithesis profile; composite hot/cold source wiring; and the performance-evidence backfill limits.
- Found: boot catch-up drains consecutive bounded batches without waiting for the 200 ms steady-state ticker; the test profile uses 32 proposals per batch with a 1 ms yield and a 2-second verifier interval. The performance suite gives even a 100,000-proposal default-cadence hot-source backfill 90 seconds. The common workload hang bound is 10 minutes. The longer 15-minute scaling bound covers operator repair while faults remain active, whereas this property runs only after Test Composer stops faults and writers. Archived-source duration is data/bytes/network dependent and has no repository latency SLA, so ticker arithmetic cannot supply a sound universal timeout.
- Not found: a PIT-specific production recovery SLA or cold-archive benchmark that justifies a different fixed wall-clock guarantee.
- Conclusion: use the existing 60-second exact-index sub-gate inside a 10-minute overall `eventually_` command, with short per-RPC deadlines and a bounded campaign data/object profile. Treat this as an Antithesis search budget, not product behavior; a stable member missing it under the fixed profile is the liveness counterexample. Full backup/PVC restore keeps its separate restore campaign and budget.

#### Are learners required to converge before promotion?

- Examined: learner inclusion in `eventually_cross_node_identity`; leader Raft progress reporting; auto-promotion conditions; direct stale-read behavior; scaling convergence helpers; and PIT's process-local readiness provider.
- Found: the existing exact-index oracle deliberately includes learners because snapshot restore must produce the same local state. Auto-promotion checks Raft activity and match distance only, not PIT readiness. A learner has its own local store and direct stale-read path, and Kubernetes readiness does not protect PIT traffic.
- Not found: any API rule preventing stale-local reads on learners or any promotion gate that waits for the local PIT projection.
- Conclusion: include every learner present in the stable leader membership snapshot. If membership or suffrage changes during measurement, restart from quiescence; the separate scale-up property may add transition-specific signals but does not replace this final learner liveness obligation.
