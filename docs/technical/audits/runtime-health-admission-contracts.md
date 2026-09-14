# Runtime health and admission contracts audit

This domain follows a changing runtime signal into an existing admission
protection and its release. It complements static configuration validation,
worker lifecycle and consensus audits: a correctly configured, live worker can
still publish or consume the wrong runtime verdict. The native JSON manifest is
the scope; this companion defines evidence and ownership, not new product policy.

## Evidence chain

For each candidate establish:

1. The current authoritative promise for the specific signal and operation.
2. The production measurement or progress source, eligibility, sampling cadence,
   retained state and treatment of missing/error observations.
3. The transition from those observations to the published verdict or waiter.
4. The actual route, leader forwarding, admission consumer and gate ordering.
5. A reachable incorrect rejection, admission or release, its external result,
   and a falsifiable reproduction that distinguishes it from earlier guards.

Record the initial state, input sequence, each observable verdict and proposal
count. Recovery needs a real blocked/failed step followed by success, not an
always-failing fixture. A guard unit test, a conversion test and a full routed
request prove different links; do not present one as evidence for the others.
Paths are entry points, not permission to audit every mechanism in a directory.
Follow extra callees only to complete this chain and load their owning contract.

## Current anchors and limits

| Signal / surface | Current anchor and evidence boundary |
| --- | --- |
| WAL/data pressure | `diskusage.Collector.collect`, `HealthChecker.check`, `Thresholds.NextDiskBlocked`; CLI Server Health Check Flags. Fractions measure filesystem usage including reserved space, not just Ledger directory bytes. Hysteresis uses successful samples, with block equality and strict below-resume release. |
| Clock skew | `HealthChecker.exceedsClockSkew`: midpoint estimate, excessive-RTT discard, disabled check and failed call are distinct from an observed skew violation. Do not impose disk hysteresis on clock skew. |
| Missing or stale observations | Collector errors retain cached values; failed peer disk calls omit that peer's sample; initial nil gate permits writes. No sample-age deadline is encoded here. Whether a different freshness/fail-closed policy is desirable remains a question unless an existing promise proves a violation. |
| Combined reasons | `gateState` is atomically published and loaded once; disk error wins over skew, while either blocks. Existing no-torn-state and nonleader-reset regressions are protections to challenge against, not new findings. |
| Admission routes | `Admission.Admit` and `Barrier` call `CheckWritesAllowed` before proposing. Resolve actual HTTP/gRPC/controller forwarding and internal callers. A technical proposal outside these methods is not automatically a bypass defect; establish the promise for that operation. No global revocation of in-flight work is implied. |
| Progress prerequisites | `Admission.Admit`, `Node.WaitLeaderReady`, `waitClusterPolicyReady`, `checkQueryCheckpointProjectionReady` and `plan.resolve` expose distinct wait/reject conditions. This domain owns signal consumption and release; the computation of consensus progress, projection correctness and cache coverage belong to their neighbors. No universal lag threshold is promised. |
| HTTP probes | `DefaultBackend` and health handlers: `/livez` is process liveness, `/readyz` local Raft-loop start, `/health` local Leader/Follower health and `/clusterz` connectivity plus elected leader. Disk/skew do not feed these readiness predicates. |
| gRPC probes | `GRPCHealthUpdater.update` combines node health, elected leader and committed policy readiness for the service endpoint. Bootstrap registers a separate Raft-server health service. These are not interchangeable with HTTP readiness. |
| Error propagation | CLI health error table, `grpc/errors.go`, `grpcerr` and HTTP error handling: disk ResourceExhausted/429, skew Unavailable/503. Cache horizon is operational Unavailable. Trace the actual reason and wire status, not just a non-nil error. |

The deployment probe table currently describes `/clusterz` as including disk and
clock checks. That conflicts with `DefaultBackend.IsClusterReady` and the more
specific CLI health contract. Record this source conflict; do not infer a new
requirement to block readiness or present the old description as a new runtime
bug. Similarly, historical incident references, tests and old threshold examples
are context, not current findings or authority to tune thresholds.

## Ownership and exclusions

| Domain | Owns the root cause / required correction |
| --- | --- |
| `runtime-health-admission-contracts` | Runtime measurement-to-verdict propagation, combined reasons, consumption by promised admission routes and release after actual prerequisite recovery. |
| `configuration-startup-contracts` | Parsing, defaults, threshold validation and static constructor wiring. A wrong validated value delivered to the checker belongs there; a correctly delivered value misused during a transition belongs here. |
| `concurrency-lifecycle-shutdown` | Goroutine ownership, cancellation/join and shutdown ordering, including stale workers. This domain can inspect atomic verdict consistency; worker lifetime defects remain with the lifecycle audit. |
| `raft-membership-leadership` | Quorum, elections, membership, transport and correctness of leader-ready progress/forwarding. This domain consumes those signals without re-auditing how consensus establishes them. |
| `read-consistency-projections` | Read horizons, query results, projection/certificate correctness and reconstruction. This domain checks consumption of an available/unavailable projection signal by admission, not projection construction. |
| `persistence-restore-replay` | FSM/cache coverage, deterministic apply and persistence/recovery correctness. This domain traces the operational cache-horizon refusal and retry release; it does not re-prove coverage or replay. |
| `api-boundary-contracts` | General request validation and error classification/wire conversion. This domain follows health errors to prove a consumer outcome; a generic mapper defect belongs to the API domain. |
| `process-boundary-recovery` | Real process startup/exit, listener sequencing and durable reopen. This domain checks steady-runtime probe predicates and transitions, not OS lifecycle proof. |
| `idempotency-retries-partial-failures` / `test-reachability-enforcement` | Generic retry outcomes and test discovery/gates respectively. Recovery probes and missing tests are supporting evidence here, not duplicate findings about those mechanisms. |

Deduplicate by root cause and required correction, including across these broad
path overlaps. Route a wholly neighboring defect to inspected areas/residual
risk rather than issuing a second finding. Use stable ids
`runtime-health-admission-contracts/<root-cause-name>`. Compare available prior
reports and active PRs; state which were inspected and do not claim Jira/backlog
deduplication without reading it. Historical fixed defects require a new,
currently reachable counterexample before becoming candidates.

Exclude performance tuning, invented freshness SLAs, arbitrary thresholds,
universal blocking requirements, new product semantics and a generic monitoring
or security audit. Unspecified behavior and conflicting authority remain audit
questions. Local health can gate admission, never reinterpret committed business
intent or deterministic FSM apply.

## Execution and manifest validation

The dynamic checklist distinguishes existing tests from proposed probes. Record
exact commands, HEAD, assertions reached, results, skipped prerequisites and
remaining evidence limits. Use the repository-managed shared cache wrapper and
pinned Nix environment; do not purge shared caches, alter host clocks, fill real
disks or mutate a live cluster. Temporary diagnostics remain within the native
read-only audit contract; tracked reproducers and fixes belong to later work.

For manifest-only validation, apply the exact `jq -e --arg id` predicate in
`scripts/ai-audit` to this JSON without executing the launcher. Check that path
globs match current files and related document paths exist. Then run
`bash scripts/agent-check` and
`AI_REVIEW_BASE_SHA=<exact-base-sha> bash scripts/agent-check-pr` for this diff.
These checks validate the artifact and repository baseline, not runtime health
correctness. No new runner or schema extension is required.

After review and merge, a separately authorized outer task may run
`bash scripts/ai-audit runtime-health-admission-contracts` and then
`bash scripts/ai-audit-challenge <audit-result.json>` at the same clean HEAD.
Provider workers remain leaves. Manifest creation launches no product audit;
first-pass findings remain hypotheses, and Jira publication and product fixes
are separately authorized downstream actions.
