# linearizable-pit-partition-fails-closed — Default linearizable PIT fails closed across Raft partitions

## Catalog entry

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety and liveness |
| **Property** | A direct PIT aggregate with no `x-consistency` header must complete a quorum-confirmed linearizable barrier before reading local primary/history state. During a leader or quorum partition, every completed fresh attempt either returns a complete, oracle-exact view from a node that can still complete that barrier, or fails with a narrowly classified transient outcome; a node isolated from quorum must never serve even an apparently exact local view. After the partition heals and membership stabilizes, every current voter eventually returns the exact PIT result through a common acknowledged log floor. |
| **Invariant** | Workload `assert.Always(outcome.Success ? (faultModel.BarrierCanComplete(target) && validCompleteView(outcome.View, ledgerID, selector, minLog) && resultMatchesOracleAt(outcome.View.LogWatermark)) : isLinearizablePITPartitionTransient(outcome.Err), "pit: default linearizable PIT is exact or transient fail closed during Raft partition", details)`. For a fresh RPC started only after the driver confirms that the target has no path to a quorum, specialize the predicate to `assert.Always(!outcome.Success && isLinearizablePITPartitionTransient(outcome.Err), "pit: quorum-isolated default PIT never serves local history", details)`. After healing, use `assert.Sometimes(allStableVotersExactAt(minLog), "pit: default linearizable PIT recovers exactly after Raft partition", details)`. |
| **Antithesis Angle** | Keep workload-to-node gRPC and the history object store reachable while partitioning Raft links: isolate the current leader from both followers, isolate one follower as a minority, and separately leave a two-node majority connected. Start each probe only after the intended cut is active, use a short deadline and no transparent retry, then heal and require exact success from every voter in one stable leader-reported membership snapshot. |
| **Why It Matters / Impact** | The default API contract is linearizable. Falling back to a local PIT projection when the node cannot prove quorum would turn a loss of coordination into a plausible successful financial answer. A permanent failure after heal would make correctness or availability depend on the replica selected by the client. |
| **Confidence** | High for the repository-defined routing, barrier, history-boundary, and error contracts. Medium for deterministic selective-link coverage in the checked-in Antithesis environment because its launch recipe does not encode the required link-selection controls. |

## Assertion rationale

- The first `Always` is the public safety oracle. Every completed response matters: success must be complete and independently exact, and failure must be one of the expected temporary consequences of losing coordination or waiting for the local history watermark.
- The quorum-isolated specialization is required because result equality alone cannot prove linearizability. A stale local projection can coincidentally equal the oracle; a fresh request begun after the target is known to have no quorum path must not succeed at all.
- `Sometimes` is appropriate for post-heal progress because the property is eventual. It must quantify over every voter in one stable membership snapshot rather than accepting recovery on whichever node happens to answer first.
- Add workload `assert.Sometimes(isLinearizablePITPartitionTransient(outcome.Err), "pit: default PIT reaches a partition-induced fail-closed outcome", details)` as a coverage checkpoint. Do not require `HISTORY_BUILDING` or `HISTORY_BEHIND` specifically: a correct execution may stop earlier at the Raft barrier.
- Add SUT `assert.Reachable("pit: aggregate stopped at default linearizable read barrier", details)` in the PIT `AggregateVolumes` routing error path. This distinguishes an application-observed fail-closed barrier from a packet-level client timeout, but is guidance rather than the external correctness oracle.

All messages above are new, stable literals with the repository-standard `pit:` prefix.

## Exact outcome classification

`isLinearizablePITPartitionTransient` must be narrower than the workload's generic `IsTransient`:

- accept gRPC `Unavailable` for `ErrNoLeader`, `ErrNotLeader`, `ErrLeadershipLost`, `ErrNodeSyncing`, a server-side context deadline, or the PIT reasons `HISTORY_BUILDING` and `HISTORY_BEHIND`;
- accept wire/client `DeadlineExceeded` when the short request deadline expires without a structured permanent PIT reason;
- treat a locally cancelled probe as inconclusive command teardown, not as a property outcome;
- reject `Unknown`, `Internal`, `Aborted`, `NotFound`, and every other status;
- reject permanent PIT reasons such as `HISTORY_SOURCE_MISSING` and `HISTORY_CORRUPT`;
- do not admit `EXTERNAL_SERVICE_ERROR`: the targeted scenario keeps the object store healthy, and broadening the classifier would hide an unrelated dependency failure;
- do not admit `READ_INDEX_NOT_CAUGHT_UP` for the unfiltered aggregate used here. The unfiltered PIT path avoids read-index filtering, so that reason would indicate the probe drifted into a different path.

The response side is equally strict. Success requires exactly one decodable `x-point-in-time-view-bin` trailer, the expected ledger incarnation, requested timestamp and axis, a non-empty token, `logWatermark >= minLog`, and the complete canonical aggregate equal to the workload's independent monetary fold through that returned watermark. Missing/duplicate trailer values, duplicate buckets, malformed values, or a partial payload are failures, not transient outcomes.

## Workload and fault sequence

1. Enable balance history, create a dedicated ledger, and generate deterministic transactions whose accepted log effects are retained in an independent prefix-indexed monetary oracle. Use an unfiltered `AggregateVolumes` request so the test does not depend on the read-side metadata index.
2. Stop this driver's writes, commit a distinctive marker transaction, retain its acknowledged log sequence as `minLog`, and wait until every current voter can serve an exact PIT view through that floor before injecting the fault. Omit `x-consistency` entirely; sending explicit `linearizable` would not test the public default.
3. Resolve the current leader and voter set from a stable leader `GetClusterState` snapshot. Construct one direct, single-address, no-retry client per target and attach a unique probe ID for correlation with SUT reachability details.
4. Exercise three driver-known cuts independently:
   - isolate the current leader from both followers while keeping the workload-to-leader service link open; a fresh leader RPC must return only a classified transient;
   - isolate one follower from the leader and the other voter while keeping its service link open; a fresh follower RPC must return only a classified transient, whether it still remembers the old leader or has already cleared it;
   - partition one follower away while the leader and the other follower retain quorum; calls on the connected majority may succeed exactly or fail transiently during election churn, but may never return an incomplete or unclassified outcome.
5. Start probes only after the fault controller reports the cut active, and do not reuse an RPC begun before activation: a barrier completed before the partition may legitimately allow that earlier request to succeed. Keep per-RPC deadlines short enough to sample the fault window without converting transparent waiting into an apparent post-heal success.
6. Heal all links and stop fault injection. Wait for one stable leader/membership snapshot and a common acknowledged `minLog`. Poll every voter directly, still with no consistency header and no retry inside an individual RPC. Membership change invalidates the sample and restarts the final phase.
7. Pass liveness only when every voter returns a complete exact view at or above the common floor within the ordinary bounded Antithesis command context. `HISTORY_BUILDING`, `HISTORY_BEHIND`, and transport transients are retryable during this phase, not terminal success.

The existing singleton quorum-recovery driver supplies a repository-controlled partial variant: after it stops the two non-leaders, the still-reachable leader lacks quorum and can be probed before force-removal/reconfiguration. It does not replace the selective network cases because stopped followers cannot be queried and membership repair changes the quorum rather than healing the original links.

## Code evidence

- `internal/adapter/grpc/consistency.go:11-20,32-39,72-92` defines linearizable as the default, leaves a missing header at that default, and only recognizes explicit `stale` and `leader` overrides. `internal/adapter/grpc/consistency_test.go:27-71,107-116` covers missing, explicit-linearizable, and invalid metadata cases.
- `internal/bootstrap/controller_routed.go:50-101` sends the default route through local `ReadIndexAndWait`. Only syncing/not-leader followers may fall back to the leader; if the leader's own barrier fails, the code explicitly refuses a stale local read. `AggregateVolumes` does not enter the controller until that routing step succeeds (`controller_routed.go:274-286`).
- `internal/infra/node/read_index.go:48-85` dispatches a uniquely correlated Raft ReadIndex and refuses a follower with no known leader. `ReadIndexAndWait` waits first for quorum confirmation and then for the local FSM to apply the returned commit index (`read_index.go:94-156`). Leadership loss resolves pending reads with an error (`read_index.go:159-167`).
- `internal/application/ctrl/controller_default.go:956-993` pins one primary read handle and raises the requested PIT history floor to the current log head observed through that same handle. The unfiltered path then aggregates from the pinned historical view (`controller_default.go:1000-1020`).
- `internal/bootstrap/balance_history_provider.go:41-66` rejects disabled, disallowed, or process-locally unready history before opening a view. `internal/application/ctrl/volume_view.go:74-117` waits for the required history watermark and constructs the immutable token from the pinned manifest.
- `internal/storage/balancehistorystore/store.go:527-552` keeps the watermark wait closed on persistent failure, incomplete history, or a manifest below the required log.
- `internal/adapter/grpc/server_bucket.go:1386-1445` validates the PIT selector, invokes the controller with `minLogSequence`, and emits the view trailer only after receiving a valid result.
- `internal/adapter/grpc/server.go:457-641` maps node/leadership/sync errors and server-side context expiry to `Unavailable`; already-formed gRPC errors pass through. `internal/adapter/grpc/errors_test.go:694-715,790-815` covers no-leader, leadership-loss, not-leader, syncing, and deadline mappings.
- `tests/antithesis/workload/internal/client.go` classifies global transient statuses, but that set also includes unrelated dependency and read-index reasons. This property therefore needs its own strict classifier.
- `tests/antithesis/workload/internal/pernode.go:82-116` installs an `UNAVAILABLE` retry policy and retry interceptors on every direct connection. Those retries can carry an attempt across the heal boundary and must be disabled for this oracle.
- `tests/e2e/cluster/point_in_time_forwarding_test.go` covers successful default leader/follower reads and explicit leader forwarding, but does not remove quorum or partition a target.
- `antithesis/scratchbook/existing-assertions.md` records no PIT-specific SDK assertion. The current assertion surface cannot distinguish a default linearizable failure from a stale local success.
- `antithesis/scratchbook/deployment-topology.md` provides three Ledger voters and direct per-node gRPC addresses, but its checked-in launch recipe does not specify selective link targeting or workload/object-store fault exclusions.

## Instrumentation status

- **Existing — routing diagnostics:** `router.read_ctrl` records `local_linearizable`, `leader_fallback`, and `leader_readindex_failed`; `node.read_index_quorum` and `node.wait_for_applied` delimit the two barrier phases. These are traces, not Antithesis assertions.
- **Existing — exact-node access and model patterns:** per-node clients, node-ID resolution, stable membership snapshots, accepted log-sequence floors, prefix-aware workload models, and quiescence loops are reusable.
- **Partial — no-quorum execution:** the singleton quorum-recovery driver creates a live leader with two voters stopped. Add the PIT probe before membership repair; this covers fail-closed leader behavior without selective network injection.
- **Missing — no-retry direct PIT client:** add a boolean retry parameter to the existing `DialPerNode` path, preserving the classification interceptor while disabling the service retry policy and retry interceptors. This follows the repository preference for a mode parameter instead of a duplicate method.
- **Missing — PIT trailer/oracle helper:** reuse the raw generated-client plus `grpc.Trailer` pattern from the PIT E2E tests, canonicalize the aggregate, and compare against the independent prefix oracle.
- **Missing — strict outcome classifier:** implement the property-local classifier above rather than weakening or reusing the generic transient set.
- **Missing — SUT reachability:** place the proposed `pit: aggregate stopped at default linearizable read barrier` signal where a PIT `AggregateVolumes` call returns from routing before the local controller is invoked. Include node ID, current leader ID, consistency, error kind, and probe ID; do not include result correctness in this reachability assertion.
- **Missing — post-heal liveness:** no existing assertion requires a default-consistency PIT success from every current voter after a partition.

## Open questions

- `(needs human input)` Can the deployed Antithesis fault controller selectively cut only Raft traffic between named Ledger pods while preserving workload-to-target gRPC and Ledger-to-MinIO traffic? The repository defines the desired topology and direct addresses but does not encode the external launch/webhook link-selection capabilities. If unavailable, run the repository-controlled live-leader/no-quorum variant now and keep the full follower/minority and majority-side cases pending environment support.

### Investigation Log

#### What does an omitted consistency header do for PIT?

- **Examined:** the gRPC consistency interceptor and tests, `RoutedController.readCtrl`, `RoutedController.AggregateVolumes`, `Node.ReadIndexAndWait`, and the PIT controller/provider/view path.
- **Found:** omission is exactly the linearizable default. The receiving node must obtain a quorum-confirmed ReadIndex and apply that index before local controller access, except that a syncing or not-leader follower may forward to the leader. The forwarded request also defaults to linearizable. No branch falls back to local stale data after a failed leader barrier.
- **Not found:** any PIT-specific bypass of routing, any default-to-stale behavior, or any successful controller invocation after a barrier error.
- **Conclusion:** the property must omit the header, probe a single receiving address, and distinguish quorum-capable exact success from quorum-isolated mandatory transient failure.

#### Which errors are legitimate fail-closed partition outcomes?

- **Examined:** node read-index errors, routed fallback conditions, gRPC error conversion/tests, balance-history provider/store errors, and the workload's global transient classifier.
- **Found:** coordination loss and server-side deadline paths become `Unavailable`; a client-side wire deadline can remain `DeadlineExceeded`; local builder catch-up uses `HISTORY_BUILDING`/`HISTORY_BEHIND`. Permanent history integrity/configuration failures and unrelated external-service errors have different meanings.
- **Not found:** a repository reason to accept `Internal`, `Unknown`, `NotFound`, `HISTORY_SOURCE_MISSING`, `HISTORY_CORRUPT`, or `EXTERNAL_SERVICE_ERROR` in a targeted Raft-only partition.
- **Conclusion:** use the narrow property-local classifier, and treat every other completed error as a counterexample rather than generic chaos noise.

#### Can the current workload observe one attempt on one node?

- **Examined:** per-node dialing, gRPC retry configuration, consistency helpers, PIT E2E trailer capture, cluster-state discovery, and the singleton quorum-recovery driver.
- **Found:** addresses and node identities are available, but `DialPerNode` transparently retries `Unavailable` up to the configured policy. Such an RPC can begin under partition and succeed after heal, destroying the temporal attribution. The quorum-recovery driver can already expose a reachable leader without quorum before it repairs membership.
- **Not found:** an existing no-retry per-node mode, PIT-specific assertion helper, or a current probe in the no-quorum window.
- **Conclusion:** add the retry mode parameter and short per-attempt contexts; integrate the leader probe into the existing no-quorum phase for immediate partial coverage.

#### Can repository configuration express the selective partition matrix?

- **Examined:** the Antithesis Kubernetes topology, workload fault exclusions, per-node addresses, fault/property mapping, and checked-in launch recipe.
- **Found:** the intended three-voter topology and the need to preserve workload access are documented. The repository can kill pods and alter membership, but the checked-in files do not describe named-link Raft-only cuts or how to exempt the object-store path from the same network fault.
- **Not found:** a repository-owned API, label, webhook parameter, or launch flag that proves those selective cuts are available.
- **Conclusion:** selective link targeting remains an environment capability question requiring human confirmation. It affects campaign reachability, not the code-level property or the live-leader/no-quorum partial variant.

#### What constitutes recovery after heal?

- **Examined:** leader membership reporting, per-node direct clients, PIT watermark semantics, and the existing quiescent convergence property.
- **Found:** one stable leader snapshot provides the voter denominator; every successful view carries the exact ledger/selector/watermark token needed for an independent prefix comparison. There is no PIT-specific production recovery SLA.
- **Not found:** justification for dropping an unreachable current voter or accepting success from only the leader.
- **Conclusion:** use the ordinary bounded Antithesis command context, restart on membership change, and require exact eventual success from every stable voter. The bound is a campaign budget, not a production latency promise.
