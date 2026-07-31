# protocol-leader-forwarding-provenance — Leader forwarding preserves the complete PIT response

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Safety |
| **Property** | A PIT read explicitly forwarded through a follower to the leader returns the same monetary result and exact immutable view record as a direct read served by that leader for the same selector and manifest. |
| **Invariant** | For a control pair bracketed by the same local `(leaderID, term)` on both endpoints and whose returned view tokens match, `AlwaysOrUnreachable(proto.Equal(directResult, forwardedResult) && proto.Equal(directView, forwardedView))`. `Sometimes(stableForwardedPITSucceeded)` proves explicit follower-to-leader forwarding completed, and `Sometimes(stableCommonManifestPair)` prevents the exact-equality assertion from passing vacuously when maintenance republishes between calls. |
| **Antithesis Angle** | Partition the follower from the leader, trigger elections, and restart either endpoint between direct and forwarded attempts. Compare only a pair known to be served by the same leader/manifest; transient routing failures remain classified instead of weakening equality. |
| **Why It Matters** | Forwarding adds an extra gRPC client/server boundary where selector fields, aggregation options, error details, or the trailing provenance record can be dropped or rewritten. |
| **Confidence** | High |

## Exact SDK assertion plan

```go
assert.AlwaysOrUnreachable(
    proto.Equal(directResult, forwardedResult) &&
        proto.Equal(directView, forwardedView),
    "pit: leader forwarding preserves result and immutable provenance",
    details,
)
assert.Sometimes(
    stableForwardedPITSucceeded,
    "pit: follower successfully forwards a PIT read to the leader",
    details,
)
assert.Sometimes(
    stableCommonManifestPair,
    "pit: direct and forwarded controls observe one leader manifest",
    details,
)
```

The comparison includes requested timestamp, axis, numeric ledger incarnation, audit/log watermarks, manifest version, availability floor, and token. It also checks every monetary bucket and group after canonical ordering. A leader/term change between the bracket probes invalidates that sample; retry the control pair instead of accepting unequal provenance. A stable leader alone does not prove the same manifest because builder publication, compaction, or tiering can increment the leader's manifest between the two RPCs. Equal returned view tokens establish the common-manifest premise; `Sometimes(stableCommonManifestPair)` makes persistent token rewriting or failure to reach a stable pair visible.

`AlwaysOrUnreachable` matches the optional completed pair under faults. `Sometimes` proves the actual follower-to-leader path, which is more informative than a generic handler reachability marker.

## Workload oracle

No test-only route diagnostic is required:

1. Use `DialPerNode` to map single-target connections to Raft node IDs.
2. Query `GetClusterState{NodeId: leaderID}` and `GetClusterState{NodeId: followerID}` immediately before the direct/forwarded calls. Require both local responses to report the same non-zero leader ID and Raft term, and require the target node IDs to identify one leader and one follower.
3. Issue the direct request to the leader connection. Issue the forwarded request to the follower connection with `x-consistency: leader`, preferably through a no-retry per-node probe so one observation is one forwarding attempt.
4. Repeat both node-specific state probes after the calls. Discard the sample unless leader ID and term are unchanged on both nodes. Raft terms are monotone, so the same pair before and after rules out an intervening election that later returned to the same node.
5. Mark `stableForwardedPITSucceeded` for a successful bracketed forwarded response. Evaluate exact result/view equality only when the direct and forwarded view tokens match, and require `stableCommonManifestPair` to occur at least once during the run.

This resolves leader identity separately from manifest identity: node-specific Raft state proves the routing window, while the immutable token proves both RPCs observed the same manifest.

## Code evidence

- `internal/adapter/grpc/consistency.go:11-23,72-92` defines `x-consistency: leader` and ignores unknown consistency values.
- `internal/bootstrap/controller_routed.go:31-48,50-101` selects a gRPC-backed leader controller for explicit leader consistency and for eligible linearizable fallback.
- `internal/bootstrap/controller_routed.go:274-287` forwards all aggregation parameters through the selected controller.
- `internal/adapter/grpc/client_bucket.go:383-421` serializes the selector/options, captures the upstream trailer, fails closed if it is missing/mismatched, and returns the decoded view to the follower server.
- `internal/adapter/grpc/server_bucket.go:1417-1445` re-emits that returned view as the downstream trailer.
- `internal/adapter/grpc/server_cluster.go:101-139` routes `GetClusterState{NodeId: 0}` to the leader and a non-zero node ID to that exact replica, so the workload can inspect both endpoints rather than trusting a round-robin observation.
- `internal/infra/node/node.go:1973-2097` samples each node's local Raft status and exposes `leader`, `local_node`, and the durable `HardState.Term` in one `ClusterState`.
- `tests/antithesis/workload/internal/pernode.go:28-40,82-175` already provides single-target Bucket/Cluster clients and maps advertised service addresses to node IDs using leader state.
- `tests/antithesis/workload/internal/k8s.go:358-385` already derives the current leader and a non-leader voter from `GetClusterState`; the property needs the stronger node-specific before/after probes described above.
- `internal/storage/balancehistorystore/publish.go:308-409`, `compact_stream.go:104-116,355-388`, and `tier.go:743-810` increment and publish new manifest versions. Therefore stable leadership and a fixed minimum log sequence do not by themselves establish identical physical provenance across two calls.
- `tests/e2e/cluster/point_in_time_forwarding_test.go:50-108` deterministically compares direct and forwarded token/manifest identity and separately notes that a local follower view may use distinct physical provenance.

## Scope boundary: local replica reads

Do **not** generalize exact token equality to independent stale-local reads. Each replica owns a physical history store and may compact or publish at different times. At a common sufficient watermark, local replicas must agree on the monetary result, selector, ledger incarnation and compatible source prefix; their token and manifest version may legitimately differ. Exact token equality belongs only to forwarding to the same leader view.

## Failure scenario

The follower's internal `BucketGrpcClient` receives the leader body but fails to capture or decode its trailer, or the follower server reconstructs a token from its local store. The client sees a successful forwarded balance with provenance that does not identify the view that answered it.

## Existing versus missing instrumentation

- **Existing deterministic coverage:** a three-node E2E test exercises proposal forwarding, direct leader PIT, explicit read forwarding, exact token relay, and local follower convergence.
- **Existing SDK coverage:** none for PIT forwarding.
- **Partially present workload support:** per-node clients, node-ID resolution, leader discovery, and Raft term/leader fields already exist. Missing pieces are a leader-consistency context helper, no-retry per-node probes for this observation, the before/after node-state bracket, and the three assertions above.
- **SUT-side instrumentation:** not required for correctness after the workload bracket and common-token gate. An internal `Sometimes` route signal could distinguish explicit forwarding from linearizable fallback in reports, but it would be supplementary search guidance rather than the oracle.

## Open Questions

- None.

### Investigation Log

#### Can the workload obtain an authoritative leader identity around both control calls without adding a test-only route diagnostic?

- **Examined:** `internal/adapter/grpc/server_cluster.go:101-165`, `internal/infra/node/node.go:1973-2097`, `tests/antithesis/workload/internal/pernode.go:28-175`, `tests/antithesis/workload/internal/k8s.go:358-385`, `internal/bootstrap/controller_routed.go:31-101`, and `tests/e2e/cluster/point_in_time_forwarding_test.go:50-108`.
- **Found:** yes. A non-zero `GetClusterStateRequest.node_id` is routed to that exact replica; its response carries local node ID, locally observed leader ID, and durable Raft term. `DialPerNode` already maps one connection per advertised node address to its node ID. Bracketing both the chosen leader and follower before and after the two calls with the same non-zero `(leaderID, term)` establishes a stable routing window without a new SUT diagnostic. Monotone Raft terms prevent an election away and back from masquerading as the same bracket.
- **Additional finding:** the stable routing window does not establish a common physical history manifest. Builder publication and local maintenance can increment the leader manifest between sequential RPCs even when leadership and the requested minimum log sequence are stable. The response token already binds manifest identity, so exact result/view comparison must be gated on equal returned tokens and paired with `Sometimes(stableCommonManifestPair)` to avoid vacuity.
- **Not found:** an existing workload helper that applies `x-consistency: leader`, a no-retry variant of `DialPerNode`, or an existing SDK assertion that performs the node-state bracket. These are workload implementation gaps, not unresolved contract questions.
- **Conclusion:** resolved. Remove the open question; use existing node-specific cluster state for leader/term identity, returned view tokens for manifest identity, and the refined oracle above.
