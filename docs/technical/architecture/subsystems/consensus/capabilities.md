# Coordinated-upgrade capabilities

Ledger's capability metadata is the protocol foundation for future incompatible
features deployed with a coordinated stop-and-restart. It is separate from the
client-facing gRPC protocol revision. This release does not implement a
mixed-version rolling activation path.

## Wire contract

`common.ClusterPolicy` carries the replicated activation state.
`raft.Proposal.required_capabilities` carries stable producer-owned capability
IDs. The fields are optional and therefore backward-readable by binaries that
predate this foundation. IDs are never reused; semantic dependencies must be
declared even when they do not add bytes to a proposal.

The producer must rebuild its plan first, select the semantics from the
committed activation policy where applicable, and stamp
`required_capabilities`. The proposal factory serializes that command and
copies the declaration into typed admission metadata. Admission reuses that
metadata without decoding the serialized proposal. The Ledger-specific
catalog owns the selection rule; the generic capabilities package evaluates
policy and membership evidence. A payload that depends on an active
capability but omits its declaration is rejected by the final `Node.Propose`
policy gate.
An activation racing the producer's policy read rejects the unmarked
proposal before Raft rather than applying it under old semantics.

## Admission and membership

The pure decision contract in `internal/infra/capabilities` evaluates every
current voter and learner. A member is represented by `(node_id, instance_id)`;
an identity refresh is a new incarnation and cannot silently inherit evidence
from the old one. Capability activation uses committed support evidence, not a
live health poll: a member that has already acknowledged the capability may be
temporarily unreachable when the activation is committed. Reachability still
blocks admission of capability-dependent writes until that member is reachable
again. Once a capability is active, every new membership announcement must
include that capability.

Restart and snapshot self-address refreshes cannot add support IDs: they retain
only the intersection of the persisted same-incarnation evidence and the local
binary's supported IDs. The activation worker first commits any missing leader
self evidence through RefreshMembership, including in a single-member cluster.
A failed or merely proposed refresh cannot make an activation ready. Address
refresh still writes routing before publishing its cache; a failed write leaves
the old cache unchanged. Snapshot refresh intersects the newly installed rows,
not the pre-snapshot cache, before rehydration.

A proposal that declares a capability is admitted only when that capability is
active and every member advertises it. Proposals with no declared capability
remain unaffected unless their order requires an active semantic capability.
FSM apply never consults node-local capability flags.
Membership evidence is an authenticated cluster fact, not a proof of code
inspection: a node can only cause activation for IDs in the leader's local
registry, and malformed (zero, duplicate, or unsorted) declarations are
rejected. Operators must therefore treat a compromised binary as outside the
trust model of this barrier.

## CreateLedger account-type validation (capability 1)

`CreateLedgerAccountTypesV2 = 1` selects the EN-2915 rule for every
`CreateLedger` order in the proposal. An unmarked proposal retains the released
behavior: validate each initial pattern's syntax and bounds, then persist its
declared constraints and any overlapping patterns. A marked proposal also
validates segment constraints and rejects equal-specificity overlapping
patterns, before allocating the ledger ID. `AddAccountType` retains its
existing validation regardless of this capability. Existing ledgers and audit
records are not rewritten; an already persisted invalid or conflicting type
remains until repaired through ordinary ledger configuration.

The marker is proposal-wide. Admission marks the whole atomic batch whenever
it contains `CreateLedger`; all such orders in that batch use the same rule.
Other order kinds have identical behavior under either marker. Successful
marked creations have the same audit log and derived state as the old processor
because their definitions satisfy both rules. The checker can replay successful
audited orders without the proposal marker; rejected marked creations have a
failure audit entry and no business effect to reconstruct.

The capability is advertised by this binary and automatically activated after
the embedded operator's mandatory stopped upgrade from RC.4, once every member
has committed support evidence for ID 1. The independent activation worker
commits the replicated activation; the ordinary policy reconciler preserves it.
Until that commit, creations retain the historical behavior. Do not activate during
a mixed-version rollout: an old voter could still apply a marked order using
the old rule. Hold new business admission until activation is committed so a
proposal prepared while the policy was inactive cannot be queued behind the
activation and retain old semantics. Once active, keep it active for all later
releases and restores. RC.4 cannot be restarted on a cluster with the active
capability or marked proposals; preserve the WAL and data volumes and recover
with RC.5.

Only CreateLedger admission adds a ClusterPolicy point read before proposal
construction. The final proposal gate already reads the authoritative policy;
its additional marker check is constant time, while capability membership
evaluation remains O(m) for m voters and learners. A marked proposal adds one
uint32 capability ID to the Raft payload; it adds no storage or preload reads
to FSM apply.

## Capabilities that change FSM semantics

Active state lives in a separate bounded aggregate. Activation commits directly
in one Raft entry and never consumes a configuration revision.
SetClusterPolicy retains its original identity and uses a configuration-only
marker to merge with the latest applied capability state inside the
FSM, so concurrent configuration cannot overwrite activation. Repeated activation
is idempotent; deactivation is rejected. A historical inactive entry is eligible
for automatic activation once membership is ready. There is no durable intent
before activation commits.

The membership coordinator fences membership changes through apply, and the
serialized Node.Propose gate rechecks member evidence before index allocation.
The same gate pauses all audited order batches whenever all current members
support a locally implemented but inactive capability. The capability remains
inactive in the committed view until activation applies, and membership is
locked through apply, so this barrier covers the entire in-flight interval.
This includes operational orders such as maintenance-mode changes and query
checkpoints. Native configuration and capability controls bypass this barrier. This
conservative MVP avoids feature-specific admission predicates and closes the
drain-release interval for every future capability. Admission retries only a
pre-submission activation-pending rejection, after releasing preparation guards.
New membership announcements must support active IDs. Before activation is
submitted, an incompatible membership change may win serialization; the final
support check then refuses activation. After submission, membership waits for
durable apply and must respect the active ID. A new leader waits for its FSM
readiness barrier before admission: inherited activation is either applied or
absent, then current membership is reevaluated. Lost responses and restart never
cause a committed activation to be undone. No intermediate `requested` state is
needed; the unreleased draft field is removed without reassigning its number.

The supported registry remains empty in this change. A feature must separately
register its stable ID, select its proposal marker at preparation, and dispatch
deterministic apply from that persisted marker. This infrastructure change does
not introduce the account-type validation feature from PR #2552.

1. The replicated `ClusterPolicy` controls whether the cluster may accept the
   behavior.
2. The committed proposal declares the capability that selects the behavior
   for its orders during FSM apply.

The serialized proposal currently carries `required_capabilities` at proposal
scope. That is sufficient only when every order in the proposal has the same
semantic contract. A proposal can batch multiple orders, so a future change
that mixes old and new semantics must either add a persisted marker to each
order (and dispatch apply from that marker) or reject mixed-semantic batches.
The conceptual marker for a homogeneous proposal is:

```text
old proposal:  required_capabilities = []
new proposal:  required_capabilities = [MirrorIngestRejectInvalidAssetV2]
```

The FSM must use the persisted declaration to select the behavior. It must not
infer the behavior from the current policy alone. An old order can be replayed
after the capability becomes active, and it must keep its old semantics.

If a producer cannot mark the new orders, the change must remain backward
compatible or add an explicit semantic version to the order. A capability by
itself is an admission barrier; it is not a historical replay version.

## Adding a protocol-breaking capability

Use this workflow when a change can make an order apply differently on two
binary versions.

1. Name the semantic contract. Describe the old behavior, the new behavior,
   and the orders that use each behavior.
2. Allocate a new capability ID. Never reuse an ID for a different contract.
3. Add the capability to `internal/infra/capabilities/catalog`. If inactive
   and active orders share a payload shape, select the declaration from
   committed policy during admission and enforce its presence at the final
   proposal gate.
4. Stamp the capability on every new order that needs the new behavior. Do not
   infer it from the current policy during FSM apply.
5. Keep the old behavior for orders that do not carry the capability. Add
   replay tests that apply both order forms with the new binary.
6. Add admission tests for an inactive capability, an active capability, and a
   member that does not advertise it.
7. Deploy the compatible binary to all current members. Let each restarted
   member refresh its membership evidence before activating the capability.
8. Activate the capability through the replicated `ClusterPolicy`. The FSM
   rejects a new member or membership refresh that omits an active capability.
9. Keep the capability active after activation. The policy processor rejects
   removal of an active capability.
10. Update this document in the same change. Record the capability ID, the
    semantic contract, the replay rule, and the rollout order.

Do not add a capability for a bug fix that restores the intended behavior when
the old behavior was invalid. Add one when valid historical orders need to keep
their old behavior while new orders use a different rule.

A change confined to read paths needs no capability either, even when it
touches a file the capability-contract gate watches. `common.proto` also
defines service response types that no order carries, and `internal/infra/plan`
also serves non-FSM reads. For example, the exact aggregate totals change
(EN-3332) retypes the `AggregatedVolume` response totals, which the mirror
stored-asset inventory also reads through `plan.Builder.AggregateVolumes`. No
order, proposal, or apply rule changes.

## Rollout and recovery

Deploy the barrier-aware binary to every member, stop the cluster, and restart
the complete cluster before enabling an incompatible feature. An old leader
cannot be made safe by upgrading only the other nodes. Restart, snapshot
install, restore, and downgrade code must preserve the policy. Membership
changes are not used as a rolling activation mechanism. `Node.Propose` evaluates the authoritative
policy while holding the proposal and index-tracker guards, before reserving a
predicted index or queueing work; this prevents a rejected proposal from
leaving a hole for queued successors. Removing a member remains allowed;
adding, promoting, or refreshing one without all active capabilities is
rejected.

`Node.Propose` is the single final policy boundary for every producer,
including mirror, events, maintenance, and backup flows; the shared plan
runner does not duplicate this check. Callers must still stamp their proposal
before use. Membership `ConfChange` operations carry and validate their
capability evidence separately.

## Admission cost budget and evidence

The proposal gate has a deliberately bounded cost shape. For a proposal that
declares `r` capabilities and a current membership of `m` voters and learners,
the pure decision is `O(r + m*r)` and allocates only its normalized input and
blocker report. The membership snapshot is copied once per gate evaluation;
the gate must not perform network calls, wait for Raft, or hold a storage
transaction while evaluating the result. Policy decoding and the snapshot
copy remain serialized by the proposal admission boundary, so they are part
of the write-path budget rather than background work.

The deterministic policy and membership matrix is covered by
`internal/infra/capabilities/capabilities_test.go`; Ledger-specific
selection and final admission checks are covered by the catalog and bootstrap
tests. The representative cost shape is measured by:

```text
go test ./internal/infra/capabilities -run '^$' -bench BenchmarkEvaluate -benchmem -count=5
```

The benchmark uses three and seven members with zero, one, and four required
capabilities. Any future capability registry or policy-provider change must
rerun it and include the result in the change review. A regression must be
explained against the `m*r` bound; if the bound no longer fits the admission
budget, move decoding/snapshot preparation outside the serialized section and
pass an immutable admission snapshot instead of widening the lock.

## Restart refresh

A normal restart does not run `JoinAsLearner`: the node restores its existing
WAL `ConfState` and keeps the same `(node_id, instance_id)` incarnation. After
the node and its Raft gRPC server are started, a node whose WAL carries
`CLUSTER_JOINED` sends an internal `RefreshMembership` request to a configured
peer. Followers forward that request to the current leader over the Raft gRPC
server. The node can therefore receive inbound Raft traffic before the refresh
commits; the refresh is an evidence update, not a pre-traffic barrier. During
the rc.2/rc.3 rolling upgrade, an older peer that returns `Unimplemented` is
treated as transient and the node keeps retrying in the background until a
compatible peer is available. The capability-dependent proposal-payload
cutover is reserved for the coordinated rc.3/rc.4 restart, after every node
runs the barrier-aware binary.
The leader validates the identity and capability evidence against the active
policy, then proposes a
`ConfChangeUpdateNode` containing the current endpoints and capabilities.

Only the committed change is authoritative. The update is replicated through
Raft and applied to every member's peer store; a local restart-time upsert is
not sufficient to activate a capability for admission. A fresh WAL instead
uses `JoinAsLearner` and `ConfChangeAddLearnerNode`.

The cluster-policy reconciler preserves committed capability activations while
reconciling ordinary policy configuration; it does not arm capabilities from
membership advertisements. The separate activation worker automatically
commits activation once every member has committed support evidence after
the coordinated restart. A future feature must register a stable activation ID,
stamp its final persisted proposals, and provide deterministic replay semantics.
The generic pending-admission fence already covers all audited order batches;
features must not add a separate activation switch.
