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

The producer must rebuild its plan first, derive the capabilities from that
final plan, stamp `required_capabilities`, and pass the typed result alongside
the serialized proposal. Admission reuses that result without decoding the
proposal a second time. A payload that depends on a capability but omits its
declaration is invalid; the shared builder and stamping helper are the
enforcement boundary for producers.

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
remain unaffected. FSM apply never consults node-local capability flags.
Membership evidence is an authenticated cluster fact, not a proof of code
inspection: a node can only cause activation for IDs in the leader's local
registry, and malformed (zero, duplicate, or unsorted) declarations are
rejected. Operators must therefore treat a compromised binary as outside the
trust model of this barrier.

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

The supported registry includes `MirrorPromotionCatchUp` (ID `1`). The leader
activates it automatically after every member has committed support evidence;
audited order batches wait while activation is pending. Each future feature
must separately register its stable ID, select its proposal marker at
preparation, and dispatch deterministic apply from that persisted marker.
The account-type validation feature from PR #2552 is not registered here.

1. The replicated `ClusterPolicy` controls whether the cluster may accept the
   behavior.
2. The order declares the capability that selects the behavior during FSM
   apply.

The serialized proposal currently carries `required_capabilities` at proposal
scope. A proposal can batch multiple orders. A proposal-level marker works for
mirror promotion because all promotions in the same proposal use the same new
rule; unrelated orders ignore it. A future change that needs different rules
for orders of the same type in one batch must add a persisted marker to each
order or reject mixed-semantic batches.
The conceptual marker for a homogeneous proposal is:

```text
old proposal:  required_capabilities = []
new proposal:  required_capabilities = [MirrorIngestRejectInvalidAssetV2]
```

The FSM must use the persisted declaration to select the behavior. It must not
infer the behavior from the current policy alone. An old order can be replayed
after the capability becomes active, and it must keep its old semantics.

Capability ID `1`, `MirrorPromotionCatchUp`, marks every newly produced mirror
promotion proposal. The FSM applies the source-head, cursor, and sync-error
precondition only to marked proposals. Unmarked promotions committed by RC.4
retain their original apply result during Raft replay. Audit replay uses its
recorded result and does not perform the live progress check.

If a producer cannot mark the new orders, the change must remain backward
compatible or add an explicit semantic version to the order. A capability by
itself is an admission barrier; it is not a historical replay version.

## Adding a protocol-breaking capability

Use this workflow when a change can make an order apply differently on two
binary versions.

1. Name the semantic contract. Describe the old behavior, the new behavior,
   and the orders that use each behavior.
2. Allocate a new capability ID. Never reuse an ID for a different contract.
3. Add the capability to the binary registry and derive it from the final
   proposal in `RequiredCapabilities`.
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

EN-2915 strengthens initial account-type validation in `CreateLedger` without
registering a capability. RC.4 accepted invalid constraints and conflicting
patterns; those successful creations remain committed business history. The
operator-required RC.4-to-RC.5 stopped upgrade drains every member and waits
for its durable FSM cursor to pass the admission cutoff, so RC.4 business
entries finish applying under RC.4 before RC.5 starts. RC.5 applies the
strengthened rule to new orders. Audit replay explicitly uses the historical
creation rule to reproduce successful old orders; successful new creations
satisfy both rules and yield the same logs. The proposal payload and capability
registry remain unchanged. A mixed-version rollout across this boundary is
unsupported.

Forwarded preload gaps also change apply at the same boundary without a
capability. RC.4 rejects a keyed batch carrying an order marked
`OrderTechnical.preload_unavailable` with the uncommitted `PRELOAD_UNAVAILABLE`
before executing any order. RC.5 executes the batch in sequence and fails the
marked order at its own position, so an earlier definitive rejection is
audited and frozen. RC.4 audited no such batch, so audit replay meets only
RC.5 outcomes. The same stopped-upgrade drain applies every RC.4 entry under
RC.4 before RC.5 starts. The proposal payload and capability registry remain
unchanged.

A change confined to read paths needs no capability either, even when it
touches a file the capability-contract gate watches. `common.proto` also
defines service response types that no order carries, and `internal/infra/plan`
also serves non-FSM reads. For example, the exact aggregate totals change
(EN-3332) retypes the `AggregatedVolume` response totals, which the mirror
stored-asset inventory also reads through `plan.Builder.AggregateVolumes`. No
order, proposal, or apply rule changes.

Removing `FailureDetails()` from uncommitted failures changes only Go types in
order processing. Every committed entry applies identically on old and new
voters: the same order fails with the same reason, an uncommitted failure still
writes no audit entry or idempotency record, and an audited failure persists the
same details. No capability, persisted migration, wire revision, or
stopped-upgrade boundary is needed.

The sink and mirror source shape checks are admission-only. They reject
incomplete sink configurations and a mirror source on a normal ledger before
proposing the order. A voter running the change still applies every
committed order under the same rules as an older voter, including historically
accepted shapes. No capability, persisted migration, wire revision, or
stopped-upgrade boundary is needed for these checks.

Posting amount presence is another admission-only shape check. A new server
rejects an explicit transaction posting without an amount before proposing it;
an explicit zero remains valid. Old and new voters apply an already committed
order with a missing amount identically as zero, including historical replay.
The proposal format, FSM behavior, and capability registry are unchanged, so
no capability ID or stopped-upgrade boundary is needed. During a rolling
upgrade, the leader's admission version determines whether a fresh request
without an amount is accepted.

The Numscript library version description (EN-3381) adds an optional
`SaveNumscriptOrder.description` that the FSM stores in `NumscriptInfo` and
echoes in `SavedNumscriptLog`. An RC.4 voter would apply the same order without
it, so old and new voters must not coexist. That release ships behind the
mandatory stopped `rc.4` to `rc.5` upgrade, which stops every RC.4 voter first.
An order without a description, including every historical order, applies and
replays exactly as before. No capability ID is needed.

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

The deterministic behavior matrix is covered by
`internal/infra/capabilities/capabilities_test.go`: empty declarations,
inactive policy, active policy with converged voters and learners, missing
evidence, and unreachable members. The representative cost shape is measured
by:

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

The rc.4-to-rc.5 peer service port migration changes the endpoint stored in
membership, not the capability evidence carried by a member or the Raft
`ConfChange` schema. The membership format revision limits address normalization
to legacy rows; committed member capabilities and the active `ClusterPolicy`
remain unchanged. This transport change therefore requires no new capability ID.

A normal restart does not run `JoinAsLearner`: the node restores its existing
WAL `ConfState` and keeps the same `(node_id, instance_id)` incarnation. After
the node and its Raft gRPC server are started, a node whose WAL carries
`CLUSTER_JOINED` sends an internal `RefreshMembership` request to a configured
peer. Followers forward that request to the current leader over the Raft gRPC
server. The node can therefore receive inbound Raft traffic before the refresh
commits; the refresh is an evidence update, not a pre-traffic barrier. During
the rc.2/rc.3 rolling upgrade, an older peer that returns `Unimplemented` is
treated as transient and the node keeps retrying in the background until a
compatible peer is available. The capability-dependent protocol cutover is
reserved for the coordinated rc.3/rc.4 restart, after every node runs the
barrier-aware binary.
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

`MirrorPromotionCatchUp` is the first registered ID. It activates automatically
after all members advertise support. New promotions require ID `1`; admission
holds audited batches while activation is pending, then applies the catch-up
rule only to marked proposals. Historical unmarked promotions retain their
original FSM behavior.
