# Rolling-upgrade capabilities

Ledger's capability barrier is the protocol foundation for a future mixed-version
rolling upgrade. It is separate from the client-facing gRPC protocol revision.

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

A proposal that declares a capability is admitted only when that capability is
active and every member advertises it. Proposals with no declared capability
remain unaffected. FSM apply never consults node-local capability flags.
Membership evidence is an authenticated cluster fact, not a proof of code
inspection: a node can only cause activation for IDs in the leader's local
registry, and malformed (zero, duplicate, or unsorted) declarations are
rejected. Operators must therefore treat a compromised binary as outside the
trust model of this barrier.

## Capabilities that change FSM semantics

A capability can describe a protocol or semantic behavior, not only a payload
feature. For example, `MirrorIngestRejectInvalidAssetV2` can identify the
behavior that rejects an asset which an older FSM accepted.

The capability has two roles in this case:

1. The replicated `ClusterPolicy` controls whether the cluster may accept the
   behavior.
2. The order declares the capability that selects the behavior during FSM
   apply.

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

## Rollout and recovery

Deploy barrier-aware binaries to every eligible member before arming an
incompatible feature. An old leader that does not understand the barrier cannot
be made safe by upgrading only the other nodes. Restart, snapshot install,
restore, and downgrade code must preserve the policy. Membership changes racing activation are
serialized at the proposal boundary. `Node.Propose` evaluates the authoritative
policy while holding the proposal and index-tracker guards, before reserving a
predicted index or queueing work; this prevents a rejected proposal from
leaving a hole for queued successors. Removing a member remains allowed;
adding, promoting, or refreshing one without all active capabilities is
rejected.

The admission path and the shared plan runner apply the policy check to
business and technical producers (including mirror, events, maintenance, and
backup flows). `Node.Propose` is the final common boundary for every producer;
callers must still stamp their proposal before use. Membership `ConfChange`
operations carry and validate their capability evidence separately.

## Restart refresh

A normal restart does not run `JoinAsLearner`: the node restores its existing
WAL `ConfState` and keeps the same `(node_id, instance_id)` incarnation. After
the node and its Raft gRPC server are started, a node whose WAL carries
`CLUSTER_JOINED` sends an internal `RefreshMembership` request to a configured
peer. Followers forward that request to the current leader over the Raft gRPC
server. The node can therefore receive inbound Raft traffic before the refresh
commits; the refresh is an evidence update, not a pre-traffic barrier. During
the rc.1/rc.2 rolling upgrade, an older peer that returns `Unimplemented` is
treated as transient and the node retries until a compatible peer is available.
The leader validates the identity and capability evidence against the active
policy, then proposes a
`ConfChangeUpdateNode` containing the current endpoints and capabilities.

Only the committed change is authoritative. The update is replicated through
Raft and applied to every member's peer store; a local restart-time upsert is
not sufficient to activate a capability for admission. A fresh WAL instead
uses `JoinAsLearner` and `ConfChangeAddLearnerNode`.

The cluster-policy reconciler currently maintains the replicated policy and
preserves its committed capability activations; it does not automatically arm
or activate a newly advertised capability. A future explicit activation path
must first verify the candidate against every current voter and learner. The
candidate set should be the intersection of the local binary registry and all
committed membership evidence; an empty or incomplete membership set must not
activate anything. Membership commits trigger reconciliation immediately, and
the periodic retry remains as a self-healing fallback for leadership or
proposal failures. Once an activation path exists and an activation is
committed, it is monotonic and a new member that does not advertise it is
rejected.
