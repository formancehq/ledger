# Rolling-upgrade capabilities

Ledger's capability barrier is the protocol foundation for a future mixed-version
rolling upgrade. It is separate from the client-facing gRPC protocol revision.

## Wire contract

`common.ClusterPolicy` carries the replicated activation state.
`raft.Proposal.required_capabilities` carries stable producer-owned capability
IDs. The fields are optional and therefore backward-readable by binaries that
predate this foundation. IDs are never reused; semantic dependencies must be
declared even when they do not add bytes to a proposal.

The final serialized proposal is the source of truth. A producer must rebuild
its plan first, then validate that the declared IDs exactly match
`required_capabilities`, immediately before enqueueing the proposal. A payload
that depends on a capability but omits its declaration is invalid.

## Admission and membership

The pure decision contract in `internal/infra/capabilities` evaluates every
current voter and learner. A member is represented by `(node_id, instance_id)`;
an identity refresh is a new incarnation and cannot silently inherit evidence
from the old one. Reachability and support are separate: an unreachable member
blocks activation just as an incapable member does. Once a capability is
active, every new membership announcement must include that capability.

A proposal that declares a capability is admitted only when that capability is
active and every member advertises it. Proposals with no declared capability
remain unaffected. FSM apply never consults node-local capability flags.

## Rollout and recovery

Deploy barrier-aware binaries to every eligible member before arming an
incompatible feature. An old leader that does not understand the barrier cannot
be made safe by upgrading only the other nodes. Restart, snapshot install,
restore, and downgrade code must preserve the policy. Membership changes racing activation are
serialized at the Raft proposal boundary and must be revalidated immediately
before `rawNode.Propose`, because `Node.Propose` queues work. Removing a member
remains allowed; adding, promoting, or refreshing one without all active
capabilities is rejected.

The admission path and the shared plan runner apply the policy check to
business and technical producers (including mirror, events, maintenance, and
backup flows). Direct `Node.Propose` callers remain low-level APIs and must
stamp their proposal before use; membership `ConfChange` operations carry and
validate their capability evidence separately.

## Restart refresh

A normal restart does not run `JoinAsLearner`: the node restores its existing
WAL `ConfState` and keeps the same `(node_id, instance_id)` incarnation. Before
inbound Raft traffic is enabled, a node whose WAL carries `CLUSTER_JOINED`
sends an internal `RefreshMembership` request to a configured peer. Followers
forward that request to the current leader over the Raft gRPC server. The
leader validates the identity and capability evidence against the active policy,
then proposes a
`ConfChangeUpdateNode` containing the current endpoints and capabilities.

Only the committed change is authoritative. The update is replicated through
Raft and applied to every member's peer store; a local restart-time upsert is
not sufficient to activate a capability for admission. A fresh WAL instead
uses `JoinAsLearner` and `ConfChangeAddLearnerNode`.

The leader's existing cluster-policy reconciler does not activate a capability
automatically. A future rollout can add an explicit `CapabilityActivation`
entry once every current voter and learner has refreshed its membership proof.
Until then the policy contains no active capability and this binary declares no
protocol feature.
