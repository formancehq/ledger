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

## Capabilities that change FSM semantics

A capability can describe a protocol or semantic behavior, not only a payload
feature. For example, `MirrorIngestRejectInvalidAssetV2` can identify the
behavior that rejects an asset which an older FSM accepted.

The capability has two roles in this case:

1. The replicated `ClusterPolicy` controls whether the cluster may accept the
   behavior.
2. The order declares the capability that selects the behavior during FSM
   apply.

The order must carry the semantic marker explicitly:

```text
old order:  required_capabilities = []
new order:  required_capabilities = [MirrorIngestRejectInvalidAssetV2]
```

The FSM must use the order's declaration to select the behavior. It must not
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
