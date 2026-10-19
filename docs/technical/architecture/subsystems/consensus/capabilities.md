# Rolling-upgrade capabilities

Ledger's capability barrier is the protocol foundation for a future mixed-version
rolling upgrade. It is separate from the client-facing gRPC protocol revision.

## Wire contract

`common.ClusterPolicy` carries the replicated activation state, admission policy,
membership revision under examination, and a monotonic minimum replay protocol.
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
blocks activation just as an incapable member does. Activation is bound to the
membership revision observed by the proposer; a changed revision requires a
fresh examination.

While armed, `FEATURE_ONLY` rejects dependent proposals and allows unrelated
business writes. `ALL_WRITES` rejects all business writes, with only the
narrow control-plane recovery and activation path exempted by its producer
inventory. FSM apply never consults node-local capability flags.

The replay floor is monotonic and is checked before decoding an entry or
snapshot that could require a newer protocol. This floor is intentionally not
`pkg/grpcprotocol.Version`.

## Rollout and recovery

Deploy barrier-aware binaries to every eligible member before arming an
incompatible feature. An old leader that does not understand the barrier cannot
be made safe by upgrading only the other nodes. Restart, snapshot install,
restore, and downgrade code must preserve the policy and floor; a downgrade
below the committed floor is refused. Membership changes racing activation are
serialized at the Raft proposal boundary and must be revalidated immediately
before `rawNode.Propose`, because `Node.Propose` queues work.

The current package is the deterministic contract and wire foundation. Wiring
each admission, mirror, events, technical-maintenance, and backup producer to
the final-proposal validator and to the node's pre-`Propose` revalidation is a
follow-up integration step; no producer may claim support until that inventory
is complete.
