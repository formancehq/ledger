# Proposal payloads

`Proposal.payload` separates an execution batch from a cluster-policy control:

```text
Proposal
  common header: identity, HLC, preload plan, predicted index, caller,
                 idempotency, signature
  payload:
    batch (11): ExecutionBatch { orders (1), technical_updates (2) }
    set_cluster_policy (12): SetClusterPolicyProposal { policy (1) }
```

Policy controls are audited. They are not technical updates. Technical updates
remain inside an execution batch with their own coverage bits. Empty legacy
proposals remain consensus barriers; an empty new batch is invalid.

## Need and migration decision

Separating policy controls makes their admission and shape constraints explicit
without changing business execution or audit evidence. The supported RC baseline
must still execute every committed entry identically during a rolling upgrade.
An old protobuf decoder silently skips unknown payload fields; accepting those
bytes is not equivalent to executing their intent.

This change is the **expand** phase. Application producers construct the oneof,
and the shared plan runner calls `proposals.LegacyWire` immediately before each
marshal, including plan rebuilds and retries. It emits only the released fields
`Proposal.orders` (2) and `Proposal.technical_updates` (9). A policy control is
lowered to the historical singleton `SystemScopedOrder.set_cluster_policy`
order (11). Header values and borrowed payload objects remain unchanged.

The new FSM understands both forms. Historical inputs retain their historical
semantics. New envelopes are validated before any entry in a decoded batch
mutates state, then adapted to the canonical execution and audit pipeline.
The decoded message retains ownership of its pooled buffers and is never
mutated by this adaptation.

There is no capability registry, membership advertisement, activation policy,
startup toggle, or dependency on the capabilities change in this phase. There
is also no dual write or historical-data rewrite. Emitting payload-only Raft
entries is deliberately deferred: a separate cutover must prove that every
participating binary understands the format and define its rollback boundary.
Do not bypass the shared runner to serialize a logical payload directly into
Raft. New decoder support alone does not authorize that cutover.

## Execution and technical updates

A policy control requires the `cluster-policy` system caller and cannot carry
client idempotency or signature data. The existing policy processor continues
to decide revision, no-op, conflict, stale-policy, maintenance, and failure
outcomes. Its canonical historical audit order and hashes do not change.

A technical-only batch creates no audit entry and does not advance business
HLC, including audit-key initialization. The only permitted new mixed batch
contains mirror-ingestion orders and `MirrorSync` updates for the same ledger.
Those status changes are staged with the business write set and discarded on
business rejection. Other technical updates must be separate, because their
handlers can write directly to the apply batch. Historical mixed proposals
retain their existing replay rules.

Checkpoint and rotation boundaries inspect batch contents before apply
splitting. The adapter borrows order and technical-update objects, preserving
coverage targets when the runner rebuilds its plan. Validation costs O(orders +
updates), with constant auxiliary space; the borrowed header adds constant
allocation overhead. Wire and WAL size are unchanged: only one representation
is serialized. `TestLegacyWireMatchesReleasedEncoding` pins exact byte parity;
`BenchmarkLegacyWire` measures representative batch cardinalities.

A local run on Apple M5 Pro with the pinned Nix toolchain measured the adapter
plus VT serialization below. These are microbenchmark observations, not a
production latency budget. Allocated bytes include the serialized command;
the allocation count stays constant as batch size grows.

| Orders | Time/op | Bytes/op | Allocations/op |
|---|---:|---:|---:|
| 1 | 70 ns | 176 | 2 |
| 100 | 1.34 us | 928 | 2 |
| 1000 | 13.0 us | 8352 | 2 |


## Persistence, restore, and rollback

Existing WAL, snapshots, and backups need no migration job or backfill. The
released field numbers and canonical audit representation remain intact.
Cluster policy is preserved by a full checkpoint and rebuilt from existing
policy audit logs in the incremental delta. The restore regression includes two
post-checkpoint policy changes, checker validation, recovery, and a subsequent
conflicting policy control.

This expansion introduces no new persisted format on production paths, so it
adds no new downgrade boundary. Rollback remains subject to the other changes
in the release. Service gRPC revision remains 40. Payload-only replay fixtures
exercise decoder readiness, not a format emitted by this release.

## Validation

Unit tests cover exact legacy wire bytes, borrowed payload immutability,
coverage targets, failed-then-successful retries, shape rejection before batch
mutation, old/new audit and recovery parity, technical-only HLC behavior,
mirror success/failure, checkpoint boundaries, and incremental restore.

The three-node e2e test intercepts actual Raft traffic, verifies that no
payload-only entry escapes the adapter, transfers leadership to a reconfigured
node to produce a policy update, restarts nodes, and checks balances. All nodes
in that test use the candidate binary.

```sh
bash scripts/check-proposal-payload v3.0.0-rc.1
go test ./internal/pkg/proposals -run '^$' -bench BenchmarkLegacyWire -benchmem
```

The compatibility script builds distinct RC.1 and candidate test binaries and
compares exact FSM/audit results using numeric wire tags independent of the
candidate codec. It does not replace the release requirement to validate a
real mixed-version rolling upgrade, including snapshot catch-up, described in
[server upgrades](../../../../ops/upgrades.md).
