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
proposals remain consensus barriers.

## Upgrade boundary

Producers serialize the oneof directly through the shared plan runner, including
plan rebuilds and retries. A batch uses field 11 and a policy control uses field
12. The deprecated fields remain available for reading historical proposals.

This format requires a **full-stop upgrade** from RC.1 to RC.2. Every source
node must stop before any target node starts. An RC.1 decoder skips the unknown
payload fields without executing their intent, so mixed binaries are unsafe,
even when business writes are paused: startup can propose technical batches.
The release depends on the operator's required-downtime transition in
[PR #2410](https://github.com/formancehq/ledger/pull/2410).
For deployments outside that operator, follow the coordinated restart procedure
in [server upgrades](../../../../ops/upgrades.md).

The new FSM reads both forms. `proposals.ExecutionView` maps a payload to the
canonical execution and audit pipeline. Historical input retains its historical
semantics. The view borrows payload objects and leaves the decoded message and
its pooled buffers unchanged. Internal producers own payload construction;
admission and processors own business validation. There is no transitional
shape check, old-format write adapter, capability registry, or activation flag.

## Execution and technical updates

Admission constructs policy controls with the `cluster-policy` system caller
and no client idempotency or signature data. The existing policy processor continues
to decide revision, no-op, conflict, stale-policy, maintenance, and failure
outcomes. Its canonical historical audit order and hashes do not change.

A technical-only batch creates no audit entry and does not advance business
HLC, including audit-key initialization. The mirror producer constructs mixed
batches containing ingestion orders and `MirrorSync` updates for the same ledger.
Those status changes are staged with the business write set and discarded on
business rejection. Other technical updates must be separate, because their
handlers can write directly to the apply batch. Historical mixed proposals
retain their existing replay rules.

Checkpoint and rotation boundaries inspect batch contents before apply
splitting. The view borrows order and technical-update objects, preserving
coverage targets. Adaptation takes constant time and auxiliary space, with a
constant allocation for the borrowed header. `TestExecutionViewMatchesReleasedEncoding`
pins the canonical execution representation, not the new wire bytes.

## Persistence, restore, and rollback

The migration reads both formats and writes the new format after the full stop.
Existing WAL, snapshots, and backups need no rewrite or backfill. Released field
numbers and canonical audit bytes remain intact. Cluster policy survives a full
checkpoint and is reconstructed from existing policy audit logs in incremental
deltas. The restore regression includes two post-checkpoint policy changes,
checker validation, recovery, and a subsequent conflicting policy control.

Do not restart RC.1 on volumes used by the target binary. The first payload
entry appended to the WAL closes the rollback window, including a technical
entry emitted during startup. Maintenance mode does not delay this boundary.
A return to RC.1 requires a separately validated recovery procedure using a
pre-upgrade backup and accounting for later writes. Service gRPC revision stays
at 40; this change affects the internal Raft format.

## Validation

Unit tests cover borrowed payload immutability, canonical execution bytes,
coverage targets, actual payload serialization on failed-then-successful retries,
old/new audit and recovery parity, technical-only HLC behavior, mirror rollback,
checkpoint boundaries, and incremental restore.

The three-node e2e test intercepts Raft log replication, requires batch and
policy payloads without legacy fields, transfers leadership, restarts nodes,
and checks balances. All nodes use the candidate binary.

```sh
bash scripts/check-proposal-payload v3.0.0-rc.1
go test ./internal/pkg/proposals -run '^$' -bench BenchmarkExecutionView -benchmem
```

The compatibility script builds distinct RC.1 and candidate test binaries.
Numeric wire tags provide an oracle independent of the candidate codec. It
compares audit entries, items, hashes, policy, applied index and HLC for both
encodings, including a business rejection. It also closes an RC.1 Pebble store
and WAL, opens them with the candidate, replays a committed legacy entry, writes
new policy and business payloads, then reopens the result in another candidate
process. The resulting report must equal the uninterrupted RC.1 reference.

This probe validates persisted-format migration, not Kubernetes sequencing or
snapshot catch-up. Release validation must also exercise the full-stop operator
transition with the final server candidate and preserved volumes.
