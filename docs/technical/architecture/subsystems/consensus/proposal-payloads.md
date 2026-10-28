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

The FSM consumes the native payload directly. Read-only accessors expose orders
and technical updates from either the current batch or historical fields;
`ProcessClusterPolicy` executes a native control without constructing an Order.
The legacy order handler shares the policy transition for historical WAL and
audit replay. Accepted messages and their pooled buffers remain unchanged.
Internal producers own payload construction; admission and processors own
business validation. There is no transitional shape check, old-format write
adapter, capability registry, or activation flag.

## Execution and technical updates

Admission constructs policy controls with the `cluster-policy` system caller
and no client idempotency or signature data. The existing policy processor continues
to decide revision, no-op, conflict, stale-policy, maintenance, and failure
outcomes. New policy controls persist native audit intent in
`AuditItem.serialized_set_cluster_policy` (field 4), containing deterministic
`SetClusterPolicyProposal` bytes. Business orders still use `serialized_order`
(field 2). A control occupies one item at index zero, without an idempotency key
or signature. No new policy is lowered into a historical order.

Readers retain both forms: the checker replays controls directly and derives
the policy projection from verified intent; the audit indexer retains the
`set_cluster_policy` filter; the CLI displays its revision. Usage and
ledger-specific restore folds skip controls, while policy restore uses the
unchanged `SetClusterPolicyLog` and is verified against native audit intent.

Historical WAL replay retains the historical audit representation and exact
hashes. The new field extends the per-item hash preimage only when populated,
with a length-prefixed `audit:set-cluster-policy:v1` tag followed by
length-prefixed immutable control bytes. The old preimage is unchanged.
`HashVersion` still identifies the hash algorithm, not an encoding version.
The checker rejects mixed order/control intent and controls embedded in batches.

A technical-only batch creates no audit entry and does not advance business
HLC, including audit-key initialization. The mirror producer constructs mixed
batches containing ingestion orders and `MirrorSync` updates for the same ledger.
Those status changes are staged with the business write set and discarded on
business rejection. Other technical updates must be separate, because their
handlers can write directly to the apply batch. Historical mixed proposals
retain their existing replay rules.

Checkpoint and rotation boundaries inspect batch contents before apply
splitting. Read accessors borrow order and technical-update objects, preserving coverage
targets. Selecting the payload takes constant time and no allocation regardless
of batch size; policy processing needs no attribute coverage. Audit capture is
linear in accepted intent bytes, with one constant-sized envelope per control.
There is no historical scan or rewrite on admission or apply.
`BenchmarkPayloadReaders` measures batches of 1, 100 and 1,000 orders: all
three allocate 0 bytes and 0 objects (about 5–8 ns/op on Apple M5 Pro, Go 1.27.3).
This measures envelope selection, not the existing per-order processing cost.

## Persistence, restore, and rollback

The migration reads both formats and writes the new format after the full stop.
Existing WAL, snapshots, and backups need no rewrite or backfill. Released field
numbers and historical audit bytes remain intact. Cluster policy survives a full
checkpoint and is reconstructed from existing policy audit logs in incremental
deltas. The restore regression includes two post-checkpoint policy changes,
checker validation, recovery, and a subsequent conflicting policy control.

Do not restart RC.1 on volumes used by the target binary. The first payload
entry appended to the WAL closes the rollback window, including a technical
entry emitted during startup. Maintenance mode does not delay this boundary.
A return to RC.1 requires a separately validated recovery procedure using a
pre-upgrade backup and accounting for later writes. Service gRPC revision stays
at 40. The audit response gains field 4 without changing existing fields.
Older clients can decode the response but cannot interpret native policy
intent; update audit consumers to read both intent fields. This is distinct
from the internal Raft incompatibility, which requires the full stop.

## Validation

Unit tests cover payload immutability, historical and native audit intent,
coverage targets, actual payload serialization on failed-then-successful retries,
old/new outcome and recovery parity, technical-only HLC behavior, mirror rollback,
checkpoint boundaries, and incremental restore.

The three-node e2e test intercepts Raft log replication, requires batch and
policy payloads without legacy fields, transfers leadership, restarts nodes,
and checks balances. All nodes use the candidate binary.

```sh
bash scripts/check-proposal-payload v3.0.0-rc.1
go test ./internal/pkg/proposals -run '^$' -bench BenchmarkPayloadReaders -benchmem
```

The compatibility script builds distinct RC.1 and candidate test binaries.
Numeric wire tags provide an oracle independent of the candidate codec. It
compares historical audit entries, items and hashes byte-for-byte between the
released and current binaries. Native controls have different audit bytes and
chain suffixes: their intent, outcome headers, policy, applied index and HLC
must match instead. Every chain link is verified, including a business rejection. It also closes an RC.1 Pebble store
and WAL, opens them with the candidate, replays a committed legacy entry, writes
new policy and business payloads, then reopens the result in another candidate
process. The historical prefix, including retained legacy WAL replay, must equal the
uninterrupted RC.1 reference byte-for-byte. New intent and state must match its
semantics, and the post-restart report must match the upgraded report exactly.

This probe validates persisted-format migration, not Kubernetes sequencing or
snapshot catch-up. Release validation must also exercise the full-stop operator
transition with the final server candidate and preserved volumes.
