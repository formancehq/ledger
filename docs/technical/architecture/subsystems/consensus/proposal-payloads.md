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

Policy controls persist operational state without business audit or logs. Technical updates
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
and no client idempotency or signature data. The policy processor decides
revision, no-op, conflict, stale-policy and maintenance outcomes. Native
controls update the replicated policy without consuming a business log or audit
sequence, freezing an idempotency outcome, or advancing business HLC. No new
policy is converted into a historical order.

Historical policy orders keep their original execution and audit representation,
including exact hashes and log slots. Audit readers, CLI and restore folds still
understand those records. The checker verifies historical business effects but
does not compare the current operational policy or reapply metadata/checkpoint
quotas during isolated replay. Semantic validations, signing, accounting and
frozen idempotency outcomes remain verified. See the explicit
[operational exemption](../../audit-vs-technical-state.md#operational-cluster-policy).

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
linear in accepted business intent bytes; policy controls need no audit envelope.
There is no historical scan or rewrite on admission or apply.
`BenchmarkPayloadReaders` measures batches of 1, 100 and 1,000 orders: all
three allocate 0 bytes and 0 objects (about 5–8 ns/op on Apple M5 Pro, Go 1.27.3).
This measures envelope selection, not the existing per-order processing cost.

## Persistence, restore, and rollback

The migration reads both formats and writes the new format after the full stop.
Existing WAL, snapshots, and backups need no rewrite or backfill. Released field
numbers and historical audit bytes remain intact. Full checkpoints include the
policy row. Incremental manifests carry an optional `clusterPolicy` snapshot
with the deterministic protobuf bytes and source applied index, taken from the
same read snapshot as the history exports. This also captures policy-only
changes. Restore folds released history first, then applies this operational
snapshot to the staging store. Older manifests continue to restore their
checkpoint and historical policy logs. See the
[incremental restore contract](../backup/incremental-restore-contract.md#operational-cluster-policy).

Do not restart RC.1 on volumes used by the target binary. The first payload
entry appended to the WAL closes the rollback window, including a technical
entry emitted during startup. Maintenance mode does not delay this boundary.
A return to RC.1 requires a separately validated recovery procedure using a
pre-upgrade backup and accounting for later writes. Service gRPC revision stays
at 40. The audit response schema and historical hash preimages are unchanged;
new operational policy changes no longer appear in business audit listings.

## Validation

Unit tests cover payload immutability, historical audit integrity and native policy audit omission,
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
released and current binaries. Native policies are unaudited: the probe asserts
exact audit counts, verifies every remaining chain link and compares final
policy, ledgers, applied index and HLC. It also closes an RC.1 Pebble store
and WAL, opens them with the candidate, replays a committed legacy entry, writes
new policy and business payloads, then reopens the result in another candidate
process. The historical prefix, including retained legacy WAL replay, must equal the
uninterrupted RC.1 reference byte-for-byte. Final business and operational state must match its
semantics, and the post-restart report must match the upgraded report exactly.

This probe validates persisted-format migration, not Kubernetes sequencing or
snapshot catch-up. Release validation must also exercise the full-stop operator
transition with the final server candidate and preserved volumes.
