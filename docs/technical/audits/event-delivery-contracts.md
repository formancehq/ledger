# Event delivery audit evidence and ownership

The [manifest](event-delivery-contracts.json) covers the boundary from already
committed global logs to selected event representations, external sink
acknowledgement and the corresponding per-sink cursor. A focused domain is
useful because optional sinks have different completion and representation
contracts that the existing lifecycle and retry domains do not inventory.
It adds scope to the native workflow, not a runner, schema or product guarantee.

## Evidence contract

For each hypothesis identify the exact audited SHA, compiled sink implementation,
reachable sink configuration, committed source log, selection decision, output
representation, external effect and acknowledgement, returned error/success,
local cursor, proposed cursor and applied cursor. Follow the registered factory
and manager wiring rather than assuming a direct constructor fixture is an
admitted production path. Inspect callers and guards outside the listed files
only far enough to establish or disprove the delivery claim, loading their
routed documentation as needed.

Use [events.md](../architecture/subsystems/events-mirror/events.md), the
`Sink.Publish` contract, concrete implementations, tests and pinned dependencies
together. A comment, test name or green result alone does not establish remote
delivery. An observation must distinguish the claimed defect from an allowed
filter, unavailable sink, best-effort status report or at-least-once replay.
P0/P1/P2 require a concrete reachable failure and impact under the native
[audit contract](../contributing/ai-audit.md). Unsupported product expectations
become questions. Every raw finding remains a hypothesis until the independent
[challenge](../contributing/ai-audit-challenge.md) at the same SHA tries to
disprove it; preserve finding identity and severity through qualification.

The outer workflow refreshes prior work and supplies deduplication context.
Providers remain read-only leaf workers: they do not invoke audit, challenge or
Jira launchers, publish issues, implement fixes or edit tracked reproducers.
Creating this manifest does not authorize running an audit or publishing Jira.

## Source map and proof boundaries

| Boundary | Existing paths and required proof |
| --- | --- |
| Log scan and selection | `internal/query/log.go`, `internal/application/events/event.go`, `emitter.go`: establish `ReadLogsSince`, `LogToEvent` and `shouldEmit` for the actual committed variant and allow-list. Unspecified/internal logs are intentionally dropped. |
| Envelope and payload | `misc/proto/events.proto`, `internal/proto/eventspb/event.go`, `internal/proto/commonpb/{log,log_payload,ledger_log,posting}.go`, `internal/adapter/json/`: compare the source to the JSON or Protobuf contract. The global sequence differs from ledger-local log and transaction IDs. |
| Analytical projection | `internal/application/events/sink_data_common.go`, `sink_clickhouse.go`, `sink_databricks.go`: compare flattened fields, target types, exact amounts, colors, metadata strings and UTC timestamp precision to the receiving column representation. This projection is not a full protobuf or backup representation. |
| Sink reachability | `internal/application/events/registry.go`, `manager.go`, `internal/domain/processing/processor_events_sink.go`: establish installed build tags, the name-keyed existence and batch-size guards, factory and emitter options. `internal/application/admission/validate_order.go` supplies source-business-input preconditions, not SinkConfig validation. These paths do not widen the domain to startup validation or lifecycle reconciliation. |
| Delivery to cursor | `internal/application/events/emitter.go`, `internal/infra/state/machine_technical_updates.go`, `internal/infra/state/batch.go`, `internal/query/sink.go`: trace publication, `EventsSinkUpdate` and named cursor/status writes and reads. Distinguish proposal acceptance from completed apply and durable state. |
| Existing probes | `internal/application/events/*_test.go`, `tests/e2e/cluster/events_sinks*_test.go`: inspect the assertion actually reached and the chosen fixture, tags and dependencies. Emitter reopen within one process does not prove OS crash recovery. |

Broad events globs include manager and shutdown tests as counterevidence, not
authorization to repeat the concurrency audit. Likewise the technical-update
file contains other subsystems; only the event handoff belongs here.

### Per-sink acknowledgement

| Sink | Publication path and evidence required | Limits |
| --- | --- | --- |
| HTTP (default build) | `HTTPSink.Publish` serializes and POSTs each event; `post` accepts 2xx. Inspect every response and the matching body, event headers and configured endpoint. | An earlier POST may succeed before a later one fails; 2xx does not prove receiver business processing. Optional HMAC binds the transmitted body; credential storage, redaction and trust policy are excluded. |
| NATS (`nats`) | `NATSSink.Publish` waits on JetStream `Publish` for each serialized event. Inspect the pinned client acknowledgement contract and `subject` routing. | A broker acknowledgement is not consumer acknowledgement. Do not require dedup headers or exactly-once delivery without a repository contract. |
| Kafka (`kafka`) | `KafkaSink.Publish` submits through an async producer, then awaits each per-message delivery channel; `dispatchDeliveries` correlates Successes/Errors through message metadata. Inspect effective Sarama acknowledgement settings. | Input-channel acceptance alone is not success. A nil result does not promise downstream consumption or all-replica durability. Ledger keys do not impose global consumer order across partitions. |
| ClickHouse (`clickhouse`) | `PrepareBatch`, per-event projection/`Append`, then `Send`. Inspect the pinned driver and effective settings before deciding what successful Send acknowledges. | Prepared/appended rows are not completion. `ReplacingMergeTree` deduplicates `(ledger, log_sequence)` eventually; use `FINAL` or grouping for exact counts. |
| Databricks (`databricks`) | Build one parameterized multi-row INSERT of the analytical projection; await `ExecContext` through the pinned SQL Warehouse driver. | Do not assume transaction/rollback guarantees, ClickHouse replacement semantics or exactly-once inserts. Configuration-only tests cannot prove remote completion. |

No finding may promote local enqueue, notification, error-status clearing or a
cursor proposal into stronger acknowledgement than the selected transport
actually provides. If stronger durability is a desired feature absent from the
current contract, record the unmet proof as a question.

### Cursor and partial effects

`processLogBatch` scans after its current cursor. Filtered logs can advance the
local position immediately only when no selected event is pending; otherwise
`pendingFilteredCursor` defers them. `publishBatch` calls the sink first, resets
in-memory failure bookkeeping after success, and then proposes the last selected
sequence with `ClearError`. The apply handler writes cursor/status through the
main-store write session. A trailing filtered scan may require another update.
Inspect completion and persistence through the actual call chain; invoking the
apply helper alone is not proof of durable commit.

Record residual state at each failure point. If publication fails after an
accepted prefix, those external effects may remain while the pending selected
batch is unacknowledged by the cursor. If publication succeeds but the cursor
outcome is failed or unknown, the external effects remain and later attempts can
redeliver. Error reports are best-effort and deduplicated. Neither a stale error
nor its absence certifies the cursor or the remote effect. Multiple ambiguous
attempts may produce repeated duplicates: there is no unconditional one-batch
duplicate-count bound or atomic transaction between a sink and Raft.

### Documentation ambiguities are not new invariants

At the discovery base, the event specification's broad “every committed log”
sentence must be read with `shouldEmit` and the event allow-list. Schema and
technical logs can remain unspecified. Analytical helper branches for signing
or sink-management payloads do not establish that the emitter selects them.
`LogToEvent` sets the created ledger's name even though an envelope comment uses
an empty-ledger example. The Kafka implementation uses an async producer with
synchronous waiting for correlated results despite the specification's
“synchronous producer” wording. HTTP publishes one POST per event despite a
contradictory batch comment. These are cautions for interpreting evidence, not
confirmed product findings or fixes in this scope.

Output formats intentionally differ: JSON metadata can lose internal type
distinctions, analytical metadata is string-valued, timestamps follow their
representation's precision, and Protobuf retains its own typed encoding. Do not
require format equality, byte-for-byte round trips, a consumer-global order, or
new event types solely because an internal payload exists. For an alleged
representation loss, name the specific supported field/value and contract.

## Ownership and deduplication

Attribute a symptom to the mechanism and required correction, not every domain
whose files it touches. Keep one logical root cause across formats, sinks and
repeated runs, with the additional affected surfaces as evidence.

| Neighbor or workstream | Boundary |
| --- | --- |
| `accounting-invariants` | Owns balancing, amounts and business outcomes. This domain checks faithful representation of already committed outcomes, not whether the original transaction was correct. |
| `persistence-restore-replay` | Owns storage, snapshots, archive retention and cross-cluster restore/reconstruction, including sink-cursor restore policy. Here the boundary is external delivery versus its cursor handoff; inspect persistence only to locate the actual handoff. |
| `idempotency-retries-partial-failures` | Owns API keys, generic attempt identity, shared technical submitter and retry policy. Here partial batches and uncertain ACKs are examined only for concrete sink completion and cursor effects. Allowed redelivery is not an idempotency defect. |
| `concurrency-lifecycle-shutdown` | Owns goroutine/resource lifetime, cancellation, generation fencing, sink replacement, Stop/Close, and stale proposals across leadership loss. Kafka delivery correlation in an active sink belongs here only when independent of those lifecycle mechanisms. |
| `api-boundary-contracts` | Owns HTTP/gRPC/CLI adaptation and shared output codecs. A shared LedgerLog JSON defect remains one API-rooted issue; event-specific envelope, projection, routing and sink insertion are owned here. |
| `configuration-startup-contracts` | Owns source precedence, startup validation, defaults and consumer wiring. Establish the selected sink settings as preconditions; do not repeat a parameter defect as delivery failure. |
| `operator-reconciliation-durability` | Owns CR-to-Ledger sink reconciliation, ownership and repeated external configuration effects. The event path starts with the configured sink. |
| `raft-membership-leadership`, `process-boundary-recovery` | Own consensus/leadership safety and real process death/re-exec. Reading the replicated cursor establishes delivery recovery position, not those broader guarantees. |
| `read-consistency-projections`, `test-reachability-enforcement` | Own generic scan/query consistency, index/GC projections, and repository test collection/enforcement. Use their mechanisms as context without a new general scan, event-GC or CI campaign. |
| Parallel `mirror-ingestion-contracts` | Owns source fetching, v2 translation/CEL, MirrorIngest and source-log continuity. This domain begins at committed logs; a mirror-generated committed log is tested here only as an event input. |
| Parallel `authentication-authorization-boundaries` and EN-1632 | Authentication, authorization, secrets, sink/mirror credential reads, redaction, rotation and storage are excluded. Synthetic signing bytes can be inspected for payload fidelity without reopening credential exposure. |

## Discovery snapshot and refresh

Snapshot: 2026-09-10, `release/v3.0` base
`f76499637a5af0d92a2cfabbdacf8247256fa4bb`. No equivalent manifest was present;
API, configuration and operator domains already existed. The mirror and
authentication domains were being prepared independently. Refresh PR/ticket
state, manifests and available qualified results in trusted outer preparation
before a later audit; this list is not a permanent status assertion.

- EN-1632 and EN-1634 were in review; [PR #1963](https://github.com/formancehq/ledger/pull/1963)
  and [PR #1970](https://github.com/formancehq/ledger/pull/1970) were open.
  EN-1796 was cancelled. Exclude this credential/read-view campaign.
- EN-1318 / [PR #1934](https://github.com/formancehq/ledger/pull/1934) was merged:
  startup retry and constructor cleanup are existing protections, owned by
  lifecycle/retry domains.
- EN-1285 / [PR #1552](https://github.com/formancehq/ledger/pull/1552) was merged:
  ClickHouse replacement semantics already address ordinary redelivery counts.
- [EN-636](https://formance-team.atlassian.net/browse/EN-636) was in progress:
  testing whether network or pod failures can lose Ledger events. Its broad
  description is not a native manifest or a confirmed root cause; check its
  current results before a later delivery campaign and route process/lifecycle
  mechanisms to their owning domains.
- [PR #1844](https://github.com/formancehq/ledger/pull/1844) was open for operator
  NATS sink reconciliation; do not duplicate its control-plane work.
- EN-1790 / [PR #1947](https://github.com/formancehq/ledger/pull/1947) was merged:
  use the current LedgerLog JSON representation rather than the old wrapper.
- The existing filtered-log failure regression in
  `emitter_integration_test.go` and technical-proposal retry/buffer tests must
  be inspected before alleging a previously unguarded cursor skip or retry.

## Permitted diagnostics and limits

The manifest suggests focused existing tests for a separately authorized audit.
Run them under the repository-pinned Nix tooling and native validation/cache
policy, with isolated test stores, local receivers and deterministic failure
barriers. Do not mutate live brokers, warehouses, deployment configuration or
real credentials. A new reproducer is a later engineering task; a read-only
provider may describe one or use permitted temporary diagnostics without
changing tracked content.

NATS tests use an embedded server. Kafka and ClickHouse tags register Docker
setup in package `TestMain`, even for narrow selectors. Databricks currently has
configuration/constructor tests rather than a remote Publish integration test.
Any future credentialed warehouse test needs separate authorization. List tests
actually selected, skipped setup and unavailable remote evidence; never label
an untagged root build as optional-sink delivery verification. Synthetic driver
responses prove local error propagation only, not the actual remote ACK contract.

For this manifest-only change, validate JSON shape with the existing launcher
predicate and verify every scope glob/document path, without invoking a real
provider. Run canonical baseline and exact-base PR checks. Those checks validate
the artifact and repository candidate; they are not an event-system audit.
