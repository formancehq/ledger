# Metrics

## Overview

Ledger v3 POC exposes OpenTelemetry-compatible metrics that can be collected via OTLP and stored in any compatible backend (VictoriaMetrics, Prometheus, etc.).

Metrics are organized into several categories:
- **System Metrics**: CPU, memory, network, and Go runtime
- **HTTP Server Metrics**: Request latency and throughput
- **Apply Path Metrics**: gRPC Apply handler and controller batch latency
- **Raft Consensus Metrics**: Performance of the consensus layer, the applier, gating and snapshots
- **Transport Metrics**: Inter-node communication (reception, sending, unreachable channels)
- **Admission Metrics**: Preloads, commands, actions, audit attribution and the propose queue
- **Storage Metrics**: Pebble storage engine performance, slow disk operations and I/O
- **Storage Disk Usage**: Disk space consumption per component and volume
- **Secondary Store Metrics**: The read index and usage store Pebble instances
- **Caching & Attributes Metrics**: Numscript cache, attribute cache and Bloom filters
- **Background Indexer Metrics**: Progress and lag of the index builder, usage builder and audit indexer
- **Mirror Metrics**: Ingestion from an external source into a mirror ledger

Every metric the server emits is documented here; a test fails when a new
instrument has no entry. For dashboards, see the
[Grafana Dashboard](#grafana-dashboards) section.

## Shared telemetry resource

Server OTLP logs, traces, and metrics use the same OpenTelemetry resource:

| Attribute | Value | Set by |
| --------- | ----- | ------ |
| `service.name` | `--otel-service-name`, default `ledger` (identical on every node) | server |
| `service.version` | semver build metadata `<version>+<commit>` (e.g. `3.0.0+abc1234`); the commit is omitted when unknown or already in the version | server |
| `service.instance.id` | `<namespace>.<pod>.<container>` under the operator; otherwise `<cluster name>.<node ID>` | operator (server default) |
| `formance.ledger.cluster.id` | `--cluster-id` | server |
| `formance.ledger.cluster.name` | `Cluster` resource name; the server defaults it to the cluster ID | operator (server default) |
| `formance.ledger.node.id` | Raft node ID | server |
| `k8s.namespace.name` | pod namespace | operator |
| `k8s.pod.name` | pod name | operator |
| `k8s.container.name` | `ledger` | operator |

`service.*` and `k8s.*` follow the OpenTelemetry semantic conventions. Every
node shares one `service.name`, which the OTLP→Prometheus translation maps to
the `job` label (prefixed by `<service.namespace>/` when that is set).
Ledger-specific attributes use the `formance.ledger` namespace rather than
extending the reserved semantic convention namespaces.

A cluster is identified by its Kubernetes namespace and its cluster **name**,
not by the cluster ID: `--cluster-id` is declared per deployment, and separate
clusters often share one (for example `default`). Dashboards and alerts filter
and group on `k8s.namespace.name` and `formance.ledger.cluster.name`. Under
the operator the name is the `Cluster` resource name, unique within its
namespace; without the operator the namespace is absent and the name falls
back to the cluster ID, so give each cluster a distinct ID or set
`formance.ledger.cluster.name` explicitly.

`service.instance.id` identifies one node, as the semantic conventions
require, and OTLP→Prometheus translation maps it to the `instance` label, so
every node has its own series and `target_info` entry even when no other
resource attribute is promoted. The operator uses the semantic-convention
Kubernetes derivation `<k8s.namespace.name>.<k8s.pod.name>.<k8s.container.name>`;
without the operator the server defaults it to `<cluster name>.<node ID>`.

Other resource attributes are not metric labels by default: OTLP→Prometheus
translation stores them on `target_info`. The pre-built dashboards filter on
`k8s.namespace.name`, `formance.ledger.cluster.name` and
`formance.ledger.node.id` as series labels, so the pipeline must promote
them, for example with the collector Prometheus
exporters' `resource_to_telemetry_conversion` or Prometheus'
`otlp.promote_resource_attributes`.

`--otel-resource-attributes` applies to all three signals; explicit
attributes take precedence over every server default above. The operator
appends its attributes after the user-supplied `spec.monitoring.attributes`,
so operator values win over user values for the same key. Do not put
`service.instance.id` or `formance.ledger.node.id` in a cluster-wide attribute
list such as `spec.monitoring.attributes`: every node would then report the
same identity.

The resource is built before the server logger starts and is supplied to the
trace and metric providers. Invalid resource attributes fail startup before
telemetry providers are created. Console log fields and API error responses
are independent of these OTLP resource attributes.

## Naming Convention

Metric names in this document use the **OpenTelemetry dot-notation**
without the namespace prefix (`admission.command.duration`,
`raft.fsm.logs_appended`). Attribute names such as `formance.ledger.node.id`
are shown as emitted; the metrics prefix never applies to them. Metric names
carry no unit: the unit is the instrument's unit field (the Unit column below),
and the Prometheus translation appends it as a suffix. Durations are always
recorded in seconds (`s`). Counters name the counted thing in the plural
(`pebble.flushes`) and never end in `total`: the Prometheus translation adds
`_total` itself. The server emits
every metric its own instrumentation creates under the
`--otel-metrics-prefix` namespace (default `formance.ledger`, env
`OTEL_METRICS_PREFIX`, `none` to disable), so
`raft.fsm.logs_appended` is emitted as
`formance.ledger.raft.fsm.logs_appended`. This includes the metrics
we name under `raft.*` and `pebble.*` — etcd-raft and Pebble do not
export OpenTelemetry themselves; those names are our emissions about
our integration with those libraries. The prefix groups the ledger
metrics together and keeps them unambiguous in a backend that also
receives other services' metrics, as the OpenTelemetry naming
guidelines recommend for application-specific names.

The OTLP→Prometheus collector that fronts most cloud Prometheus
backends sanitises dots in names: `formance.ledger.node.id` becomes
`formance_ledger_node_id`, `formance.ledger.raft.fsm.logs_appended` becomes
`formance_ledger_raft_fsm_logs_appended`. The server itself always
emits dot notation.

| Source | Emitted by the server | After a dot-sanitising collector |
| ------ | --------------------- | -------------------------------- |
| `admission.command.duration` (we emit) | `formance.ledger.admission.command.duration` | `formance_ledger_admission_command_duration` |
| `raft.fsm.logs_appended` (we emit, instruments etcd-raft) | `formance.ledger.raft.fsm.logs_appended` | `formance_ledger_raft_fsm_logs_appended` |
| `pebble.flushes` (we emit, instruments Pebble) | `formance.ledger.pebble.flushes` | `formance_ledger_pebble_flushes` |
| `http.server.request.duration` (OTel auto-instr) | `http.server.request.duration` | `http_server_request_duration` |
| `go.memory.allocated` (OTel auto-instr) | `go.memory.allocated` | `go_memory_allocated` |
| `formance.ledger.node.id` (attribute, never prefixed) | `formance.ledger.node.id` | `formance_ledger_node_id` |

OpenTelemetry semantic-convention auto-instrumentation (`go.*`,
`process.*`, `system.*`, `http.*`, `rpc.*`) is emitted via the
global MeterProvider, which go-libs leaves as the raw SDK provider —
only the MeterProvider injected into the ledger's own components is
prefixed, so those metrics keep their upstream names.

Eight pre-built Grafana dashboards ship under
`misc/devenv/monitoring-dashboards/config/dashboards/`. Pick the
one that matches your combination of *(server `--otel-metrics-prefix`,
names as stored in Prometheus, histogram representation)*. A custom
prefix has no pre-built dashboard.

| Server prefix | Stored names | Histograms | File |
| ------------- | ------------ | ---------- | ---- |
| `formance.ledger` (default) | dots preserved                       | classic | `ledger-metrics-otel.json` |
| `formance.ledger` (default) | dots → underscores only              | classic | `ledger-metrics-prom.json` |
| `formance.ledger` (default) | full normalisation (unit + `_total`) | classic | `ledger-metrics-prom-normalized.json` |
| `formance.ledger` (default) | full normalisation (unit + `_total`) | native  | `ledger-metrics-prom-normalized-native.json` |
| `none` | dots preserved                       | classic | `ledger-metrics-otel-noprefix.json` |
| `none` | dots → underscores only              | classic | `ledger-metrics-prom-noprefix.json` |
| `none` | full normalisation (unit + `_total`) | classic | `ledger-metrics-prom-noprefix-normalized.json` |
| `none` | full normalisation (unit + `_total`) | native  | `ledger-metrics-prom-noprefix-normalized-native.json` |

The **normalised** variants additionally embed the UCUM unit
suffix the collector appends (`s` → `_seconds`, `By` →
`_bytes`, …), the `_total` suffix for monotonic counters, and the
`_ratio` suffix for dimensionless gauges. This is the default
behaviour of the contrib OTel collector, the Prometheus 3.x OTLP
receiver, Thanos and VictoriaMetrics with `OTLP_NORMALIZE=true`.

The **native** variants target Prometheus 3.x with the OTLP
receiver in its default mode (or an OTel collector with
`prometheusremotewrite.convertHistogramsToNHCB=true`): OTel
histograms are stored as a single time series carrying the bucket
data, without `_bucket` / `_count` / `_sum` split and without the
`le` label. The dashboard uses `histogram_quantile(rate(metric))`
and `histogram_avg` directly on those names.

Histogram sums and observation rates are also representation-aware:
the generator uses `_sum`/`_count` for classic histograms and
`histogram_sum`/`histogram_count` for native histograms. Generated
classic dashboards default to the `Prometheus` Grafana datasource;
native dashboards default to `Prometheus Native`. The standard
devenv stack uses
`ledger-metrics-prom-normalized-native.json`.

All eight are regenerated from the same Jsonnet source via
`just generate-dashboards`. See
[`misc/devenv/monitoring-dashboards/README.md`](../../misc/devenv/monitoring-dashboards/README.md).

## System Metrics

System and Go runtime metrics are provided by the OpenTelemetry SDK and `go-libs` modules.

### Process Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `process.cpu.time` | Counter | s | CPU time spent by the process (user/system) |
| `system.memory.usage` | Gauge | By | System memory usage by state (used, free, cached, etc.) |
| `system.memory.utilization` | Gauge | 1 | System memory utilization ratio (0-1) |
| `system.network.io` | Counter | By | Network I/O bytes (receive/transmit) |

### Go Runtime Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `go.goroutine.count` | Gauge | `{goroutine}` | Current number of goroutines |
| `go.memory.allocated` | Counter | By | Total bytes allocated (cumulative) |
| `go.memory.allocations` | Counter | `{allocation}` | Total number of allocations |
| `go.memory.used` | Gauge | By | Memory currently in use by type (stack, heap) |
| `go.memory.gc.goal` | Gauge | By | Target heap size for next GC cycle |
| `go.processor.limit` | Gauge | `{thread}` | Number of OS threads that can execute user-level Go code |

## HTTP Server Metrics

HTTP server metrics are provided by `go-libs/httpserver` instrumentation.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `http.server.request.duration` | Histogram | s | Time to process HTTP requests |

**Attributes**:
- `http.request.method`: HTTP method (GET, POST, PUT, DELETE, etc.)
- `http.response.status_code`: HTTP response status code
- `http.route`: Request route pattern
- `url.scheme`: URL scheme (http, https)

## Apply Path Metrics

A write batch reaches the controller through the gRPC Apply handler.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `grpc.apply.duration` | Histogram | s | Total duration of the gRPC Apply handler: forwarded identity, `ctrl.Apply`, checkpoint wait and response signing. Recorded on every exit, failures included. |
| `ctrl.apply.duration` | Histogram | s | Duration of a batch's admission in the controller. Recorded on every exit, failures included. |

**Attributes**:
- `batch_size`: number of requests in the batch
- `status`: `success` or `error`; filter on `success` for latency SLOs, since fast rejections pull the error distribution down

## Raft Consensus Metrics

### FSM Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.fsm.logs_appended` | Counter | `{log}` | Total number of logs appended to the store. Use `rate()` to get logs per second. This is the primary throughput metric. |
| `raft.fsm.prepare.duration` | Histogram | s | Time spent in PrepareEntries (processing and merge, without the commit). |
| `raft.fsm.batch_commit.duration` | Histogram | s | Time spent in the Pebble `batch.Commit()` during ApplyEntries. |
| `raft.fsm.rotation.duration` | Histogram | s | Time spent in cache generation rotation (volume compaction) during ApplyEntries. |
| `raft.fsm.preload.coverage_misses` | Counter | `{read}` | Reads on the FSM hot path of keys the proposal's ExecutionPlan did not declare. The order observing the miss is rejected with a business error. **Alert if non-zero**: it means a preload declaration is incomplete. |

**Attributes** (`raft.fsm.preload.coverage_misses`):
- `kind`: attribute kind of the undeclared key

### Node Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.node.leader` | Gauge | - | 1 while this node recognizes a Raft leader, identified by the `leader_id` attribute (decimal Raft node ID); 0 while it knows none. All nodes of a healthy cluster report the same `leader_id`. |
| `raft.apply_entries.duration` | Histogram | s | Time spent applying committed log entries to the FSM. This is the critical path for transaction processing. |
| `raft.entries_applied` | Counter | `{entry}` | Total count of entries applied (cumulative). Use `rate()` to get entries/second. |
| `raft.apply_entries.batch_size` | Histogram | `{entry}` | Distribution of batch sizes when applying entries. Higher batches indicate better throughput efficiency. |
| `raft.append_entries.duration` | Histogram | s | Time spent appending entries to the Write-Ahead Log (WAL) before replication. |
| `raft.process_entry.duration` | Histogram | s | Time spent processing a ready state from the Raft library. Includes sending messages, applying entries, and advancing state. |
| `raft.ready.committed_entries` | Histogram | `{entry}` | Number of committed entries per Raft Ready. |
| `raft.node.ready.wait.duration` | Histogram | s | Time the processReadies goroutine waits for the next Ready from Raft. |
| `raft.node.ready_terminated.wait.duration` | Histogram | s | Time spent waiting for the orchestrate loop to consume a processed Ready (`readyTerminated`). |
| `raft.read_index.duration` | Histogram | s | Time spent in ReadIndex plus WaitForApplied for linearizable reads. |

### Applier Metrics

The applier prepares committed entries against the FSM and commits them in batches, overlapping the next prepare with the previous commit.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.applier.batch_wait.duration` | Histogram | s | Time the applier spends idle waiting for the next batch of entries. |
| `raft.applier.commit_wait.duration` | Histogram | s | Time spent waiting for the previous batch's commit to finish before starting the next prepare. High values mean commits, not preparation, bound throughput. |

### Gating Metrics

Gating occurs when the node performs a maintenance task (snapshot install, checkpoint restore). During gating, Raft Readies are spooled instead of applied directly to the FSM.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.node.gating.wait.duration` | Histogram | s | Time spent waiting for gatingTerminated (maintenance task completion) in the processReadies goroutine. High values indicate long snapshot/restore operations stalling the ready pipeline. |
| `raft.node.gating.readies_processed` | Histogram | `{ready}` | Number of Raft Readies processed during each gating period. Higher values indicate more Readies were spooled while the maintenance task was running. |
| `raft.node.unspool.duration` | Histogram | s | Time spent in unspoolAndResume after a maintenance task (snapshot or checkpoint), replaying the Readies spooled while gated. |

### WAL Metrics

The Write-Ahead Log (WAL) metrics track the performance of the WAL append operations.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `wal.append.save.duration` | Histogram | s | Time spent saving entries to the WAL on disk. This is the actual disk I/O time. |
| `wal.append.batch_size` | Histogram | `{entry}` | Number of entries appended at once. Higher values indicate efficient batching under load. |

### Snapshot Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.node.maintenance.snapshot_creation.duration` | Histogram | s | Time spent creating the snapshot during a maintenance task, excluding the spool replay. Snapshots are taken periodically to compact the log. |
| `raft.node.maintenance.replay_spool.duration` | Histogram | s | Time spent replaying the spooled entries after the snapshot is created in a maintenance task. |

### Propose Queue Metrics

The propose queue buffers proposals (transactions) before they are submitted to Raft consensus.
Admission instruments it: see `admission.propose_queue.load` and
`admission.propose_queue.overflows` in the [admission metrics](#admission-metrics).

## Transport Metrics

Transport metrics track inter-node gRPC communication for Raft consensus.

### Global Transport Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.transport.ping.duration` | Histogram | s | Round-trip latency of ping requests to peer nodes. Useful for detecting network issues. |
| `raft.transport.sending.pending_response.count` | UpDownCounter | `{response}` | Number of pending responses awaited from peer nodes. High values may indicate slow peers. |

**Attributes**:
- `peer`: Peer node ID

### Pending Send Queue Metrics

Outgoing messages are first queued in a global pending send queue before being distributed to per-peer queues.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.send.pending_batch.load` | Histogram | `{batch}` | Current load of the pending send queue. High values indicate messages are being queued faster than they can be dispatched to peers. |
| `raft.send.pending_batch.overflows` | Counter | `{batch}` | Number of times the pending send queue was full. **Alert if non-zero**. |

### Reception Channel Metrics

Messages received from other nodes are queued in 3 priority reception channels. The Raft node consumes directly from these channels with priority ordering (high > medium > low).

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.transport.recv.load` | Histogram | `{batch}` | Current load of the reception queue per priority. Measures queue depth over time. |
| `raft.transport.recv.overflows` | Counter | `{batch}` | Number of times the reception queue was full. **Alert if non-zero**. |

**Attributes**:
- `priority`: Queue priority level (0 = high, 1 = medium, 2 = low)
- `priority_name`: Human-readable priority name (`high`, `medium`, `low`)

**Priority Classification**:
| Priority | Name | Message Types |
|----------|------|---------------|
| 0 | high | `MsgHeartbeat`, `MsgHeartbeatResp` |
| 1 | medium | `MsgVote`, `MsgVoteResp`, `MsgPreVote`, `MsgPreVoteResp`, `MsgAppResp` |
| 2 | low | All others (`MsgApp`, `MsgSnap`, etc.) |

### Unreachable Channel Metrics

Tracks notifications when a peer becomes unreachable.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.transport.unreachable.load` | Histogram | `{peer}` | Current load of the unreachable notification queue. |
| `raft.transport.unreachable.overflows` | Counter | `{peer}` | Number of times the unreachable queue was full. |

### Per-Peer Sending Metrics

Each peer connection has 3 priority queues for sending messages (one per priority level).

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `raft.transport.peer.sending.load` | Histogram | `{batch}` | Current load of the per-peer sending queue. |
| `raft.transport.peer.sending.overflows` | Counter | `{batch}` | Number of times the per-peer sending queue was full. **Alert if consistently non-zero**. |

**Attributes**:
- `peer`: Peer node ID
- `priority`: Queue priority level (0 = high, 1 = medium, 2 = low)
- `priority_name`: Human-readable priority name (`high`, `medium`, `low`)

## Admission Metrics

The admission service handles order processing before Raft consensus. It preloads attribute values from the store when they are not available in the cache.

### Preload Metrics

Before proposing a batch, admission builds its preloads: for every key the batch's orders need (volumes, reversion status, idempotency keys, references, boundaries, ledgers), it uses the cached value when the cache generation guarantees it, and reads the persistent store otherwise. These metrics cover one build per batch; they carry no attributes.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `admission.preload.duration` | Histogram | s | Time to build the preloads of one batch (one `Builder.Build` call), including its store reads and computations. Recorded whether the build succeeds or fails. High values indicate slow storage or expensive computations. |
| `admission.preloads` | Counter | `{preload}` | Successful preload builds: one per admitted batch, whatever the number of keys it needs. |
| `admission.preload.keys_needed` | Counter | `{key}` | Keys the successful builds needed, before cache filtering: the total preload demand. |
| `admission.preload.cache_hits` | Counter | `{key}` | Keys those builds found guaranteed in cache (no store read needed). |

**Derived Metrics** (all counters are cumulative, so use rates over one window):
- **Cache hit ratio**: `rate(cache_hits) / rate(keys_needed)` — fraction of keys served from cache
- **Store read ratio**: `(rate(keys_needed) - rate(cache_hits)) / rate(keys_needed)` — fraction of keys read from the store
- **Keys per batch**: `rate(keys_needed) / rate(preloads)`

These metrics help identify:
- Storage performance issues (high preload duration)
- Cache efficiency problems (low cache hit ratio after warmup)
- Cold start behavior (expected low cache hit ratio initially)

### Command Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `admission.command.duration` | Histogram | s | Total time from Apply call to future resolution. Includes preload, proposal, and FSM application. |
| `admission.propose.duration` | Histogram | s | Time waiting for Raft to accept and replicate a proposal (Propose + Wait). |
| `admission.fsm_future.wait.duration` | Histogram | s | Time waiting for the FSM to apply the command after Raft accepted it. Spikes indicate gating or apply-pipeline stalls. |
| `admission.proposal_guard.duration` | Histogram | s | Time from requesting the proposal guard until the proposal is handed to Raft: the wait to acquire the guard plus the time holding it. This is the contended portion of the propose path. |
| `admission.proposal_guard.rebuilds` | Counter | `{rebuild}` | Number of times the proposal guard had to rebuild preloads because a boundary shifted. |
| `admission.command.size` | Histogram | By | Size of marshalled Raft commands in bytes. Large commands may indicate many postings or metadata. |
| `admission.resolve_batch.duration` | Histogram | s | Time verifying the batch signature and unmarshalling the trusted ApplyBatch. |
| `admission.orders_preparation.duration` | Histogram | s | Time converting requests to orders and extracting their preload needs (script-dependent needs excluded). |
| `admission.script.duration` | Histogram | s | Time resolving Numscript references and enriching the preload needs with the volumes and metadata scripts discover. |
| `admission.response_resolution.duration` | Histogram | s | Time resolving FSM results into logs after apply, including the log reads for idempotent replays. Zero on the common create path. |

### Action Metrics

An `Admit` call carries one batch = one Raft command, and a batch can hold multiple, mixed orders (actions). `command.duration` observes once per batch, so it cannot break work down by action. These two counters do, keyed by action type.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `admission.actions` | Counter | `{action}` | Number of orders (actions) admission processed, by `order_type`. Counts **every order attempted** in the batch, regardless of outcome — it is not gated on the FSM apply result. |
| `admission.action.errors` | Counter | `{action}` | Number of orders whose admission batch ended in error, by `order_type`. A **strict subset** of `admission.actions`. |

**Attributes**:
- `order_type`: The action kind, e.g. `create_transaction`, `revert_transaction`, `add_metadata`, `create_ledger`, `delete_ledger`, `save_numscript`, `create_index`, `register_signing_key`, … This is the same stable vocabulary used by the audit filter DSL (`domain.AuditOrderType`); it is extended additively and tokens are never renamed.

**Per-action error rate**: `admission.action.errors / admission.actions` — a ratio in `[0, 1]` because errors is a strict subset of total.

**Semantics and attribution**:
- **Attempted, not performed**: `admission.actions` increments for every order in the batch even when the batch ultimately fails. This is deliberate — it is what makes the error rate a clean ratio.
- **Atomic batch**: a batch is one Raft command with a single outcome. On failure, **every order in the batch is counted as errored** under its own `order_type` — none of them applied. For the common single-order batch this is exact; for a mixed batch the failure is attributed to each action type present.
- **All admission-observed errors**: both admission-side rejections raised after orders are built (numscript resolution, preload, per-order validation) and FSM business rejections (insufficient funds, conflicts) surfaced when the command resolves.
- **Carve-out**: failures *before* orders are built — write gate, leader readiness, bad batch signature, maintenance mode, request-to-order conversion — have no `order_type` to attribute to and are **not** counted here. They are batch-level/structural failures, not per-action business outcomes.
- **Leader-local**: like `command.duration`, recording happens on the admission path on the leader only. Recording never touches the FSM apply path, preserving FSM determinism.

### Audit Attribution Metrics

Admission checks, after commit, that every write is attributable to a caller
in the audit chain. See [authentication](../technical/architecture/subsystems/api/auth.md)
for how the caller snapshot is resolved.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `admission.audit.missing_caller.writes` | Counter | `{write}` | Committed writes whose caller snapshot is missing or has no principal; the audit entry is unattributed. Logged at error level. **Alert if non-zero**: resolution is normally total. |
| `admission.audit.empty_caller_subject.writes` | Counter | `{write}` | Committed user writes whose caller has a source (key ID or issuer) but an empty subject, for example an Ed25519 token without `sub`. The entry is attributable by source only. Logged at info level. |

### Propose Queue Metrics

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `admission.propose_queue.load` | Histogram | `{proposal}` | Current number of in-flight proposals. High values indicate backpressure from Raft consensus. |
| `admission.propose_queue.overflows` | Counter | `{proposal}` | Number of times the propose queue was full and proposals were rejected. **Alert if non-zero**. |

## Pebble Storage Metrics

The Pebble storage driver exposes metrics via an event listener. Pebble is used for the runtime store (balances, metadata).

### Flush Metrics

Flushes write data from memory (memtable) to disk (SSTable).

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `pebble.flushes` | Counter | `{flush}` | Number of Pebble flush operations |
| `pebble.flush.duration` | Histogram | s | Duration of Pebble flush operations (CPU + I/O time) |
| `pebble.flush.input.size` | Histogram | By | Input bytes flushed from memtables to SSTables |

**Attributes**:
- `reason`: Flush reason (e.g., `capacity`, `delete_only_compaction`)
- `status`: `ok` or `error`

### Compaction Metrics

Compactions merge and reorganize SSTables to optimize read performance and reclaim space.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `pebble.compactions` | Counter | `{compaction}` | Number of Pebble compaction operations |
| `pebble.compaction.duration` | Histogram | s | Duration of Pebble compactions |

**Attributes**:
- `reason`: Compaction reason (e.g., `elision`, `default`, `move`)
- `status`: `ok` or `error`

### Write Stall Metrics

Write stalls occur when Pebble cannot keep up with write rate due to compaction backlog.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `pebble.write_stalls` | Counter | `{stall}` | Number of Pebble write stalls |
| `pebble.write_stall.duration` | Histogram | s | Duration of Pebble write stalls |
| `pebble.write_stall.active` | Gauge | - | Whether Pebble is currently stalling writes (1/0) |

**Attributes**:
- `reason`: Stall reason (e.g., `memtable`, `l0`, `flush_slowdown`)

> **Warning**: A high `pebble.write_stalls` or `pebble.write_stall.active = 1` indicates that Pebble is experiencing backpressure. This typically means the disk cannot keep up with the write rate. Consider:
> - Using faster storage (NVMe SSD)
> - Increasing Pebble cache size
> - Reducing write rate
> - Scaling horizontally

### Disk Slow Metrics

Pebble reports a disk operation that exceeds its slowness threshold.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `pebble.disk_slow.operations` | Counter | `{operation}` | Number of disk operations Pebble reported as slow |
| `pebble.disk_slow.duration` | Histogram | s | Duration of the slow disk operations |

**Attributes**:
- `op`: the slow operation type, as reported by Pebble (e.g. `write`, `sync`)

### VFS Metrics

The main store counts the I/O operations it issues through Pebble's virtual filesystem.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `pebble.vfs.read.ops` | Counter (observable) | `{operation}` | Total read operations |
| `pebble.vfs.write.ops` | Counter (observable) | `{operation}` | Total write operations |
| `pebble.vfs.sync.ops` | Counter (observable) | `{operation}` | Total sync operations |

Use `rate()` for IOPS.

## Storage Disk Usage Metrics

Filesystem-level disk usage is tracked per volume via `syscall.Statfs`. A background collector samples usage at a regular interval (default 5s).

Each WAL and data sample is published atomically with its last successful
observation time and the validity of the latest collection attempt. If
`Statfs` fails, the collector preserves the last successful byte values and
timestamp for diagnostics, marks the sample invalid, and stops emitting that
volume through the gauge until collection recovers. Health and automatic PVC
expansion reject invalid or older-than-one-minute samples.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `storage.disk.volume.usage` | Gauge | By | Disk space used on a storage volume |

**Attributes**:
- `volume`: Storage volume name

| Volume | Path | Description |
|--------|------|-------------|
| `wal` | `{walDir}/` | WAL volume containing spool + WAL data |
| `data` | `{dataDir}/` | Data volume containing the Pebble database |

The leader also polls each peer's disk usage to gate writes when a node runs
out of space.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `health.disk.poll.failures` | Counter | `{failure}` | Failed disk usage polls to a peer. A failure never clears an existing disk write gate. |

**Attributes**:
- `node_id`: the Raft ID of the polled **peer**, not of the reporting node

## Secondary Store Metrics

The read index (`readindex.*`) and the usage store (`usagestore.*`) are
separate Pebble instances beside the main store. Each reports the same
internal metrics under its own namespace.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `readindex.level.size` / `usagestore.level.size` | Gauge | By | Total bytes in each Pebble level |
| `readindex.memtable.size` / `usagestore.memtable.size` | Gauge | By | Current memtable size |
| `readindex.cache.hits` / `usagestore.cache.hits` | Counter (observable) | `{hit}` | Block cache hits since the store opened |
| `readindex.cache.misses` / `usagestore.cache.misses` | Counter (observable) | `{miss}` | Block cache misses since the store opened |

**Attributes** (`*.level.size`):
- `level`: Pebble LSM level (`0` to `6`)

The cache hits and misses are Pebble's cumulative counts, exported as
counters, so compute a hit ratio over one window:
`rate(hits[5m]) / (rate(hits[5m]) + rate(misses[5m]))`.

## Caching & Attributes Metrics

### Numscript Cache Metrics

The Numscript cache stores parsed Numscript programs to avoid re-parsing identical scripts.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `numscript.cache.size` | Gauge | `{entry}` | Number of entries currently in the cache, per `cache` attribute: `parsed` (parsed scripts) and `compiled` (verified VM artifacts, each holding a warm VM) |

### Attribute Cache Metrics

The attribute cache stores computed attribute values (volumes, metadata) in memory to avoid disk lookups.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `cache.rotations` | Counter | `{rotation}` | Number of cache generation rotations |
| `cache.generation` | Gauge | - | Current cache generation number |
| `cache.size` | Gauge | `{entry}` | Number of entries in the cache by attribute type |

**Attributes**:
- `type`: Attribute type (`input`, `output`, `account_metadata`, `ledger_metadata`, `reversions`, `idempotency_keys`)

**Attribute Types**:
| Type | Description |
|------|-------------|
| `input` | Account input volumes (credits) |
| `output` | Account output volumes (debits) |
| `account_metadata` | Account metadata key/value pairs |
| `ledger_metadata` | Ledger metadata key/value pairs |
| `reversions` | Transaction reversion status |
| `idempotency_keys` | Idempotency key mappings |

**Cache Generations**: The cache uses a dual-generation system where old data is gradually evicted. Each "rotation" promotes Gen0 to Gen1 and discards the old Gen1, triggered by raft index thresholds.

### Bloom Filter Metrics

Bloom filters provide probabilistic key existence checks to avoid unnecessary Pebble Gets during preloading. They are configured per attribute type via `--bloom-*` flags.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `bloom.lookups` | Counter | `{lookup}` | Total bloom filter checks (MayContain calls) |
| `bloom.negatives` | Counter | `{lookup}` | Checks that returned definitely-not-present (Pebble Get avoided) |
| `bloom.false_positives` | Counter | `{lookup}` | Checks that returned maybe-present but Pebble Get found nothing |
| `bloom.adds` | Counter | `{key}` | Keys added to the bloom filter |
| `bloom.ready` | Gauge | - | Readiness when reported (1 = ready, 0 = rebuilding after a configuration change); it may be absent when filters are disabled or before first population completes |

**Attributes**:
- `type` on the counters: attribute type (`volumes`, `metadata`, `references`,
  `ledgers`, `boundaries`, `transactions`, `sink_configs`,
  `numscript_versions`, `numscript_contents`, `ledger_metadata`,
  `prepared_queries`, or `indexes`). `bloom.ready` is global and has no
  `type` attribute.

**Key ratios**:
- **Negative rate** = `negatives / lookups` — fraction of lookups that avoided Pebble I/O. Higher is better.
- **Observed absent-key false positive rate** = `false_positives / (negatives + false_positives)` — approximate fraction of known-absent lookups that the filter failed to reject. Compare it with the configured `fpRate` (default 1%).

**Lifecycle**: Dirty Bloom blocks are persisted incrementally in Pebble. An
unchanged restart restores those blocks synchronously and replays the two cache
generations to cover recent writes. First boot, missing or stale persisted
blocks, and configuration changes trigger a full asynchronous attribute scan.
During a full rebuild, preloads bypass the filter and read Pebble directly, so
the optimization is unavailable but false negatives are not introduced.
`bloom.ready` is `0` during a configuration-change rebuild but may be absent
until an initial population completes; an enabled filter set is usable only
after the gauge reports `1`.

## Background Indexer Metrics

Three background workers tail an upstream sequence into a derived store: the
index builder (main store logs into the read index), the usage builder (audit
chain into the usage store) and the audit indexer (audit chain into the audit
index). Each reports how far it has got and how far it lags.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `index.builder.last_indexed_sequence` | Gauge | - | Last log sequence the index builder indexed |
| `index.builder.pebble_last_sequence` | Gauge | - | Last log sequence available in the main store |
| `index.builder.lag` | Gauge | `{sequence}` | Log sequences the index builder is behind the main store |
| `index.builder.logs_indexed` | Counter (observable) | `{log}` | Logs indexed since the process started |
| `usage.builder.last_indexed_sequence` | Gauge | - | Last audit sequence the usage builder processed |
| `usage.builder.audit_last_sequence` | Gauge | - | Last audit sequence available upstream |
| `usage.builder.lag` | Gauge | `{sequence}` | Audit sequences the usage builder is behind |
| `audit.indexer.last_indexed_sequence` | Gauge | - | Last audit sequence the audit indexer indexed |
| `audit.indexer.audit_last_sequence` | Gauge | - | Last audit sequence available upstream |
| `audit.indexer.lag` | Gauge | `{sequence}` | Audit sequences the audit indexer is behind |

A `lag` that keeps growing means the worker cannot keep up with the write rate;
queries served from that derived store then return progressively staler data.

## Mirror Metrics

A [mirror ledger](../technical/architecture/subsystems/events-mirror/mirror.md)
ingests its transactions from an external source, typically Ledger v2. One
mirror worker runs per mirror ledger, on the leader only, and processes the
source in batches: fetch, translate to v3 orders, preload, propose to Raft,
then wait for the FSM to apply.

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `mirror.fetch.duration` | Histogram | s | Time fetching a batch of logs from the source |
| `mirror.translate.duration` | Histogram | s | Time translating the fetched logs into v3 orders |
| `mirror.preload.duration` | Histogram | s | Time preloading the attributes the batch's orders need |
| `mirror.propose.duration` | Histogram | s | Time from the Propose call until Raft accepted the batch |
| `mirror.fsm_wait.duration` | Histogram | s | Time waiting for the FSM to apply the proposed batch |
| `mirror.batch.duration` | Histogram | s | Total time of a successful batch, from start to FSM application |
| `mirror.command.size` | Histogram | By | Size of the marshalled Raft command proposed for a batch |
| `mirror.logs_ingested` | Counter | `{log}` | Source logs fetched into a batch, counted before translation, so a batch that later fails still counts |
| `mirror.batches` | Counter | `{batch}` | Batches that reached an FSM outcome, by `status`. Failures before that (fetch, translate, propose) are not counted; they surface in the worker's logs and retries. |

**Attributes**:
- `ledger`: the mirror ledger name (all mirror metrics)
- `status` (`mirror.batches`): `success` or `error`

## Configuration

### Enabling Metrics Export

Configure metrics export via environment variables or command-line flags:

```bash
# Environment variables
export OTEL_METRICS_ENABLED=true
export OTEL_METRICS_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=otel-collector:4317
export OTEL_METRICS_EXPORTER_INTERVAL=15s

# Or via Helm values
config:
  monitoring:
    metrics:
      enabled: true
      exporter: "otlp"
      endpoint: "otel-collector"
      port: "4317"
      exporterPushInterval: "15s"
```

### Runtime Metrics

Go runtime metrics can be enabled:

```yaml
config:
  monitoring:
    metrics:
      runtime: true
      runtimeMinimumReadMemStatsInterval: "15s"
```

This exposes standard Go runtime metrics including:
- Memory allocation statistics
- Garbage collection metrics
- Goroutine counts

## Histogram Bucket Boundaries

Every duration histogram records seconds as a float (unit `s`), so
sub-microsecond phases keep their precision.

The server exports every histogram with Base2 exponential aggregation (set by
the go-libs metrics module), which adapts its buckets to the recorded values
and ignores explicit boundaries. Many instruments still declare explicit
boundaries as advice: they apply only under a meter provider that uses
explicit-bucket aggregation, such as tests or a custom setup. Some duration
histograms declare none. Examples:

### Apply Entries Duration

`raft.apply_entries.duration` (seconds):
```
0, 0.005, 0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5
```

### Admission Phase Durations

`admission.resolve_batch.duration`, `admission.orders_preparation.duration`,
`admission.script.duration` and `admission.response_resolution.duration`
(seconds, starting at 1µs because the fast phases run in single-digit
microseconds):
```
0, 0.000001, 0.000005, 0.00001, 0.000025, 0.00005, 0.0001, 0.0005, 0.002, 0.01, 0.05, 0.2, 1
```

### Snapshot Creation Duration

`raft.node.maintenance.snapshot_creation.duration` (seconds):
```
0, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5
```

### Queue Load

Queue load uses logarithmic bucket boundaries calculated based on queue capacity for better distribution across the full range.

## Grafana Dashboards

The development environment includes pre-configured Grafana dashboards:

### Ledger Metrics Dashboard

Generated in eight variants under `misc/devenv/monitoring-dashboards/config/dashboards/`
(see [Naming Convention](#naming-convention) for which one matches your
pipeline); the standard devenv stack uses `ledger-metrics-prom-normalized-native.json`.

The dashboard is organized into the following sections:

**System Section**:
- Logs per Second
- Ping Latency
- HTTP Requests Count
- Memory Utilization
- Process CPU Time
- System Network Traffic
- System Memory Usage
- Go Memory Allocated
- Leadership Status
- Goroutine Count
- Go Memory Used
- Go Memory Allocations
- Go GC Goal

**Transport & Queues Section**:
- Reception Queue Throughput (by priority)
- Reception Queue Load Heatmap (by priority)
- Reception Queue Full Counter
- Pending Send Queue Throughput
- Pending Send Queue Load Heatmap
- Pending Send Queue Full Counter
- Per-Peer Send Queue Throughput (by priority)
- Per-Peer Send Queue Load Heatmap (by priority)
- Per-Peer Send Queue Full Counter
- Unreachable Channel Throughput
- Unreachable Channel Load Heatmap
- Unreachable Channel Full Counter
- Propose Queue Throughput
- Propose Queue Load Heatmap
- Propose Queue Full Counter
- Ping Latency
- Pending Responses
- Snapshot Creation

**Ready Loop Section**:
- Process Ready Entry Time Passed
- Applying Entries Time Passed
- Append Entries Time Passed
- Batch Size Distribution
- Applying Entries Percentiles
- Applying Entries Rate
- WAL Cache Update Time
- WAL Save Time
- WAL Append Batch Size
- Gating Wait Duration
- Readies During Gating

**Pebble Section**:
- Flush / Second
- Flush Duration
- Flush Input Bytes / Second
- Compactions / Second
- Compaction Duration
- Compaction Errors / Second
- Write Stall Active (max)
- Write Stalls / Second
- Write Stall Duration

**Caching & Attributes Section**:
- Numscript Cache Size
- Cache Generation & Rotations
- Cache Size by Type

**Admission Section**:
- Preload Duration (p50, p95, p99)
- Preload Builds Rate
- Command Duration Percentiles
- Propose Duration Percentiles
- Command Size Distribution
- Preload Keys Needed Rate
- Preload Cache Hit Ratio (%)
- Preload Store Reads vs Cache Hits
- Propose Queue Load

## Alerting Recommendations

The queries use the fully normalised Prometheus names (default
`formance.ledger` prefix, unit and `_total` suffixes) and thresholds in
seconds. They group by namespace and cluster name as well as the node,
because Raft node IDs repeat across clusters and declared cluster IDs can too.

The server exports histograms with exponential aggregation, which
Prometheus 3 stores as native histograms (the standard devenv stack). Each
histogram alert therefore gives the native query first; the classic
`_bucket` form applies when the pipeline converts histograms to explicit
buckets.

### Critical Alerts

1. **No Leader**
   ```promql
   max by (k8s_namespace_name, formance_ledger_cluster_name) (formance_ledger_raft_node_leader) == 0
   ```
   Duration: 30s
   
2. **Pebble Write Stall Active**
   ```promql
   formance_ledger_pebble_write_stall_active == 1
   ```
   Duration: 10s

3. **High Apply Entries Latency**
   ```promql
   histogram_quantile(0.99, sum by (k8s_namespace_name, formance_ledger_cluster_name, formance_ledger_node_id) (rate(formance_ledger_raft_apply_entries_duration_seconds[5m]))) > 0.1
   ```
   Classic histograms:
   ```promql
   histogram_quantile(0.99, sum by (k8s_namespace_name, formance_ledger_cluster_name, formance_ledger_node_id, le) (rate(formance_ledger_raft_apply_entries_duration_seconds_bucket[5m]))) > 0.1
   ```
   Duration: 5m

4. **Queue Full Events**
   ```promql
   sum by (k8s_namespace_name, formance_ledger_cluster_name, formance_ledger_node_id) (increase(formance_ledger_admission_propose_queue_overflows_total[5m])) > 0
   ```
   Duration: 1m

### Warning Alerts

1. **Queue Near Capacity**
   ```promql
   histogram_quantile(0.95, sum by (k8s_namespace_name, formance_ledger_cluster_name, formance_ledger_node_id) (rate(formance_ledger_admission_propose_queue_load[5m]))) > 0.8 * <queue_capacity>
   ```
   Classic histograms:
   ```promql
   histogram_quantile(0.95, sum by (k8s_namespace_name, formance_ledger_cluster_name, formance_ledger_node_id, le) (rate(formance_ledger_admission_propose_queue_load_bucket[5m]))) > 0.8 * <queue_capacity>
   ```
   Duration: 1m

2. **High Snapshot Duration**
   ```promql
   histogram_quantile(0.99, sum by (k8s_namespace_name, formance_ledger_cluster_name, formance_ledger_node_id) (rate(formance_ledger_raft_node_maintenance_snapshot_creation_duration_seconds[5m]))) > 1
   ```
   Classic histograms:
   ```promql
   histogram_quantile(0.99, sum by (k8s_namespace_name, formance_ledger_cluster_name, formance_ledger_node_id, le) (rate(formance_ledger_raft_node_maintenance_snapshot_creation_duration_seconds_bucket[5m]))) > 1
   ```
   Duration: 5m

3. **High Ping Latency**
   ```promql
   histogram_quantile(0.99, sum by (k8s_namespace_name, formance_ledger_cluster_name, formance_ledger_node_id, peer) (rate(formance_ledger_raft_transport_ping_duration_seconds[5m]))) > 0.01
   ```
   Classic histograms:
   ```promql
   histogram_quantile(0.99, sum by (k8s_namespace_name, formance_ledger_cluster_name, formance_ledger_node_id, peer, le) (rate(formance_ledger_raft_transport_ping_duration_seconds_bucket[5m]))) > 0.01
   ```
   Duration: 5m

## Continuous Profiling with Pyroscope

Ledger v3 POC supports continuous profiling with [Grafana Pyroscope](https://grafana.com/docs/pyroscope/latest/), enabling deep performance analysis and bottleneck identification.

### Overview

Pyroscope collects profiling data (CPU, memory, goroutines, etc.) continuously, allowing you to:
- Identify performance bottlenecks in production
- Analyze CPU and memory usage patterns
- Debug contention issues (mutex, blocking)
- Correlate profiles with traces and metrics

### Configuration

Enable Pyroscope profiling via environment variables or command-line flags:

```bash
# Enable Pyroscope profiling
export PYROSCOPE_ENABLED=true
export PYROSCOPE_SERVER_ADDRESS=http://pyroscope:4040
export PYROSCOPE_APPLICATION_NAME=ledger

# Optional: Authentication for Grafana Cloud
export PYROSCOPE_AUTH_TOKEN=your-grafana-cloud-token
export PYROSCOPE_TENANT_ID=your-tenant-id

# Optional: Basic auth
export PYROSCOPE_BASIC_AUTH_USER=user
export PYROSCOPE_BASIC_AUTH_PASSWORD=password

# Optional: Additional tags (can be specified multiple times)
export PYROSCOPE_TAGS=env=production,region=us-east-1

# Optional: Profile types (default: cpu,alloc_objects,alloc_space,inuse_objects,inuse_space)
export PYROSCOPE_PROFILE_TYPES=cpu,alloc_objects,alloc_space,inuse_objects,inuse_space,goroutines,mutex_count,mutex_duration,block_count,block_duration

# Optional: Upload rate (default: 15s)
export PYROSCOPE_UPLOAD_RATE=15s

# Optional: Mutex and block profiling rates (default: 5)
export PYROSCOPE_MUTEX_PROFILE_FRACTION=5
export PYROSCOPE_BLOCK_PROFILE_RATE=5

# Optional: Disable GC runs between heap profiles
export PYROSCOPE_DISABLE_GC_RUNS=false
```

### Command-Line Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--pyroscope-enabled` | Enable Pyroscope profiling | `false` |
| `--pyroscope-server-address` | Pyroscope server address | `http://localhost:4040` |
| `--pyroscope-application-name` | Application name in Pyroscope | Service name |
| `--pyroscope-auth-token` | Auth token for Grafana Cloud | - |
| `--pyroscope-tenant-id` | Tenant ID for multi-tenant Pyroscope | - |
| `--pyroscope-basic-auth-user` | Basic auth username | - |
| `--pyroscope-basic-auth-password` | Basic auth password | - |
| `--pyroscope-upload-rate` | Profile upload interval | `15s` |
| `--pyroscope-tags` | Additional tags (key=value, repeatable); see [Profile Labels](#profile-labels) for tags the server sets | - |
| `--pyroscope-profile-types` | Profile types to enable (repeatable) | See below |
| `--pyroscope-mutex-profile-fraction` | Mutex profile fraction | `5` |
| `--pyroscope-block-profile-rate` | Block profile rate | `5` |
| `--pyroscope-disable-gc-runs` | Disable GC runs between heap profiles | `false` |

### Profile Types

Available profile types:
- `cpu` - CPU usage
- `alloc_objects` - Number of allocated objects
- `alloc_space` - Total allocated memory
- `inuse_objects` - Objects currently in use
- `inuse_space` - Memory currently in use
- `goroutines` - Goroutine stacks
- `mutex_count` - Mutex contention count
- `mutex_duration` - Mutex contention duration
- `block_count` - Blocking operations count
- `block_duration` - Blocking operations duration

### Kubernetes Deployment

Set `spec.monitoring.pyroscope` on the operator's `Cluster` resource:

```yaml
spec:
  monitoring:
    pyroscope:
      enabled: true
      serverAddress: "https://profiles-prod-001.grafana.net"
      applicationName: "ledger"
      authTokenFrom:
        name: pyroscope-auth
        key: token
      tenantId: "your-tenant-id"
      tags: "env=production"
      profileTypes: "cpu,alloc_objects,alloc_space,inuse_objects,inuse_space"
```

Create the referenced Secret in the Cluster namespace. For basic authentication,
use `basicAuthUser` and `basicAuthPasswordFrom: {name: pyroscope-auth, key: password}`.
Credentials are delivered through Pod `secretKeyRef` entries. See
[deployment](deployment.md#pyroscope-continuous-profiling) for omission, missing
Secret and rotation behavior. Kubernetes manifests must use these references;
the runtime environment variables above are populated by Kubernetes.

### Profile Labels

The server adds these tags to every profile, overriding a `--pyroscope-tags`
entry with the same name. They are the labels the metrics carry after the
OTLP→Prometheus translation, so the same selector works for both signals:

| Tag | Value |
|-----|-------|
| `k8s_namespace_name` | `k8s.namespace.name` resource attribute, when set (by the operator) |
| `formance_ledger_cluster_name` | `formance.ledger.cluster.name` resource attribute: the `Cluster` resource name, or the cluster ID without the operator |
| `formance_ledger_node_id` | `formance.ledger.node.id` resource attribute: the Raft node ID |

They follow the [shared telemetry resource](#shared-telemetry-resource), so
`--otel-resource-attributes` (or `OTEL_RESOURCE_ATTRIBUTES`) overrides them
like any other identity attribute. The Grafana dashboards select profiles with
`{k8s_namespace_name=~"$namespace", formance_ledger_cluster_name=~"$cluster", formance_ledger_node_id=~"$node"}`.

The server no longer tags profiles with the bare Raft node ID (`node_id`) or
the declared cluster ID (`cluster_id`): the node ID repeats across clusters and
separate clusters can share a cluster ID, so neither identifies a node or a
cluster on its own. A `--pyroscope-tags` entry with either name is still kept.

The application name defaults to the OTel service name, `ledger` unless
`--otel-service-name` is set, so every node reports under one application and
these tags tell clusters and nodes apart.

### Best Practices

1. **Start with default profile types**: CPU and memory profiles provide the most value with minimal overhead.

2. **Enable mutex/block profiling selectively**: These profiles add overhead and should only be enabled when debugging contention issues.

3. **Use appropriate upload rate**: 15 seconds is a good default. Shorter intervals provide more granularity but increase overhead.

4. **Tag your profiles**: Use tags to differentiate between environments, regions, or versions.

5. **Monitor overhead**: Continuous profiling adds ~1-2% CPU overhead. Monitor your application's resource usage after enabling.

### Integration with Grafana

When using Grafana Cloud or self-hosted Grafana with Pyroscope:

1. Add Pyroscope as a data source in Grafana
2. Use the Profiles panel to view flame graphs
3. Correlate profiles with traces using the same service name
4. Use the "Profiles Drilldown" plugin for advanced analysis

### Troubleshooting

**Profiles not appearing in Pyroscope:**
- Verify `PYROSCOPE_ENABLED=true`
- Check network connectivity to Pyroscope server
- Verify authentication credentials if using Grafana Cloud
- Check application logs for Pyroscope-related errors

**High overhead:**
- Reduce the number of profile types
- Increase upload rate
- Disable mutex and block profiling

## Next Steps

- [Deployment](./deployment.md) - Configure observability stack
- [Architecture](../technical/architecture/overview.md) - Understand system components
- [Storage](../technical/architecture/subsystems/storage/storage.md) - Storage configuration and tuning
