// Admission section — auto-scaffolded from the Grafana export.
// Edit freely: panel constructors live in ../lib/panels.libsonnet.

local panels = import '../lib/panels.libsonnet';

panels.row('Admission', 169, [
  panels.timeseries(
    'Preload Duration (p50, p95, p99)',
    { h: 8, w: 12, x: 0, y: 90 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(admission.preload.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, type))', legendFormat: 'p50 - {{type}}' },
      { expr: 'histogram_quantile(0.95, sum(rate(admission.preload.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, type))', legendFormat: 'p95 - {{type}}' },
      { expr: 'histogram_quantile(0.99, sum(rate(admission.preload.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, type))', legendFormat: 'p99 - {{type}}' },
    ], unit='µs',
    description=|||
      Time spent loading preload values from the persistent store during admission. High values indicate slow storage or expensive attribute computations.
      
      Breakdown by attribute type:
      - volumes: Account volumes (input/output)
      - reversions: Transaction reversion status
      - idempotency_keys: Idempotency key mappings
      - boundaries: Ledger boundaries
      - ledgers: Ledger info
      - references: Transaction references
   |||,
  ),

  panels.timeseries(
    'Preload Rate (by type)',
    { h: 8, w: 12, x: 12, y: 90 },
    [
      { expr: 'sum(rate(admission.preload.total{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id)', legendFormat: 'Node {{service.node_id}}' },
    ], unit='ops',
    description=|||
      Rate of preload operations from the persistent store per second. High rates after warmup may indicate cache efficiency issues.
      
      Breakdown by attribute type:
      - volumes: Account volumes (input/output)
      - reversions: Transaction reversion status
      - idempotency_keys: Idempotency key mappings
      - boundaries: Ledger boundaries
      - ledgers: Ledger info
      - references: Transaction references
   |||, opts={ stackMode: 'normal' },
  ),

  panels.timeseries(
    'Preload Keys Needed Rate (by type)',
    { h: 8, w: 12, x: 0, y: 98 },
    [
      { expr: 'sum(rate(admission.preload.keys_needed{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id)', legendFormat: 'Node {{service.node_id}}' },
    ], unit='ops',
    description=|||
      Rate of keys that need resolving during preload, broken down by attribute type.
      This shows the total demand on the preload system before cache filtering.
   |||, opts={ stackMode: 'normal' },
  ),

  panels.timeseries(
    'Preload Cache Hit Ratio (%)',
    { h: 8, w: 12, x: 12, y: 98 },
    [
      { expr: 'sum(rate(admission.preload.cache_hits{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id) / sum(rate(admission.preload.keys_needed{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id) * 100', legendFormat: 'Node {{service.node_id}}' },
    ], unit='percent',
    description=|||
      Percentage of preload keys found guaranteed in cache (no store read needed).
      Computed as: cache_hits / keys_needed * 100.
      
      High values (close to 100%) indicate good cache efficiency.
      Low values indicate many cache misses requiring store reads.
   |||,
  ),

  panels.timeseries(
    'Preload Store Reads vs Cache Hits (by type)',
    { h: 8, w: 12, x: 0, y: 106 },
    [
      { expr: 'sum(rate(admission.preload.keys_needed{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id) - sum(rate(admission.preload.cache_hits{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id)', legendFormat: 'store reads - Node {{service.node_id}}' },
      { expr: 'sum(rate(admission.preload.cache_hits{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id)', legendFormat: 'cache hits - Node {{service.node_id}}' },
    ], unit='ops',
    description=|||
      Comparison of store reads (cache misses) vs cache hits by attribute type.
      Helps visualize the cache effectiveness at a glance.
   |||, opts={ stackMode: 'normal' },
  ),

  panels.timeseries(
    'Propose Queue Load',
    { h: 8, w: 12, x: 12, y: 106 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(admission.propose_queue.load_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p50' },
      { expr: 'histogram_quantile(0.95, sum(rate(admission.propose_queue.load_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p95' },
      { expr: 'histogram_quantile(0.99, sum(rate(admission.propose_queue.load_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le))', legendFormat: 'p99' },
    ], unit='short',
    description=|||
      Current load of the propose queue. High values indicate backpressure from Raft consensus.
      Alert if propose queue full counter is non-zero.
   |||,
  ),

  panels.timeseries(
    'Command Duration (p50, p95, p99)',
    { h: 8, w: 12, x: 0, y: 114 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(admission.command.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p50 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.95, sum(rate(admission.command.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p95 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.99, sum(rate(admission.command.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p99 - Node {{service.node_id}}' },
    ], unit='µs',
    description='Total time from Apply call to future resolution. Includes preload time, proposal time, and FSM application time.',
  ),

  panels.histogram(
    'Command Size Distribution',
    { h: 8, w: 12, x: 12, y: 114 },
    'sum(rate(admission.command.size_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le)', unit='ops',
    description='Distribution of Raft command sizes in bytes. Large commands may indicate many postings or large metadata.', opts={ legendFormat: '{{le}}' },
  ),

  panels.timeseries(
    'Command Size (p50, p95, p99)',
    { h: 8, w: 12, x: 0, y: 122 },
    [
      { expr: 'histogram_quantile(0.5, rate(admission.command.size_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval]))', legendFormat: 'p50 (Node {{service.node_id}})' },
      { expr: 'histogram_quantile(0.95, rate(admission.command.size_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval]))', legendFormat: 'p95 (Node {{service.node_id}})' },
      { expr: 'histogram_quantile(0.99, rate(admission.command.size_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval]))', legendFormat: 'p99 (Node {{service.node_id}})' },
    ], unit='decbytes',
    description='Size of marshalled Raft commands in bytes. Large commands (>50KB) can cause memory issues when many requests are queued.',
  ),

  panels.timeseries(
    'Command Size Distribution (stacked)',
    { h: 8, w: 12, x: 12, y: 122 },
    [
      { expr: 'sum by (le) (rate(admission.command.size_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval]))', legendFormat: '≤ {{le}} bytes' },
    ], unit='ops',
    description='Rate of commands by size bucket. Helps identify the distribution of command sizes.', opts={ stackMode: 'normal' },
  ),

  panels.timeseries(
    'Propose Duration (p50, p95, p99)',
    { h: 8, w: 12, x: 0, y: 130 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(admission.propose.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p50 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.95, sum(rate(admission.propose.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p95 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.99, sum(rate(admission.propose.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p99 - Node {{service.node_id}}' },
    ], unit='µs',
    description='Time waiting for Raft to accept and replicate a proposal (Propose + Wait). Lower is better.',
  ),

  panels.timeseries(
    'FSM Future Wait Duration (p50, p95, p99)',
    { h: 8, w: 12, x: 0, y: 108 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(admission.fsm_future.wait.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p50 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.95, sum(rate(admission.fsm_future.wait.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p95 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.99, sum(rate(admission.fsm_future.wait.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p99 - Node {{service.node_id}}' },
    ], unit='µs',
    description=|||
      Time waiting for the FSM to apply the command after Raft has accepted the proposal.
      
      This measures the gap between proposal acceptance and future resolution. Spikes here confirm that snapshot gating or pipeline stalls are blocking command application.
      
      Compare with 'Propose Duration' which measures only Raft acceptance time.
   |||, opts={ showPoints: 'auto' },
  ),

  panels.timeseries(
    'Proposal Guard Duration (p50, p95, p99)',
    { h: 8, w: 12, x: 12, y: 108 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(admission.proposal_guard.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'p50 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.95, sum(rate(admission.proposal_guard.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'p95 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.99, sum(rate(admission.proposal_guard.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (le, service.node_id))', legendFormat: 'p99 - Node {{service.node_id}}' },
    ], unit='µs',
    description='Time spent waiting to acquire the proposal guard lock. High values indicate contention on the proposal mutex, which serializes boundary validation and Propose calls.',
  ),

  panels.timeseries(
    'Proposal Guard Rebuild Rate & Ratio',
    { h: 8, w: 12, x: 0, y: 116 },
    [
      { expr: 'sum(rate(admission.proposal_guard.rebuild{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id) / sum(rate(admission.proposal_guard.duration_count{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id)', legendFormat: 'Rebuild ratio - Node {{service.node_id}}' },
      { expr: 'sum(rate(admission.proposal_guard.rebuild{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id)', legendFormat: 'Rebuilds/s - Node {{service.node_id}}' },
    ], unit='percentunit',
    description=|||
      Rate of proposal guard rebuilds (boundary shifted) vs total guard acquisitions. A high ratio means the cache generation boundary is frequently shifting between optimistic preload and proposal, causing expensive re-builds under lock.

      The denominator is admission.proposal_guard.duration_count — the number of times the proposal guard was actually acquired (one per proposal attempt that reached builder.Run). It is NOT admission.command.duration_count, which also counts commands rejected before proposal (bad signature, maintenance mode, validation/preload errors); using that would dilute the ratio with attempts the guard never saw.
    |||,
  ),

  panels.timeseries(
    'Resolve Batch Duration (p50, p95, p99)',
    { h: 8, w: 12, x: 12, y: 116 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(admission.resolve_batch.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p50 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.95, sum(rate(admission.resolve_batch.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p95 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.99, sum(rate(admission.resolve_batch.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p99 - Node {{service.node_id}}' },
    ], unit='µs',
    description='Time spent verifying the batch signature and unmarshaling the trusted ApplyBatch. First phase of the command lifecycle decomposed by admission.command.duration.',
  ),

  panels.timeseries(
    'Orders Preparation Duration (p50, p95, p99)',
    { h: 8, w: 12, x: 0, y: 124 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(admission.orders_preparation.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p50 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.95, sum(rate(admission.orders_preparation.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p95 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.99, sum(rate(admission.orders_preparation.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p99 - Node {{service.node_id}}' },
    ], unit='µs',
    description='Time spent converting requests to orders and extracting preload needs (excludes script-dependent needs). Phase of admission.command.duration.',
  ),

  panels.timeseries(
    'Scripts Duration (p50, p95, p99)',
    { h: 8, w: 12, x: 12, y: 124 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(admission.scripts.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p50 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.95, sum(rate(admission.scripts.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p95 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.99, sum(rate(admission.scripts.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p99 - Node {{service.node_id}}' },
    ], unit='µs',
    description='Time spent resolving Numscript references and enriching preload needs with script-discovered volumes/metadata. Phase of admission.command.duration.',
  ),

  panels.timeseries(
    'Response Resolution Duration (p50, p95, p99)',
    { h: 8, w: 12, x: 0, y: 132 },
    [
      { expr: 'histogram_quantile(0.50, sum(rate(admission.response_resolution.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p50 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.95, sum(rate(admission.response_resolution.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p95 - Node {{service.node_id}}' },
      { expr: 'histogram_quantile(0.99, sum(rate(admission.response_resolution.duration_bucket{service.cluster=~"$cluster", service.node_id=~"$node"}[$__rate_interval])) by (service.node_id, le))', legendFormat: 'p99 - Node {{service.node_id}}' },
    ], unit='µs',
    description='Time spent resolving FSM results into concrete logs after apply, including the ReadLogBySequence reads done for idempotent replays (ReferenceSequence entries). Final phase of admission.command.duration; zero on the common create path.',
  ),
])
