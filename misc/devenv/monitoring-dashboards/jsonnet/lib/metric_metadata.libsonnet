// Metadata for every metric the dashboard references.
//
// The first group (metrics we emit) is extracted from the Go call
// sites by tools/extract-metric-metadata (one-shot) and verified
// against the source code by
// internal/infra/monitoring/metrics/registry_test.go.
//
// The second group (`go.*` / `process.*` / `system.*` / `http.*`)
// covers OpenTelemetry semantic-convention auto-instrumentation
// metrics that our dashboard references — those metrics are emitted
// by upstream libraries (`go.opentelemetry.io/contrib/instrumentation/runtime`,
// `otelhttp`, host metrics) through the global MeterProvider and
// are not visible to the Go-side registry test. They are listed
// here so the prom-normalized variant can apply the unit and
// `_total` suffixes the collector adds.
//
// Used by lib/naming.libsonnet to apply the OTel→Prometheus
// unit-suffix and `_total` rules when generating the
// prom-normalized dashboard variant.
{
  'admission.command.duration': { kind: 'histogram', unit: 'us' },
  'admission.command.size': { kind: 'histogram', unit: 'By' },
  'admission.fsm_future.wait.duration': { kind: 'histogram', unit: 'us' },
  'admission.orders_preparation.duration': { kind: 'histogram', unit: 'us' },
  'admission.preload.cache_hits': { kind: 'counter', unit: '1' },
  'admission.preload.duration': { kind: 'histogram', unit: 'us' },
  'admission.preload.keys_needed': { kind: 'counter', unit: '1' },
  'admission.preload.total': { kind: 'counter', unit: '1' },
  'admission.proposal_guard.duration': { kind: 'histogram', unit: 'us' },
  'admission.proposal_guard.rebuild': { kind: 'counter', unit: '1' },
  'admission.propose.duration': { kind: 'histogram', unit: 'us' },
  'admission.propose_queue.full': { kind: 'counter', unit: '1' },
  'admission.propose_queue.load': { kind: 'histogram', unit: '1' },
  'admission.resolve_batch.duration': { kind: 'histogram', unit: 'us' },
  'admission.response_resolution.duration': { kind: 'histogram', unit: 'us' },
  'admission.scripts.duration': { kind: 'histogram', unit: 'us' },
  'bloom.adds': { kind: 'counter', unit: null },
  'bloom.false_positives': { kind: 'counter', unit: null },
  'bloom.lookups': { kind: 'counter', unit: null },
  'bloom.negatives': { kind: 'counter', unit: null },
  'bloom.ready': { kind: 'gauge', unit: null },
  'cache.generation': { kind: 'gauge', unit: null },
  'cache.rotations': { kind: 'counter', unit: null },
  'cache.size': { kind: 'gauge', unit: null },
  'ctrl.apply.duration': { kind: 'histogram', unit: 'us' },
  'grpc.apply.duration': { kind: 'histogram', unit: 'us' },
  'index.builder.lag': { kind: 'gauge', unit: null },
  'index.builder.last_indexed_sequence': { kind: 'gauge', unit: null },
  'index.builder.logs_indexed_total': { kind: 'gauge', unit: null },
  'index.builder.pebble_last_sequence': { kind: 'gauge', unit: null },
  'ledger.preload.coverage_miss': { kind: 'counter', unit: '1' },
  'mirror.batch.duration': { kind: 'histogram', unit: 'us' },
  'mirror.batch.total': { kind: 'counter', unit: '1' },
  'mirror.command.size': { kind: 'histogram', unit: 'By' },
  'mirror.fetch.duration': { kind: 'histogram', unit: 'us' },
  'mirror.fsm_wait.duration': { kind: 'histogram', unit: 'us' },
  'mirror.logs.ingested': { kind: 'counter', unit: '1' },
  'mirror.preload.duration': { kind: 'histogram', unit: 'us' },
  'mirror.propose.duration': { kind: 'histogram', unit: 'us' },
  'mirror.translate.duration': { kind: 'histogram', unit: 'us' },
  'numscript.cache.size': { kind: 'gauge', unit: null },
  'pebble.compaction.duration.milliseconds': { kind: 'histogram', unit: 'ms' },
  'pebble.compaction.total': { kind: 'counter', unit: null },
  'pebble.disk_slow.duration.milliseconds': { kind: 'histogram', unit: 'ms' },
  'pebble.disk_slow.total': { kind: 'counter', unit: null },
  'pebble.flush.duration.milliseconds': { kind: 'histogram', unit: 'ms' },
  'pebble.flush.input.bytes': { kind: 'histogram', unit: 'By' },
  'pebble.flush.total': { kind: 'counter', unit: null },
  'pebble.vfs.read.ops': { kind: 'counter', unit: null },
  'pebble.vfs.sync.ops': { kind: 'counter', unit: null },
  'pebble.vfs.write.ops': { kind: 'counter', unit: null },
  'pebble.write_stall.active': { kind: 'gauge', unit: null },
  'pebble.write_stall.duration.milliseconds': { kind: 'histogram', unit: 'ms' },
  'pebble.write_stall.total': { kind: 'counter', unit: null },
  'raft.append_entries': { kind: 'histogram', unit: 'us' },
  'raft.applier.batch_wait.duration': { kind: 'histogram', unit: 'us' },
  'raft.applier.commit_wait.duration': { kind: 'histogram', unit: 'us' },
  'raft.apply_entries.batch_size': { kind: 'counter', unit: '1' },
  'raft.apply_entries.batch_size_distribution': { kind: 'histogram', unit: '1' },
  'raft.apply_entries.duration': { kind: 'histogram', unit: 'us' },
  'raft.fsm.batch_commit.duration': { kind: 'histogram', unit: 'us' },
  'raft.fsm.logs_appended': { kind: 'counter', unit: '1' },
  'raft.fsm.prepare.duration': { kind: 'histogram', unit: 'us' },
  'raft.fsm.rotation.duration': { kind: 'histogram', unit: 'us' },
  'raft.node.gating.readies_processed': { kind: 'histogram', unit: '1' },
  'raft.node.gating.wait_duration': { kind: 'histogram', unit: 'us' },
  'raft.node.lead': { kind: 'gauge', unit: null },
  'raft.node.maintenance.replay_spool.duration': { kind: 'histogram', unit: 'us' },
  'raft.node.maintenance.snapshot_creation.duration': { kind: 'histogram', unit: 'us' },
  'raft.node.ready.wait_duration': { kind: 'histogram', unit: 'us' },
  'raft.node.ready_terminated.wait_duration': { kind: 'histogram', unit: 'us' },
  'raft.node.unspool.duration': { kind: 'histogram', unit: 'us' },
  'raft.process_entry': { kind: 'histogram', unit: 'us' },
  'raft.read_index.duration': { kind: 'histogram', unit: 'us' },
  'raft.ready.committed_entries': { kind: 'histogram', unit: '1' },
  'raft.send.pending_messages.full': { kind: 'counter', unit: '1' },
  'raft.send.pending_messages.load': { kind: 'histogram', unit: '1' },
  'raft.transport.peer.sending.full': { kind: 'counter', unit: '1' },
  'raft.transport.peer.sending.load': { kind: 'histogram', unit: '1' },
  'raft.transport.ping.latency': { kind: 'histogram', unit: 'microseconds' },
  'raft.transport.recv.full': { kind: 'counter', unit: '1' },
  'raft.transport.recv.load': { kind: 'histogram', unit: '1' },
  'raft.transport.sending.pending_response': { kind: 'gauge', unit: null },
  'raft.transport.unreachable.full': { kind: 'counter', unit: '1' },
  'raft.transport.unreachable.load': { kind: 'histogram', unit: '1' },
  'readindex.cache.hits': { kind: 'gauge', unit: '{hits}' },
  'readindex.cache.misses': { kind: 'gauge', unit: '{misses}' },
  'readindex.level.bytes': { kind: 'gauge', unit: 'By' },
  'readindex.memtable.bytes': { kind: 'gauge', unit: 'By' },
  'storage.disk.volume.bytes': { kind: 'gauge', unit: 'By' },
  'wal.append.batch_size': { kind: 'histogram', unit: '1' },
  'wal.append.save.duration': { kind: 'histogram', unit: 'us' },

  // --- OTel semantic-convention auto-instrumentation ---
  // Emitted upstream via the global MeterProvider; the registry
  // test does not see them, but the prom-normalized dashboard
  // needs their unit + counter shape to produce the correct
  // Prometheus name.
  'go.goroutine.count': { kind: 'gauge', unit: '{goroutine}' },
  'go.memory.allocated': { kind: 'counter', unit: 'By' },
  'go.memory.allocations': { kind: 'counter', unit: '{allocation}' },
  'go.memory.gc.goal': { kind: 'gauge', unit: 'By' },
  'go.memory.used': { kind: 'gauge', unit: 'By' },
  'go.processor.limit': { kind: 'gauge', unit: '{cpu}' },
  'http.server.request.duration': { kind: 'histogram', unit: 's' },
  'process.cpu.time': { kind: 'counter', unit: 's' },
  'system.memory.usage': { kind: 'gauge', unit: 'By' },
  'system.memory.utilization': { kind: 'gauge', unit: '1' },
  'system.network.io': { kind: 'counter', unit: 'By' },
}
