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
  'admission.command.duration': { kind: 'histogram', unit: 's' },
  'admission.command.size': { kind: 'histogram', unit: 'By' },
  'admission.fsm_future.wait.duration': { kind: 'histogram', unit: 's' },
  'admission.orders_preparation.duration': { kind: 'histogram', unit: 's' },
  'admission.preload.cache_hits': { kind: 'counter', unit: '{key}' },
  'admission.preload.duration': { kind: 'histogram', unit: 's' },
  'admission.preload.keys_needed': { kind: 'counter', unit: '{key}' },
  'admission.preloads': { kind: 'counter', unit: '{preload}' },
  'admission.proposal_guard.duration': { kind: 'histogram', unit: 's' },
  'admission.proposal_guard.rebuilds': { kind: 'counter', unit: '{rebuild}' },
  'admission.propose.duration': { kind: 'histogram', unit: 's' },
  'admission.propose_queue.overflows': { kind: 'counter', unit: '{proposal}' },
  'admission.propose_queue.load': { kind: 'histogram', unit: '{proposal}' },
  'admission.resolve_batch.duration': { kind: 'histogram', unit: 's' },
  'admission.response_resolution.duration': { kind: 'histogram', unit: 's' },
  'admission.scripts.duration': { kind: 'histogram', unit: 's' },
  'bloom.adds': { kind: 'counter', unit: '{key}' },
  'bloom.false_positives': { kind: 'counter', unit: '{lookup}' },
  'bloom.lookups': { kind: 'counter', unit: '{lookup}' },
  'bloom.negatives': { kind: 'counter', unit: '{lookup}' },
  'bloom.ready': { kind: 'gauge', unit: null },
  'cache.generation': { kind: 'gauge', unit: null },
  'cache.rotations': { kind: 'counter', unit: '{rotation}' },
  'cache.size': { kind: 'gauge', unit: '{entry}' },
  'ctrl.apply.duration': { kind: 'histogram', unit: 's' },
  'grpc.apply.duration': { kind: 'histogram', unit: 's' },
  'index.builder.lag': { kind: 'gauge', unit: '{sequence}' },
  'index.builder.last_indexed_sequence': { kind: 'gauge', unit: null },
  'index.builder.logs_indexed': { kind: 'counter', unit: '{log}' },
  'index.builder.pebble_last_sequence': { kind: 'gauge', unit: null },
  'mirror.batch.duration': { kind: 'histogram', unit: 's' },
  'mirror.batches': { kind: 'counter', unit: '{batch}' },
  'mirror.command.size': { kind: 'histogram', unit: 'By' },
  'mirror.fetch.duration': { kind: 'histogram', unit: 's' },
  'mirror.fsm_wait.duration': { kind: 'histogram', unit: 's' },
  'mirror.logs_ingested': { kind: 'counter', unit: '{log}' },
  'mirror.preload.duration': { kind: 'histogram', unit: 's' },
  'mirror.propose.duration': { kind: 'histogram', unit: 's' },
  'mirror.translate.duration': { kind: 'histogram', unit: 's' },
  'numscript.cache.size': { kind: 'gauge', unit: '{entry}' },
  'pebble.compaction.duration': { kind: 'histogram', unit: 's' },
  'pebble.compactions': { kind: 'counter', unit: '{compaction}' },
  'pebble.disk_slow.duration': { kind: 'histogram', unit: 's' },
  'pebble.disk_slow.operations': { kind: 'counter', unit: '{operation}' },
  'pebble.flush.duration': { kind: 'histogram', unit: 's' },
  'pebble.flush.input.size': { kind: 'histogram', unit: 'By' },
  'pebble.flushes': { kind: 'counter', unit: '{flush}' },
  'pebble.vfs.read.ops': { kind: 'counter', unit: '{operation}' },
  'pebble.vfs.sync.ops': { kind: 'counter', unit: '{operation}' },
  'pebble.vfs.write.ops': { kind: 'counter', unit: '{operation}' },
  'pebble.write_stall.active': { kind: 'gauge', unit: null },
  'pebble.write_stall.duration': { kind: 'histogram', unit: 's' },
  'pebble.write_stalls': { kind: 'counter', unit: '{stall}' },
  'raft.append_entries.duration': { kind: 'histogram', unit: 's' },
  'raft.applier.batch_wait.duration': { kind: 'histogram', unit: 's' },
  'raft.applier.commit_wait.duration': { kind: 'histogram', unit: 's' },
  'raft.entries_applied': { kind: 'counter', unit: '{entry}' },
  'raft.apply_entries.batch_size': { kind: 'histogram', unit: '{entry}' },
  'raft.apply_entries.duration': { kind: 'histogram', unit: 's' },
  'raft.fsm.batch_commit.duration': { kind: 'histogram', unit: 's' },
  'raft.fsm.logs_appended': { kind: 'counter', unit: '{log}' },
  'raft.fsm.preload.coverage_misses': { kind: 'counter', unit: '{read}' },
  'raft.fsm.prepare.duration': { kind: 'histogram', unit: 's' },
  'raft.fsm.rotation.duration': { kind: 'histogram', unit: 's' },
  'raft.node.gating.readies_processed': { kind: 'histogram', unit: '{ready}' },
  'raft.node.gating.wait.duration': { kind: 'histogram', unit: 's' },
  'raft.node.leader': { kind: 'gauge', unit: null },
  'raft.node.maintenance.replay_spool.duration': { kind: 'histogram', unit: 's' },
  'raft.node.maintenance.snapshot_creation.duration': { kind: 'histogram', unit: 's' },
  'raft.node.ready.wait.duration': { kind: 'histogram', unit: 's' },
  'raft.node.ready_terminated.wait.duration': { kind: 'histogram', unit: 's' },
  'raft.node.unspool.duration': { kind: 'histogram', unit: 's' },
  'raft.process_entry.duration': { kind: 'histogram', unit: 's' },
  'raft.read_index.duration': { kind: 'histogram', unit: 's' },
  'raft.ready.committed_entries': { kind: 'histogram', unit: '{entry}' },
  'raft.send.pending_messages.overflows': { kind: 'counter', unit: '{batch}' },
  'raft.send.pending_messages.load': { kind: 'histogram', unit: '{batch}' },
  'raft.transport.peer.sending.overflows': { kind: 'counter', unit: '{batch}' },
  'raft.transport.peer.sending.load': { kind: 'histogram', unit: '{batch}' },
  'raft.transport.ping.duration': { kind: 'histogram', unit: 's' },
  'raft.transport.recv.overflows': { kind: 'counter', unit: '{batch}' },
  'raft.transport.recv.load': { kind: 'histogram', unit: '{batch}' },
  'raft.transport.sending.pending_response.count': { kind: 'gauge', unit: '{response}' },
  'raft.transport.unreachable.overflows': { kind: 'counter', unit: '{peer}' },
  'raft.transport.unreachable.load': { kind: 'histogram', unit: '{peer}' },
  'readindex.cache.hits': { kind: 'counter', unit: '{hit}' },
  'readindex.cache.misses': { kind: 'counter', unit: '{miss}' },
  'readindex.level.size': { kind: 'gauge', unit: 'By' },
  'readindex.memtable.size': { kind: 'gauge', unit: 'By' },
  'storage.disk.volume.usage': { kind: 'gauge', unit: 'By' },
  'wal.append.batch_size': { kind: 'histogram', unit: '{entry}' },
  'wal.append.save.duration': { kind: 'histogram', unit: 's' },

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
