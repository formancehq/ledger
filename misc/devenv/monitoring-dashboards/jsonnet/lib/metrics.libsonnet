// Registry of every metric name emitted by the ledger server.
//
// This is the single source of truth used by dashboards (and will
// be consumed by alert rules in the future). The Go-side test
// internal/infra/monitoring/metrics/registry_test.go cross-checks
// that every name listed here is actually created by a call site in
// the codebase, and conversely that every instrument our code
// creates appears here.
//
// OpenTelemetry semantic-convention auto-instrumentation (http.*,
// go.*, process.*, system.*, …) targets the *global* MeterProvider —
// our code does not emit those names, so they are NOT listed here.
{
  // admission — internal/application/admission/admission.go
  admission:: {
    command_duration: 'admission.command.duration',
    command_size: 'admission.command.size',
    propose_queue_load: 'admission.propose_queue.load',
    propose_queue_full: 'admission.propose_queue.full',
    propose_duration: 'admission.propose.duration',
    fsm_future_wait_duration: 'admission.fsm_future.wait.duration',
    proposal_guard_duration: 'admission.proposal_guard.duration',
    proposal_guard_rebuild: 'admission.proposal_guard.rebuild',
    preload_duration: 'admission.preload.duration',
    preload_total: 'admission.preload.total',
    preload_keys_needed: 'admission.preload.keys_needed',
    preload_cache_hits: 'admission.preload.cache_hits',
    audit_missing_caller: 'admission.audit.missing_caller',
    audit_caller_subject_empty: 'admission.audit.caller_subject_empty',
    resolve_batch_duration: 'admission.resolve_batch.duration',
    orders_preparation_duration: 'admission.orders_preparation.duration',
    scripts_duration: 'admission.scripts.duration',
    response_resolution_duration: 'admission.response_resolution.duration',
  },

  // bloom — internal/infra/bloom/bloom.go
  bloom:: {
    lookups: 'bloom.lookups',
    negatives: 'bloom.negatives',
    adds: 'bloom.adds',
    false_positives: 'bloom.false_positives',
    ready: 'bloom.ready',
  },

  // cache — internal/infra/cache/cache.go
  cache:: {
    rotations: 'cache.rotations',
    generation: 'cache.generation',
    size: 'cache.size',
  },

  // ctrl — internal/application/ctrl/controller_default.go
  ctrl:: {
    apply_duration: 'ctrl.apply.duration',
  },

  // grpc (custom) — internal/adapter/grpc/server_bucket.go
  grpc:: {
    apply_duration: 'grpc.apply.duration',
  },

  // index.builder — internal/application/indexbuilder/builder.go
  index_builder:: {
    last_indexed_sequence: 'index.builder.last_indexed_sequence',
    pebble_last_sequence: 'index.builder.pebble_last_sequence',
    lag: 'index.builder.lag',
    logs_indexed_total: 'index.builder.logs_indexed_total',
  },

  // audit_index — internal/application/auditindexer/indexer.go
  audit_index:: {
    last_indexed_sequence: 'audit_index.last_indexed_sequence',
    audit_last_sequence: 'audit_index.audit_last_sequence',
    lag: 'audit_index.lag',
  },

  // numscript — internal/domain/processing/numscript/cache.go
  numscript:: {
    cache_size: 'numscript.cache.size',
  },

  // mirror — internal/application/mirror/worker.go
  mirror:: {
    fetch_duration: 'mirror.fetch.duration',
    translate_duration: 'mirror.translate.duration',
    preload_duration: 'mirror.preload.duration',
    propose_duration: 'mirror.propose.duration',
    fsm_wait_duration: 'mirror.fsm_wait.duration',
    batch_duration: 'mirror.batch.duration',
    command_size: 'mirror.command.size',
    logs_ingested: 'mirror.logs.ingested',
    batch_total: 'mirror.batch.total',
  },

  // pebble — internal/storage/dal/metrics.go (our wrapper around
  // Pebble's EventListener; Pebble itself does not expose OTel).
  pebble:: {
    flush_total: 'pebble.flush.total',
    flush_duration_milliseconds: 'pebble.flush.duration.milliseconds',
    flush_input_bytes: 'pebble.flush.input.bytes',
    compaction_total: 'pebble.compaction.total',
    compaction_duration_milliseconds: 'pebble.compaction.duration.milliseconds',
    write_stall_total: 'pebble.write_stall.total',
    write_stall_duration_milliseconds: 'pebble.write_stall.duration.milliseconds',
    write_stall_active: 'pebble.write_stall.active',
    disk_slow_total: 'pebble.disk_slow.total',
    disk_slow_duration_milliseconds: 'pebble.disk_slow.duration.milliseconds',
    vfs_read_ops: 'pebble.vfs.read.ops',
    vfs_write_ops: 'pebble.vfs.write.ops',
    vfs_sync_ops: 'pebble.vfs.sync.ops',
  },

  // preload — internal/infra/state/machine.go
  preload:: {
    coverage_miss: 'ledger.preload.coverage_miss',
  },

  // raft — internal/infra/state/machine.go (FSM), node.go,
  // applier.go, transport.go (etcd-raft is a protocol library and
  // does not export OTel metrics; we instrument our own integration).
  raft:: {
    fsm_logs_appended: 'raft.fsm.logs_appended',
    fsm_rotation_duration: 'raft.fsm.rotation.duration',
    fsm_batch_commit_duration: 'raft.fsm.batch_commit.duration',
    fsm_prepare_duration: 'raft.fsm.prepare.duration',
    apply_entries_duration: 'raft.apply_entries.duration',
    apply_entries_batch_size: 'raft.apply_entries.batch_size',
    apply_entries_batch_size_distribution: 'raft.apply_entries.batch_size_distribution',
    applier_batch_wait_duration: 'raft.applier.batch_wait.duration',
    applier_commit_wait_duration: 'raft.applier.commit_wait.duration',
    append_entries: 'raft.append_entries',
    process_entry: 'raft.process_entry',
    read_index_duration: 'raft.read_index.duration',
    ready_committed_entries: 'raft.ready.committed_entries',
    node_lead: 'raft.node.lead',
    node_gating_wait_duration: 'raft.node.gating.wait_duration',
    node_gating_readies_processed: 'raft.node.gating.readies_processed',
    node_ready_wait_duration: 'raft.node.ready.wait_duration',
    node_ready_terminated_wait_duration: 'raft.node.ready_terminated.wait_duration',
    node_unspool_duration: 'raft.node.unspool.duration',
    node_maintenance_snapshot_creation_duration: 'raft.node.maintenance.snapshot_creation.duration',
    node_maintenance_replay_spool_duration: 'raft.node.maintenance.replay_spool.duration',
    send_pending_messages_full: 'raft.send.pending_messages.full',
    send_pending_messages_load: 'raft.send.pending_messages.load',
    transport_recv_load: 'raft.transport.recv.load',
    transport_recv_full: 'raft.transport.recv.full',
    transport_peer_sending_load: 'raft.transport.peer.sending.load',
    transport_peer_sending_full: 'raft.transport.peer.sending.full',
    transport_unreachable_load: 'raft.transport.unreachable.load',
    transport_unreachable_full: 'raft.transport.unreachable.full',
    transport_sending_pending_response: 'raft.transport.sending.pending_response',
    transport_ping_latency: 'raft.transport.ping.latency',
  },

  // readindex — internal/storage/readstore/metrics.go
  readindex:: {
    level_bytes: 'readindex.level.bytes',
    memtable_bytes: 'readindex.memtable.bytes',
    cache_hits: 'readindex.cache.hits',
    cache_misses: 'readindex.cache.misses',
  },

  // health — internal/infra/health/healthcheck.go
  health:: {
    disk_poll_failures: 'health.disk.poll.failures',
  },

  // storage (custom — disk usage) — internal/infra/monitoring/diskusage/diskusage.go
  storage:: {
    disk_volume_bytes: 'storage.disk.volume.bytes',
  },

  // wal — internal/storage/wal/wal_default.go
  wal:: {
    append_save_duration: 'wal.append.save.duration',
    append_batch_size: 'wal.append.batch_size',
  },
}
