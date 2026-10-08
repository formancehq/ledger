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
    propose_queue_overflows: 'admission.propose_queue.overflows',
    propose_duration: 'admission.propose.duration',
    fsm_future_wait_duration: 'admission.fsm_future.wait.duration',
    proposal_guard_duration: 'admission.proposal_guard.duration',
    proposal_guard_rebuilds: 'admission.proposal_guard.rebuilds',
    preload_duration: 'admission.preload.duration',
    preloads: 'admission.preloads',
    preload_keys_needed: 'admission.preload.keys_needed',
    preload_cache_hits: 'admission.preload.cache_hits',
    audit_missing_callers: 'admission.audit.missing_callers',
    audit_empty_caller_subjects: 'admission.audit.empty_caller_subjects',
    resolve_batch_duration: 'admission.resolve_batch.duration',
    orders_preparation_duration: 'admission.orders_preparation.duration',
    scripts_duration: 'admission.scripts.duration',
    response_resolution_duration: 'admission.response_resolution.duration',
    actions: 'admission.actions',
    action_errors: 'admission.action.errors',
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
    logs_indexed: 'index.builder.logs_indexed',
  },

  // audit.indexer — internal/application/auditindexer/indexer.go
  audit_indexer:: {
    last_indexed_sequence: 'audit.indexer.last_indexed_sequence',
    audit_last_sequence: 'audit.indexer.audit_last_sequence',
    lag: 'audit.indexer.lag',
  },

  // usage.builder — internal/application/usagebuilder/builder.go
  usage_builder:: {
    last_indexed_sequence: 'usage.builder.last_indexed_sequence',
    audit_last_sequence: 'usage.builder.audit_last_sequence',
    lag: 'usage.builder.lag',
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
    logs_ingested: 'mirror.logs_ingested',
    batches: 'mirror.batches',
  },

  // pebble — internal/storage/dal/metrics.go (our wrapper around
  // Pebble's EventListener; Pebble itself does not expose OTel).
  pebble:: {
    flushes: 'pebble.flushes',
    flush_duration: 'pebble.flush.duration',
    flush_input_size: 'pebble.flush.input.size',
    compactions: 'pebble.compactions',
    compaction_duration: 'pebble.compaction.duration',
    write_stalls: 'pebble.write_stalls',
    write_stall_duration: 'pebble.write_stall.duration',
    write_stall_active: 'pebble.write_stall.active',
    disk_slow_operations: 'pebble.disk_slow.operations',
    disk_slow_duration: 'pebble.disk_slow.duration',
    vfs_read_ops: 'pebble.vfs.read.ops',
    vfs_write_ops: 'pebble.vfs.write.ops',
    vfs_sync_ops: 'pebble.vfs.sync.ops',
  },

  // preload — internal/infra/state/machine.go
  preload:: {
    coverage_misses: 'raft.fsm.preload.coverage_misses',
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
    entries_applied: 'raft.entries_applied',
    apply_entries_batch_size: 'raft.apply_entries.batch_size',
    applier_batch_wait_duration: 'raft.applier.batch_wait.duration',
    applier_commit_wait_duration: 'raft.applier.commit_wait.duration',
    append_entries_duration: 'raft.append_entries.duration',
    process_entry_duration: 'raft.process_entry.duration',
    read_index_duration: 'raft.read_index.duration',
    ready_committed_entries: 'raft.ready.committed_entries',
    node_lead: 'raft.node.lead',
    node_gating_wait_duration: 'raft.node.gating.wait.duration',
    node_gating_readies_processed: 'raft.node.gating.readies_processed',
    node_ready_wait_duration: 'raft.node.ready.wait.duration',
    node_ready_terminated_wait_duration: 'raft.node.ready_terminated.wait.duration',
    node_unspool_duration: 'raft.node.unspool.duration',
    node_maintenance_snapshot_creation_duration: 'raft.node.maintenance.snapshot_creation.duration',
    node_maintenance_replay_spool_duration: 'raft.node.maintenance.replay_spool.duration',
    send_pending_messages_overflows: 'raft.send.pending_messages.overflows',
    send_pending_messages_load: 'raft.send.pending_messages.load',
    transport_recv_load: 'raft.transport.recv.load',
    transport_recv_overflows: 'raft.transport.recv.overflows',
    transport_peer_sending_load: 'raft.transport.peer.sending.load',
    transport_peer_sending_overflows: 'raft.transport.peer.sending.overflows',
    transport_unreachable_load: 'raft.transport.unreachable.load',
    transport_unreachable_overflows: 'raft.transport.unreachable.overflows',
    transport_sending_pending_response_count: 'raft.transport.sending.pending_response.count',
    transport_ping_duration: 'raft.transport.ping.duration',
  },

  // readindex — internal/storage/readstore/metrics.go
  readindex:: {
    level_size: 'readindex.level.size',
    memtable_size: 'readindex.memtable.size',
    cache_hits: 'readindex.cache.hits',
    cache_misses: 'readindex.cache.misses',
  },

  // usagestore — internal/storage/usagestore/metrics.go
  usagestore:: {
    level_size: 'usagestore.level.size',
    memtable_size: 'usagestore.memtable.size',
    cache_hits: 'usagestore.cache.hits',
    cache_misses: 'usagestore.cache.misses',
  },

  // health — internal/infra/health/healthcheck.go
  health:: {
    disk_poll_failures: 'health.disk.poll.failures',
  },

  // storage (custom — disk usage) — internal/infra/monitoring/diskusage/diskusage.go
  storage:: {
    disk_volume_usage: 'storage.disk.volume.usage',
  },

  // wal — internal/storage/wal/wal_default.go
  wal:: {
    append_save_duration: 'wal.append.save.duration',
    append_batch_size: 'wal.append.batch_size',
  },
}
