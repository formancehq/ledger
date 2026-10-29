# Technical-update composition

Operational updates (event-sink progress, configuration, idempotency eviction,
audit-key initialization, and backup lifecycle) write directly to the shared
apply batch and can change in-memory operational state before durable commit.
They are submitted separately from business orders. MirrorSync is different:
it queues source-head/status changes in the business WriteSet, so rejected
mirror ingestion must not publish that progress.

## Submission boundary

`plan.Builder.Run`, shared by all technical-update producers, rejects proposals
containing more than one technical update. A single update may accompany orders
only when it is MirrorSync. Proposals containing only orders, no operations,
or one standalone technical update retain their existing behavior. Rejection
returns `ErrUnsupportedTechnicalComposition` before serialization, proposal-lock
acquisition, or Raft submission and releases the build's preload loaders.
This is an internal constructor error, not a new public API rejection.

The guard prevents a direct-writing update from leaving effects in the shared
batch when a later update or order soft-rejects. Production constructors already
use these shapes; the guard locks down that boundary instead of introducing
whole-proposal rollback for arbitrary protobuf-valid compositions.

## Apply, durability, and compatibility

The committed-entry interpreter is unchanged. The guard runs only at submission;
it must not be added to replay or used to reinterpret historical entries.
Existing business data, audit bytes, operational state, and supported mixed-version
apply semantics remain unchanged. There is no new Raft field, format migration,
service protocol revision, or staged interpreter activation requirement.

MirrorSync reads through its declared coverage and queues writes in the existing
WriteSet. Order processing and proposal-wide validation run before Merge. A
rejection discards the queued mirror progress together with business writes;
the normal failure-audit contract still applies. A successful Merge mutates the
cache and stages Pebble writes; the existing committer makes the batch durable.
Merge/storage failures remain fatal, with no general in-memory rollback after
publication starts.

The guard uses constant time and space, independent of order count, cache size,
and backup-job count; it adds no store reads, scans, cloning, locks or wire bytes.
Existing apply and restore costs are unchanged.

## Regression evidence

Runner tests reject direct updates paired with orders or another update before
marshalling on both preload paths, verify loader release, and preserve standalone
operational updates and MirrorSync with or without orders. The FSM regression
pairs a real mirror-ingest order with MirrorSync: successful ingestion advances
both, while a prior ledger promotion rejects the in-flight ingest without
advancing the source head or ingestion position, including after recovery.
