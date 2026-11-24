# Persistence, checkpoint restore, and replay evidence contract

This is the companion to
[`persistence-restore-replay.json`](./persistence-restore-replay.json).
The audit covers primary-store durability, backup publication, cross-cluster
checkpoint restore, and committed-history replay. It does not turn an audit run
into permission to modify code, tickets, or GitHub state.

## Evidence boundary

A successful backup captures a flushed Pebble checkpoint. The manifest points to
content-addressed objects for **all** files of that checkpoint, including Pebble
WAL files if present. The manifest is published after required uploads and
before stale-object pruning. A failed upload or manifest write must leave the
previous published backup restorable. A failed prune may leave harmless orphans.

For restore parity, take a first checkpoint, commit a distinctive transaction
or other persisted mutation, then take a second checkpoint. Restore the second
checkpoint through a production entry point, compare exact business values,
audit and boundary metadata, and run `CheckStore`. A third checkpoint before
restore invalidates this evidence. A no-op repeated backup need not upload zero
files because backup admission itself changes persisted job state; object reuse
should be assessed through stable content hashes and manifest references.

`AccountMetadataUpdated` is a committed business effect even when its Numscript
execution creates no transaction. After a first checkpoint, apply a metadata-only
script and take a second checkpoint. Restore that checkpoint and compare account
metadata and transaction sets directly: the metadata must change without an
invented transaction, reference, or next-ID advance. Run `CheckStore` and a
subsequent admitted scripted operation. `TestBackup_NumscriptRestoreParity`
exercises this composition.

Both gRPC restore and offline bootstrap reject old manifests containing exports
or a separate cluster-policy override. Staging reset, path containment, file
size/hash validation, validation before activation, and failure residue remain
part of the restore contract. The legacy export application code is removed;
the Raft FSM still applies committed legacy backup orders during a rolling
upgrade, but new backup and restore entry points do not use the export format.
The rejection is the maintainer-approved PR #2600 RC artifact exception, not
evidence that an old incremental backup remains recoverable after source loss.
The audit must still prove current checkpoint restore parity and that both
entry points fail closed on the unsupported manifest.

## State classification

Business and audit state that survives cross-cluster restore must reside in the
checkpoint at its capture point. Cluster-local identity and caches may be
discarded by `PrepareForBackup` and re-seeded at the destination. The audit
secret remains with the chain. Source Raft applied indexes in persisted records
are provenance, not certificates in the new cluster. The checker independently
verifies primary-store projections, subject to documented exemptions.

A change to an audited order, persistence path, projection, or deletion cascade
must show which persisted values it writes and how a second checkpoint carries
them. Compare source and restored values after non-empty post-first-checkpoint
work, including later update/delete/recreate paths as relevant. Check future
admission or query behavior when a wrong restored value could remain latent.

## Dynamic checks and ownership

The manifest lists required adversarial questions and dynamic checks. Prefer
fault injection at upload, manifest write, prune, download, validation, and
activation boundaries. Inspect the published manifest and residual staging state
after each failure. Mutate isolated manifests and files, never a live store.

This domain owns failures in durable persistence, checkpoint publication, and
checkpoint restore. The checker domain owns whether its verification algorithm
correctly detects an inconsistent row. General idempotency and transport retry
semantics belong to their own domains; a retry that corrupts a backup or restore
belongs here.
