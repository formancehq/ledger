# Incremental Restore Parity

## Invariant

For every committed effect intended to survive a cross-cluster restore, applying
the complete history live must produce the same logical persisted state as
restoring a checkpoint of the history prefix and rebuilding the exported delta:

```text
live(prefix + delta) == restore(checkpoint(prefix), delta)
```

The comparison is logical, not necessarily byte-for-byte. Protobuf map encoding,
cluster-local state, caches, and other explicitly reset data may differ in bytes
or be absent after restore. Every difference requires a documented lifecycle
classification; it must not arise accidentally from an omitted replay effect.

This is a correctness invariant, not merely a backup test convention. The FSM
apply path and `RebuildDelta` are two folds of the same committed business
history. If one advances, deletes, or creates a durable projection and the other
does not, the restored store can contain business data that disagrees with the
state used to admit or apply the next order.

## Restore evidence and responsibilities

A full checkpoint carries the Pebble state at its log and audit sequence
boundaries. Incremental backup segments after that boundary carry raw log,
audit-entry, audit-item, and applied-proposal rows. `ApplyExports` restores those
rows, then `RebuildDelta` reconstructs the derived state that the live FSM wrote
after the checkpoint.

Before changing an audited order, an FSM handler, a persisted projection, or a
delete/purge cascade, classify every affected value:

| Classification | Required behavior |
|---|---|
| Preserved | The checkpoint value remains correct because the delta cannot change it. Document why. |
| Rebuilt | The delta can change the value. Identify the exported evidence and fold it into the restored projection. |
| Discarded | The value is cluster-local, cache-like, or otherwise invalid after cross-cluster restore. Delete it in the restore preparation lifecycle and document how it is re-seeded. |

"Present in the checkpoint" is not a valid explanation for a value that can
change in the delta. Rebuild requirements include deletes and resets, not only
creations and updates.

For a rebuilt value, answer all of the following before implementation:

1. Which live apply path mutates it?
2. Which exported record contains enough authoritative evidence to reproduce the mutation?
3. What is the fold rule: assignment, maximum, sum, set membership, tombstone, or another deterministic operation?
4. How is the checkpoint value used as the fold seed?
5. How do later delete, promote, purge, or recreate operations affect the result?
6. Which checker pass independently re-derives or validates the final value?

If the ledger-log payload omits information that the live apply used, inspect the
chain-bound serialized order in `AuditItem` and the `AppliedProposal` companion
record. Do not silently default a missing fact. Prefer a shared, pure decoder for
authoritative replay facts, while retaining a test oracle independent of the
restore writer so a shared mistake cannot make both sides agree incorrectly.

## Required tests

A change with restore impact is not complete with an isolated `RebuildDelta`
unit test alone. Cover the behavior at the lowest useful level and prove the
cross-lifecycle composition.

The restore regression must:

1. create meaningful state before the full checkpoint;
2. perform the affected successful operation after the checkpoint;
3. assert that the manifest contains a non-empty export covering that operation;
4. restore the checkpoint plus delta through `ApplyExportsAndRebuild` or the real restore service;
5. compare the restored logical projection with the live source state;
6. run `CheckStore` on the restored store and require no integrity findings;
7. exercise the next admission or apply decision that consumes the projection when practical.

Choose assertions that expose duplicate or forgotten effects. Counts alone are
often insufficient: a replay can overwrite a row with the same identifier while
applying postings or counters twice. Compare balances and volumes, boundary
fields, metadata values, idempotency outcomes, lifecycle modes, and rejection
reasons as applicable.

For field or order-kind changes, use table-driven coverage for every relevant
variant. A test covering only an uncommon variant does not establish the common
path. When practical, demonstrate that disabling the new rebuild fold makes the
regression fail; this falsifies a vacuous test that never reached the delta path.

## Review checklist

- The delta is non-empty and contains the intended operation.
- Live apply and restore use the same authoritative evidence but are compared by an independent oracle.
- Checkpoint seeding and delta folding are both exercised.
- Create, update, delete, and lifecycle transitions relevant to the projection are covered.
- Archived-baseline behavior is considered when the projection survives log purging.
- `CheckStore` verifies the restored primary-store projection, or the documented exemption remains valid.
- Tests assert business values and future gate behavior, not only row presence or cardinality.
- The subsystem documentation and code comments describe any new non-obvious restore rule.

## Code and test map

| Concern | Location |
|---|---|
| Export application and rebuild orchestration | `internal/infra/backup/restore.go` |
| Delta reconstruction | `internal/infra/backup/rebuild.go` |
| Shared audit/order replay facts | `internal/domain/replay/` |
| Live persisted mutations | `internal/domain/processing/`, `internal/infra/state/` |
| Integrity oracle | `internal/application/check/` |
| Rebuild unit tests | `internal/infra/backup/rebuild_test.go` |
| Cross-lifecycle restore tests | `tests/e2e/cluster/restore*_test.go` |
| Model-driven restore cycles | `tests/antithesis/run_model_test.sh --restore` |

Also read [Audit-Bound vs Technical State](../../audit-vs-technical-state.md)
before deciding whether a value is authoritative, checker-verified, rebuildable,
or deliberately excluded from cross-cluster restore.
