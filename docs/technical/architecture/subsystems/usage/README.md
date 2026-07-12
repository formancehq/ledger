# Usage

The background worker (`internal/application/usagebuilder`) that turns committed audit entries into per-ledger housekeeping counters — postings, reverts, numscript executions, references, ephemeral evictions, transient uses, live volume cardinality — plus per-Numscript-template invocation records. Runs on every node — leader and followers — independently, mirroring the indexer's decoupling from the FSM hot path: the FSM commits and signals; the usagebuilder reads its own Pebble read handle and writes to a dedicated secondary store (`internal/storage/usagestore`).

The projections it maintains are exposed by the API — `GET /v3/{ledger}/stats` for the aggregate counters, `GET /v3/{ledger}/numscripts/{name}/usage` for per-template invocation counts.

## Documents

| Document | Description |
|----------|-------------|
| [usagebuilder.md](usagebuilder.md) | Pipeline mechanics: builder loop, audit-chain tailing, per-batch commit, log-payload consumption. |
| [counters.md](counters.md) | Counter definitions, storage keys, `LedgerLog.{purged,new_kept,ephemeral}_volumes` split, formula for each counter. |

## Related

- [FSM](../fsm/) — emits the `LedgerLog.new_kept_volumes` / `ephemeral_volumes` / `purged_volumes` annotations the usagebuilder consumes. Depends on the volume preload contract (see AGENTS.md invariant #6).
- [Indexer](../indexer/) — sibling subsystem with the same audit-tailing shape, but writes to a different secondary store (`readstore`).
- [Checker](../checker/) — verifies the projections against the audit chain (`compareExclusionProjections` for ephemeral/transient tuples).
- [Storage](../storage/) — the usage store is a peer Pebble instance to the readstore, WAL disabled, own comparer, own Pebble tuning.
- [Restore-generation watermark](../storage/restore-generation-watermark.md) — how the builder detects a primary-store rollback (the catch-up race) and rebuilds the counters.
