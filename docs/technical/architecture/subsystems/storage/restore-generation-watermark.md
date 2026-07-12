# Restore-generation watermark

How the projection tail-workers (`usagebuilder`, `auditindexer`, `indexbuilder`)
detect a primary-store rollback they would otherwise silently skip.

## The problem: the catch-up race

Each projection tail-worker advances a **persisted cursor** over the primary
store's log/audit stream: on every tick it reads entries *since* its cursor,
projects them, and persists the new cursor. The cursor lives in a rebuildable
side store (the `usagestore` for `usagebuilder`; the read store for
`auditindexer` and `indexbuilder`) — never in the main store it is tailing.

A follower that falls behind is repaired by `Store.RestoreCheckpoint`
(`internal/storage/dal/store.go:1211`), reached at runtime through
`Synchronizer` → `SynchronizeWithLeader`. It hard-links a leader checkpoint into
the live directory and republishes it by rename, which **rolls the primary
store's head backwards** — potentially *below* a projection cursor that had
already advanced past that point.

The obvious guard is a point-in-time comparison, `cursor > head`
(`cursorAheadOfHead`): if the cursor sits ahead of the restored head, reset and
rebuild. That guard is **not sufficient on its own**, because it is momentary.
Consider:

```
cursor = 100, head = 100
  └─ RestoreCheckpoint rolls head back to 60      (cursor now ahead: 100 > 60)
  └─ new writes arrive, head re-grows to 100+      (cursor no longer ahead)
  └─ worker samples head only now → sees head ≥ cursor → guard does NOT fire
  └─ worker resumes from cursor 100, having never projected the *restored*
     entries 61..100 → the projection is permanently wrong for that range
```

The rollback happened, but by the time the worker looked, the head had caught
back up and erased the `cursor > head` signature. A gap-size threshold
(`gapExceedsThreshold` / `RebuildThreshold`) narrows the window but cannot close
it — a rollback smaller than the threshold, or any rollback fully masked by
catch-up, still slips through.

## The mechanism: a monotonic restore counter

`dal.Store` carries a **process-local monotonic counter**,
`restoreGeneration atomic.Uint64` (`internal/storage/dal/store.go:125`).
`RestoreCheckpoint` bumps it exactly once, on success, after the new checkpoint
is published (`store.go:1421`). Readers observe it through
`Store.RestoreGeneration()` (`store.go:1436`).

The counter is a **fingerprint of "how many times this store was rolled back",
independent of position**. A worker records the generation it is running under
and re-reads it every tick; if the value ever differs from what it recorded, a
restore happened since it last projected — regardless of where the head landed
relative to the cursor — so it resets and rebuilds.

Every tail-worker uses the identical shape:

1. **Seed at boot** — snapshot `RestoreGeneration()` into a local
   `restoreGen atomic.Uint64` *before* reading the cursor, so a restore that
   races boot is re-detected on the first tick rather than swallowed.
2. **Gate every tick** — `if store.RestoreGeneration() != restoreGen.Load()`
   → reset the projection, re-sync `restoreGen` inside the reset (so a restore
   *during* the rebuild is caught again), and rebuild from scratch.

| Worker | field | boot seed | tick gate | reset action |
|--------|-------|-----------|-----------|--------------|
| `usagebuilder` | `builder.go:87` | `builder.go:178` | `builder.go:268` | `resetProjection()` — wipe + replay the `usagestore` counters |
| `auditindexer` | `indexer.go:71` | `indexer.go:147` | `processTick` (`indexer.go:341`) | rebuild the audit index in the read store |
| `indexbuilder` | `builder.go:66` | `builder.go:624` | `maybeResetOnRestore` (`builder.go:569`) | `resetAndReinit()` — see below |

## Two layers of defence

The watermark does not replace the position heuristic; the two cover disjoint
cases.

- **Runtime catch-up race → the generation watermark.** `RestoreCheckpoint`
  only runs while the process is live (via `SynchronizeWithLeader`), and
  stopping background tasks does **not** stop these pollers, so the bump is
  always observed on the next tick. This is the case the point-in-time guard
  cannot see.
- **Process-restart-after-offline-restore → the boot position heuristic.**
  `restoreGeneration` is process-local; a restart resets it to `0` on both the
  store and every worker, so the generation gate is inert across a restart. That
  case is instead caught at boot by `shouldRebuildOnBoot` = `cursorAheadOfHead`
  **OR** a boot-only gap-threshold check
  (`usagebuilder` `gapExceedsThreshold` / `auditindexer` &
  `indexbuilder` `RebuildThreshold`). This is exactly the offline
  backup/restore scenario that heuristic was built for, and it is now applied
  uniformly across all three workers.

An in-memory counter is therefore *correct*, not a shortcut: the only rollback a
restart can hide is an offline one, which the boot heuristic already owns.

## Per-worker reset cost

The gate is identical; the reset behind it is not.

- `usagebuilder` / `auditindexer` reset is close to "rewind the cursor to 0 and
  replay" over their rebuildable side store.
- `indexbuilder` is heavier: a read-index rebuild must also re-derive backfill
  tasks, schema-rewrite progress, and index-version state, so its tick-time
  reset routes through a **boot-equivalent re-init** (`resetAndReinit`,
  `builder.go:672`) rather than a bare cursor rewind. `readstore.ResetIndexes`
  wipes only the builder-owned keyspace (index rows, log/applied-proposal
  cursors, backfill cursors, version state) and then re-runs `initIndexConfig`
  against the emptied store. It deliberately **does not** touch the
  `auditindexer`-owned audit index — that worker owns its own detector and its
  own reset — a boundary pinned by a dedicated test.

## Invariants

This lives entirely on the projection/side-store path: no FSM hot-path Pebble
reads (invariant #3), deterministic per-replica (the counter is node-local and
touches no Raft state, so it cannot desync the FSM), and reset/rebuild is the
rebuildable-projection recovery path — the audit hash chain, the sole source of
truth (invariant #8), is never involved. A corrupted or stale projection is an
availability/eventual-consistency concern with an operator escape hatch
(`ledgerctl store rebuild-usage`, `ledgerctl store rebuild-indexes`), not a
tampering vector.

## Related

- [storage.md](storage.md) — `RestoreCheckpoint`, checkpoints, and follower sync.
- [Usage](../usage/) — `usagebuilder` and the `usagestore` projection.
- [Indexer](../indexer/) — `indexbuilder`, `auditindexer`, and the read store.
