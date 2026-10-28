# logs_blocks primary key catch-up (migration 53)

Bucket migration `53-ensure-logs-blocks-composite-pkey` makes sure that the
`logs_blocks` primary key is `(ledger, previous)`. It changes only buckets where
`51-fix-logs-blocks-pkey-collision` was recorded as applied but never ran.

## Affected upgrade paths

Bucket migrations run by position. The migrator compares the number of known
migrations with the last applied version; it does not compare names.

| Position | v2.4.10 | v2.4.11 and later |
|---|---|---|
| 52 | `51-add-transactions-sources-array-index` | `51-add-transactions-sources-array-index` |
| 53 | `52-add-transactions-destinations-array-index` | `51-fix-logs-blocks-pkey-collision` |
| 54 | — | `52-add-transactions-destinations-array-index` |
| 55 | — | `53-ensure-logs-blocks-composite-pkey` |

A bucket migrated with v2.4.10 is at version 53. When v2.4.11 or later upgrades
it, the migrator runs position 54 only. That migration creates an index that
already exists, so it does nothing. The bucket records version 54, but its
primary key stays on `(previous)`.

| Upgrade path | Result of migration 53 |
|---|---|
| v2.4.10 → v2.4.11..v2.4.15 → this release | Rebuilds the primary key. |
| v2.4.10 → this release | Not needed: position 53 runs `51-fix-logs-blocks-pkey-collision`. Migration 53 finds the composite key and does nothing. |
| v2.4.9 or earlier → this release | Same as above. |
| v2.4.11 or later (from a fresh bucket) → this release | Does nothing. |
| New bucket | Does nothing. |

Impact before the fix: the async block worker (`AsyncBlockRunner`, ledgers with
`HASH_LOGS=ASYNC`) starts each ledger chain with `previous = 0`. With the
single-column key, the second ledger of a bucket fails on its first block.
`create_blocks` then returns an error, the worker logs `error running block
runner`, and that run stops for the remaining ledgers. No block row is written
for the failed ledger, so no data repair is needed. The worker retries on its
next schedule and continues from the existing chain head.

`WalkMigrations` breaks ties on the numeric prefix by directory name. This keeps
the released order of the two `51-*` migrations stable.
`TestWalkMigrationsKeepsReleasedOrder` pins positions 51 to 55.

## Classification and compatibility

- Class: **corrective**. It restores the schema that v2.4.11 already intended.
  It does not change persisted data.
- Destructive: no. It removes a constraint and adds a wider one in the same
  statement. The new key accepts every row that the old key accepted.
- Compatibility: all v2.4 binaries are compatible with the composite key. No
  Go code writes to `logs_blocks`. The only writer is the `create_blocks`
  procedure from migration 38, which uses a plain `INSERT` without
  `ON CONFLICT`. Old and new replicas can run during the rollout.
- Later contract release: none.

## Execution owner

The bucket migration runner executes it, the same way as every bucket
migration: `ledger migrate`, `ledger buckets upgrade`, or
`ledger serve --auto-upgrade`. The runner serializes execution per bucket with a
Postgres advisory lock on the bucket's `goose_db_version` table. A duplicate run
waits on that lock and then does nothing.

The advisory lock wait itself has no timeout. This is a limitation of the
existing runner; this migration does not change it.

## Locking and availability

The rebuild takes an `ACCESS EXCLUSIVE` lock on `logs_blocks` and builds a
unique index on it. The table has one row per block, so the build is short.
Ledger API requests do not read or write `logs_blocks`; only the async block
worker waits.

The migration bounds its lock wait. It keeps the session `lock_timeout` when
one is set, and uses 30 seconds otherwise (`set local`, so the setting ends
with the migration transaction).

## Failure, retry and rollback

The migration is one `DO` statement, so it is atomic: the bucket keeps either
the old or the new primary key.

- Lock timeout (`canceling statement due to lock timeout`): nothing has
  changed. Retry the upgrade. If the worker keeps the lock, retry when it is
  idle, or stop the worker during the upgrade.
- `column "ledger" of relation "logs_blocks" contains null values`: a block row
  has no ledger. `create_blocks` always sets it, so this means manual changes.
  Escalate; do not delete rows without review.
- Rollback: no database rollback is needed. A previous v2.4 binary works with
  the composite key. Do not restore the single-column key: it brings back the
  collision.

## Success criteria

The bucket is at version 55 or later, and the `logs_blocks` primary key is
`(ledger, previous)`. Version 55 means this migration has run; a full upgrade
of this release line ends at version 56, after
[`54-fix-log-hash-lookup`](log-hash-lookup.md):

```sql
select a.attname
from pg_constraint c
cross join lateral unnest(c.conkey) with ordinality as k(attnum, ord)
join pg_attribute a on a.attrelid = c.conrelid and a.attnum = k.attnum
where c.conrelid = '"<bucket>".logs_blocks'::regclass and c.contype = 'p'
order by k.ord;
```

## Validation

- `TestMigrationEnsureLogsBlocksCompositePrimaryKey` covers the skipped and the
  applied fix. In the skipped case, two ledgers can insert `previous = 0` after
  the migration.
- `TestMigrationEnsureLogsBlocksCompositePrimaryKeyLockTimeout` holds a lock on
  `logs_blocks`. It verifies that the migration fails on the lock timeout
  without changes, and that a retry succeeds.
- `TestWalkMigrationsKeepsReleasedOrder` pins the migration order.
- Manual upgrade check with the migrator of the v2.4.10 and v2.4.11 tags on
  Postgres 16: v2.4.10 → v2.4.11 leaves version 54 with `[previous]`; this
  release then sets `[ledger previous]` at version 55. The direct v2.4.10, the
  v2.4.11 and the fresh paths end at version 55 with `[ledger previous]`.
- Not measured: lock duration on production-size buckets.

Run the storage integration tests with:

```sh
go test -count=1 -tags it -run 'TestMigration|TestWalkMigrations' ./internal/storage/bucket
```

## Release note

> Migration 53 (`53-ensure-logs-blocks-composite-pkey`, corrective) repairs
> buckets first migrated with v2.4.10 and then upgraded to v2.4.11–v2.4.15.
> On those buckets, the `logs_blocks` primary key stayed on `previous`, and the
> async block worker failed for the second ledger of a bucket with
> `HASH_LOGS=ASYNC`. The migration runs automatically with the usual bucket
> upgrade. It does nothing on other buckets. It briefly locks `logs_blocks`;
> on a lock timeout, retry the upgrade. No operator action or database rollback
> is needed.
