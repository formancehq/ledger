# Synchronous log hash lookup (migration 54)

Bucket migration `54-fix-log-hash-lookup` replaces `set_log_hash()` to avoid
scanning other ledgers' logs when looking up the previous hash. It applies to
ledgers using `HASH_LOGS=SYNC` in a shared bucket.

## Cause and correction

Each ledger has its own log ID sequence, but ledgers in a bucket share the
`logs` table. For a common ledger whose highest ID is below another ledger's,
PostgreSQL can choose an ID-only backward scan on `logs_ids`, expecting to
find a matching ledger quickly. Instead, it filters potentially hundreds of
thousands of unrelated rows while the writer holds its ledger advisory lock.
This can happen in custom plans on fresh connections; generic plans can also
choose the ID-only index, depending on statistics and data distribution.

The migration changes only the previous-hash lookup:

```sql
select hash into previousHash
from logs
where ledger >= new.ledger and ledger <= new.ledger
order by ledger desc, id desc
limit 1;
```

Equal range bounds select the same ledger as equality, but preserve `ledger`
in the sort keys. The existing composite `(ledger, id)` index, attached to
`logs_pkey` by migration 45, can supply the full order directly. Adding
`ledger` to `ORDER BY` while keeping the equality is insufficient: PostgreSQL
can remove the constant sort key and still choose `logs_ids`.

The hash algorithm, trigger bindings, locking and log data are unchanged.
An empty ledger still starts its hash chain with a null previous hash. There
is no new index, index rebuild or backfill. The planner remains cost-based;
validate the plan on supported PostgreSQL versions rather than treating this
query as an index hint.

## Deployment and compatibility

- Class: **corrective**; non-destructive, with no later contract phase.
- Owner: the existing bucket migration runner, through `ledger migrate`,
  `ledger buckets upgrade`, or `ledger serve --auto-upgrade`.
- Migration order: appended after `53-ensure-logs-blocks-composite-pkey`, at
  migration position 56. Both existing and new buckets receive the function.
- Compatibility: replicas already running a previous v2.4 binary keep working
  with the new function; they use the same hash representation and ledger
  locking protocol. A previous binary cannot start once the bucket is at
  position 56: driver initialization rejects a bucket version above its own
  migration count as a downgrade. Restart or scale all replicas on the new
  binary only, or drain older replicas before the upgrade.
- Expected work: replace one function definition per bucket. There is no
  table scan or explicit lock on the `logs` table in this migration. Catalog
  locking and migration-runner coordination can still cause waits; duration
  under production load has not been measured.
- Execution bounds: this migration adds no timeout. The runner's existing
  advisory-lock wait is unbounded; configure session timeouts as needed.
- Failure: replacement is a single atomic statement. If it fails, the old
  function remains. Retry through the normal bucket upgrade command.
- Duplicate execution: replacing the function again has the same result;
  the runner serializes and records migrations per bucket.
- Rollback: no data rollback is required, but downgrading the binary is not
  possible once the bucket is at position 56, for the reason above. Restoring
  the original function body reintroduces the slow lookup and should only be
  considered for a confirmed regression.

The replacement preserves `SECURITY DEFINER` and explicitly sets the function's
`search_path` to its bucket. Historical migration files remain unchanged.

## Validation

`TestMigrationLogHashLookup` upgrades a bucket from the preceding migration
with existing logs and an existing hash trigger. It compares hashes from the
original and corrected trigger for interleaved ledgers, empty ledgers, ID gaps,
large numeric IDs, case-sensitive names and the maximum ledger-name length.

Using `auto_explain` on nested statements in fresh sessions, it first checks
that the original lookup filters unrelated ledgers on the test data, so the
dataset reproduces the defect. It then verifies that the corrected lookup uses
an index condition without filtering unrelated ledgers, in both custom and
generic plans. It does not assert wall-clock
timings or planner costs. The same test checks function security and bucket
scope; the migration suite covers fresh bucket creation, and the migration
order test pins the appended position.

```sh
nix develop --impure --command go test -race -tags it -count=1 \
  -run 'TestMigration|TestWalkMigrations' ./internal/storage/bucket
```

Production-scale timings, concurrent API writes during migration and additional
PostgreSQL versions require validation before release.

## Release note

> Migration 54 fixes slow synchronous log hashing in buckets with multiple
> ledgers and uneven log ID ranges. It replaces the previous-hash lookup to
> use the existing composite index efficiently, without changing hash chains
> or rebuilding indexes. It runs during the normal bucket upgrade. Once a
> bucket is upgraded, previous binaries refuse to start on it, so roll out the
> new binary to every replica. No data repair is needed.
