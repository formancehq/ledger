# Migration temporary table cleanup

Bucket migrations 11, 17, 18 and 20 use temporary tables named `tmp_volumes`,
`transactions_ids` and `logs_transactions`. Their cleanup must target `pg_temp`
explicitly so that a persistent table with the same name is never dropped.

The pre-creation cleanup removes scratch tables left on a reused database session.
The final cleanup runs after the backfill loop. Do not replace it with
`ON COMMIT DROP`: these backfills commit between batches and still need the table
for subsequent batches.

## Compatibility and execution

This is a corrective change to session state. It adds no persistent schema,
data format or migration version. The existing bucket migration runner owns its
execution during provisioning and upgrades. Already-applied migrations are not
replayed; pending migrations use the corrected cleanup.

The historical scripts need this correction because a new, later migration
cannot run if provisioning already fails in migration 17. This rationale must be
included when reviewing the exception to migration immutability.

Old and new binaries use the same persistent schema and migration version records.
No database rollback is needed when reverting the binary, but reverting this fix
restores the unsafe cleanup behavior; reverting the original reuse fix also
restores the temporary-table collision.

## Regression coverage

`TestMigrationsPreservePermanentTables` runs each affected migration with a
persistent namesake containing a sentinel row, with and without a leftover
temporary table. It verifies that the persistent content survives and that the
scratch table is removed. `TestCreateLedgerWithReusedConnection` verifies that two
new buckets can be created sequentially on one retained connection.

Run both storage integration suites, including these tests, with:

```sh
go test -race -count=1 -tags it ./internal/storage/driver ./internal/storage/bucket
```
