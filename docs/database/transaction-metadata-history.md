# Transaction metadata history

With `TRANSACTION_METADATA_HISTORY=SYNC`, transaction creation records the initial
metadata and subsequent metadata changes append a revision. Reverting a transaction
without changing its metadata must not append a revision.

New ledgers install an `AFTER UPDATE OF metadata` trigger. Migration
`55-fix-transaction-metadata-history` also protects existing ledgers by replacing
the shared `update_transaction_metadata_history()` function with a version that
returns immediately when `OLD.metadata` and `NEW.metadata` are identical. Existing
triggers remain valid, including the older `AFTER UPDATE` triggers. Existing
revisions, including duplicates, are retained.

## Migration and compatibility

This is a corrective migration owned by the existing bucket migrator. Apply it
through the usual bucket upgrade command before resuming writes. It replaces one
function per bucket, performs no data scan or backfill, and can be reapplied safely.
Function DDL can wait for concurrent database activity; the existing migrator's
locking and timeout behavior still applies. This change does not add new timeout
or interruption controls.

The corrected function keeps the previous SQL contract, including for broad
triggers. However, the previous binary rejects a bucket schema version newer than
its known migrations. An application-only rollback to that binary is therefore
unsupported after this migration. Older running processes also reject ledger
creation in an upgraded bucket. Stop old writers before upgrading and use the
new binary for subsequent operations; a rolling deployment across this schema
boundary is not supported by the existing version checks.

Before deployment, retain a database recovery point and validate its restore
procedure. After a successful migration, prefer a forward repair. Returning to the
previous binary requires restoring the previous database state and reconciling
any writes since that recovery point. Do not lower the migration version manually.
If the migration fails, retry the bucket upgrade. Do not remove historical
revisions as a recovery step. No deployment or production restore was performed
as part of the local verification.

The integration tests cover the first ledger in a new bucket and a second ledger
in an existing bucket. The migration fixtures reproduce an old broad trigger and
verify that the upgrade preserves historical rows, prevents further duplicates,
and still records real metadata changes.
