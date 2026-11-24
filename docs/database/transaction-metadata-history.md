# Transaction metadata history

With `TRANSACTION_METADATA_HISTORY=SYNC`, transaction creation records the initial
metadata and subsequent metadata changes append a revision. Reverting a transaction
without changing its metadata must not append a revision.

New ledgers install an `AFTER UPDATE OF metadata` trigger. Existing ledgers may
still have broad `AFTER UPDATE` triggers. The compatible correction
`0-transaction-metadata-history.sql` replaces the shared
`update_transaction_metadata_history()` function with a version that returns
immediately when `OLD.metadata` and `NEW.metadata` are identical. It preserves
real metadata changes and deletions. Existing revisions, including duplicates,
are retained; this change does not clean historical data.

## Migration and compatibility

Class: **corrective**, with no contract or data cleanup phase. The existing bucket
upgrade path owns both migration streams. It first completes the main migrations,
then applies compatible corrections through the same go-libs migrator using the
bucket-local `compatible_migrations` table. That table records version 1 for this
correction. Main bucket version `goose_db_version` remains 56; the released main
migration files and order are unchanged.

The separate registry is restricted to corrections that preserve the SQL and data
contract of every supported binary at the current main version. Incompatible
changes must use the main migration stream. Applied corrections are immutable.
Older binaries ignore the additive registry, accept main version 56 and keep
using the same shared function. `detectDowngrades` and the ledger-creation version
check remain strict and unchanged. No compatibility exception is required for
this correction against the preceding version-56 binary; this does not claim
compatibility for earlier binaries that reject version 56 itself.

Both fresh bucket creation and the usual bucket upgrade commands apply the
correction. Run `ledger buckets upgrade '*'`, `ledger migrate`, or
`ledger serve --auto-upgrade` even when the main migration status says current:
that status describes only the main stream. The compatible runner serializes
repairs by its own qualified versions table. The correction and its completion
record commit in one transaction (a savepoint during ledger creation). On error,
both roll back; retry the usual upgrade command. The correction replaces one
function per bucket without a scan or backfill. The SQL statement is
atomic and safe to reapply. Compatible corrections have a 30-second deadline,
including transaction-lock and catalog-lock waits, and honor shorter caller
deadlines. Transaction locks are released on commit or rollback. The principal
migrator retains its existing timeout behavior; configure session timeouts for
that stream as needed. Future principal migrations that replace the shared
function must retain this guard or append a new compatible correction: completed
corrections are not replayed on later upgrades. A future correction is eligible
only if older writers, readers, provisioning, restart and application rollback
remain valid; it must not remove objects, change stored meaning or require an
atomic replica switch. Review and test those properties before appending it.

## Rolling deployment and rollback

1. The operator runs the bucket upgrade with the corrected binary. Existing
   version-56 replicas may keep running and restart against repaired buckets.
2. The operator rolls out the corrected binary. An older replica can still
   create a ledger in a repaired bucket: its broad trigger invokes the corrected
   shared function. A new bucket created entirely by an older replica retains
   the original bug until it receives the correction.
3. The operator reruns the bucket upgrade after the last older replica stops,
   covering buckets created during the rollout. Verify version 1 is applied in
   each bucket's `compatible_migrations` table.

An application-only rollback to the preceding version-56 binary remains possible.
Leave the compatible registry and corrected function in place; no data restore
or history rewrite is required. Older code may create new affected buckets after
rollback, so the final upgrade scan is still needed when rolling forward again.
Never lower the main schema version. A general backup/restore policy still
applies independently of this non-destructive correction.

## Validation

The driver tests cover first and second ledgers, legacy broad triggers at version
56, historical-row preservation, real metadata updates and deletions, duplicate
and concurrent execution with both waiters observed in PostgreSQL, and a
rolled-back attempt followed by retry. Lock tests exercise both the internal
30-second cap without a caller deadline and a shorter caller deadline. The
compatibility fixture uses the unchanged version-56 main registry and version
checks, then reinstalls the old broad trigger to test old ledger-creation
behavior. An unknown main version is still rejected.

A separate PostgreSQL 16 binary check compiled the preceding release-branch
commit `148a77477a5f041fabce85d99238d5bd73fcab22`. That binary reproduced the
original duplicate, then restarted against the corrected database and created a
second ledger through its HTTP API. Reverts added no duplicate; real metadata
updates and deletions remained historized. Running the old and new bucket upgrade
commands afterwards preserved the corrected function definition and historical
rows; another old-binary restart and ledger creation succeeded. This exercises
the actual preceding `CreateLedger` and `detectDowngrades`, beyond the fixture's
unchanged version count. The full suite covers normal log restoration. This is
local test evidence, not a production deployment, a production restore exercise
or a production-scale lock measurement.
