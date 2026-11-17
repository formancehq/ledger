# Canonical derived volumes on log import (SEC-90)

`POST /v2/{ledger}/logs/import` persists each log payload into `logs.data` verbatim through
`Store.InsertLog`. Before this change, an imported `NEW_TRANSACTION` or
`REVERTED_TRANSACTION` log could carry `postCommitVolumes` /
`postCommitEffectiveVolumes` supplied by the export, including stale or
repaired values from historical ledgers (see bucket migrations
`19-transactions-fill-pcv` and `28-fix-pcv-missing-asset`), and
account/asset entries that no posting of the transaction touches. That broke
audit consistency between `logs.data` and the `transactions`/`accounts_volumes`
tables, which always hold recomputed values.

## Restore contract

`DefaultController.importLog` now canonicalizes the derived volume fields
before `InsertLog`:

- `Store.CommitTransaction` recomputes `PostCommitVolumes` in place from the
  postings (cumulative per account/asset, see `Store.UpdateVolumes`), and with
  both `MOVES_HISTORY=ON` and
  `MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES=SYNC` also recomputes
  `PostCommitEffectiveVolumes` from the inserted moves. The persisted log
  payload carries exactly those recomputed values for the transaction(s) of the
  log.
- For `REVERTED_TRANSACTION`, the reverted original transaction is taken from
  the row returned by `Store.RevertTransaction` (already marked reverted, with
  its persisted `postCommitVolumes`), not from the supplied payload.
- Account/asset entries present only in the supplied derived volumes (accounts
  or assets not touched by the transaction's postings) are dropped.
- When `MOVES_HISTORY` is not `ON`, or
  `MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES` is not `SYNC`, the supplied `postCommitEffectiveVolumes` are cleared: the field is scan-only on
  transactions and must never be persisted from an export.
- Valid exports whose supplied volumes already match the recomputation import
  unchanged; historical exports with missing volume fields get the canonical
  values.

## Hash and memento

Unchanged. `Log.ComputeHash` and `GetMemento` exclude the volume fields, so the
log hash chain and the `memento` column are byte-identical before and after
this change for the same postings. With `HASH_LOGS=SYNC`, import still compares
the supplied hash to the post-insert value and rejects mismatches.

## Scope

Only the import path (`importLog`) is affected. Live creation/revert paths
already persist canonical volumes. Admission and posting invariants are out of
scope here (SEC-79).
