# EN-1771 — durable ledger history classification

## Plan

- [x] Read repository instructions and the relevant indexbuilder, readstore, protobuf, backup, checker, and testing documentation.
- [x] Read EN-1564, EN-1771, and EN-1774 and inspect recent branches/PRs for overlapping work.
- [x] Refresh GitNexus and inspect the impact of the live-log, boot, backfill, and index-version paths.
- [x] Add a deterministic pre-fix regression and performance observation proving the unnecessary global-log scan.
- [x] Add an exhaustive protobuf-owned HISTORY/CONTROL classifier and remove `CreatedIndexLog.initial`.
- [x] Add the durable indexbuilder-owned EMPTY/NON_EMPTY tracker with commit-time overlay semantics.
- [x] Make boot/restart registry resolution depend on replayed tracker/version state and fail loudly on inconsistencies.
- [x] Use the generated classifier in generic backfill while preserving full ledger-local `log_date` behavior.
- [x] Cover delete/recreate, rollback/retry, restart, postings, metadata, `log_date`, and checkpoint/restore behavior.
- [x] Update architecture and protobuf documentation, including residual true-backfill cost.
- [x] Run protobuf generation, focused tests, `scripts/agent-check`, exact-merge-base `scripts/agent-check-pr`, and inspect the final diff.

## Review

- Root-cause chain verified: `CreatedIndexLog.initial` only encoded same-Apply emptiness, so proposal boundaries lost the fact and scheduled a global replay.
- Same-class bug sweep completed: generic/posting backfills, all transaction builtins, metadata, `log_date`, historical delete, and high-water overflow are covered.
- Atomicity and restart review completed: tracker, version, cursor, task, and config mutations roll back together; boot uses one coherent read-store snapshot.
- Documentation reviewed against runtime ordering: persisted mutation/commit/rollback, restart, checkpoint, and fresh-readstore restore behavior are explicit.
- Final validation and diff review: focused tests, focused race, `agent-check`, and exact-base `agent-check-pr` (full race + business/cluster e2e) pass; diff is clean and scoped.
