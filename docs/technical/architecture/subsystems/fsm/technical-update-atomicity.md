# Proposal technical-update atomicity

## Replicated interpreter selection

`Proposal.atomic_technical_updates` (internal Raft field 10) selects the
interpreter for the entire proposal. An absent or false marker always uses the
legacy interpreter, including during WAL replay and after future activation.
A true marker selects the whole-proposal interpreter described below. The
choice comes only from committed command bytes, never a node flag, current
binary default, cache residency, or current cluster configuration.

The bridge release carries both interpreters. `Builder.Run` explicitly stamps
false before either submission path, including the no-preload fast path, so
normal and technical-only producers continue emitting legacy commands. This
release does not activate the new behavior. Activation belongs to the following
adjacent RC after every voter and learner supports the marker; see the
[upgrade procedure](../../../../ops/upgrades.md#technical-update-atomicity-bridge).

The marker is internal execution metadata. It does not change public request
fields, the service protocol revision, order business-intent hashes, or audit
hash input. Existing persisted field numbers and historical audit bytes retain
their meaning. Legacy rejected proposals may retain their historical technical
prefix effects; replay must reproduce those effects rather than reinterpret
old commands under the new atomicity rule.

## Whole-proposal preparation and publication

The marked path retains the existing attribution, stale-plan, preload,
sequence, and idempotency guards. In particular, deduplication reads committed
outcomes from before the proposal; an eviction in the same proposal cannot
remove a frozen outcome to force execution again.

Preparation walks technical updates in order and constructs their individual
coverage scopes. Mirror updates stage their mutations in the proposal's
`WriteSet`, making them available to subsequent declared reads. Other technical
effects remain deferred. Backup-bearing proposals reduce their ordered backup
transitions against private state so that later transitions see earlier staged
ones; proposals without backup transitions do not clone backup state.

Orders then execute against the same proposal overlay and pass the usual
proposal-wide validation. Per-update and per-order coverage remains mandatory.
A cluster configuration update does not reset the cache during preparation:
Mirror and order reads use the declared pre-reset cache state, even when the
configuration envelope appears earlier in the proposal.

Only after the whole proposal succeeds does publication run:

1. Publish deferred non-configuration technical effects in their proposal order.
2. Merge the `WriteSet`, including Mirror and business effects, into the cache
   and the current write-only Pebble batch.
3. Apply deferred cluster configuration updates in their proposal order. Cache
   resets therefore purge cache mirrors after Merge rather than leaving merged
   rows under obsolete generation metadata.
4. Publish the successful proposal's idempotency outcome, Bloom additions, and
   audit evidence through the existing success path.

The enclosing prepared batch becomes durable only when the existing committer
commits it. Publication and Merge can mutate memory and stage durable writes
before that point. Any publication, Merge, or storage failure is fatal to the
applier; the uncommitted batch is discarded and recovery reconstructs from the
last durable boundary. There is no general in-memory rollback guarantee after
publication begins, and a post-commit verification failure cannot undo durable
writes.

## Rejection and replay boundaries

For a marked proposal, every non-successful disposition drops all prepared
technical effects, including earlier valid updates:

| Disposition | Technical effects | Audit, idempotency, and HLC |
| --- | --- | --- |
| Technical-update or scope rejection | Discard | Preserve the existing unaudited rejection contract. |
| Uncommitted order/validation failure | Discard together with staged business writes | No new audit, log, or frozen outcome; restore the previous HLC. |
| Replay of an existing outcome | Discard | Return the frozen outcome without a new execution or audit; preserve the previous HLC. |
| Key conflict | Discard | Retain the existing fresh conflict audit and effective HLC; preserve the original frozen outcome rather than replacing it. |
| Definitive business rejection | Discard together with staged business writes | Retain the existing failure audit, keyed frozen failure where applicable, and audited effective HLC. |
| Successful proposal, including an allowed skipped order | Publish | Follow the existing success/audit contract; a skipped order is not a proposal rejection. |

Preload/cache-rotation housekeeping is still governed by the existing cache
contract. Discarding proposal technical effects does not mean that a rejected
Raft entry leaves every incidental cache byte untouched. The applied Raft index
still advances past deterministically rejected entries.

## Recovery, restore, and integrity

WAL replay selects the interpreter from the stored marker. Old unmarked entries
must keep their original technical-prefix effects; marked rejection must not
resurrect a deferred prefix after restart. Snapshots carry the resulting state
through the existing store format; the marker adds no independent primary-store
projection or new checker exemption.

Successful Mirror mutations continue to use the existing persisted projection
and checker contracts. Event delivery cursors/status, backup job state,
idempotency eviction progress, cache configuration, Bloom blocks, and the audit
key keep their existing lifecycle classifications. The marker does not make
operational technical updates audit-bound or add them to incremental exports.
Cross-cluster restore discards cluster-local/cache state through the existing
preparation path; the once-initialized audit key remains checkpoint-preserved.
Business projections and frozen outcomes continue to rebuild from their
existing exported authority.

Restore parity includes the absence of rejected effects. Pair a post-checkpoint
marked rejection or replay with a later definitive business proposal so that
there is a non-empty delta. Compare restored and live logical business state,
audit history, HLC, and the next keyed retry; do not demand an export segment for
an unaudited rejection or compare deliberately discarded source-cluster jobs.
See the [incremental restore contract](../backup/incremental-restore-contract.md).

## Required evidence

Preparation retains O(U) deferred intents for U technical updates; ordinary
non-backup proposals do not scan or clone the idempotency registry, cache, or
active backup jobs. Let J be the maximum active-job cardinality reached during
preparation or publication, including jobs started earlier in the proposal.
Backup-bearing marked proposals deep-clone at most J initial jobs once, then
use the existing linear job-ID lookups for each lifecycle transition:
O(U + J + UJ) work and O(J + U) auxiliary state. Starting U distinct jobs from
an empty map can therefore take O(U²) work. Publication repeats the validated
lifecycle transitions. No accumulated Pebble batch is copied, no new store reads
occur, and the marker adds at most two wire bytes per proposal.

The bridge emits false, so the new state-dependent preparation cost is dormant
in production. `BenchmarkTechnicalAtomicityApply` compares the real apply/commit
boundary for legacy and marked sink proposals at increasing U;
`BenchmarkBackupJobsStatePrepare` varies J independently. There is currently no
hard active-destination cap. Before producer activation, release validation must
establish the supported active-job cardinality and operational budget, and
revisit staging if those measurements exceed it; a small benchmark is not a
cardinality guarantee. The trigger is a backup-bearing marked proposal, not
ordinary writes or scheduler reads.

- Cover every technical-update variant alone and after an earlier valid update,
  including later scope failure, backup conflict, and every order disposition.
- Compare absent/false legacy command behavior with true-marked behavior from
  identical prior state. Test production builder paths independently to prove
  the bridge never emits true, even if the caller supplied it.
- Cover configuration before and after Mirror/order mutations and multiple
  configuration updates; assert the final generation metadata and cache mirror
  rows through restart, not merely the in-memory result.
- Cover same-proposal eviction plus an existing keyed success/failure, conflict,
  and uncommitted retry. Assert exact audit and frozen-outcome absence/presence.
- Prove successful mixed publication, ordered backup transitions, immutable
  accepted order payloads, and unchanged per-operation coverage enforcement.
- Exercise real WAL recovery for marked and legacy commands, a non-empty
  checkpoint-plus-delta restore, and the next consuming operation. Check the
  restored primary store independently with `CheckStore`.
- Before activation, demonstrate adjacent-release mixed-version writes,
  leadership transfer, learner catch-up, restart, and replay using identified
  source and target builds. Same-binary fixtures are useful regression evidence,
  but do not establish release compatibility.
