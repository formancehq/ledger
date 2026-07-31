---
sut_path: /Users/flemzord/Developer/Formance/ledger
commit: fb3f9f8334c7fbbcc27c2c211c77da36762e6aaf
updated: 2026-07-31
external_references: []
---

# Existing Antithesis assertions

## Scope and method

The inventory covers every Go file under `internal/`, `pkg/`, and
`tests/antithesis/` that imports
`github.com/antithesishq/antithesis-sdk-go/assert`. Calls were parsed through
the Go AST so multiline calls and helper packages also count. Testify calls
were excluded by resolving the import path, rather than matching only the
package name.

The checked-in baseline contains 369 Antithesis assertion calls:

| Assertion | Calls |
|---|---:|
| `Always` | 43 |
| `AlwaysGreaterThan` | 1 |
| `AlwaysGreaterThanOrEqualTo` | 2 |
| `AlwaysOrUnreachable` | 61 |
| `Reachable` | 59 |
| `Sometimes` | 92 |
| `Unreachable` | 111 |

Of these calls, 42 are inside the SUT and 327 are in the workload. There are
no PIT-specific SDK assertions in `internal/application/balancehistory`,
`internal/storage/balancehistorystore`, `internal/storage/balancehistoryarchive`,
or `tests/antithesis`.

## SUT-side assertion coverage

| Area | Existing signal | Reuse for PIT |
|---|---|---|
| Admission and FSM | Impossible order/preload/coverage states are `Unreachable`; cache/FSM recovery paths emit `Reachable` | Retain unchanged. PIT is asynchronous and must not add an FSM edge. |
| Raft applier and WAL | Leadership, spool replay/prune, recovery and WAL edge cases are instrumented | These faults create valuable PIT lag and replay interleavings, but they do not verify the history projection. |
| Health and sentinel | Disk thresholds and committed sentinel survival are asserted | Live health is intentionally independent of PIT readiness, so these cannot be used as a PIT-ready oracle. |
| HTTP helper | Unexpected adapter states can be reported through a dynamic `Unreachable` wrapper | PIT should keep stable, dedicated property names rather than route all failures through this helper. |

The SUT currently has no reachability signal for balance-history publication,
quarantine, certified repair, compaction, tier upload, cold fetch, cache lease,
or remote-GC deletion. Black-box workload assertions can prove public monetary
correctness, but cannot prove that these internal transitions were reached.

## Workload-side assertion coverage

The existing `main` template already covers transactions, reversals,
idempotency, metadata, account types, indexes, barriers, compaction, chapter
closure, backups, restore, rolling restart, scaling, leadership transfer and
query checkpoints. The reusable parts for PIT are:

- `internal.RunDriver`, deterministic random selection and bounded contexts;
- global RPC error classification in `internal/client.go`;
- `DialPerNode` for replica-local reads;
- volume consistency helpers in `internal/checks.go`;
- quiescence and cross-node comparison patterns in `eventually_correct` and
  `eventually_cross_node_identity`;
- rolling-restart, quorum-recovery, scaling, chapter and backup drivers that
  already generate useful concurrent cluster transitions.

The `model` template has an independent state machine and a full
backup/teardown/restore cycle, but Antithesis selects one template per history.
It is therefore a later, separate PIT campaign rather than an oracle shared
with the `main` template.

## Error-classification gap for PIT

The current retry interceptor treats `Unavailable` as transient. This includes
`HISTORY_BUILDING` and `HISTORY_BEHIND`, so a normal client may retry away the
evidence that a PIT replica was behind. Conversely, `HISTORY_SOURCE_MISSING`
and `HISTORY_CORRUPT` map to `Internal`; the global classifier currently treats
all unrecognized `Internal` responses as an assertion failure.

PIT probes therefore need a no-retry per-node client and exact matching on the
`ErrorInfo.reason`. Final convergence checks can keep the normal retrying
client. Deliberate missing/corrupt-source scenarios must scope their accepted
reason to that operation; the global classifier must not broadly tolerate all
`Internal` responses.

## Name-collision baseline

The repository convention requires globally unique assertion names. The AST
inventory found seven static names with more than one call site:

| Name | Sites | Assessment |
|---|---:|---|
| `should be able to save numscript` | 2 | Distinct lifecycle and ordinary-save drivers; rename before relying on either signal. |
| `should be able to add account type` | 2 | Distinct negative and normal drivers, with different assertion types. |
| `prepared query creation returned unexpected error` | 2 | Distinct create and update-query drivers. |
| `every RPC error must be classified (workload predicate set complete)` | 2 | Unary and stream interceptors intentionally share a concept, but collapse two failure sites. |
| `double-entry: sum of balances should be 0` | 2 | Empty and non-empty branches of the same checker; still two SDK call sites. |
| `disk usage exceeds threshold` | 2 | Distinct filesystem checks collapse into one signal. |
| `Apply succeeded but returned no committed log` | 2 | Audit and log drivers collapse into one signal. |

Seven calls use dynamic names: two generic SUT wrappers, three numeric
comparison helpers in `parallel_driver_barrier_monotonic`, and the success and
failure names derived from scenario-block names. Those are safe only if their
inputs are stable and globally disjoint. New PIT properties use literal names
prefixed with `pit:` and do not reuse any baseline string.

## PIT assertion strategy

Add assertions in two layers:

1. Workload assertions are authoritative for public behavior: successful PIT
   results match an independent monetary oracle, required watermarks are
   honored, failures are exact and fail closed, and replicas eventually
   converge after faults stop.
2. Small SUT-side assertions are reachability and local-safety probes for
   otherwise invisible transitions: publication, rollback/reset, full replay
   certification, tier phase boundaries, cold fetch, pinned-view protection,
   mutation-epoch invalidation and remote delete-before-ack.

Do not use Antithesis as a latency benchmark. PIT latency and write overhead
remain in the deterministic performance harness; the Antithesis workload tests
correctness and recovery under adversarial scheduling.
