# idempotency-keyed-apply-changes-pit-once — Ambiguous keyed retries contribute one PIT effect

## Catalog entry

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | Retrying the same keyed Apply after an ambiguous response or leadership change produces one authoritative audit/log outcome and changes PIT monetary history exactly once. |
| **Invariant** | `Always(keyed_attempts_pit_delta == committed_original_delta, "pit: keyed apply retries contribute one committed monetary effect")` plus `Sometimes(postCommitPreResponseReached && ambiguousRetryReconciled)`. The workload reconciles attempts through the hash-chain-bound idempotency key and committed log references, then compares PIT at the returned watermark. |
| **Antithesis Angle** | Force `DeadlineExceeded` after the server may have committed, change leaders before retry, route retries through different replicas, and query PIT while replica-local builders are at different watermarks. |
| **Why It Matters** | Client retries are expected under faults. If a replay appends a second audit/log or PIT reduces the replay again, one business transaction becomes two historical monetary movements. |
| **Confidence** | High for primary deduplication and PIT's audit-derived input; real post-commit timeout placement remains untested. |
| **Focus** | Idempotency and Replay |

**Open Questions:**

- None. Add an Antithesis-only checkpoint immediately after the keyed proposal
  future resolves successfully and before the gRPC response is serialized. It
  emits `Reachable("pit: keyed apply committed before response")` with a
  workload-supplied probe ID and can block until cancellation or process death.

## Evidence trail

- `internal/infra/state/machine.go:1312-1369` hashes the complete ordered proposal under one idempotency key; a matching live outcome returns references or the frozen error without processing orders, appending logs, or writing an audit entry.
- `internal/infra/state/machine.go:1576-1591` persists the success key, original first log sequence, and log count in the same FSM write path as the committed proposal.
- `internal/infra/state/idempotency_apply_test.go:24-88` proves a matching duplicate returns the original log reference while a changed payload conflicts.
- `internal/infra/state/idempotency_apply_test.go:163-224` proves success and failure replays do not extend the audit sequence or hash chain.
- `tests/antithesis/workload/internal/client.go:31-44,157-190` documents retries as becoming definitive through the server idempotency cache and classifies `DeadlineExceeded` as the ambiguous-commit case.
- `tests/antithesis/workload/bin/cmds/main/parallel_driver_idempotency/main.go:13-59` already checks repeated keyed calls return the same transaction ID, but never checks the audit chain or PIT.
- The PIT builder reads only authoritative audit entries and referenced logs (`internal/application/balancehistory/builder.go:786-925`); a deduplicated response that creates no new audit entry supplies no second source event.

## Failure scenario and oracle

1. Send a uniquely identifiable transaction with a fresh idempotency key.
2. Cause an ambiguous result after the commit may have landed, then retry the identical request through another leader/replica.
3. Resolve the outcome from audit entries carrying that key and their created/reference logs; do not infer commitment solely from the first RPC response.
4. Wait for a PIT view covering the committed log and compare the account/asset/color delta to exactly one resolved posting set.
5. Repeat after node restart so the primary idempotency bridge must reload from Pebble.

## Instrumentation status

- **Existing SDK instrumentation:** partial. The existing idempotency driver checks equal transaction IDs, and the model workload uses keyed retries; neither assertion mentions PIT or proves a single audit/log source event.
- **Required SUT-side guidance:** add the post-commit/pre-response checkpoint,
  `Reachable` for a keyed success replay returning original log references
  without `AuditEntryWritten`, and a PIT-specific `AlwaysOrUnreachable` at
  builder publication that a keyed audit/log identity is never reduced twice.
- **Missing workload check:** correlate the key with authoritative audit/log state and compare the eventual PIT delta, including the ambiguous first-response branch.
