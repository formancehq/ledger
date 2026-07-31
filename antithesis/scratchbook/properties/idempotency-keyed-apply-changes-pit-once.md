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
| **Confidence** | High. The workload observes the exact post-commit/pre-response boundary and reconciles the same operation through transaction identity, hash-chain-bound audit/log identity, and both PIT axes. |
| **Focus** | Idempotency and Replay |

**Open Questions:**

- None. The checkpoint is compiled only with the `antithesis` build tag, and
  the ordinary server build keeps a no-op at the same call site.

## Evidence trail

- `internal/adapter/grpc/server_bucket.go:169-177` places the test-only checkpoint after the committed Apply future and before receipt signing or response serialization.
- `internal/adapter/grpc/idempotency_probe_antithesis.go:22-66` authenticates a workload probe ID in the response header, emits the post-commit `Reachable`, and holds the response until cancellation or a bounded fallback.
- `internal/infra/state/machine.go:1312-1379` hashes the complete ordered proposal under one idempotency key; a matching live outcome returns references or the frozen error without processing orders, appending logs, or writing an audit entry, and emits a replay `Reachable`.
- `internal/infra/state/machine.go:1576-1591` persists the success key, original first log sequence, and log count in the same FSM write path as the committed proposal.
- `internal/infra/state/idempotency_apply_test.go:24-88` proves a matching duplicate returns the original log reference while a changed payload conflicts.
- `internal/infra/state/idempotency_apply_test.go:163-224` proves success and failure replays do not extend the audit sequence or hash chain.
- `tests/antithesis/workload/internal/client.go:31-44,157-190` documents retries as becoming definitive through the server idempotency cache and classifies `DeadlineExceeded` as the ambiguous-commit case.
- `internal/application/balancehistory/builder.go:917-939` records the keyed source identities only after immutable publication; the tagged guard in `idempotency_probe_antithesis.go:21-60` rejects publishing the same audit/log identity twice while allowing legitimate key reuse after TTL expiry.
- `tests/antithesis/workload/bin/cmds/main/parallel_driver_idempotency/main.go:29-266` creates the ambiguous first response, retries through the normal multi-node client, proves one detailed audit entry and one log, then requires one monetary input on effective and insertion PIT views.
- `tests/antithesis/workload/internal/pit_idempotency.go` owns exact 64/255/256-byte key generation, response-header authentication, audit validation, and canonical monetary validation.
- The PIT builder reads only authoritative audit entries and referenced logs (`internal/application/balancehistory/builder.go:786-925`); a deduplicated response that creates no new audit entry supplies no second source event.

## Failure scenario and oracle

1. Send one property-owned forced transaction with a fresh 64, 255 or 256-byte idempotency key through a direct no-retry client.
2. Have the tagged SUT acknowledge the post-commit boundary in a response header, then hold the response until the client deadline makes the result ambiguous.
3. Retry the identical request twice through the normal multi-node client and require the original transaction identity.
4. Resolve the outcome from ledger-scoped audit entries carrying that key and require exactly one successful audit entry, one item and one fresh log range.
5. Retry the effective/insertion pair as one bounded, context-aware unit while history reports a classified fail-closed state; expose those samples through a dedicated coverage sonde.
6. Once both axes succeed, require each view to cover the committed audit/log outcome and contain exactly one property-account input.
7. Let Antithesis transfer leadership, partition or restart nodes around these steps; the replay and builder probes expose the internal paths reached.

## Instrumentation status

- **Implementation:** complete for the scoped property. The tagged gRPC probe creates and authenticates the ambiguous response window; the FSM replay and builder-publication assertions expose the two internal deduplication boundaries.
- **Workload oracle:** complete. It correlates transaction identity, the single hash-chain-bound audit/log outcome and the exact monetary effect on both PIT axes. A bounded pair-level convergence loop prevents normal builder lag from making the P0 oracle vacuous, while a separate `Reachable` records classified fail-closed samples.
- **Boundary handling:** builder instrumentation keys its in-memory guard by `(audit sequence, min log sequence, max log sequence)`, not by idempotency-key text. Reusing an expired key for a later authoritative outcome is therefore valid and covered by a tagged unit test.
- **Remaining lifecycle coverage:** restart/restore and cold-tier recovery are exercised by their dedicated P0 properties rather than weakening this property with additional oracles.

## Validation evidence

- Tagged and untagged unit tests pass for the gRPC checkpoint, FSM idempotency path, builder identity guard and workload oracle helpers.
- The instrumented `linux/amd64` Ledger and filtered workload images build successfully. The filtered workload contains `main/first_default_ledger` and `main/parallel_driver_idempotency` only.
- `snouty validate` starts the real three-node Compose topology, observes one leader and three voters, receives setup-complete, and discovers one `first` plus one parallel driver.
- A direct local execution of the instrumented driver reached the authenticated post-commit timeout (`DeadlineExceeded`), returned transaction ID 1 on both retries, found one detailed audit entry for one log, validated both PIT axes, and satisfied the ambiguous-retry coverage sonde. The captured SDK stream contained nine reached true assertions and zero reached false assertions.
