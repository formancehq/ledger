# Admission

The admission pipeline (`internal/application/admission`) is the gateway every write request goes through before reaching the FSM. It authenticates the request, validates signatures, converts external requests into internal orders, preloads dependent state, and proposes the resulting command into Raft.

## Documents

| Document | Description |
|----------|-------------|
| [pipeline.md](pipeline.md) | End-to-end pipeline from gRPC request to Raft proposal: gate, signature, order conversion, numscript, preload, proposal guard, predicted-index trick. |
| [signing.md](signing.md) | Ed25519 request and response signing — keys, lifecycle, cross-language constraint, audit-chain propagation, replay nuance. |
| [validation.md](validation.md) | Structural validation (including HTTP mirror URLs before creation) vs behavioural validation (FSM, audit-bound). Reserved ledger names and shared sentinels. |
| [idempotency.md](idempotency.md) | Idempotency key mechanism, hash-based conflict detection, and TTL eviction. |
| [metadata-limits.md](metadata-limits.md) | Metadata size contract — the ceilings, how they are measured, where they are enforced (admission and FSM), and the replicated configuration. |
| [admission-cache-horizon.md](admission-cache-horizon.md) | Rejecting proposals when the predicted apply-time generation is ≥ 2 ahead of the FSM's current generation. |

## Related

- [Read path](../read-path/) — the read counterpart that bypasses Raft via ReadIndex.
- [FSM](../fsm/) — the apply-side that admission proposes into.
- [Checker & audit](../checker/) — the audit chain admission's commands are bound by.

## Decision record — no transaction receipts (EN-1952)

An earlier design issued an HS256 JWT **receipt** for every created
transaction (signed in the gRPC adapter with a cluster-local symmetric key,
recomputed on `GetTransaction`) and let a revert request carry the receipt so
admission could derive revert planning postings from its signed claims instead
of reading `TransactionState`.

- **Need.** Receipts existed so a revert could be planned even after chapter
  archival purged the original transaction's postings from the primary store.
- **Limitation.** With chapters and cold storage removed (EN-1945), history is
  permanent and the authoritative `TransactionState` always retains the
  postings, so the receipt's only runtime benefit was skipping one point read.
  The token was never an independently verifiable proof — clients could not
  validate it without the cluster's symmetric secret — and the FSM never
  consumed it: reverts always execute from the coverage-gated
  `TransactionState`.
- **Decision.** Remove the feature entirely rather than keep or gate it: wire
  fields (`Log.receipt`, `GetTransactionResponse.receipt`,
  `RevertTransactionPayload.receipt`), the `INVALID_RECEIPT` error reason,
  `internal/infra/receipt`, receipt signing/verification in admission and the
  gRPC adapter, `--receipt-signing-key`/`RECEIPT_SIGNING_KEY`, operator
  `spec.receiptSigning`, and the CLI/HTTP/OpenAPI surface. Revert planning
  resolves original postings from `TransactionState` unconditionally; a fetch
  miss still yields nil postings and the FSM apply remains the audit authority
  for the business rejection. Doing nothing was rejected because the surface
  (a second signing configuration and cluster-wide secret, checkpoint- and
  forwarding-aware recomputation, protobuf/API/operator plumbing) bought no
  verifiability and no correctness.
- **Trade-off accepted.** Revert admission always performs the
  `TransactionState` point read. If that ever proves too slow, optimize the
  internal state-read/preload pipeline from measurements rather than
  resurrecting a client-carried protocol.
- **Revisit criteria.** EN-1873 (externally verifiable, ledger-signed read
  attestations) remains a separate product decision; it requires public-key
  verification, so the removed HS256 receipt is neither a foundation nor a
  compatibility constraint for it.
- **Validation.** Removal verified by the `feature-removal-residue` audit
  domain (`docs/technical/audits/feature-removal-residue.json`) plus its
  independent challenge, the full e2e business/cluster suites, and the
  Antithesis model driver's retained revert coverage (success, force,
  effective-date, already-reverted, missing-target).
- **Amendment.** The decision stands — admission still does not
  reject a missing target, and the FSM apply remains the audit authority — but
  the fetch miss is no longer left unqualified. Admission's read has no read
  barrier, so a target that is committed but not yet applied on that node also
  reads as absent, and the order then declares no volume coverage while apply
  reads the real postings. Admission now binds what it observed into
  `OrderTechnical.revert_target_digest`, and the FSM re-derives that digest from
  the `TransactionState` it already reads through the coverage gate. See
  [the revert-target observation](#revert-target-observation).

<a id="revert-target-observation"></a>
## Revert-target observation

Admission declares a revert's volume coverage from the target transaction's
stored postings, read by `Admission.observeRevertTarget` with
`Attribute.Get` — a raw point read of the local store. There is no read barrier
on that path: `waitLeaderReady` covers a leadership transition, not steady-state
concurrency.

So a target that is committed but not yet applied on the admitting node reads as
absent. Admission declares no volume keys for it, apply reads the real postings
through the coverage gate, and the reversed postings touch volumes the plan never
declared. Left alone that surfaces as `COVERAGE_MISS`, which the
[coverage gate](../fsm/coverage-gate.md) documents as an admission bug — so a
legitimate revert would be reported as a server defect and, being
`codes.Internal`, would not be retried by the client.

Admission therefore binds what it observed into
`OrderTechnical.revert_target_digest` (`domain.RevertTargetDigest`), and
`processRevertTransaction` re-derives the same digest from the `TransactionState`
it already reads through the gate. The check runs **before** the reversed
postings are built, so this cause can never reach the gate. It sits after the
handler's existing checks on the target, which keep precedence:

| Target as apply reads it | Outcome | Unchanged because |
|---|---|---|
| Id beyond the ledger boundary | `TRANSACTION_NOT_FOUND` | Nothing was allocated, so there is no observation to compare |
| Already reverted | `TRANSACTION_ALREADY_REVERTED` | The revert is refused on its merits; a retry would be refused the same way |
| Allocated with no state, or no postings | `TRANSACTION_STATE_INCONSISTENT` | A broken projection must surface as such (invariant #7) rather than be softened |

`TestProcessRevertTransaction_NotFoundBeatsObservationCheck`,
`…_AlreadyRevertedBeatsObservationCheck` and `…_InconsistentStateNotSoftened`
pin that ordering.

A mismatch that does reach the observation check is then classified by whether a
re-admission could ever converge:

| Target | Outcome | Why |
|---|---|---|
| Existed before this batch | `STALE_INPUTS_RESOLUTION` (`Unavailable`, retryable, never frozen) | Re-admission reads a view that now includes it |
| Created by this batch | `REVERT_TARGET_CREATED_IN_BATCH` (`Validation`, permanent, freezable) | The batch is rejected, so the create never lands and every retry reproduces the same observation |

Freezable is not the same as frozen: `recordIdempotencyFailure` retains an outcome
only when the batch carried an idempotency key. An unkeyed batch is rejected just
as permanently, but leaves nothing behind to replay.

The same-batch case is decided from the ledger's `NextTransactionId` as it stood
before the batch, captured by `processApply` on the first apply order for that
ledger. It is derived from committed state, so every replica computes it
identically.

Reverting a transaction the same batch creates is therefore not supported today:
the bulk overlay does not carry transactions the batch itself creates, so
admission cannot declare their volume coverage. Supporting it needs admission to
predict the transaction ids apply will allocate, which must share one sequential
pass with skip prediction and script resolution — see the follow-up ticket rather
than making the rejection retryable.
