# Admission

The admission pipeline (`internal/application/admission`) is the gateway every write request goes through before reaching the FSM. It authenticates the request, validates signatures, converts external requests into internal orders, preloads dependent state, and proposes the resulting command into Raft.

## Documents

| Document | Description |
|----------|-------------|
| [pipeline.md](pipeline.md) | End-to-end pipeline from gRPC request to Raft proposal: gate, signature, order conversion, numscript, preload, proposal guard, predicted-index trick. |
| [signing.md](signing.md) | Ed25519 request and response signing — keys, lifecycle, cross-language constraint, audit-chain propagation, replay nuance. |
| [validation.md](validation.md) | Structural validation (admission, fast UX feedback) vs behavioural validation (FSM, audit-bound). Shared sentinels. |
| [idempotency.md](idempotency.md) | Idempotency key mechanism, hash-based conflict detection, and TTL eviction. |
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
