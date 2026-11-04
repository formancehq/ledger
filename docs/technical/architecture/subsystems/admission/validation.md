# Validation — Admission vs FSM

## Principle

Every write request is validated **twice**: once at admission (before consensus), once at the FSM (after consensus). The two layers don't duplicate each other — they catch different things, on purpose.

| Layer | Validates | Why here |
|-------|-----------|----------|
| **Admission** | Structural correctness of the *request* — shapes, sizes, allowed character sets, well-formed identifiers, parsing of inline scripts. | UX — reject malformed input fast, give the client a clear error before paying for consensus. |
| **FSM** | Behavioural invariants of the *execution* — balance sufficiency, account-type rules, overflow, already-reverted transactions, script runtime errors. | Audit trail — once the proposal is in the chain, any rejection is itself hash-bound and replayable. |

The rule is in [`feedback_validation_defense_in_depth`](../../../../../AGENTS.md): **do not move validators out of the FSM "for replay determinism" — FSM rejects are a feature**. They are how the audit chain records the fact that some specific batch was attempted and refused, and how the checker can later re-derive the same conclusion from the audit alone.

The complementary rule is in [`feedback_admission_structural_fsm_invariant`](../../../../../AGENTS.md): **admission validates that the order is well-constructed (structural); FSM validates that execution respects invariants (behavioural, post-producer).** Where both layers could detect the same end-state, they share a single sentinel rather than minting two parallel error types.

### Replicated-policy refusals

For an authenticated, structurally valid client request, a definitive refusal
whose outcome depends on mutable, Raft-replicated cluster policy belongs in the
FSM. The FSM evaluates the policy committed before that proposal at its apply
index, after checking whether the idempotency key already has a recorded
outcome. A new policy-based refusal is an audited failure; a replay returns the
recorded outcome. An admission decision based on the currently observed policy
can become stale while the proposal is waiting for commit.

Admission remains responsible for authentication, signatures, request shape,
safe order construction and preload coverage, and operational readiness. A
limit needed to keep a request or proposal safe to construct is a separate
structural or operational constraint; it must not silently substitute for a
business refusal governed by replicated policy. Node-local settings must never
change the result of applying a committed entry. Internal producers need their
own retry and failure analysis before moving a gate into the FSM.

This is the placement rule for new validators and for changes to existing
validators. The inventory below records current behavior, including existing
checks that need a separate migration to satisfy the rule; this document does
not move any validator or change client behavior.

## What each layer catches

| Rule | Admission | FSM | Notes |
|------|-----------|-----|-------|
| Account address shape (length, allowed chars) | ✅ | — | `ValidateAccountAddress` (`internal/domain/validation.go`). |
| Asset code shape and precision range | ✅ | — | `ValidateAsset`. Precision encoded as `uint8` in Pebble keys; admission ensures it fits. |
| Metadata key shape (non-empty, charset `[a-zA-Z0-9._:/-]`) | ✅ | — | `ValidateMetadataKey`. Characters outside the set — a null byte above all — would break Pebble key boundaries. Shape only: the key's *size* is a separate rule, below. |
| Metadata value shape (no null byte) | ✅ | — | `ValidateMetadataValue`. |
| Metadata size: entry count, key bytes, value bytes, per-entity bytes, per-command bytes | — | ✅ | Existing handlers check the policy committed at apply after idempotency lookup. Fresh refusals are audited; retained outcomes replay. Public admission retains structural checks. Mirror workers keep their separate preflight and retry contract. See [metadata limits](metadata-limits.md). |
| Metadata size of the map Numscript merges into the caller's | — | ✅ | `processCreateTransaction` re-checks the *merged* transaction and per-account maps. Admission cannot: the union of caller metadata and `set_tx_meta` / `set_account_meta` output only exists after the script runs. |
| HTTP mirror base URL syntax, HTTP(S) scheme and nonempty hostname | ✅ | — | Rejected with a safe `VALIDATION` error before Raft or ledger creation. No connectivity probe or configuration rewrite; raw URL parser errors may contain credentials and must not be exposed. |
| Ledger name shape (no null, printable ASCII, ≤ `dal.LedgerNameFixedSize`) | ✅ | — | `ValidateLedgerName`. The fixed size matters because ledger names pad into Pebble keys. |
| Account type name ≤ 256 bytes and pattern ≤ 4096 bytes | ✅ | ✅ | Admission rejects oversized inputs before consensus. The FSM repeats the bounds for replay and directly proposed orders so error facts retain the original value rather than a digest. |
| Transaction reference ≤ 4096 bytes | ✅ | ✅ | The conflict fact contains the exact reference; both layers reject an oversized reference before a conflict can be audited. |
| Idempotency key (UTF-8, ≤ 256 bytes) | ✅ | — | The *uniqueness* of the key is checked at FSM time against `SubIdempKeys`; the *shape* is checked at admission. |
| Numscript parses + dependency analysis | ✅ | — | `DiscoverNumscriptDependencies` in admission. Syntax errors caught here. |
| Numscript runtime execution | — | ✅ | `processCreateTransaction` runs the program with preloaded state. Runtime errors → `AuditFailure`. |
| Account balance sufficient for posting | — | ✅ | Requires committed state — admission can't know. `processPosting`. |
| Account-type constraints (SOURCE_ONLY, DEST_ONLY) | — | ✅ | Per-ledger account-type schema. |
| Uint256 overflow on posting deltas | — | ✅ | `calculateAmountDelta`. |
| Already-reverted transaction | — | ✅ | Requires the reversion bitset. `processRevertTransaction`. |
| Existing-ledger / unknown-ledger checks | — | ✅ | The set of ledgers is committed state. |

### Transaction account metadata addresses

Caller `CreateTransaction.accountMetadata` addresses are validated structurally
before preload planning and Raft proposal. Competing invalid addresses select
the lexicographically smallest invalid address, including the empty string.
Clients receive the existing typed `VALIDATION` failure without creating an
audit entry or freezing an idempotency result. The processor repeats address
validation on merged caller and script metadata after the metadata limits; this
keeps committed entries deterministic even when they bypass admission.

## Shared sentinels

Where admission and the FSM can both reach the same **business** end-state, they raise the **same** `domain.ErrXxx`, wrapped in a `domain.BusinessError`. The gRPC layer maps the business error to a stable gRPC status code, so the client sees the same code regardless of which layer produced the error.

Trust-boundary violations are an explicit exception: caller-attribution failures are internal `Classifiable` errors, not `domain.BusinessError` values. Admission and the FSM return the sentinel directly so malformed or forged provenance cannot acquire a public reason, `ErrorInfo`, HTTP failure facts, or an auditable business outcome. See [the admission pipeline](pipeline.md#validation-structural-vs-behavioural) for the rationale.

Example: an empty account address can be caught by `ValidateAccountAddress` at admission (the request had `""`) **or** by the FSM if a numscript program generates an empty source/destination at runtime. Both paths return `domain.ErrAccountAddressEmpty`. The client sees a uniform error contract; the auditor sees the FSM-side rejection in the audit chain when it happens that way.

## Why FSM rejections matter

A rejection at admission is invisible to the audit chain — the proposal never reached Raft, there is nothing to record. That is the right answer for malformed requests (clients shouldn't be able to fill the audit log with garbage), but it is the wrong answer for behavioural failures.

When a Raft proposal is committed and the FSM then rejects it, the rejection produces an `AuditEntry` with `outcome = Failure` carrying the order index, the reason, and the same signature the request was submitted with. That `AuditEntry` is hash-bound exactly like a successful one (see [audit-chain.md](../checker/audit-chain.md)). The checker re-derives expected outcomes from the audit chain and verifies the frozen idempotency outcomes match — meaning a behavioural failure is, in the long run, just as auditable as a success.

Moving a behavioural check out of the FSM into admission would silently delete that record. That's the trade-off the rule guards against.

## Mid-batch failure handling

The FSM applies orders in batch order. If order N fails behaviourally, the whole proposal returns `Failure` for that batch — orders N+1…M are not applied. The audit entry captures the failure at order N; the client sees per-request error reporting in the response.

(There is no partial-success mode: the cluster either applies the full batch or none of it. This is required for the idempotency replay cache to behave correctly — the cache stores one outcome per proposal, not one per order.)

## The domain package

`internal/domain/validation.go` is where the structural validators live and is the natural home for new ones today. The longer-term direction (see [`feedback_formance_domain_repo`](../../../../../AGENTS.md)) is to migrate cross-SDK rules — anything that needs to behave identically in Go, in client SDKs, and in offline tooling — into a shared `formancehq/invariants` repository, leaving Go-only rules under `internal/domain/validation.go`. That repository is **not currently consumed** by this codebase; new shared rules should still land in `internal/domain/validation.go` for now, with the understanding that they may move once the shared package exists.

## Reading the error in tests

The standard pattern in tests is to assert on the domain sentinel, not the gRPC status code:

```go
err := admission.Admit(ctx, batch)
require.ErrorIs(t, err, domain.ErrAccountAddressEmpty)
```

`require.ErrorIs` matches both admission-side wraps and FSM-side wraps because the sentinel is shared. If a future refactor moves a check between layers, the test does not have to change.

## Reserved ledger names

Admission rejects the exact names `_` (system API routes) and `_system`
(system events) on every ledger-scoped write, before proposing to Raft.
This applies equally to HTTP and gRPC. Other underscore-prefixed names,
including `_systemx` and `_System`, remain valid.

## Connector URL admission

New PostgreSQL mirror and NATS/ClickHouse sink requests must use explicit URL
forms. Restricting the accepted syntax keeps public credential projection
explainable without a second PostgreSQL keyword parser. The alternative of
retaining keyword DSNs requires maintaining two lexical grammars; fully masking
legacy non-URL values preserves confidentiality without that parser.

| Field | Accepted form |
| --- | --- |
| `mirrorSource.postgres.dsn` | `postgres://` or `postgresql://`; socket/default-host URLs such as `postgresql:///ledger?host=%2Ftmp` and port-only authorities such as `postgres://:5433/db?host=/var/run/postgresql` remain valid |
| `sink.nats.url` | Comma-separated server URLs, each with `nats://`, `tls://`, `ws://` or `wss://`; empty/default hosts, including `nats://:4222`, and surrounding entry whitespace are allowed |
| `sink.clickhouse.dsn` | `clickhouse://`, `http://` or `https://` with a hostname; comma-separated hosts remain one URL authority |

The shared lexical parser rejects missing schemes, opaque URLs, invalid escapes
and invalid ports without returning parser errors that can echo credentials.
PostgreSQL scheme spelling is lowercase, matching pgx URL recognition. Connector
options, connectivity and credentials are still interpreted by the worker's
driver. AWS IAM region and explicit TLS requirements remain in force.

Validation runs through `validateOrder` for HTTP, gRPC and bulk requests before
proposal, without rewriting the configuration. Rejections use admission-only
`VALIDATION` errors with safe messages; the internal sentinels are
`ErrMirrorPostgresURLInvalid`, `ErrSinkNATSURLInvalid` and
`ErrSinkClickHouseURLInvalid`. Their admission-only type owns safe public
details rather than an audited failure-detail code. They produce no audited
failure fact. The three connector checks run regardless of optional build tags.

This is a request policy, not an FSM invariant. Existing committed configurations,
Raft entries, audit history, checkpoints and restores retain their prior runtime
semantics. Older and newer voters apply the same committed entries identically;
an older admitting leader may still accept non-URL input during a rolling
upgrade. No data migration or protocol revision change is needed. Before
submitting new configurations to upgraded servers, convert keyword DSNs to URL
form and add a scheme to every NATS server. Existing legacy values can appear
fully masked in public reads while workers continue using their original bytes.

Work is proportional to submitted connection-string bytes and NATS entry count.
It introduces no storage scans, network probes, locks, preload keys, proposal/WAL
amplification or FSM work. `connection_url_test.go` covers accepted and rejected
forms, immutable input, safe errors and rejection through `Admission.Admit` with
a proposer mock that permits no calls. The public error renderer is exercised
for each connector to pin safe validation messages and avoid audited-detail
serialization. Projection tests retain independent URL,
legacy keyword, malformed, list, proxy and detached-message observations.
