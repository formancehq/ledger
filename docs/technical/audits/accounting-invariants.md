# Accounting invariants: boundaries and evidence

The [native manifest](accounting-invariants.json) defines the reusable audit for
Ledger business arithmetic and transaction effects. Its observable requirement
is that every accepted transaction conserves value per asset and color, that
amounts remain exact, and that all authoritative representations describe one
atomic business outcome. This companion supplies the independent oracle,
production anchors and root-cause boundaries. It adds no runner, product
guarantee or authorization to execute an audit.

## Independent accounting oracle

For each candidate, record the exact audited SHA, ledger policy, ordered request
or serialized accepted intent, commit outcome, audit/log sequence, and the pinned
read horizon. Model volumes by `(ledger, account, asset, color)` as two
arbitrary-precision non-negative integers, `input` and `output`. For every
successful posting of amount `a`, add `a` to the source `output` and destination
`input` in the same asset/color bucket. The balance is `input - output`; the
non-world source floor applies after each posting unless the committed operation
sets `force`. Conservation is checked independently for every asset/color:

```text
sum(delta output) == sum(delta input) == sum(posting amounts)
```

The oracle must parse and add amounts independently of production `Uint256`,
posting, replay, aggregate and model helpers. It must keep full transaction
fingerprints: id, effective timestamp, ordered postings, metadata, reference,
reversion links and outcome. A successful revert creates a new transaction whose
postings exchange every source and destination of the immutable target while
preserving amount, asset and color. A skipped or rejected order contributes no
successful transaction or volume delta.

Comparing two stored totals is not independent evidence when both were produced
from the same helper. Production replay, checker reconstruction and
`tests/oracle` are valuable comparison surfaces, but agreement among them is
supplementary until the small independent fold also agrees. P0/P1/P2 candidates
need the concrete failure path required by the [audit contract](../contributing/ai-audit.md)
and remain hypotheses until the independent
[challenge pass](../contributing/ai-audit-challenge.md) at the same SHA.

## Production source map

| Boundary | Sources and required evidence |
| --- | --- |
| Posting arithmetic | `internal/domain/processing/processor_posting.go`, `processor_volumes.go`, `internal/proto/commonpb/`: trace amount decoding, ordered source-floor checks and overflow on both input/output sides. Prove the exact bucket key, prior value and typed error; a wire parse failure is not apply-arithmetic evidence. |
| Transaction creation | `processor_transaction.go`, `processor_transaction_numscript.go`, `processor_skippable.go`, `processor_apply.go`: follow dry validation, posting production, ID allocation, transaction/reference/metadata writes and the operation outcome. Capture mutations before each failure and the final merged `WriteSet`. |
| Reversion | `processor_revert_transaction.go` and the transaction-state accessors: establish target existence, immutable original postings, reverse links, double-revert behavior, force and effective-date semantics. |
| Bulk atomicity | `processor.go`, overlay/scope files and `internal/infra/state/write_set*.go`: determine which per-order effects compose, which error discards the batch and what a skippable reason retains. Cache/preload enforcement is counterevidence here; its structural correctness belongs to the FSM domain. |
| Numscript equivalence | `processor_transaction_numscript.go`, `numscript_store_adapter_test.go`, `internal/application/admission/numscript*`, and scripting docs: compare the resolved posting/result semantics with an equivalent direct request. Do not assume identical error text where the documented producer context differs. |
| Audit and replay observations | `internal/infra/state/audit_envelope.go`, `internal/domain/replay/`, and `internal/application/check/`: bind accepted business intent to the committed success/skip outcome and compare replayed state with the independent fold. A missing verifier pass is not an accounting finding unless a distinct producer defect is established. |
| Primary authoritative state | transaction, reference, boundary, metadata and volume accessors under `internal/storage/dal/`, plus controller/read handlers: compare complete logical rows at one snapshot/horizon. Serialization byte equality is unnecessary for map-bearing values. |
| Secondary/derived observations | `internal/application/usagebuilder/`, `internal/storage/usagestore/`, `internal/storage/readstore/` and aggregate queries: require documented progress/readiness and compare complete keyed quantities. Lag, readiness and candidate selection are neighboring contracts. |
| Existing evidence | focused processing/state/replay tests, `tests/oracle/`, `tests/e2e/business/`, `tests/e2e/cluster/` and Antithesis workloads: record the exact branch and assertion reached. Test names, green suites and an implementation-shaped model are not independent proof. |

## Atomicity and outcome ledger

For each adversarial trace, construct an outcome ledger before judging a partial
effect. It lists, per order, the accepted business intent, success log or typed
skip/failure, transaction and log IDs, reference owner, transaction state,
metadata changes, touched volume deltas, audit item and idempotency outcome. Mark
the precise operation/bulk commit boundary and compare the final state with the
promised successful prefix.

Temporary overlay mutations are allowed before a later validation fails; leaking
them into the committed `WriteSet` is not. Conversely, audit entries, skip logs
and frozen idempotency outcomes may deliberately describe a failed or skipped
attempt. Their presence is not a partial successful transaction. Sequence
allocation must follow the current documented outcome semantics; do not infer a
gap merely from a non-contiguous identifier without proving that the relevant
failure promised rollback of that allocation.

Within a bulk, use ordered witnesses. An earlier successful posting can fund a
later posting because both see the running overlay. A failure probe that executes
only one isolated order cannot prove bulk composition or rollback. Pair each
failure with a following successful observer and compare skippable versus
non-skippable execution.

## Projection and replay boundaries

The audit chain is business truth; primary transaction and volume rows are
authoritative projections and must equal the independent fold at the same
horizon. Replay is inspected because restore and checker code reconstruct these
projections, but this domain owns a candidate only when the transaction semantics
or arithmetic are wrong in uninterrupted production too, or when a shared
accounting helper is the root cause. A replay-only loss belongs to
`persistence-restore-replay`; an independently missing detector belongs to
`integrity-verifier-soundness`.

Readstore and usagestore are per-replica secondary projections. A stale or
uncertified result belongs to `read-consistency-projections`; a wrong query
candidate or bucket shape at fixed state belongs to `query-semantic-equivalence`.
Once the documented progress/readiness precondition is satisfied and the correct
entities were selected, wrong counter arithmetic, volume annotation or amount
belongs here. Always record the source audit horizon and projection cursor so a
timing race cannot masquerade as a financial discrepancy.

Zero-volume lifecycle needs explicit classification. Persistent, draining,
new-kept, ephemeral and transient cells have different storage and annotation
paths. The independent oracle retains the arithmetic value and separately models
whether the row should exist. Missing a deliberately purged zero row is not lost
money; keeping a wrong non-zero value or computing the wrong derived count is.

## Ownership and exclusions

Assign one root cause by the violated contract and required correction. Shared
files are evidence surfaces, not permission to clone findings.

| Owner | Boundary |
| --- | --- |
| `accounting-invariants` | Conservation, exact amount/balance arithmetic, posting/revert semantics, transaction business-effect atomicity, and arithmetic correctness of a current projection. |
| `fsm-determinism-cache-coverage` | Replica equivalence, declared preload coverage, gated cache access, generation rotation, no Pebble reads in apply, impossible-state signaling and accepted-order immutability as structural mechanisms. An accounting symptom is handed here when the correction is to that mechanism rather than posting semantics. |
| `integrity-verifier-soundness` | Missing/unsound verification, reconstruction independence, range coverage or loss of checker evidence. A demonstrated accounting producer defect remains accounting-owned even when it also escapes checking; create an additional verifier finding only for a separately demonstrable missing protection with an independent witness. |
| `persistence-restore-replay` | Snapshot, delta, backup, restore and replay-only loss or duplication. This domain may use restored state as a comparator but does not reopen restore parity. |
| `read-consistency-projections` | Temporal horizons, read barriers, projection progress, rebuild/readiness publication and concurrent visibility. |
| `query-semantic-equivalence` | Fixed-state candidate selection, predicate/iterator semantics, pagination and result shape. Once membership is established, arithmetic of selected amounts remains accounting-owned. |
| `idempotency-retries-partial-failures` | Retry identity, frozen outcome reuse, ambiguous transport outcome and retention. This domain owns whether a single accepted execution produced the right accounting effect. |
| `api-boundary-contracts` | HTTP/gRPC/JSON/protobuf conversion, presence and error mapping. An exact value accepted by the typed business command but miscomputed later is accounting-owned. |
| Account type, metadata and usage semantics | These are in scope only where they gate or accompany an accounting transaction atomically. General schema lifecycle, metadata query semantics and product counter definitions are not a second accounting campaign. |

Raft quorum/election, generic cache consistency, generic concurrency/shutdown,
secret exposure, authentication, performance, historical v2 compatibility and
new product semantics are excluded. An undocumented expectation is an audit
question, not a defect inferred from conventional bookkeeping preferences.

## Falsifiable scenarios and rejection criteria

Every candidate must include a minimal ordered trace, independent expected
state, actual state, exact production branch and a matched healthy control.
Useful scenario families are:

- same-cell and self-postings, interleaved asset/color buckets, world and forced
  sources, maximum values and overflow on each accumulation side;
- multi-order bulks where an earlier order funds a later order, and failures
  occur before production, after staged postings, after ID allocation or during
  metadata/account-type validation;
- direct versus inline/stored Numscript transactions with the same resolved
  postings, including script metadata collisions and an empty generated result;
- reverts with effective timestamps, force, repeated attempts, same-bulk target
  dependencies, and missing or inconsistent allocated target state;
- pinned-horizon comparison of independent fold, primary projections, checker
  replay and authoritative reads, followed by current secondary projection
  comparison after an exact progress barrier;
- zero-volume persistent/draining/ephemeral/transient transitions and ledger
  deletion, with row existence separated from arithmetic value.

Reject or re-route a candidate when the alleged imbalance comes from comparing
different horizons; the index/usage cursor was not ready; the oracle imports the
same production helper; only serialization order differs; the operation was
rejected before the claimed branch; a zero row was contractually purged; the
effect is an explicit skip/audit/idempotency outcome; or an existing guard makes
the state unreachable. Missing tests alone are not findings. Preserve the trace
as counterevidence and route distinct verifier, retry, restore, query or FSM
mechanisms to their owning manifest.

## Validation and later execution

For a manifest-only change, validate the JSON with the launcher's exact `jq`
predicate, verify every path glob and related-document target, run
`bash scripts/agent-check`, then run
`AI_REVIEW_BASE_SHA=<exact-base-sha> bash scripts/agent-check-pr`. These commands
validate the artifact and candidate; they do not establish accounting
correctness.

Only a separately authorized task after merge runs
`bash scripts/ai-audit accounting-invariants` at a clean exact `HEAD`, followed
by `bash scripts/ai-audit-challenge <audit-result>` at that same SHA. Product
probes, fixes and Jira publication are separate phases. Revising this manifest
does not run the product audit.
