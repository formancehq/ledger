# Mirror CEL rewrite engine

The mirror CEL rewrite engine transforms transactions as a v2 ledger is mirrored into a v3 ledger. It replaces the earlier regex-only account-address rewriter with a general, rule-based engine that can rename address segments, transform metadata, and conditionally drop transactions.

Code: `internal/adapter/v2/celrewrite`. Configured via `MirrorSourceConfig.rewrite_rules` (`misc/proto/common.proto`).

## Where it runs

Rewriting happens during v2→v3 translation, on the mirror **leader**, before coverage/preload and the Raft proposal. The engine is applied once per assembled `MirrorLogEntry` in `TranslateBatch` (`internal/adapter/v2/translator.go`). Because coverage/preload and the FSM read the already-rewritten payload, no FSM or coverage change is required — the rewritten bytes are baked into the proposed order and every follower applies them verbatim.

## Determinism

Determinism is a hard invariant (see the top-level `CLAUDE.md`): the leader computes the rewrite once and the result is replicated, so all nodes must produce identical bytes. The CEL environment is built without any non-deterministic function (no wall-clock, no randomness); every helper is pure. Evaluation is additionally bounded by a CEL cost limit and by static caps (rule count, expression length, regex pattern/replacement length) enforced at compile time in `NewRewriter` — the same compilation admission runs, so a config that admits is guaranteed to build on the worker.

## Rules

A rule is `{match, cel, stop}`:

- `match` — a CEL **boolean** expression over the transaction (`tx`) selecting which entries the rule fires on. Empty means "always".
- `cel` — a CEL expression evaluating to the rewritten `tx`, built with the helper functions below.
- `stop` — when true and the rule matches, no further rules are evaluated.

Rules are evaluated top-to-bottom; every rule whose `match` holds applies its `cel`, feeding its output into the next rule (sequential chaining).

## The `tx` surface

Read-only fields (usable in `match` and `cel`):

| field | type | notes |
|---|---|---|
| `tx.type` | `string` | `created` \| `reverted` \| `setMetadata` \| `deleteMetadata` |
| `tx.reference` | `string` | created transactions (read-only in v1) |
| `tx.metadata` | `map(string, string)` | transaction metadata (created/reverted) or the saved metadata (setMetadata) |
| `tx.postings` | `list({source, destination, amount, asset})` | all strings; amount is decimal, read-only |
| `tx.accountMetadata` | `map(string, map(string, string))` | per-account metadata (created/reverted) |
| `tx.target` | `string` | target account address for account-targeted metadata ops (empty for tx-targeted) |
| `tx.metadataKey` | `string` | key for deleteMetadata |

Helper member functions (each returns a new copy-on-write `tx`; helpers never mutate in place, and the result is committed to the proto only after the whole rule chain succeeds):

- `tx.rewriteAddress(pattern, replacement)` — RE2 replace across every address (posting source/destination, target, account-metadata keys). Account-metadata keys are rewritten in sorted order with a deterministic last-writer-wins merge on collision.
- `tx.setMetadata(key, value)` / `tx.deleteMetadata(key)` — edit the entry's metadata map.
- `tx.setAccountMetadata(account, key, value)` / `tx.deleteAccountMetadata(account, key)` — created/reverted only.
- `tx.drop()` — mark the entry to be dropped (see below).

### Mutable scope (v1)

Metadata (transaction-level and per-account), posting addresses, and metadata-op target addresses. Amounts, assets, IDs, and `reference` are immutable — amounts/assets to preserve balance integrity, `reference` because mirror creation writes the reference projection without a uniqueness check (reference rewriting is deferred until proper preload/coverage/FSM conflict handling exists).

## Dropping transactions

`tx.drop()` turns the entry into a `MirrorFillGap` that preserves both **log-ID contiguity** (same `v2_log_id`) and **transaction-ID advancement**: the dropped `transaction_id` (created) / `new_transaction_id` (reverted) is recorded in `skipped_transaction_ids`, so the FSM still advances `NextTransactionId` and a dropped v2 transaction ID can never be reused.

## Validation

- **At admission** — `internal/application/admission/validate_order.go` compiles the rules (via `celrewrite.NewRewriter`) and rejects a bad rule set with `ErrMirrorRewriteRuleInvalid` before the config reaches the audit chain.
- **At translation time** — every output address (posting source/destination, target, account-metadata keys) is validated with `invariants.ValidateLedgerAccountAddress` before the entry is returned; an invalid rewrite fails the batch, so the cursor does not advance and the worker retries.

## Configuration surfaces

- **HTTP** — `mirrorSource.rewriteRules: [{match, cel, stop}]` on the create-ledger body (`openapi.yml`, `internal/adapter/http/handlers_create_ledger.go`).
- **CLI** — `ledgerctl ledgers create --mirror-rewrite-file <path>` (YAML/JSON list) for humans, or repeatable `--mirror-rewrite-rule '<json>'` for programmatic use.
- **Operator** — `spec.mirrorSource.rewriteRules` on the `Ledger` CRD; the controller passes each rule to `ledgerctl` as a `--mirror-rewrite-rule` JSON argument.

## Example

```yaml
- match: null # always
  cel: | # strip lock-avoidance shards
    tx.rewriteAddress(":worker:\\d+", "")
- match: |
    "type" in tx.metadata && tx.metadata["type"] == "payout"
  cel: |
    tx.setMetadata("category", "external")
  stop: true
- match: |
    "internal" in tx.metadata && tx.metadata["internal"] == "true"
  cel: | # never mirror internal txs
    tx.drop()
```
