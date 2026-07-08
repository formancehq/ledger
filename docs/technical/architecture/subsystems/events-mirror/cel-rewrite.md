# Mirror rewrite engine

The mirror rewrite engine transforms mirror log entries as a v2 ledger is mirrored into a v3 ledger. Each rule is scoped to exactly one variant of `MirrorLogEntry.data` via a proto oneof: that scope determines both the CEL type of `log` in the rule's `match` predicate and the set of actions the rule is allowed to carry. Invalid combinations — say `set_metadata` on a `deleted_metadata` rule — cannot be represented on the wire.

Code: `internal/adapter/v2/celrewrite`. Configured via `MirrorSourceConfig.rewrite_rules` (`misc/proto/common.proto`).

## Where it runs

Rewriting happens during v2→v3 translation, on the mirror **leader**, before coverage/preload and the Raft proposal. The engine is applied once per assembled `MirrorLogEntry` in `TranslateBatch` (`internal/adapter/v2/translator.go`). Because coverage/preload and the FSM read the already-rewritten payload, no FSM or coverage change is required — the rewritten bytes are baked into the proposed order and every follower applies them verbatim.

## Determinism

Determinism is a hard invariant (see the top-level `CLAUDE.md`): the leader computes the rewrite once and the result is replicated, so all nodes must produce identical bytes. The CEL environment is built without any non-deterministic function (no wall-clock, no randomness); actions are typed Go closures over pre-validated parameters. Evaluation of `match` is bounded by a CEL cost limit; static caps (rule count, expression length, regex pattern length) are enforced at compile time in `NewRewriter`.

## The rule shape

A rule is `{scope, actions[], stop}`. The `scope` field is a proto oneof — exactly one of five must be set:

| Scope | Predicate variable | Available actions |
|---|---|---|
| `created_transaction` | `log` = `MirrorCreatedTransaction` | rewrite_address, set_metadata, delete_metadata, set_account_metadata, delete_account_metadata, set_account_metadata_from_address, drop |
| `reverted_transaction` | `log` = `MirrorRevertedTransaction` | rewrite_address, set_metadata, delete_metadata, drop |
| `saved_metadata` | `log` = `MirrorSavedMetadata` | rewrite_address (target only), set_metadata, delete_metadata, drop |
| `deleted_metadata` | `log` = `MirrorDeletedMetadata` | rewrite_address (target only), drop |
| `any_variant` | `log` = `MirrorLogEntry` (needs `has()`) | rewrite_address, drop |

Rules are evaluated top-to-bottom on every log entry. A rule whose scope doesn't match the entry's variant is silently skipped. When `stop` is true and the rule matches (scope + predicate), no further rules are evaluated.

CEL is used **only** for the `match` predicate. Actions are typed protobuf messages executed by a Go dispatcher; they carry their parameters (`key`, `value`, `pattern`, `replacement`, …) directly. No expression evaluation at action time.

## Example

```yaml
rewrite_rules:
  # Strip ":worker:<n>" from every address, whichever variant carries it.
  - anyVariant:
      actions:
        - rewriteAddress: {pattern: ":worker:\\d+", replacement: ""}

  # Tag every created transaction of type "payout" as external.
  - createdTransaction:
      match: log.metadata["type"].string_value == "payout"
      actions:
        - setMetadata: {key: category, value: external}
    stop: true

  # Never mirror internal transactions.
  - createdTransaction:
      match: has(log.metadata.internal) && log.metadata["internal"].string_value == "true"
      actions:
        - drop: {}
```

Copying a transaction field into metadata via `value_expr`:

```yaml
- createdTransaction:
    actions:
      - setMetadata: {key: original_ref, value_expr: log.reference}
```

Chaining literal and computed: `value_expr` sees the log AFTER any previous action has mutated it, so the second action below reads the value the first one wrote:

```yaml
- createdTransaction:
    actions:
      - setMetadata: {key: seed, value: hello}
      - setMetadata: {key: echo, value_expr: log.metadata["seed"].string_value}
```

Deriving account metadata from an address — capture the last segment of every matching acquirer account and store it as `acquirer-type` (metadata keys use the charset `[a-zA-Z0-9._:/-]`):

```yaml
- createdTransaction:
    actions:
      - setAccountMetadataFromAddress:
          pattern: "^acquirer:acme:worker:\\d+:([^:]+)$"
          replacements:
            - key: acquirer-type
              replacement: "$1"
```

For `acquirer:acme:worker:001:bank` this sets that account's `acquirer-type` metadata to `bank`.

## Actions

Actions never call CEL. Each carries plain parameters:

- **rewriteAddress(pattern, replacement)** — RE2 replace across every address slot on the current variant (posting sources/destinations, target, account-metadata keys on Created). Pattern must be a valid RE2 expression; empty pattern is rejected.
- **setMetadata(key, value | value_expr, [type])** — set the transaction/entry metadata key. `value` is a literal string validated at admission; `value_expr` is a CEL expression compiled against the current variant and evaluated at commit time (e.g. `value_expr: log.reference` copies the transaction reference into a metadata key). CEL runtime errors on `value_expr` fail the batch loudly (unlike `match` errors which skip); the produced string is re-validated against the metadata charset since it wasn't checked at admission. Optional `type`: one of `string` (default), `int64`, `bool`, `uint64`, `int8/16/32`, `uint8/16/32`, `datetime` — coerces the produced string into the matching typed `MetadataValue` via the platform conversion matrix (a value that doesn't parse becomes a null value preserving the original string).
- **deleteMetadata(key)** — delete a metadata key.
- **setAccountMetadata(account, key, value | value_expr, [type])** — Created only. Same literal/expression choice + optional type as `setMetadata`.
- **deleteAccountMetadata(account, key)** — Created only.
- **setAccountMetadataFromAddress(pattern, replacements[])** — Created only. For each posting address matching `pattern`, iterate every entry in `replacements` and set `accountMetadata[address][key]` to the RE2 replacement of the address. Each replacement carries its own optional `type` for the produced value. Anchor with `^…$` because the replace uses `ReplaceAllString` (unmatched tail leaks otherwise). Example — capture two fields from one match:
  ```yaml
  - setAccountMetadataFromAddress:
      pattern: "^acquirer:([^:]+):worker:(\\d+):.*$"
      replacements:
        - {key: acquirer,  replacement: "$1"}
        - {key: worker_id, replacement: "$2", type: int64}
  ```
- **drop** — turns the entry into a `MirrorFillGap` preserving `V2LogId` and, for created/reverted variants, carrying the source transaction id in `SkippedTransactionIds` so the FSM keeps advancing `NextTransactionId`.

## Validation

- **At admission** — `internal/application/admission/validate_order.go` compiles every rule via `celrewrite.NewRewriter`. The CEL predicate is compiled and typed against the scope's variant; every action's parameters are validated (regex compiles as RE2, metadata key/value pass the platform charset). A bad rule is rejected before the config reaches the audit chain.
- **At translation time** — every output address (posting source/destination, metadata-op target, account-metadata keys) is validated with `invariants.ValidateLedgerAccountAddress` before the entry is returned. An invalid rewrite fails the batch, so the cursor does not advance and the worker retries.

## Behavioural notes

- **Match runtime error → rule skipped.** Indexing a missing metadata key in `match` (e.g. `log.metadata["missing"].string_value == "yes"`) is a CEL runtime error. The rule is skipped rather than failing the batch — a data-dependent predicate would otherwise stall the mirror. Guards like `has(log.metadata.k)` avoid this.
- **`value_expr` runtime error → batch fails.** Unlike `match`, a `value_expr` runtime error fails the batch loudly. `value_expr` participates in mutation, and a silent skip would leave the metadata write unapplied without telling the operator.
- **Scope filter is silent.** A rule scoped to `created_transaction` applied to a saved-metadata log entry is skipped without error.
- **Drop terminates the rule chain.** Once the entry's data is a fill-gap, no subsequent rule can address a variant on it.
- **Do not iterate proto maps in CEL.** cel-go does not sort map iteration (it inherits Go's randomized order), so expressions such as `log.metadata.map(k, k).join(",")` produce non-deterministic strings across runs on the same input. The rewrite runs leader-side so this **does not desync followers** — they replay the already-rewritten bytes — but it breaks the "same input ⇒ same output" contract: two independent runs against the same source can mirror different metadata. Prefer direct key access (`log.metadata["k"].string_value`) or `"k" in log.metadata` guards. If the requirement is genuinely "any of these keys, deterministic," expose the ordered lookup as a fresh helper rather than iterating.

## Configuration surfaces

- **HTTP** — `mirrorSource.rewriteRules` on the create-ledger body (`openapi.yml`, `internal/adapter/http/handlers_create_ledger.go`). Each rule is decoded via `protojson` so proto oneofs dispatch correctly.
- **CLI** — `ledgerctl ledgers create --mirror-rewrite-file <path>` (YAML/JSON list) for humans; repeatable `--mirror-rewrite-rule '<json>'` for programmatic use. Both routes bridge YAML→JSON→protojson.
- **Operator** — `spec.mirrorSource.rewriteRules` on the `Ledger` CRD. The controller passes each rule to ledgerctl as a `--mirror-rewrite-rule` JSON argument.
