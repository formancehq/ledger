# Mirror CEL rewrite engine

The mirror CEL rewrite engine transforms transactions as a v2 ledger is mirrored into a v3 ledger. It replaces the earlier regex-only account-address rewriter with a general, rule-based engine that can rename address segments, transform metadata, and conditionally drop transactions.

Code: `internal/adapter/v2/celrewrite`. Configured via `MirrorSourceConfig.rewrite_rules` (`misc/proto/common.proto`).

## Where it runs

Rewriting happens during v2→v3 translation, on the mirror **leader**, before coverage/preload and the Raft proposal. The engine is applied once per assembled `MirrorLogEntry` in `TranslateBatch` (`internal/adapter/v2/translator.go`). Because coverage/preload and the FSM read the already-rewritten payload, no FSM or coverage change is required — the rewritten bytes are baked into the proposed order and every follower applies them verbatim.

## Determinism

Determinism is a hard invariant (see the top-level `CLAUDE.md`): the leader computes the rewrite once and the result is replicated, so all nodes must produce identical bytes. The CEL environment is built without any non-deterministic function (no wall-clock, no randomness); every helper is pure. Evaluation is additionally bounded by a CEL cost limit and by static caps (rule count, expression length, regex pattern/replacement length) enforced at compile time in `NewRewriter` — the same compilation admission runs, so a config that admits is guaranteed to build on the worker.

Because the rewrite runs only on the leader and is replicated verbatim, a non-deterministic expression cannot desync nodes — but it would still make mirror output non-reproducible across runs, breaking the purity contract. The one non-obvious source is **map iteration**: a CEL comprehension that maps or filters a map (`log.created.metadata`, `log.created.accountMetadata`) into a list visits keys in Go's randomized order, so its result — and anything derived from it, e.g. `join` — varies run to run. Such expressions are **rejected at admission**. Order-insensitive predicates over a map (`exists`/`all`/`exists_one`, which return a bool) stay allowed, as do comprehensions over an ordered list (`log.created.postings`, `log.addresses()` — the latter is what `mapAddress` expands to). Where an operator needs an ordered projection of addresses, `log.addresses()` already yields them in a fixed order.

## Rules

A rule is `{match, cel, stop}`:

- `match` — a CEL **boolean** expression over the log entry (`log`) selecting which entries the rule fires on. Empty means "always".
- `cel` — a CEL expression evaluating to the rewritten `log`, built with the helper functions below.
- `stop` — when true and the rule matches, no further rules are evaluated.

Rules are evaluated top-to-bottom; every rule whose `match` holds applies its `cel`, feeding its output into the next rule (sequential chaining).

## The `log` surface

The CEL variable is `log`: the mirror log entry, a **sum type** over its four rewritable variants. Exactly one variant is present per entry; access it as `log.created`, `log.reverted`, `log.savedMetadata` or `log.deletedMetadata`, guarded with `has(log.created)` etc. This mirrors `MirrorLogEntry.data` on the wire (`raft_cmd.proto`) and replaces the earlier flat view's invented `tx.type` tag.

Read-only variant fields (usable in `match` and `cel`):

| variant | fields |
|---|---|
| `log.created` | `reference`, `postings` (`{source, destination, amount, asset}`), `metadata` (`map(string,string)`), `accountMetadata` (`map(string, map(string,string))`) |
| `log.reverted` | `postings` (the reverse postings), `metadata` |
| `log.savedMetadata` | `target` (account address), `metadata` |
| `log.deletedMetadata` | `target`, `key` |

Posting `amount`/`asset` are read-only; only `source`/`destination` are written back.

**Cross-cutting helpers on `log`** (work on whichever variant is present; each returns a new copy-on-write `log`):

- `log.rewriteAddress(pattern, replacement)` — RE2 replace across every address of the active variant (posting source/destination, target, created account-metadata keys). Account-metadata keys are rewritten in sorted order with a deterministic last-writer-wins merge on collision.
- `log.mapAddress(a, expr)` — maps a CEL expression over **every** address (`a` bound to each), replacing it with `expr`. The open-ended, computed transform a constant regex can't express — e.g. `log.mapAddress(a, a.split(":").reverse().join(":"))` reverses the segments of every address for arbitrary arity. The address count can't change; each result is validated at commit. `mapAddress` is the **only** way to write addresses (it transforms each in place, so it cannot reorder or drop). The read-only companion `log.addresses()` returns the ordered address list (usable in `match`); there is deliberately no raw list-write helper, since a positional write could silently reassign addresses across postings/roles while every output still validated.
- `log.drop()` — mark the entry to be dropped (see below).

**Variant-scoped metadata helpers.** These hang off the variant that carries the field and return the variant view; lift the result back into the entry with a `log.withX(...)` wrapper (a variant view alone cannot rebuild its parent log). All are copy-on-write, committed only after the whole rule chain succeeds.

- `log.created.setMetadata(key, value [, type])` / `deleteMetadata(key)` — created transaction metadata. Same on `log.reverted` and `log.savedMetadata`.
- `log.created.setAccountMetadata(account, key, value [, type])` / `deleteAccountMetadata(account, key)` — **created only** (only created transactions carry account metadata on the wire).
- `log.created.setAccountMetadataFromAddress(pattern, key, replacement [, type])` — **created only**; for every posting account matching `pattern`, sets `accountMetadata[address][key]` to `ReplaceAllString(address, replacement)`. Because it uses `ReplaceAllString`, the pattern must match the **whole** address (anchor with `^…$`).
- `log.withCreated(created)` / `log.withReverted(reverted)` / `log.withSavedMetadata(saved)` — lift a transformed variant back into the log. Each accepts **only** its variant, so wrapping the wrong variant is a compile error.

`log.deletedMetadata` exposes no metadata helpers — a deleteMetadata op has no metadata map, only its target (rewritten via `log.rewriteAddress`/`mapAddress`) and its read-only `key`.

**The variant types do the safety work.** Because `setAccountMetadata` exists only on `CreatedView` and `deletedMetadata` has no `setMetadata`, a helper the variant cannot persist is a **compile-time type error** — not the silent commit-time drop the earlier flat view allowed. Guard variant access with `has(...)`: accessing a foreign variant unguarded reads a zero view that `withX` would merge in, producing a two-variant entry that is **rejected loudly at commit** (a rule may only transform the source variant):

> **Guard reads too, not just writes.** Reading a foreign variant in a `match` or a predicate — e.g. `log.created.metadata.size() == 0` on a reverted entry — also sees the zero view, so the predicate silently holds and the rule fires on kinds you did not intend (the output stays a valid single-variant entry, so this is a footgun, not corruption). Always scope a variant-specific rule with `has(log.created)` (or an equivalent `match`).

```yaml
- cel: |
    has(log.created) ? log.withCreated(log.created.setAccountMetadata("orders:pending", "region", "eu")) : log
```

**Regex patterns.** The `pattern` argument of `rewriteAddress` and `setAccountMetadataFromAddress` **must be a constant** string. It is compiled at admission, so an invalid or empty pattern is rejected up front rather than failing (and stalling) a mirror batch at run time.

**Metadata types.** The setters take an optional `type` — one of the schema types `string` (default), `int64`, `bool`, `uint64`, `int8/16/32`, `uint8/16/32`, `datetime` — that coerces the string value into the typed `MetadataValue`. The type token **must be a constant** (not a computed expression), so it is fully validated at admission; an unknown type is rejected there. A value that does not parse as the declared type is stored as a null value preserving the original string (the platform conversion matrix). Mirror source metadata is otherwise always string-typed, so this is the only way to emit typed metadata into a mirror.

### Helper reference

All helpers are pure and copy-on-write. Cross-cutting helpers receive `log` and return `log`; metadata helpers receive a variant view and return that same view (lift it back into the entry with a `with…` wrapper). `[, type]` is the optional constant metadata type token.

| Helper | Receiver → returns | Description | Example |
|---|---|---|---|
| `rewriteAddress(pattern, replacement)` | `log` → `log` | RE2 replace across every address of the active variant (postings, target, created account-metadata keys). `pattern` constant. Account keys re-key with a sorted, last-writer-wins merge on collision. | `log.rewriteAddress(":worker:\\d+", "")` |
| `mapAddress(a, expr)` | `log` → `log` | Map a CEL `expr` over every address (`a` bound to each). Computed transform a regex can't express; address count can't change. | `log.mapAddress(a, a.split(":").reverse().join(":"))` |
| `addresses()` | `log` → `list(string)` | Ordered list of the active variant's addresses (read-only; usable in `match`). | `log.addresses().exists(a, a.startsWith("acquirer:"))` |
| `drop()` | `log` → `log` | Mark the entry to become a `MirrorFillGap` (see below). | `log.drop()` |
| `withCreated(created)` | `log` → `log` | Lift a transformed `created` view back into the entry. Accepts only `CreatedView`. | `log.withCreated(log.created.setMetadata("k", "v"))` |
| `withReverted(reverted)` | `log` → `log` | Lift a transformed `reverted` view. Accepts only `RevertedView`. | `log.withReverted(log.reverted.setMetadata("k", "v"))` |
| `withSavedMetadata(saved)` | `log` → `log` | Lift a transformed `savedMetadata` view. Accepts only `SavedMetadataView`. | `log.withSavedMetadata(log.savedMetadata.setMetadata("k", "v"))` |
| `setMetadata(key, value [, type])` | `created` / `reverted` / `savedMetadata` → same | Set an entry-metadata key. | `log.created.setMetadata("count", "42", "int64")` |
| `deleteMetadata(key)` | `created` / `reverted` / `savedMetadata` → same | Remove an entry-metadata key. | `log.reverted.deleteMetadata("internal")` |
| `setAccountMetadata(account, key, value [, type])` | `created` → `created` | Set per-account metadata (created only). | `log.created.setAccountMetadata("orders:pending", "region", "eu")` |
| `deleteAccountMetadata(account, key)` | `created` → `created` | Remove per-account metadata (created only). | `log.created.deleteAccountMetadata("orders:pending", "region")` |
| `setAccountMetadataFromAddress(pattern, key, replacement [, type])` | `created` → `created` | For each posting account matching `pattern`, set `accountMetadata[address][key]` to `ReplaceAllString(address, replacement)`. `pattern` constant, must match the whole address (`^…$`). | `log.created.setAccountMetadataFromAddress("^acquirer:acme:worker:(\\d+):.*$", "worker-id", "$1", "int64")` |

`log.deletedMetadata` has no metadata helpers — a deleteMetadata op carries no metadata map; only its target address is mutable (via `rewriteAddress`/`mapAddress`), and its `key` is read-only.

### Mutable scope (v1)

Metadata (transaction-level and per-account), posting addresses, and metadata-op target addresses. Amounts, assets, IDs, and `reference` are immutable — amounts/assets to preserve balance integrity, `reference` because mirror creation writes the reference projection without a uniqueness check (reference rewriting is deferred until proper preload/coverage/FSM conflict handling exists).

## Dropping transactions

`log.drop()` turns the entry into a `MirrorFillGap` that preserves both **log-ID contiguity** (same `v2_log_id`) and **transaction-ID advancement**: the dropped `transaction_id` (created) / `new_transaction_id` (reverted) is recorded in `skipped_transaction_ids`, so the FSM still advances `NextTransactionId` and a dropped v2 transaction ID can never be reused.

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
  cel: | # strip lock-avoidance shards from every address
    log.rewriteAddress(":worker:\\d+", "")
- match: |
    has(log.created) && "type" in log.created.metadata && log.created.metadata["type"] == "payout"
  cel: |
    log.withCreated(log.created.setMetadata("category", "external"))
  stop: true
- match: |
    has(log.created) && "internal" in log.created.metadata && log.created.metadata["internal"] == "true"
  cel: | # never mirror internal txs
    log.drop()
```

Deriving account metadata from an address — capture the last segment of every
matching acquirer account and store it as `acquirer-type` (metadata keys use the
charset `[a-zA-Z0-9._:/-]`, so a namespaced key like `formance.com/acquirer-type`
is also valid):

```yaml
- match: has(log.created)
  cel: |
    log.withCreated(log.created.setAccountMetadataFromAddress("^acquirer:acme:worker:\\d+:([^:]+)$", "acquirer-type", "$1"))
```

For `acquirer:acme:worker:001:bank` this sets that account's
`acquirer-type` metadata to `bank`.

Capturing a typed value — store the numeric worker id as an `int64` (the pattern
matches the whole address so only the captured group remains):

```yaml
- match: has(log.created)
  cel: |
    log.withCreated(log.created.setAccountMetadataFromAddress("^acquirer:acme:worker:(\\d+):.*$", "worker-id", "$1", "int64"))
```
