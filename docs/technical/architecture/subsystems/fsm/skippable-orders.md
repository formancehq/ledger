# Skippable orders (continue-on-failure batches)

By default a `LedgerApplyRequest` batch is atomic: if any order trips a business
error the whole proposal fails. Skippable orders let a caller opt an individual
order out of that all-or-nothing rule for a **whitelisted** set of idempotency-
style failures — the tripping order is dropped and the batch continues with
`Outcome=Success`, recording a `common.OrderSkippedLog` in its place. See
EN-1356.

## Opt-in surface

- **gRPC**: `LedgerApplyRequest.skippable_reasons` (`repeated common.ErrorReason`).
- **HTTP bulk**: per-entry `skippableReasons`; a skipped entry returns an
  `OrderSkippedResponse` (`skipped: true`, `reason`, reason-specific `context`).
- The unitary `POST` transaction endpoint intentionally does **not** expose it.

The set of reasons an action may skip is a **per-action whitelist declared
inline on the proto** via the `(allowed_skippable_reasons)` field option on each
`LedgerAction.data` oneof case (`misc/proto/bucket.proto`). A custom protoc
plugin (`tools/protoc-gen-skippable`) generates `SkippableReasonsForLedgerAction`
from those annotations, so the policy is co-located with the wire declaration and
cannot drift. Current whitelist:

| Action | Skippable reason |
|--------|------------------|
| `create_transaction` | `TRANSACTION_REFERENCE_CONFLICT` |
| `revert_transaction` | `TRANSACTION_ALREADY_REVERTED` |
| `delete_metadata` | `METADATA_NOT_FOUND` |
| `add_account_type` | `ACCOUNT_TYPE_ALREADY_EXISTS` |
| `remove_account_type` | `ACCOUNT_TYPE_NOT_FOUND` |

**Admission** rejects `UNSPECIFIED` / out-of-whitelist reasons, and a non-empty
`skippable_reasons` on an action with no annotation, up front
(`ErrInvalidSkippableReason` → 4xx). Empty (default) preserves fail-fast.

## FSM mechanism: commit-or-discard overlay

For each order that opts in, `ProcessOrders` runs it against an
`orderOverlayScope` (`internal/domain/processing/overlay_scope.go`) wrapped in a
`skipSafeScope` (`skip_safe_scope.go`):

- **`orderOverlayScope`** buffers every mutation of the kinds a skip-tolerant
  order can touch (ledgers, boundaries, volumes, account/ledger metadata,
  transaction references/states, prepared queries, indexes) in a per-kind
  `stagedAccessor`; counter increments are buffered as deltas; `Reverted` stays a
  discrete staged map. The order sees its own writes (read-your-writes) while the
  parent `Scope` sees nothing yet.
  - On **success**: `Commit()` flushes the staged writes to the parent.
  - On a **whitelisted failure**: the overlay is dropped **without** `Commit()`,
    so the parent never observes any of the order's writes — the rollback is
    structural, not discipline-based.
- **`skipSafeScope`** makes that guarantee exhaustive at compile time: every
  `Scope` method must be explicitly classified as read-passthrough,
  buffered-write-passthrough, or non-buffered-write-trap. A sub-processor that
  mutates a non-buffered surface (signing keys, chapters, maintenance mode,
  numscript library, query-checkpoint state) routes through `trapUnbuffered`,
  which fires `assert.Unreachable` (a first-class Antithesis finding) and panics
  — a new skippable action that writes an unbuffered kind fails loudly instead of
  leaking past rollback.

On a matched skip the FSM emits an `OrderSkippedLog{reason, context}` (the
`context` carries reason-specific correlators, e.g. `reference` /
`existingTransactionId` for a reference conflict).

## Observability

`OrderSkippedLog` maps to the `SKIPPED_ORDER` event type
(`events.LogToEvent`), so skips reach event sinks with `skippedReason` +
`skippedContext`. See [../events-mirror/events.md](../events-mirror/events.md).

## Checker verification (invariant #8)

`LedgerLog` (and therefore `OrderSkipped`) is a projection, not hash-bound, so
the checker must re-derive it. `verifySkippedOrder`
(`internal/application/check/checker.go`) reverifies every persisted
`OrderSkipped` against the audit-bound `Order.skippable_reasons` whitelist and a
reason-specific correlator (reference, transaction id, metadata target/key,
account-type name), emitting `CHECK_STORE_ERROR_TYPE_INVALID_SKIP` on
divergence. A fail-loud `default:` forces the checker to be extended whenever a
new reason is annotated in `.proto`.

## Invariants

- **#2 Determinism** — the skip decision is `whitelist ∩ err.Reason()`, both
  pure functions of chain-bound state; no node-local input.
- **#3 No Pebble reads in the hot path** — the overlay reads only through the
  parent `Scope` (cache), never Pebble.
- **#7 Never silently skip a should-not-happen branch** — the unbuffered-write
  trap panics rather than leaking.
