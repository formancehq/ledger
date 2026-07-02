# Read Snapshot Consistency

A read that assembles its response from more than one Pebble read must reflect a
single committed state. Otherwise it can *tear*: stitch together values from two
different points in time that never coexisted, and return a response that
matches no state the ledger was ever in.

## The rule

**Any controller read that stitches a `LedgerInfo` lookup together with
follow-up attribute reads opens ONE `*dal.ReadHandle` and routes every read
through it — the existence/name lookup included.**

`*dal.Store.NewReadHandle()` opens a Pebble snapshot: a fixed point-in-time view
that ignores writes committed after it. Reading everything from one handle
guarantees the whole response comes from one committed state.

### Canonical pattern

```go
// One snapshot up front; the existence check and the payload reads share it.
handle, err := ctrl.store.NewReadHandle()
if err != nil {
    return nil, fmt.Errorf("creating read handle: %w", err)
}
defer func() { _ = handle.Close() }()

ledgerInfo, err := query.GetLedgerByName(ctx, handle, ledgerName) // via handle
if err != nil {
    ...
}

// ...all subsequent attribute reads also go through `handle`.
```

Cursor-returning reads keep the handle open for the cursor's lifetime via
`cursor.NewClosingCursor(c, handle)` and close it on every error path instead of
`defer`.

### Anti-pattern

```go
// TORN: LedgerInfo is read from the live store, the payload from a *later*
// snapshot — the two can straddle a commit.
ledgerInfo, err := query.GetLedgerByName(ctx, ctrl.store, ledgerName) // live store
...
handle, err := ctrl.store.NewReadHandle()                            // separate snapshot
```

The tell is `query.GetLedgerByName(ctx, ctrl.store, …)` (or `impl.store`) — the
name/existence lookup reading the *live store* rather than a snapshot handle.

## Why

`GetLedger` returns account types (from the `LedgerInfo` proto) **and** ledger
metadata (a separate attribute) in one response. Read from two points in time,
the pair can reflect no committed state — an externally observable
inconsistency. `ListTransactions`, `ListAccounts`, `AnalyzeTransactions`, and the
rest of the read handlers were swept onto the single-handle pattern for this
reason.

Most of those handlers do not *today* embed two mutually-constrained mutable
values in their response — often the `LedgerInfo` read only supplies the
immutable ledger name — so the sweep is largely **defense-in-depth**. The point
of the rule is that it holds *by construction*: a future change that starts
embedding `LedgerInfo` content (schema, account types, metadata) next to
attribute data cannot silently reintroduce a torn read.

## Transaction receipts (forwarded reads)

`GetTransaction` computes a signed receipt from the transaction's creation log.
Two rules keep it consistent under routing (`internal/adapter/grpc/server_bucket.go`):

- **Open the receipt snapshot *after* the transaction read**, never before. The
  read may go through a ReadIndex barrier or be forwarded to the leader and thus
  land at a later state; a snapshot opened earlier could predate the creation
  log and yield an empty receipt for an existing transaction.
- **Reuse a forwarded receipt as authoritative.** When the read is forwarded, the
  serving node already signed the receipt; the contacted node relays it verbatim
  (`Controller.GetTransaction` returns a `*string` — non-nil means authoritative,
  possibly empty for a reversal) rather than re-deriving it from a possibly-stale
  local snapshot. Relaying does not require a local signer.

## Exceptions

These read from the live store / a direct handle deliberately; a snapshot would
add nothing or actively hurt:

- **`GetMetadataSchemaStatus`** — a single read (no follow-up), so it is
  self-consistent regardless of snapshotting.
- **`GetIndexStatus`** — reports the *lag* between the main store and the read
  store; a snapshot of one store would make the reported lag stale.
- **`ListIndexes`** — a forward-only streaming scan of the index registry; a
  long-lived snapshot would pin SSTs and block compaction for the scan's
  duration. It already routes all reads through one direct handle.

A new read handler that stitches `LedgerInfo` with attribute reads and does *not*
open a single handle is a bug unless it is added to this list with a reason.

## Enforcement

Convention and review today. A precise lint is expressible with `go-ruleguard`
(match `query.GetLedgerByName($ctx, $r, …)` where `$r` is `ctrl.store` /
`impl.store`, outside the exceptions above); `forbidigo` cannot express it
because it matches call names, not arguments.

See also [query-checkpoints.md](../data-model/query-checkpoints.md) for
point-in-time reads across the main store and the read index.
