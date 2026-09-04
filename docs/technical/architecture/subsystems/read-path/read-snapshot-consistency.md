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

See also [query-checkpoints.md](query-checkpoints.md) for point-in-time reads
across the main store and the read index.

## Cross-store alignment (EN-1748)

The single-handle rule covers the main store; an *indexed* query additionally
reads the peer read index, which folds asynchronously behind it. Two
independently-taken views therefore disagree about the freshest commits: a
transaction visible in the main handle whose index rows have not folded yet
leaks through complements (`not(ts[..])` returns "a transaction with no
timestamp") and mis-windows conjunctions — a response no single committed
state ever produced.

**Rule: an indexed read opens the main `ReadHandle` first, then takes its
index snapshot through `query.AlignedIndexSnapshot`, and wraps the compiled
iterator with `query.MainHorizonKeep`.**

`AlignedIndexSnapshot` returns a read-index snapshot whose fold cursor covers
the handle's last applied sequence (verified through the snapshot itself, so
the check cannot race it), waiting for the fold for as long as the caller's
context allows. There is deliberately no server-side cap: alignment is not
optional for a filtered read, so how much latency to spend on it belongs to
the caller, and a cap would diverge rather than converge — the pin is fixed
for the life of the handle, so waiting makes progress, while a retry opens a
new handle at a higher sequence and leaves the fold further behind than the
attempt that gave up. It also
registers the handle's sequence as a *read lease*, bounding the event GC (see
below); the caller releases it with the returned closure when iteration ends.
Because the fold is ordered, such a snapshot holds every index event at or
below the handle's sequence. That sequence is the query's **pin**:

- main-store leaves and enrichment reflect the pin exactly;
- metadata and exists index leaves resolve their append-only event groups at
  the pin (see [readstore-event-keys.md](readstore-event-keys.md)) — events
  folded past the handle are invisible, so selection and enrichment cannot
  disagree about a value flip in either direction;
- add-only index leaves (timestamp, inserted_at, address→tx, reference,
  has-asset, reverted_at) keep plain keys and can only *exceed* the handle
  (entities committed after it); the `MainHorizonKeep` filter trims those
  back for TRANSACTIONS and LOGS. ACCOUNTS get no trim: main-store existence
  is not a horizon signal there (purged accounts legitimately live on in the
  monotonic has-asset and metadata indexes), and account enrichment renders
  absent accounts as address-only rows, so index membership is served as
  folded.

Consumers: `listEntities` (ListTransactions/ListAccounts/ListLogs — including
the reverse LOGS arm, whose unfiltered scan also iterates the read index),
`AggregateVolumes`, and the prepared-query executor. Index-introspection
endpoints (GetIndexStatus, InspectIndex, GetIndexEntryStatus) read only the
index snapshot and need no alignment.

**Version activation**: a rewrite stamps every event it writes with the one
FSM sequence it read from, so a promoted version resolves as EMPTY at any pin
below that sequence — and the pin can sit arbitrarily far below the serving
snapshot's cursor, since the bounded wait sits between the two acquisitions.
`IndexVersionState.ActivationSequence` records that sequence and
`PinnedVersionResolver` withholds the version from pins beneath it, so the
query returns `ErrIndexBuilding` rather than an empty page for a fully
populated index (see
[readstore-event-keys.md](readstore-event-keys.md)).

**Index removal is a rejection, not a stale read**: `checkIndexed` resolves
index existence from the mainstore handle, so it reflects the read's pin,
while the rows and the per-replica `IndexVersionState` come from the readstore
snapshot, which can already have folded a removal. The rows are gone at that
point, so the read cannot be served — but it must be rejected *honestly*. The
builder always writes the version record when it folds `CreateIndex`, and
alignment guarantees the fold cursor has reached the pin, so an absent record
can only mean the index was removed after the pin. `requireIndexReady` reads
it that way and returns `ErrIndexNotFound`; a record present at version 0
still means a build in progress and returns `ErrIndexBuilding`. Telling a
client to wait for readiness that will never arrive is the failure this
prevents.
