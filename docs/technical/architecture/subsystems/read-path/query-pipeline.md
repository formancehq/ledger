# Query Pipeline

## Overview

Indexed entity-list reads go through four stages: a **Raft `ReadIndex`** barrier
for linearizability, a fixed main-store horizon, per-projection **Raft progress**
waits, coordinated **Pebble snapshots**, and a **composable iterator pipeline**.
The result is streamed back through gRPC with cursor-based pagination. Point
reads and main-store-only queries use their own single main-store handle and do
not wait for projections they do not consult.

The indexed-list pipeline is deliberately uniform across `ListAccounts`, `ListTransactions`, and `ListLogs`. Point reads such as `GetAccount` and `GetTransaction`, plus main-store-only queries such as `ListLedgers`, use the controller layer but not the read-store iterator algebra. The differences between indexed lists are which read-store prefix is scanned and how iterators are composed.

```mermaid
sequenceDiagram
    autonumber
    actor C as Client
    participant G as gRPC<br/>(server_bucket.go)
    participant Ctrl as Controller
    participant N as Node<br/>(ReadIndex)
    participant Raft as Raft peers
    participant FSM
    participant Main as Pebble<br/>(main store)
    participant Index as Pebble<br/>(read store)

    C->>G: ListAccounts(filter, cursor)
    G->>G: Auth + consistency selection
    G->>Ctrl: ListAccounts(...)
    Ctrl->>N: ReadIndexAndWait(ctx)
    N->>Raft: ReadIndex
    Raft-->>N: commitIndex
    N->>FSM: WaitForApplied(commitIndex)
    FSM-->>N: applied up to commitIndex
    N-->>Ctrl: ReadBarrierInfo(R)
    Ctrl->>Main: store.NewReadHandle()
    Ctrl->>Main: Read durable LastAppliedIndex H; assert H >= R
    Ctrl->>Index: Wait RaftProgress >= H; open snapshot; re-read certificate
    Note over Ctrl,Index: H is fixed; the wait never chases the moving head
    Ctrl->>Ctrl: Resolve ledger + schema
    Ctrl->>Ctrl: Compile filter → iterator tree
    Ctrl->>Index: Iterate read store
    Ctrl->>Main: Enrich from main store
    Ctrl-->>G: Cursor[T] (page + next cursor)
    G-->>C: Streamed response
```

## Entry points

`internal/adapter/grpc/server_bucket.go` — every read RPC:

| RPC | Server method (line) | Controller method |
|-----|----------------------|-------------------|
| `ListAccounts` | `:624` | `ListAccounts` |
| `ListTransactions` | `:442` | `ListTransactions` |
| `ListLogs` | `:1121` | `ListLogs` |
| `GetTransaction` | `:299` | `GetTransaction` |
| `GetAccount` | `:606` | `GetAccount` |
| `ListLedgers` | — | `ListLedgers` |
| `ListPreparedQueries` | — | `ListPreparedQueries` |
| `ExecutePreparedQuery` | — | (see [prepared-queries.md](prepared-queries.md)) |

The HTTP REST surface (`internal/adapter/http/`) is a thin wrapper that routes to the same controller methods.

### Filter input: one `*commonpb.QueryFilter`

Whatever the transport, a filter reaches the pipeline as a single
`*commonpb.QueryFilter`. Callers express it in either the textual `filterexpr`
grammar or the structured v2 JSON DSL; both are decoded by
`filterexpr.DecodeDualFormat` and pass the per-target validity gate before the
pipeline sees them. The canonical contract — the two serializations, the
parameter classification, expressiveness asymmetries, date coercion, AND-combination
and audit's textual-only rule — lives in
[query-filter.md](query-filter.md). The pipeline itself is agnostic to which form
was used.

## Linearizability — `ReadIndex`

`internal/infra/node/read_index.go:101` — `ReadIndexAndWait(ctx)`:

1. Call `node.ReadIndex(ctx)` — Raft sends a heartbeat round-trip to confirm quorum and returns the current commit index.
2. `fsm.WaitForApplied(commitIndex)` — block until the local FSM has applied every entry up to that commit index.

Once both succeed, the local Pebble snapshot reflects state at least as fresh as the moment the request reached the cluster. This guarantees **linearizable reads on any node**: a read started after a successful write returns at least that write's effects, regardless of which node serves the read.

If the node is syncing or otherwise unable to confirm `ReadIndex`, the call fails — callers either retry or forward to the leader.

## Projection alignment — fixed Raft horizon

The public `min_log_sequence` gate was removed by EN-1946. A projection-backed
read now aligns automatically against the exact main snapshot it will use:

1. A linearizable read obtains Raft horizon `R` through `ReadIndexAndWait`.
   `stale` deliberately skips this step.
2. The controller opens one main-store snapshot, reads its durable
   `LastAppliedIndex` as fixed horizon `H`, and verifies `H >= R` when `R` is
   present.
3. It waits only for projections the query actually uses. Each must publish a
   Raft progress certificate `>= H`.
4. It opens each projection snapshot and re-reads the certificate from that
   same snapshot before compiling or iterating the query.

The indexer captures its own bounded main-store snapshot and publishes `H` only
after processing every native item visible in it. Intermediate batches advance
only the native cursor; the terminal projection writes and certificate are one
atomic Pebble batch. Raft entries that emit no log or audit item still advance
the certificate. Native log/audit cursors remain separate because folds,
history resolution, and trimming use them.

For account and transaction queries, `AlignmentOwed` walks the complete
boolean filter tree. Main-store-only leaves (transaction ID, reverted status,
and account-target address matching) do not acquire a read-index wait; an AND,
OR, or NOT tree containing any indexed leaf does. LOGS always uses the read
index because even its unfiltered universe is projected.

`stale` therefore means “no quorum barrier”, not “permit torn projections”: it
uses the local main snapshot's fixed `H` and performs the same projection waits.
Per-index build/rewrite readiness remains explicit through
`IndexVersionState`; a Raft certificate does not promote an unfinished build.

## Pebble snapshot

`store.NewReadHandle()` returns a Pebble snapshot. Within one controller request,
main-store leaves and enrichment all use that **one** handle. Read-index
iterators use a separate snapshot certified at the main handle's applied-index
horizon. The projection may be ahead, so `query.MainHorizonKeep` still trims by
the main snapshot's native log sequence for TRANSACTIONS and LOGS (ACCOUNTS are
served as folded). The Raft certificate does not replace this native trimming
cursor. The detailed per-target rules live in
[read-snapshot-consistency.md](read-snapshot-consistency.md#cross-store-alignment-en-1748).
The main handle and reclamation reservation live with the returned cursor; the
projection snapshot and read lease are released after index iteration.

The cursor carries only the exclusive resume position (for example, an account address or transaction ID); it does not identify or retain the Pebble snapshot. Within one request/page, results are served under the coordinated consistency contract described above: main-store leaves and enrichment reflect that request's single pin, subject to the per-target cross-store exceptions — ACCOUNTS membership is served as folded, so a page may include index members absent from the pinned main store. Because the cursor does not retain that snapshot state, there is no general snapshot-consistency guarantee across separate pages. Inserts, deletes, or updates committed between requests may therefore affect later pages according to the documented cursor ordering and filtering semantics. Duplications or omissions across pages under concurrent writes are not, by themselves, evidence of a product defect unless an API contract explicitly promises a cross-page snapshot.

Multiple concurrent readers share snapshots cheaply (Pebble's snapshot is a versioned reference, not a copy).

For `ListLogs`, the index snapshot and read lease remain live through
`ReadLedgerLogsCompiled` while the page is loaded; the earlier release timing
applies to the `ListAccounts` and `ListTransactions` paths.

## The generic list pipeline

`internal/application/ctrl/list_entities.go:57` — `listEntities[T]` is the shared dispatcher for everything that returns a page of entities:

1. Resolve the ledger (`query.GetLedgerByName`) and its declared-metadata schema (so filter conditions can be typed).
2. Compile the filter (if any) into an iterator tree (`internal/query/compile.go:90`).
3. Build the leaf iterators against the read store at the version returned by `SnapshotVersionResolver` (so an index undergoing rewrite still serves under `v_current`).
4. Apply the cursor — fast-forward iterators past the resume position.
5. Read up to `pageSize + 1` entities; the +1 is the *peek* that lets the streamer detect whether more pages exist without leaking a phantom cursor.
6. Enrich each candidate entity with its volumes / metadata / transaction body from the main store.
7. Return a `Cursor[T]` whose next cursor is derived from the **last sent** entity (not the peeked one).

## Iterator algebra

`internal/storage/readstore/iterator_*.go` — the iterators implement a small set of composable operators, all sharing an `EntityIterator` interface (`Next`, `Current`, `SeekGE`, `Err`, `Close`):

| Operator | Purpose |
|----------|---------|
| `PebbleAccountIterator`, `PebbleReverseTxIterator`, `LedgerLogsIterator`, … | Leaf scans over one read-store prefix. |
| `AndIterator` | Merge-intersect of sorted child iterators. |
| `OrIterator` | Merge-union. |
| `NotIterator` | Difference against the entity-existence index (`0x02`). |
| address-prefix iterator | Leaf scan with a chart-of-accounts prefix predicate. |

The filter compiler turns a `QueryFilter` proto into a tree of these. `SeekGE`/`SeekLE` are **absolute** repositions — `AndIterator.SeekGE` force-seeks *every* child to the target (EN-1597; a child left ahead would skip valid intersections), and the ahead-child leapfrog survives only inside `converge`'s merge loop. Exhausted leaves stay re-seekable; the `seekFloor`/`seekCeil` cache keeps repeated re-seeks of a proven-empty child O(1). See [iterator-seek-contract.md](iterator-seek-contract.md).

## Pagination

A `Cursor[T]` is opaque to the client. Internally the cursor encodes the position of the *last returned* entity — a transaction ID as a decimal string (cursor.go:508), an account address as-is (`:676`), etc. The streamer (`server_bucket.go` → `sendPagedToStream`, `internal/adapter/grpc/stream_helper.go:44`):

1. Reads `pageSize + 1` entities.
2. If exactly `pageSize` are read, emits them with **no next cursor** (end of stream).
3. If `pageSize + 1` are read, emits the first `pageSize` and computes the next cursor from the **last sent** (the `+1`th is dropped — it was a peek).

This avoids the classic "phantom trailing cursor" bug where a result set of exactly `pageSize` items would advertise a non-existent next page.

The cursor is sent back as an `x-next-cursor` gRPC trailer.

## Special read paths

**Query checkpoints.** When the request carries a `checkpoint_id > 0`, the
controller resolves the checkpoint to its frozen main-store + read-store pair
and verifies the stored projection certificate against the checkpoint's durable
applied index. Useful for reconciliation and auditing. See
[query-checkpoints.md](query-checkpoints.md).

**Aggregate volumes.** `ExecutePreparedQuery` with the `AGGREGATE_VOLUMES` mode runs the same compiled filter to obtain a candidate account set, then loops over per-account asset volumes in the main store and sums per asset. The aggregation is computed at request time — there is no precomputed aggregate table.

**Inspect index** is documented under the indexer subsystem — see [indexer / indexes.md](../indexer/indexes.md#statistics-computed-on-demand).

## Where to look in the code

| Concern | Where |
|---------|-------|
| gRPC entry + auth | `internal/adapter/grpc/server_bucket.go` |
| Controller read methods | `internal/application/ctrl/controller_default.go` |
| `ReadIndexAndWait` | `internal/infra/node/read_index.go:101` |
| Generic list | `internal/application/ctrl/list_entities.go:57` |
| Filter compile | `internal/query/compile.go:90` |
| Iterator algebra | `internal/storage/readstore/iterator_*.go` |
| Cursor + streamer | `internal/pkg/cursor/cursor.go`, `internal/adapter/grpc/stream_helper.go:44` |
