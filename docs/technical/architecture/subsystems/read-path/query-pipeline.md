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
    Ctrl-->>G: Cursor[T] (up to pageSize + 1 rows)
    G-->>C: Streamed page + x-next-cursor / x-previous-cursor trailers
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
A switch that is committed but not yet flushed to stable storage is served from
the version it replaced (a retype retains it) or, for an initial build, refused as
building (`INDEX_BUILDING`, `Unavailable`, retryable) until the flush completes —
see [indexer / Changing a Metadata Key's Type](../indexer/indexer.md#changing-a-metadata-keys-type-setmetadatafieldtype).

## Pebble snapshot

The query and audit read paths report distinct Antithesis safety properties
when a main snapshot violates `H >= R`, preserving their existing errors.
Projection lag and context expiry remain ordinary wait/error paths. A
successful `AlignedIndexSnapshot` return after actual projection lag emits
`indexed snapshot aligned after waiting for projection`; cancellation does
not satisfy it, and it does not claim that the subsequent query succeeded.
Every aligned read reaches that evaluation, including the already-aligned fast
path, so the site sits behind `assert.Enabled` — a constant that is false in an
unarmed build, which makes the compiler discard the call and its details map
rather than merely skipping them.
See the [assertion catalog and applicability](../../../contributing/antithesis-assertions.md).

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

The page token carries only the exclusive resume position and its direction (for example, an account address or transaction ID); it does not identify or retain the Pebble snapshot. Within one request/page, results are served under the coordinated consistency contract described above: main-store leaves and enrichment reflect that request's single pin, subject to the per-target cross-store exceptions — ACCOUNTS membership is served as folded, so a page may include index members absent from the pinned main store. Because the cursor does not retain that snapshot state, there is no general snapshot-consistency guarantee across separate pages. Inserts, deletes, or updates committed between requests may therefore affect later pages according to the documented cursor ordering and filtering semantics. Duplications or omissions across pages under concurrent writes are not, by themselves, evidence of a product defect unless an API contract explicitly promises a cross-page snapshot.

Multiple concurrent readers share snapshots cheaply (Pebble's snapshot is a versioned reference, not a copy).

For `ListLogs`, the index snapshot and read lease remain live through
`ReadLedgerLogsCompiled` while the page is loaded; the earlier release timing
applies to the `ListAccounts` and `ListTransactions` paths.

## The generic list pipeline

`internal/application/ctrl/list_entities.go:57` — `listEntities[T]` is the shared dispatcher for everything that returns a page of entities:

1. Resolve the ledger (`query.GetLedgerByName`) and its declared-metadata schema (so filter conditions can be typed).
2. Compile the filter (if any) into an iterator tree (`internal/query/compile.go:90`).
3. Build the leaf iterators against the read store at the version returned by `SnapshotVersionResolver` (so an index undergoing rewrite still serves under `v_current`).
4. Apply the cursor — position iterators strictly past the resume key, in the read direction the transport resolved with `Cursor.ReadReverse`.
5. Read up to `pageSize + 1` entities; the +1 is the *peek* that lets the streamer detect whether more pages exist without advertising a phantom page.
6. Enrich each candidate entity with its volumes / metadata / transaction body from the main store.
7. Return a `Cursor[T]` over the rows in read order; the streamer turns it into a page and derives the adjacent page tokens from the rows it sends, never from the peeked one (see [Pagination](#pagination)).

## Iterator algebra

`internal/storage/readstore/` — the iterators implement a small set of composable operators, all sharing one interface, `Iterator[D Direction]` (`Next`, `Current`, `Seek`, `Err`, `Close`), declared in `iterator.go`. `Direction` is the sealed pair `Asc`/`Desc`; `EntityIterator` and `ReverseIterator` are aliases for `Iterator[Asc]` and `Iterator[Desc]`.

| Operator | File | Purpose |
|----------|------|---------|
| `PebbleAccountIterator`, `PebbleReverseTxIterator`, `LedgerLogIterator`, `PrefixIterator`/`ReversePrefixIterator`, … | `iterator_*.go` | Leaf scans over one read-store prefix. Direction-specific: a Pebble cursor walked `First`/`Next` is not the one walked `Last`/`Prev`. |
| `BoundedEntityIterator` with `LedgerLogRangeIterator`/`PebbleTxRangeIterator` wrappers | `iterator_bounded_entity.go` | Streams fixed-width entity ranges without materialization, enforcing key shape and half-open bounds. |
| `AndIterator[D]` | `combinator_and.go` | Merge-intersect of sorted child iterators. |
| `OrIterator[D]` | `combinator_or.go` | Merge-union. |
| `NotIterator[D]` | `combinator_not.go` | Difference against the entity-existence index (`0x02`). |
| `FilterIterator[D]` | `combinator_filter.go` | Predicate wrapper (for example the main-store horizon trim). |
| `SliceIterator[D]` | `combinator_slice.go` | Borrowed view over an already sorted, materialized result. |
| address-prefix iterator | `iterator_pebble.go` | Leaf scan with a chart-of-accounts prefix predicate. |

The boolean combinators are **direction-parameterized, not duplicated**: each is one implementation whose only direction-dependent input is `D`'s comparator, so ascending and descending composition cannot drift apart (EN-1966). Only the leaves listed as direction-specific above have two implementations.

The filter compiler turns a `QueryFilter` proto into a tree of these. `Seek` is an **absolute** reposition to the first entity at or after the target *in the iterator's direction* — `AndIterator.Seek` force-seeks *every* child to the target (EN-1597; a child left ahead would skip valid intersections), and the ahead-child leapfrog survives only inside `converge`'s merge loop. Exhausted leaves stay re-seekable; the `seekFloor`/`seekCeil` cache keeps repeated re-seeks of a proven-empty child O(1). See [iterator-seek-contract.md](iterator-seek-contract.md).

### Direction is compiled, not applied afterwards

The compiler has two entry points over one recursion shape: `query.Compile` builds the ascending tree, `query.CompileReverse` (`internal/query/compile_reverse.go`) builds the descending one, out of the shared combinators and the direction-specific leaves. `listDescFiltered` therefore has the same shape as `listAscending` — compile, trim to the main-store horizon, hand to `PaginateReverse` — and a descending page seeks to its cursor and stops after the lookahead.

Before EN-1966 a filtered descending page drained **every** match into a slice, reversed it in place, and only then applied the cursor and page size: O(M) visits and O(M) memory per page, on what is the public default direction for transactions. Measured on one leaf at 100k matches, a first page went from ~6.3 ms / 100,000 rows visited / ~200k allocations to ~15 µs / 101 rows visited / ~282 allocations, and rows-visited is now flat in the match count rather than linear.

Iterator *construction* is what is duplicated between the two entry points — and that is more than the leaves. `compileRev` carries its own recursion shape: the depth guard, the per-target `rejectInvalidCondition` check, the filter-type dispatch switch, and the AND/OR/NOT/universe composition together with its profile-tree wiring. A change to ascending composition semantics does **not** propagate on its own; it has to be mirrored by hand in `compile_reverse.go`.

What *is* shared is what a filter means: predicate resolution, schema validation, index-readiness gating and bound computation are single functions called from both directions (`resolveFieldMetadataCtx`, `resolveIntBounds`/`resolveUintBounds`, `requireIndexReady`, `mergeFieldRanges`, `resolveTxTimestampArm`/`resolveLogDateArm`, `intRangeBounds`/`uintRangeBounds`/`timestampRangeBounds`). So the two directions cannot disagree about which entities a filter selects, nor about accepting or refusing it — only about which way they walk the result. `TestCompileErrorParity` pins the refusal half: for the target guard, the depth guard, the per-target validity table, schema and coercion checks, index readiness and the "condition has no value" arms, both entry points must fail with the *identical* error, and `TestCompileErrorParity_AcceptanceIsAlsoShared` pins the converse — every filter the ascending compiler accepts must compile descending too.

**Two leaf classes stay materializing**, both because their order genuinely forbids streaming:

| Fallback | Why it does not stream backwards | Descending behaviour |
|---|---|---|
| Value-ordered ranges — int/uint metadata ranges, transaction timestamp / inserted-at / reverted-at, log date | Intrinsic. The scan spans several index-value buckets, so rows surface in `(value, entity)` order and "the next entity below X" is undefined without the sorted result | `materializeReverse` reuses the ascending path's single `materializeEntities` drain and hands out a borrowed `SliceIterator[Desc]` over that one slice |
| `AddressTxIterator[D]` — the account→transaction union, including the exact-address form | Intrinsic. Members come from N per-account scans, each ascending but collectively unordered | Both directions share one `addressTxUnion` and its one sorted slice, walked through a `SliceIterator[D]` |

Materializing is also **not** a descending-only cost, and neither is a regression introduced by direction support: the ascending compiler drains the same two leaves through `materializeIterator`. The descending page costs exactly the one materialization the ascending page already pays, with no second complete-result collection for the reversal. Both stay visible in the iterator tree under their own `Kind`, so [query-profile](query-profile.md) still attributes their cost.

Entity-keyed ranges are *not* in this table. The Pebble transaction zone is keyed by txID, so `compileTxIDConditionRev` builds a `PebbleReverseTxRangeIterator`; the ledger-log index is `llog:<ledger>` followed directly by the big-endian log ID, so `compileLogIdConditionRev` builds a `ReverseLedgerLogRangeIterator`. Both stream, exactly as their ascending twins do, and a descending page over either reads about one page (`TestReverseLogPageIsBoundedByThePage`).

A gate that hides rows must hide them in **both** directions. `ReversePrefixIterator` carries the same fold-sequence stamp gate as `PrefixIterator`, and `ReverseEventResolveIterator` resolves each group at the same pin as its ascending twin — walking a group backwards, the *first* event with `seq <= pin` is the latest one at or below it, which is the event the forward pass settles on. A gate present on one side only is a direction-dependent visibility bug that a whole-set parity test cannot see, because both directions are compared against the same pinned view; the registry-driven conformance suite in `internal/storage/readstore/iterator_conformance_test.go` compares each direction against the independently declared set at a pin instead.

The acceptance oracle for the compiled path is `internal/query/compile_reverse_parity_test.go`: for **every supported target** — ACCOUNTS, TRANSACTIONS and LOGS — crossed with the filter families the per-target validity table allows on it and six page sizes, a full descending traversal *by pages* equals the reversed ascending reference. Target is part of the case matrix rather than a constant because TRANSACTIONS is the public default descending direction, so an ACCOUNTS-only oracle would prove the criterion on the wrong surface.

Concretely the matrix drives, per target: on ACCOUNTS the string / uint / int / bool metadata leaves, both `exists` arms including the null OR, the account address prefix and exact forms, and the stamp-gated has-asset scan; on TRANSACTIONS the streaming id range, the timestamp and inserted-at materializing fallbacks, the reference prefix, the reversion bitset and its complement, and the account→transaction union in both match forms and on a role bucket; on LOGS the id leaves and the log-date fallback — each crossed with AND/OR/NOT compositions and an empty-result shape.

Three guards keep the matrix from passing for the wrong reason. `TestDescendingParity_EveryTargetIsCovered` fails if a target drops out. `TestDescendingParity_NonEmptyFixtures` fails if a target's universe is unseeded. `TestDescendingParity_LeafFixtureSizes` pins the exact result size of each leaf family, because a case whose index rows are missing or written under the wrong prefix still compiles and still yields an empty reference — so `descending == reverse(ascending)` holds on `[] == []` and proves nothing about the leaf it was added for. One has-asset row is deliberately stamped **above** the read pin, so a direction that drops the gate serves a row the other hides and the oracle fails rather than agreeing on the same over-wide view.

## Pagination

### Page tokens

Every paged list exchanges **page tokens** built by `pkg/pagecursor`. A token is base64url (unpadded, RFC 4648 §5) of the JSON object `{"key": <string>, "back": <bool>}`; both fields are omitted when empty and unknown fields are rejected. `key` is a position in the endpoint's textual form:

| Endpoint | Key |
|----------|-----|
| Transactions | transaction id, decimal |
| Logs | ledger-local log id, decimal |
| Audit entries | audit sequence, decimal |
| Accounts | address |
| Ledgers, numscripts | name |
| Signing keys | key id |
| Prepared queries | decimal id for TRANSACTIONS / LOGS targets, address for ACCOUNTS |
| Index inspection | base64url of the encoded metadata value |

Both directions are exclusive of the key and always return rows in the **requested** order (`reverse` included):

| Token | Page served |
|-------|-------------|
| empty / absent, or `{}` (`e30`) | the first page |
| `{key: K}` (forward) | the rows strictly after K |
| `{key: K, back: true}` | the page that ends strictly before K |
| `{back: true}` (empty key) | the last page |

A token that does not decode, or whose key is not a valid position for the endpoint (for example a non-decimal key on transactions), is `InvalidArgument` over gRPC and `400 INVALID_REQUEST` over HTTP (`query.ErrInvalidCursor`). The token carries no snapshot: see the cross-page consistency note [above](#pebble-snapshot).

### Serving a page

`Cursor.ReadReverse(reverse)` gives the order the source is read in: the requested order for a forward token, the opposite order for a back token. Either way the source resumes from the same exclusive key and is sized `pageSize + 1`; the extra row is a *peek* that proves a further page exists without advertising a phantom one on a result of exactly `pageSize` rows. The streamer (`sendPagedToStream`, `internal/adapter/grpc/stream_helper.go`) then:

1. **Forward page:** streams rows as they are read, up to `pageSize`.
2. **Back page:** buffers at most `pageSize + 1` rows, drops the peek, and sends the page reversed, so it reaches the client in the requested order.

HTTP list handlers do the same through `pagecursor.Page` (`internal/adapter/http/pagination.go`). Ledgers, signing keys and numscripts are small collections: the handler sorts them by key and pages the slice in memory (`pageSorted` over HTTP, `ApplyHandlerPagination` over gRPC).

### Links

`Cursor.Links` derives the adjacent tokens from the page's first and last keys (in requested order), its row count, and whether the peek fired:

| Request | `next` | `previous` |
|---------|--------|------------|
| forward, key empty (first page) | `{last row}` if the peek fired | none |
| forward, key K | `{last row}` if the peek fired | `{first row, back}`; `{back}` (the last page) if the page is empty |
| back, key K | `{last row}`; the first-page token if the page is empty | `{first row, back}` if the peek fired |
| back, key empty (last page) | none | `{first row, back}` if the peek fired |

`hasMore` is set iff `next` is. An empty row key (a log without an apply payload) yields no link through that row. gRPC publishes the tokens in the `x-next-cursor` and `x-previous-cursor` trailers; HTTP returns them as `next` / `previous` beside `data`; prepared queries return them in `PreparedQueryCursor.next` / `previous`; index inspection in `next_cursor` / `previous_cursor`.

### Routed reads

A follower that routes a read to the leader (`BucketGrpcClient`) has already resolved the token: it forwards the exclusive position as a forward token, with the read direction it computed as `reverse`, and builds the links itself from the rows it receives. The leader caps its response at its own page limit, so a follower that asked for `MaxPageSize + 1` rows can see a full page end in EOF. The routed cursor (`upstreamPeekCursor`) therefore treats the leader's `x-next-cursor` trailer only as a "more rows exist" signal, exposed through `cursor.MoreReporter` (`internal/pkg/cursor/cursor.go`) and read with `cursor.SourceHasMore`; the leader's token itself is never relayed.

## Special read paths

**Query checkpoints.** When the request carries a `checkpoint_id > 0`, the
controller resolves the checkpoint to its frozen main-store + read-store pair
and verifies the stored projection certificate against the checkpoint's durable
applied index. Useful for reconciliation and auditing. See
[query-checkpoints.md](query-checkpoints.md).

**Aggregate volumes.** `ExecutePreparedQuery` with the `AGGREGATE_VOLUMES` mode
and an exactly nil filter calls `AggregateAllVolumes`, sharing the direct
unfiltered aggregation path. It scans the ledger's volume entries through one
iterator on the already-open main-store handle, bypassing filter compilation
and account enumeration. Accounts containing only metadata contribute no volume
rows. Any non-nil filter retains the compiled candidate-account path followed by
per-account volume scans through `AggregateVolumes`; empty or parameterized
filters do not qualify for the nil-filter shortcut. Both paths sum per asset at
request time, with the same uint256 overflow checks and no precomputed aggregate
table. The shortcut runs after the ledger and prepared-query definition have
been loaded and the mode validated through the reserved main-store handle.
It releases the event-history reservation before scanning volumes and opens no
read-index snapshot. The controller's read barrier and the fixed main-store
snapshot remain unchanged.

**Inspect index** is documented under the indexer subsystem — see [indexer / indexes.md](../indexer/indexes.md#statistics-computed-on-demand).

## Where to look in the code

| Concern | Where |
|---------|-------|
| gRPC entry + auth | `internal/adapter/grpc/server_bucket.go` |
| Controller read methods | `internal/application/ctrl/controller_default.go` |
| `ReadIndexAndWait` | `internal/infra/node/read_index.go:101` |
| Generic list | `internal/application/ctrl/list_entities.go:57` |
| Filter compile (ascending) | `internal/query/compile.go:90` |
| Filter compile (descending) | `internal/query/compile_reverse.go` |
| Iterator algebra | `internal/storage/readstore/iterator_*.go` |
| Page tokens + links | `pkg/pagecursor/pagecursor.go` |
| Cursor + streamer | `internal/pkg/cursor/cursor.go`, `internal/adapter/grpc/stream_helper.go` |
| HTTP paging | `internal/adapter/http/pagination.go` |
