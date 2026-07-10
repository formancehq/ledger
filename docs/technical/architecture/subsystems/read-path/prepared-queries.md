# Prepared Queries

## Overview

Prepared queries are **named, parameterizable query templates** registered cluster-wide per ledger. A client creates a prepared query once (with a filter expression), then references it by name at execution time. The server compiles the filter against the current ledger schema, runs it against the read store, and streams the result back.

The motivation is twofold:

1. **Pre-validation**: the filter expression is parsed and checked at creation time, against the ledger's declared metadata schema. Invalid queries are rejected before any client ever tries to execute them.
2. **Stable accelerators**: a named query is a fixed shape, which lets the indexer build and maintain accelerators (e.g. the bloom filter integration landed by EN-1321) that wouldn't be possible for ad-hoc queries.

The fuller design rationale lives in [`drafts/prepared-queries.md`](../../../../drafts/prepared-queries.md). This page documents the current implementation.

## Lifecycle

Three Raft orders mutate the prepared-query registry, each producing a corresponding audit log:

| Order | Log payload | FSM processor |
|-------|-------------|----------------|
| `CreatePreparedQueryOrder` | `CreatedPreparedQueryLog` | `processCreatePreparedQuery` |
| `UpdatePreparedQueryOrder` | `UpdatedPreparedQueryLog` (carries before + after filter) | `processUpdatePreparedQuery` |
| `DeletePreparedQueryOrder` | `DeletedPreparedQueryLog` | `processDeletePreparedQuery` |

Source: `internal/domain/processing/processor_prepared_query.go`.

Storage is per-ledger under the attributes zone, keyed by `PreparedQueryKey{LedgerName, Name}` (`internal/domain/keys.go:384`). The canonical key layout is the standard 64-byte padded ledger name followed by the query name string.

There is no explicit version field. An update mutates the filter atomically — the previous filter is captured in the audit log payload (for the chain) but is **not** queryable through any read API. Clients see only the current filter via `ListPreparedQueries`. If a query needs historical visibility, the audit chain is the source.

## Filter expressions

Prepared queries carry a `QueryFilter` proto built from a textual filter language. The parser lives in `internal/pkg/filterexpr/parser.go`:

```
Filter   := OrExpr
OrExpr   := AndExpr ('or' AndExpr)*
AndExpr  := UnaryExpr ('and' UnaryExpr)*
UnaryExpr:= 'not' UnaryExpr | Primary
Primary  := '(' Filter ')' | Condition
Condition: metadata[key] == value
         | metadata[key] in (v1, v2, ...)
         | metadata[key] between (lo, hi)
         | address == "foo:*"
         | source == ...
         | destination == ...
         | exists metadata[key]
         | exists transaction <reference>
```

The reserved words are: `and`, `or`, `not`, `in`, `between`, `metadata`, `address`, `source`, `destination`, `exists`, `true`, `false`. Nesting depth is capped at 200 tokens to keep parsing bounded.

The parser produces a `*commonpb.QueryFilter` — a typed proto that the FSM stores verbatim. The compiler (`internal/query/compile.go:90`) turns that proto into an iterator tree at execution time, gated through `requireIndexReady` so an index in mid-rewrite returns `ErrIndexBuilding` rather than producing partial results.

## Validation

Two layers, following the project-wide pattern (see [admission / validation.md](../admission/validation.md)):

**Admission (structural)** — `ValidatePreparedQueryName` (`internal/domain/validation.go:131`): non-empty, printable ASCII, ≤ 256 bytes. The filter is also parsed at admission, so a syntactic error is rejected immediately. Both checks happen before the order ever reaches Raft.

**FSM (behavioural)** — `processCreatePreparedQuery` (`processor_prepared_query.go:28-59`):

- Ledger must exist (rejects `ErrLedgerNotFound` otherwise).
- Name must not already be in use (rejects on duplicate — there is no implicit upsert; clients must explicitly `Update`).
- Filter must compile against the ledger's current declared-metadata schema (`Compile()` with the standard `MaxFilterDepth=100` guard).

A compile error at FSM time is hash-bound as an `AuditFailure`, so a checker run can re-derive the rejection from the audit chain.

## Execution

`ExecutePreparedQueryRequest` carries the ledger name, the query name, an execution mode (`LIST` or `AGGREGATE_VOLUMES`), and any runtime parameters. The executor:

1. Reads the prepared query via `ReadPreparedQuery` (`internal/query/executor.go:67`).
2. Verifies the requested mode is compatible with the query's `target` (e.g. `AGGREGATE_VOLUMES` only makes sense for accounts).
3. Calls the standard pipeline: `ReadIndexAndWait`, Pebble snapshot, `Compile(indexSnap, kb, pq.GetFilter(), ...)`, iterator execution.
4. For `LIST`, streams the matching entities through the standard cursor pipeline (see [query-pipeline.md](query-pipeline.md)).
5. For `AGGREGATE_VOLUMES`, loops over the candidate account set and sums per-asset volumes from the main store. The aggregation is **computed at request time** — there is no materialised aggregate table.

Source: `internal/query/executor.go`.

## Acceleration via the bloom layer

Commit `c1f79db80` (EN-1321) wired prepared-query keys into the standard bloom-filter infrastructure (see [attributes / bloom.md](../attributes/bloom.md)). A `MayContain` lookup on the prepared-query bloom now short-circuits the "query doesn't exist" path at preload time, putting prepared queries on the same footing as every other attribute kind.

Commit `7662d2bae` (`optimized-prepared-queries`) added monotonic-skip and probe-based optimisations to the `AndIterator` used by the filter compiler, which the prepared-query execution path benefits from directly.

These accelerators are correctness-neutral: turning them off (e.g. by a saturated bloom) only slows execution.

## Scope and naming

**Per-ledger uniqueness.** Two ledgers can each have a prepared query named `top-customers`; the key is `(LedgerName, Name)`. There is no global namespace.

**No cross-ledger queries.** A prepared query targets one ledger by construction. Cross-ledger reads are a separate read-path concern handled at the controller level (see [`project_cross_ledger_queries`](../../../../../AGENTS.md)).

**Cursor for `ListPreparedQueries`.** The cursor is the query name itself (printable ASCII, exactly what the name-validator requires). Listing is a single-ledger scan.

## Canonical `QueryFilter` JSON shape (REST) — v2-aligned query DSL

`QueryFilter` is a protobuf oneof; naively routing it through `protojson` leaks the
proto-internal oneof/wrapper names onto the public REST surface (`and.filters[]`,
`not.filter`, `field.field.metadata`, `stringCond`, `hardcodedExact`, and the
`QUERY_TARGET_*` enum prefix). To keep the REST wire aligned with the rest of the
Formance platform, `QueryFilter` carries a **hand-written JSON codec**
(`internal/proto/commonpb/query_filter.go`) that mirrors the shared Formance query
DSL (`go-libs/pkg/query`, as used by ledger v2). `PreparedQuery.MarshalJSON`
(`internal/proto/commonpb/common.pb.json.go`) uses it plus a string target enum;
the HTTP decoder (`internal/adapter/http/prepared_query_filter.go`) decodes through
the same codec.

The canonical shape (each node has **exactly one** top-level `$`-operator key):

- **Combinators**: `{"$and": [QueryFilter, ...]}`, `{"$or": [QueryFilter, ...]}`
  (both non-empty arrays), `{"$not": QueryFilter}`.
- **Leaf conditions** — single-key operator over a one-field body:
  - `{"$match": {"<field>": <value>}}` — equality; for `address`/`source`/`destination`
    a trailing `:` means prefix, otherwise exact.
  - `{"$gt"|"$gte"|"$lt"|"$lte": {"<field>": <value>}}` — range bound; a closed range
    is an `$and` of a lower + an upper bound on the same field (the decoder folds it
    back into one range condition).
  - `{"$exists": {"metadata": "<key>"}}` — metadata key present.
  - `{"$exists": {"asset": "<assetRef>"}}` — account has held the asset (`BASE` or
    `BASE/PRECISION`, e.g. `USD/2`); this is how `AccountHasAssetCondition` is expressed.

Fields: `metadata[<key>]`, `address`/`source`/`destination`, `reference`,
`reverted` (bool), `ledger`, `id`, `timestamp`, `insertedAt`, `revertedAt`, `logId`,
`date`. A value is a JSON literal or a parameter reference `{"$param": "<name>"}`
(prepared-query parameters). An address prefix is expressed with a trailing `:` under
`$match` (e.g. `{"$match": {"address": "users:"}}`); a parameterised prefix has no
DSL form (a param value cannot carry the trailing `:` marker), and the byte-level
`AddressMatch_ParamPrefix`/`HardcodedPrefix` proto arms are not producible through
the JSON/REST surface. uint64 fields are carried as decimal strings to stay lossless
above 2^53.

The codec is bidirectional and lossless. It fails loudly on an unknown operator, an
empty `$and`/`$or`, an operator body without exactly one field, an unsupported field,
an empty `$param` name, and a proto oneof arm it does not recognise — so a new proto
arm cannot silently drop a filter. The full DSL is documented under `QueryFilter` in
`openapi.yml`. Round-trip and error-path tests live in
`internal/proto/commonpb/query_filter_test.go`; the REST wire is asserted in
`tests/e2e/business/prepared_query_rest_shape_test.go`.

This shape is shared by every filtered endpoint (and by the audit conditions in
EN-1548): new conditions extend it as new `$match`/`$gt`/… over new field names.

**REST prepared-query targets are `ACCOUNTS` / `TRANSACTIONS` only.** `LOGS` is a
valid direct `ListLogs` target but not (yet) a usable *prepared-query* target:
`query.Execute` hydrates only account/transaction data (`PreparedQueryCursor` has no
log result field), so a LOGS prepared query would execute to an empty cursor. The
REST create handler rejects it; if a LOGS query is created via gRPC/CLI, the REST
list/get path still echoes its true target faithfully.

## Recent changes

| Commit | Effect |
|--------|--------|
| `c1f79db80` (EN-1321) | Wire prepared queries into the per-attribute bloom registry. |
| `7662d2bae` | Monotonic-skip and probe-based `AndIterator` optimisations for filter execution. |
| `dedb005bc` (fix/376) | Fix protojson oneOf/enum encoding for prepared-query payloads. |
| EN-1465 | v2-aligned `QueryFilter` JSON codec (`$and`/`$match`/…), replacing the protojson leak; string target enum on `PreparedQuery`. |

## What's not (yet) here

- **History**: there is no public read API for prior versions of a prepared query. The audit chain has them, but client tooling does not surface them.
- **Cross-ledger aggregation**: see the design notes referenced above.
- **Plan stability across schema changes**: today a `SetMetadataFieldType` on a key used by a prepared query is permitted and the next execution will compile against the new schema. There is no "validate against pre-existing queries" check on schema changes.
