# HTTP API

## Overview

Ledger v3 exposes two types of APIs:

1. **HTTP REST API**: Public API for clients (documented here)
2. **gRPC API**: Inter-node communication and programmatic API (see [gRPC API](grpc-api.md))

## HTTP REST API

### Base URL

By default: `http://localhost:9000`

### API Versioning

All business routes are served under the `/v3/` prefix. Ops routes (`/health`, `/livez`, `/readyz`, `/clusterz`, `/_info`, `/debug/pprof/`) are unversioned and served at the root.

### Authentication

The server supports optional JWT/OIDC authentication with scope-based authorization. When enabled via `--auth-enabled`, all API requests must carry a valid Bearer token in the `Authorization` header. See [Authentication Guide](../../../../ops/authentication.md) for configuration details.

#### The route-to-scope contract is pinned by a test

`NewHandler` (`internal/adapter/http/handler.go`) expresses the scope a route requires **only** by
which `RequireScope(...)` `Group`/`Route` block lexically encloses the route's line. Nothing in the
router records that decision, so moving a route between two adjacent groups is a one-line diff that
silently changes its authorization.

`internal/adapter/http/route_scope_exhaustiveness_test.go` is what makes that binding a checked
contract. It holds a declared table of every `(method, pattern)` the router registers and the granular
scope each one requires, and it enforces the table two ways:

- **Reconciliation** against `chi.Walk`, in both directions — a route with no table row fails ("no
  scope decision"), and a table row matching no route fails ("stale row"). This is what stops a new
  route under the `/v3/_/…` subtree from silently inheriting `ledger:OpsRead`.
- **End-to-end probes** — for each guarded route, a token carrying the declared scope must be
  admitted, and a token carrying **every** other granular scope must be refused with 403. That proves
  exactly one scope opens each route.

**When you add a route, add its table row.** The test names the route and tells you what to add.

Two properties of this test are load-bearing and easy to break by "simplifying" it:

- **The middleware chain is not a usable oracle for the scope.** `chi.Walk` hands you the middleware
  slice, but `With(mw).Route(prefix, …)` attaches the middleware to the sub-mux's *handler* rather
  than to `Middlewares()`, so `GET /v3/_/signing-keys` reports no `RequireScope` while returning 401 to a
  tokenless request. All `RequireScope` closures also share one code pointer, so reflection cannot
  tell them apart. Only a real request through `ServeHTTP` reveals the scope.
- **Probes use granular scopes only, never the aggregate `ledger:read`.** The aggregate expands to
  both `ledger:LedgerRead` and `ledger:OpsRead`, which is precisely what hid the EN-1508 drift from
  every caller not using granular scopes.

The same asymmetry noted above has a second consequence worth knowing: because a `Route` sub-mux
refuses before routing the remainder of the path, `chi.Context.RoutePattern()` is only complete once
the request reaches the endpoint — so the test asserts the matched pattern on admitted probes only.

The gRPC analogue is `internal/adapter/auth/request_scope_exhaustiveness_test.go`, which gives the
same guarantee for the `Request` oneof.

### Response Format

#### Success

```json
{
  "data": { ... }
}
```

#### Error

```json
{
  "errorCode": "ERROR_CODE",
  "errorMessage": "Human readable error message"
}
```

### HTTP Status Codes

- `200 OK`: Request successful
- `201 Created`: Resource created
- `204 No Content`: Resource deleted successfully
- `400 Bad Request`: Invalid request
- `404 Not Found`: Resource not found
- `409 Conflict`: Conflict (ex: resource already exists)
- `503 Service Unavailable`: the server cannot serve the request right now but a retry can succeed — no leader elected, admission cache horizon exceeded, any `KindUnavailable` domain error (index still building, writes blocked on clock skew, node syncing), or an internal forwarding failure surfaced as gRPC `Unavailable` (always with `Retry-After` header)
- `500 Internal Server Error`: Server error

### Internal Errors and Sanitization

Business errors (validation, not-found, conflict, etc.) map to specific status codes and carry a machine-readable `errorCode` and a descriptive `errorMessage`, mirroring the gRPC adapter's `Describable` contract.

Three paths need correlated server-side diagnostics because the raw value can contain filesystem paths, wrapped Pebble/storage errors, or internal invariant strings:

1. **Panic recovery** (`jsonRecoverer`) — a panic in any handler.
2. **Unmapped errors** (`handleError` fallthrough → `writeInternalServerError`) — any error that is not a domain `Describable` or a known sentinel.
3. **`KindInternal` domain errors** — recognized internal failures whose status and reason are preserved. `INDEX_INCONSISTENT` and `COVERAGE_MISS` supply public messages (`index is inconsistent` and `preload coverage miss`) that omit internal identifiers and storage details, including when wrapped. Other recognized error presentations are unchanged.

Type-owned public details are selected through `domain.PublicErrorDetails` at the response boundary. Diagnostic `Error()` and `Metadata()` values remain unchanged, including the coverage failure context in the authoritative audit chain.

Every path logs the raw cause **server-side** with a `correlation_id` field. When the request span is recording, the log also carries `trace_id` and `span_id`, and the span records both the correlation ID and the error. Panic spans additionally carry the panic value and stack. Unmapped errors and panics remain sanitized identically to the gRPC adapter, so the client receives only a generic body:

```json
{
  "errorCode": "INTERNAL_ERROR",
  "errorMessage": "internal server error (correlation ID: <id>)"
}
```

The correlation ID reuses the request's `X-Request-Id` (Chi `RequestID`) when it is valid, so operators can grep the server logs for the exact ID a caller reports. Empty IDs, values longer than 128 bytes, invalid UTF-8, and values containing control characters are replaced with a generated token before they reach logs or responses. Adding a new persisted error path that reaches `handleError`'s fallthrough inherits this sanitization automatically; do not add a branch that serializes a raw non-domain error into the response body.

### Forwarded Writes and the Transport Seam

Every REST write is routed to the Raft leader. When the node serving the HTTP
request is not the leader, `RoutedController.getLeaderCtrl`
(`internal/bootstrap/controller_routed.go`) forwards it over gRPC, so the FSM
that produces the business error runs on a different process from the handler
that must map it to a status code.

The leader serialises the error faithfully — the kind selects the gRPC status
code and the reason plus metadata ride in an `errdetails.ErrorInfo` — but a
`*status.Error` carries no semantic classification of its own. Without a
decoding step, `handleError` and the bulk per-element mapper, which both
dispatch on the error boundary contract described below, fall through to the
sanitizer: the caller receives `500 INTERNAL_ERROR` with a correlation ID for
what is a plain 4xx, and the stable `errorCode` and the human-readable message
are both lost (EN-1636).

`grpcerr.NewConn` (`internal/adapter/grpcerr`) decorates the leader connection
so that decoding happens once, at the transport seam. It reconstructs exactly
two shapes:

- a status carrying a ledger-domain `ErrorInfo` — **whatever its code** —
  becomes an `*apierr.Remote`;
- a **bare** `codes.NotFound` — one carrying no `ErrorInfo` at all — becomes a
  `*commonpb.NotFoundError`, which the handler already maps to `404`. A
  `NotFound` stamped with another service's `ErrorInfo` is not bare: it is that
  service's typed failure and is left untouched, since rewriting it as a ledger
  `NotFoundError` would answer a foreign contract as this one.

Read the rest of the rule as the complement of those two rather than as a list
of excluded codes: **everything else passes through unchanged**. That covers
three groups.

- A **bare** `codes.Canceled`, because the cursor layer keys end-of-stream
  detection off it and normalises it to `io.EOF`. The ledger `ErrorInfo` is
  still decoded first: no `ErrorKind` maps to `codes.Canceled`, so a reason
  this build knows arriving under it is a contradiction, and answering the
  status before the decode would exempt the one code with no legitimate reason
  from the mismatch policy below and hand the peer's message to the client.
  Pagination is unaffected either way — a reconstructed value keeps answering
  `GRPCStatus()` with the received `Canceled` status.
- A **bare** status of any code — no ledger `ErrorInfo`, so no reason to
  recover. A bare `codes.Unavailable` already reaches the right outcome
  (`handleError` answers that code with `503` + `Retry-After` on its own);
  bare `Internal`, `Unknown` and `DeadlineExceeded` are server faults or
  transport conditions; and a bare `codes.Unauthenticated` or
  `codes.PermissionDenied` from the leader denotes a cluster-secret failure
  *between nodes*, not a caller credential problem, so it correctly stays a
  `500` rather than telling the caller to fix something it does not control.
- An `ErrorInfo` stamped with another service's domain, left for that
  service's client.

Note the qualifier on the second group. `codes.Unavailable` and
`codes.Internal` are exactly the codes a `KindUnavailable` or `KindInternal`
`Describable` is *sent* under, so those arrive **with** an `ErrorInfo` and are
reconstructed like any other: `INDEX_BUILDING`, `BALANCE_NOT_PRELOADED` and
`COVERAGE_MISS` all keep their reason across the hop. Only the bare form passes
through.

The HTTP layer needs no status-code branch of its own: both mappers read the
failure through `apierr.Describe`, which answers identically for a locally
raised error and a decoded one, so a follower answers with the same status and
`errorCode` as the leader and the bulk path is repaired by the same change.

The decorator wraps the connection rather than the generated client's 37
methods (11 of them server-streaming), because six of the `BucketGrpcClient`
list methods return a lazy cursor — `ListLedgers`, `ListTransactions`,
`ListAccounts`, `ListLogs`, `ListAuditEntries`, `ListIndexes` — and surface the
leader's error from a later `Recv()`, after the method itself already returned
`nil`; and because every method generated in future routes through `Invoke` or
`NewStream` regardless.

Forwarded **reads** cross the same seam, but only while a node is syncing
(`readCtrl` falls back to the leader on `ErrNodeSyncing`/`ErrNotLeader`), so
they are not reachable deterministically from a healthy cluster.

### The Error Boundary Contract

`internal/adapter/apierr` is the contract every business-facing surface reads a
failure through: `handleError`, the bulk per-element mapper and `ledgerctl`.
`apierr.Describe(err)` returns a `Descriptor` — semantic `Kind`, stable
`Reason`, client-safe `Message`, structured `Metadata` — and normalises the two
provenances a failure can have:

| Provenance | Classification |
|---|---|
| Raised locally (a `domain.Describable` from admission, the FSM, a read path) | `domain.Kind(d)`, a pure function of the reason |
| Decoded from a peer (`*apierr.Remote`, produced only by `grpcerr`) | the kind the wire carried |

The order matters. An `*apierr.Remote` is checked first, because a reason from a
newer server is absent from this build's `ErrorReason` enum: re-deriving its
kind would yield `KindInternal` and answer `500` for what the sender classified
as a caller error.

`Message` and `Metadata` are the client-safe presentation, not the diagnostic
identity: a locally raised failure is read through `domain.PublicErrorDetails`,
so a type that owns a separate public presentation (EN-1623) reaches a surface
redacted, and `Descriptor.PublicOverride` tells the surface to render `Message`
in place of the wrapped chain that presentation exists to withhold. A decoded
failure needs no such selection — the sender applied it before serialising — so
`PublicOverride` is false for an `*apierr.Remote` and the consumer keeps
rendering its own outer context, exactly as it did before the hop.

`apierr` imports `internal/domain` and nothing else, so HTTP reads a decoded
failure without linking any gRPC detail. The reverse direction is a layering
violation: no package under `internal/domain`, `internal/application/admission`,
`internal/infra/state`, `internal/infra/plan` or `internal/infra/preload` may
import it, which `scripts/check-repo-invariants` enforces. A decoded failure
must never be raised by admission, the FSM or order processing, nor persisted,
frozen, audited or hashed — the audit chain hashes an error's `Error()` string,
and a message that varies with a peer's build would break the chain.

### Two Classification Axes

A failure crossing a gRPC hop carries two independent axes.

| Axis | What it answers | Where it lives |
|---|---|---|
| Semantic `ErrorKind` | the client: an HTTP status, a CLI message | `apierr.Descriptor.Kind` |
| gRPC status code | transport behaviour: retry and hop semantics | the original `*status.Status`, kept by the decoder's carrier through `GRPCStatus()` |

The mapping between them is lossy in both directions, which is why the decoder
preserves the received status verbatim instead of rebuilding it from the kind.
`grpcerr.CodeForKind` sends both `KindConflict` and `KindPrecondition` as
`codes.FailedPrecondition`, so a code cannot name a kind; and a reason from a
newer build carries a kind this enum cannot derive at all. The axes may also
disagree deliberately: the removed `READ_INDEX_NOT_CAUGHT_UP` was semantically
`KindUnavailable` but travelled as `codes.FailedPrecondition`, because
`actions.GRPCRetryPolicy` retries `codes.Unavailable` fifty times at 0.2s and
the semantic code would have turned a read-index lag into a ten-second
client-side hang. No reason needs that treatment today.

**Kind derivation is reason-first.** `grpcerr` derives the `ErrorKind` from the
reason when this build's `ErrorReason` enum knows it, and falls back to the
status code otherwise. Neither source suffices alone: `grpcerr.CodeForKind`
sends both `KindConflict` and `KindPrecondition` as
`codes.FailedPrecondition`, so code-only derivation answers `400` for a
`KindConflict` reason (such as `LEDGER_DELETED`) where the leader answers
`409`; and reason-only derivation collapses a reason from a newer server to
`KindInternal`, turning a caller mistake into a `500`.

For an unknown reason the exact upstream code is preserved across a second hop,
and the code still supplies the classification where it has one — an unknown
reason carried by `codes.AlreadyExists` stays `KindAlreadyExists` and `409`,
rather than collapsing to `500`.

### Reason/Wire-Code Mismatch Policy

A reason this build knows is a reason whose legitimate wire codes it knows too.
The allowed set is `grpcerr.CodeForKind(KindForReason(reason))` — today exactly
one code, because every enum reason reaches the wire through
`describableToGRPCStatus`, which derives the status from `CodeForKind` and
nothing else. The check stays reason-keyed rather than kind-keyed so a reason
that must travel under a second code can be widened on its own, as the removed
`READ_INDEX_NOT_CAUGHT_UP` did, without relaxing the kind mapping the encoder
shares. The three reasons the server hand-builds an `ErrorInfo` for
(`EXTERNAL_SERVICE_ERROR`, `RAFT_NODE_NOT_IN_CLUSTER`,
`RAFT_NODE_REMOVAL_COMMITTED`) are not enum members, so they take the
unknown-reason path and are never validated here.

A reason arriving under a code outside its allowed set is a **protocol fault**,
not a business outcome — the peer is not speaking this contract. The decoder
returns an `*apierr.InvalidWireError`. It implements neither the boundary
contract nor `GRPCStatus()`, so a surface that forgets the case still degrades
to its internal-error path rather than answering the pair as a business
outcome — but each consumer branches on `apierr.InvalidWire` explicitly so the
guarantee does not rest on that method set:

| Surface | Client-visible answer |
|---|---|
| REST, unitary (`handleError`) | `500 INTERNAL_ERROR` + correlation ID, logged server-side |
| REST, bulk element (`writeBulkResponse`) | element `errorCode: INTERNAL_ERROR` + correlation ID, logged server-side |
| gRPC | `codes.Unknown` + correlation ID |
| `ledgerctl` (`FormatGRPCError`) | the invalid-pair message: the reason and codes, which are this build's own enum values |

The received message and metadata are dropped on every surface rather than
answered — without that, a contradicting `codes.InvalidArgument` pair would be
echoed to the caller as a trusted `400`, and the peer's free-form message would
be presented as though this build had produced it. The bulk element carries the
same generic correlated description as the unitary path, so the claimed reason
never reaches `errorCode` or `errorDescription`.

An **unknown** reason cannot be validated — this build has no policy for it —
so its reason, message, metadata and exact status are preserved verbatim.

"Unknown" means the `ErrorReason` enum does not declare the name, which the
decoder reads from `domain.LookupReasonCode`'s second result rather than from
the `ERROR_REASON_UNSPECIFIED` zero value `domain.ReasonCode` returns for it.
The two are not the same condition: `UNSPECIFIED` is a name this build *does*
declare, and one no ledger error emits — every `Describable`'s `Reason()` names
a real reason, so `describableToGRPCStatus` cannot stamp it. Its allowed set is
therefore empty and the pair is rejected under **every** code, including the
`codes.Internal` that `CodeForKind(KindForReason(UNSPECIFIED))` would otherwise
license. Keying the passthrough on the zero value instead let the sentinel take
the forward-compatibility path, which answered a client `400` with
`errorCode: "UNSPECIFIED"` and the peer's own message.

### Retry-After Header

The `Retry-After` header is used to indicate when a client should retry a request after receiving a `503 Service Unavailable` response. Every `503` the adapter emits carries it — `503` is by definition the retry-now class.

#### When It's Returned

`handleError` (`internal/adapter/http/error_handler.go`) maps four error families to `503` + `Retry-After`:

1. **No leader** (`errorCode: NO_LEADER`) — the Raft cluster has no elected leader: an election is in progress, a partition or insufficient nodes prevent a quorum, or the cluster is still starting up.

2. **Admission cache horizon exceeded** (`errorCode: CACHE_HORIZON_EXCEEDED`) — admission predicted the proposal would outlive its preload (2+ cache rotations between propose and apply); retry against a fresher admission snapshot.

3. **`KindUnavailable` domain errors** (`errorCode` = the error's `Reason()`) — the whole kind shares the contract: an index still building, writes blocked on clock skew, a node syncing. Anything `kindToHTTPStatus` maps to `503` gets the header.

4. **gRPC `Unavailable` statuses** (`errorCode: UNAVAILABLE`) — an internal forwarding failure surfaced by the read path: a forwarded stream torn down mid-transfer (peer connection churn during a syncing follower's leader fallback), or a peer connection missing from the pool. The original transport message is preserved in `errorMessage`.

#### Response Format

- **HTTP Status Code**: `503 Service Unavailable`
- **Header**: `Retry-After: 1` (seconds)
- **Response Body** (the `errorCode` discriminates the family, e.g. no leader):
```json
{
  "errorCode": "NO_LEADER",
  "errorMessage": "No Leader"
}
```

#### Header Value

The `Retry-After` header value is set to `1` second, indicating that clients should wait at least 1 second before retrying the request.

**Note**: This is a conservative value. In practice, leader elections typically complete within a few hundred milliseconds, but the 1-second delay ensures the cluster has time to stabilize.

#### Client Behavior

Clients should:

1. **Respect the header**: Wait at least the specified duration before retrying
2. **Implement exponential backoff**: Increase wait time between retries to avoid overwhelming the cluster
3. **Set a maximum retry limit**: Avoid infinite retry loops
4. **Handle gracefully**: Show appropriate error messages to users during leader elections

#### Best Practices

- **Read operations**: Can be served by any node (if implemented), avoiding leader dependency
- **Write operations**: Must go through the leader, so will fail during leader elections
- **Idempotency**: Ensure write operations are idempotent to safely retry after leader election
- **Monitoring**: Track `503` responses to monitor cluster health and leader election frequency

## Main Endpoints

### Ledgers

#### Create a Ledger

```http
POST /v3/{ledgerName}
Content-Type: application/json

{
  "metadata": {
    "key": "value"
  }
}
```

**Response**:
```json
{
  "data": {
    "name": "my-ledger",
    "id": 1,
    "metadata": {
      "key": "value"
    },
    "createdAt": "2024-01-01T00:00:00Z"
  }
}
```

#### Get a Ledger

```http
GET /v3/{ledgerName}
```

**Response**:
```json
{
  "data": {
    "name": "my-ledger",
    "id": 1,
    "metadata": {},
    "createdAt": "2024-01-01T00:00:00Z"
  }
}
```

#### List All Ledgers

```http
GET /v3/
```

**Response**:
```json
{
  "data": [
    {
      "name": "ledger1",
      "id": 1,
      "metadata": {},
      "createdAt": "2024-01-01T00:00:00Z"
    },
    {
      "name": "ledger2",
      "id": 2,
      "metadata": {},
      "createdAt": "2024-01-01T00:00:00Z"
    }
  ]
}
```

#### Delete a Ledger

```http
DELETE /v3/{ledgerName}
```

**Response**: `204 No Content`

### Transactions

#### Create a Transaction

```http
POST /v3/{ledgerName}/transactions
Content-Type: application/json
Idempotency-Key: optional-key

{
  "postings": [
    {
      "source": "world",
      "destination": "bank",
      "amount": 100,
      "asset": "USD"
    }
  ],
  "metadata": {
    "description": "Payment"
  },
  "reference": "optional-reference"
}
```

**Query Parameters**:
- `dryRun=true`: Validate without applying

**Response**:
```json
{
  "data": {
    "transaction": {
      "id": 1,
      "postings": [...],
      "timestamp": "2024-01-01T00:00:00Z",
      "metadata": {...},
      "reference": "optional-reference"
    },
    "accountMetadata": {...}
  }
}
```

#### Save Transaction Metadata

```http
POST /v3/{ledgerName}/transactions/{transactionId}/metadata
Content-Type: application/json

{
  "key1": "value1",
  "key2": "value2"
}
```

**Response**: `204 No Content`

#### Delete Transaction Metadata

```http
DELETE /v3/{ledgerName}/transactions/{transactionId}/metadata/{key}
```

**Response**: `204 No Content`

#### Bulk Operations

```http
POST /v3/{ledgerName}/bulk
Content-Type: application/json

[
  {
    "action": "CREATE_TRANSACTION",
    "ik": "idempotency-key",
    "data": {
      "postings": [...]
    }
  },
  {
    "action": "ADD_METADATA",
    "data": {
      "targetType": "TRANSACTION",
      "targetId": 1,
      "metadata": {...}
    }
  }
]
```

**Query Parameters**:
- `continueOnFailure=true`: When the request is accepted, keep processing subsequent elements after a per-element **business** failure (validation / not-found / conflict / precondition / permission) instead of aborting. Business failures surface as `errorCode` on each element and the overall status stays `200`. Request-level failures (malformed body, missing scope, oversized) and processing-time infra/retryable failures (`ErrNoLeader`, cache-horizon exceeded, `KindInternal`, `KindResourceExhausted`, `KindUnavailable`) still surface as non-2xx (`4xx`, `429`, `503`, `500`) regardless of this flag — see the `POST /v3/{ledgerName}/bulk` operation in `openapi.yml` for the full status matrix.

Bulk internal failures are logged and stamped on the recording request span
with a validated correlation ID, including sequential and atomic apply failures.
Unknown errors expose only `internal server error (correlation ID: <id>)` in
`errorDescription`; the existing `ERROR` code and bulk envelope stay unchanged.
Typed internal failures keep their reason and use their type-owned public
presentation when provided; actionable Numscript diagnostics remain visible.
The `continueOnFailure` rollup and retry semantics are unaffected.

- `atomic=true`: Execute atomically (all or nothing) - not yet supported

### Account Metadata

#### Save Account Metadata

```http
POST /v3/{ledgerName}/accounts/{address}/metadata
Content-Type: application/json

{
  "key1": "value1",
  "key2": "value2"
}
```

**Response**:
```json
{
  "data": {
    "address": "account-address",
    "metadata": {
      "key1": "value1",
      "key2": "value2"
    }
  }
}
```

#### Delete Account Metadata

```http
DELETE /v3/{ledgerName}/accounts/{address}/metadata/{key}
```

**Response**: `204 No Content`

### Audit

Audit reads expose the tamper-evident audit trail over HTTP (mirroring the gRPC
`BucketService.ListAuditEntries` / `GetAuditEntry`). Audit reads are
**bucket-wide, not ledger-scoped** — a single proposal can touch several ledgers
— so these routes sit at the top level (no `{ledgerName}`) and ledger scope is
expressed as a filter condition. Both require the `ledger:AuditRead` scope.

#### List Audit Entries

```http
GET /v3/_/audit-entries?pageSize=100&after=42&reverse=false&filter=outcome%20%3D%3D%20failure
```

Query parameters:
- `pageSize`: max entries (default 100, capped at 1000)
- `after`: audit sequence to start after (exclusive, opaque cursor)
- `reverse`: `true` iterates newest-first
- `filter`: a filter expression restricted to bare audit fields (`outcome`,
  `ledger`, `seq`, `proposal_id`, `timestamp`, `log_seq`, `caller_subject`,
  `order_type`), using the same grammar as `ledgerctl audit list --filter` (e.g.
  `outcome == failure`, `ledger == main`,
  `order_type in (create_transaction, revert_transaction)`). The fields are
  written without any prefix and resolved against the audit query target
  (EN-1549 — this replaced the old `audit[...]` namespaced syntax, a breaking
  change with no backward compatibility); bare `timestamp` and `ledger` resolve
  to the audit condition only on the audit target, which is why audit fields are
  valid on this endpoint alone. This is the shared `filterexpr` DSL — **not** the
  JSON `QueryFilter` DSL used by prepared queries, which cannot represent audit
  conditions. Filters that need indexed fields are compiled from an audit-index
  snapshot whose Raft certificate covers the fixed main-store snapshot horizon;
  matching `AuditEntry` values are loaded from that same main snapshot.
  Unfiltered reads and sequence-only bounds scan the authoritative audit zone
  directly and do not wait for the asynchronous audit index. A disabled,
  rebuilding, cancelled, or deadline-bound required index fails explicitly
  rather than returning a partial page. The consistency guarantee matches the
  gRPC surface.

**Response**: `{ "data": [ AuditEntry, ... ] }` (list omits per-order `items`).

#### Get Audit Entry

```http
GET /v3/_/audit-entries/{sequence}
```

Returns a single `AuditEntry` by its global sequence, with per-order `items`
populated. Missing sequences return `404`.
### Indexes

Per-ledger and bucket-wide index management. See `openapi.yml` for the exhaustive schema; the flow below covers the create → observe → drop cycle.

**Canonical form**: index identifiers are exchanged as opaque strings produced by `indexes.Canonical` (`internal/domain/indexes/id.go`) — e.g. `metadata:TARGET_TYPE_ACCOUNT:color`, `tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP`, `log_builtin:LOG_BUILTIN_INDEX_DATE`. Only metadata indexes carry a target + key; builtin indexes name a proto enum value.

#### Create an index

```http
POST /v3/{ledgerName}/indexes
Content-Type: application/json

{
  "id": "metadata:TARGET_TYPE_ACCOUNT:color"
}
```

Returns `201 Created` once the FSM has queued the backfill. Poll `GET /v3/{ledgerName}/indexes/{canonicalId}/status` and wait for `currentVersion > 0` with `pendingVersion == 0` before running queries that need it. After a *retype*, the pre-retype keyspace stays live, so capture `currentVersion` before issuing the change and wait until it has advanced past that value with `pendingVersion == 0` (per-replica version numbers are local and not comparable to `forwardEncodingVersion`).

#### List indexes on a ledger

```http
GET /v3/{ledgerName}/indexes
```

Returns the `Index` registry entries owned by the ledger.

#### Get a single index

```http
GET /v3/{ledgerName}/indexes/{canonicalId}
```

Returns the `Index` registry entry (id, ledger, created_at, forward_encoding_version).

#### Get per-replica index status

```http
GET /v3/{ledgerName}/indexes/{canonicalId}/status
```

Returns the `IndexEntry` — the registry entry joined with the backfill cursor and the per-replica `IndexVersionState` (current + pending version). Use this to poll for backfill completion.

#### Inspect a metadata index

```http
GET /v3/{ledgerName}/indexes/{canonicalId}/inspect?mode=summary
```

Scans the index and returns distinct values, facets, or a summary (`mode=distinctValues|facets|summary`). Only metadata indexes are inspectable; builtin canonicals return `400`.

#### Drop an index

```http
DELETE /v3/{ledgerName}/indexes/{canonicalId}
```

Returns `204 No Content` once the FSM has committed the drop.

#### Bucket-wide index reads

Cluster-wide observability (registry entries whose owning ledger is empty, aggregated indexer progress):

```http
GET /v3/_/indexes                        # ?scope=all (default) | bucket
GET /v3/_/indexes/status?ledger=         # aggregate: LastIndexedSequence, Lag, IndexFileSize
GET /v3/_/indexes/{canonicalId}          # single bucket-scoped Index entry
GET /v3/_/indexes/{canonicalId}/status   # single bucket-scoped IndexEntry
```

The bucket-scoped single-index routes are a hook — no production write hits `SubAttrIndex` with an empty ledger today. The audit index (cross-ledger by nature) lives in a dedicated read-store keyspace and will be exposed on `GET /v3/audit-entries` per EN-1481, not through this hook.

### Cluster

Cluster operations are available via the `ClusterService` gRPC API (port 8888) and the `ledgerctl cluster` CLI commands.

**Available RPCs**:
- `GetClusterState`: Current Raft cluster state (leader, voters, learners)
- `GetDiskUsage`: Local node disk usage
- `GetNodeTime`: Node's physical clock time
- `TransferLeadership`: Transfer Raft leadership to another node
- `Backup`: Point-in-time backup as tar archive
- `AddLearner`: Add a non-voting node to the cluster
- `PromoteLearner`: Promote a learner to full voter

### Health

#### Health Check

```http
GET /health
```

**Response**: `200 OK` (no body)

### Debug Endpoints

The API exposes pprof endpoints for debugging and profiling:

- `GET /debug/pprof/` - Index page
- `GET /debug/pprof/profile` - CPU profile
- `GET /debug/pprof/heap` - Heap profile
- `GET /debug/pprof/goroutine` - Goroutine dump
- `GET /debug/pprof/trace` - Execution trace

## gRPC API

The gRPC API provides a programmatic interface for interacting with the ledger cluster. It uses a unified `BucketService` with the `Apply` method for all write operations.

**Key features:**
- Unified `Apply` method for all write operations (create ledger, transactions, metadata)
- Batch operations support
- Automatic request forwarding from followers to leader
- Idempotency key support

For detailed documentation, examples, and client code, see [gRPC API](grpc-api.md).

## Service Interfaces

### Controller

The main interface for read and write operations:

```go
// internal/application/ctrl/controller.go
type Controller interface {
    Apply(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error)
    GetAccount(ctx context.Context, ledger string, address string) (*commonpb.Account, error)
    GetTransaction(ctx context.Context, ledger string, txID uint64) (*commonpb.Transaction, error)
    GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error)
    ListLedgers(ctx context.Context) (data.Cursor[*commonpb.LedgerInfo], error)
    ListTransactions(ctx context.Context, ledger string, pageSize uint32, afterTxID uint64) (data.Cursor[*commonpb.Transaction], error)
}
```

### Routed Controller

The `RoutedController` wraps the `Controller` to handle leader forwarding:
- **Write operations** (`Apply`): Forwarded to the leader via gRPC if the node is a follower
- **Read operations** (`GetAccount`, `GetTransaction`, etc.): Served locally from the Pebble store

## Request Forwarding

### Principle

When a node receives a write request but is not the leader:

1. The node checks if it is leader
2. If not leader, it identifies the leader
3. It forwards the request to the leader via gRPC
4. The leader processes and returns the response

### Implementation

The `RoutedController` checks if the node is the leader. If not, it forwards the request to the leader's service port via gRPC:

```go
// Simplified from internal/bootstrap/controller_routed.go
func (r *RoutedController) Apply(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
    if r.isLeader() {
        return r.localController.Apply(ctx, requests...)
    }
    // Forward to leader via ServiceConnectionPool
    leaderClient := r.getLeaderClient()
    resp, err := leaderClient.Apply(ctx, &servicepb.ApplyRequest{Envelopes: servicepb.UnsignedEnvelopes(requests...)})
    return resp.Logs, err
}
```

## Response serialization

Two encoders serve HTTP response bodies, and the choice is **not** a matter of taste:

| Writer | Encoder | Use for |
|---|---|---|
| `writeOK` / `writeOKChecked` | sonic (`internal/adapter/json`) | Anything whose type has a custom `MarshalJSON`, and all hand-written DTOs |
| `writeProtoOK` / `writeProtoListOK` | `protojson` | Proto messages with **no** custom `MarshalJSON` |

**The rule: when a type has a hand-written `MarshalJSON`, that method is the public contract.** Route it through `writeOKChecked`. `protojson` works off protobuf reflection and ignores `json.Marshaler`, so sending such a type through it silently discards the intended shape.

The camelCase convention cannot arbitrate between the two — both encoders satisfy it. The deciding fact is whether a marshaller exists. `internal/adapter/http/encoder_contract_test.go` enforces this in both directions.

Prefer `writeOKChecked` over `writeOK` when the marshaller can fail (e.g. it marshals a metadata map): `writeOK` streams, so a mid-encode failure appends an error object to an already-committed 200.

This was EN-1622: the transactions list and single-log routes sent marshaller-carrying types through `protojson` and shipped `amount: {"v0":"12345"}`, `timestamp: {"data":"1786540255458491"}`, base64 hashes where the contract is hex, and quoted numeric ids.

**Before adding a `MarshalJSON` to a proto type, check the blast radius.** `cmd/ledgerctl/cmdutil/output.go` also prefers a custom marshaller when one exists, so adding one changes CLI output too — and `misc/operator` parses `ledgerctl indexes list --json` with a struct that hard-codes the protojson shape, in a separate Go module that a root `go build ./...` never compiles. For types that need a clean HTTP shape without moving the CLI, use an HTTP-local response DTO instead.

### Ledger-log JSON output

Single-log responses, ledger-log lists, prepared-query `logData`, JSON event
sinks, and `ledgerctl` JSON/YAML output share the same nested `LedgerLog`
encoding: `type`, `data`, and optional `date` and `id`. As in Ledger v2, `data`
contains the payload directly, without a protobuf oneof field-name wrapper.
For example, a created transaction has
`{"type":"NEW_TRANSACTION","data":{"transaction":{...}}}`. The unique `type`
identifies the payload, so clients need only one discriminator dispatch.

| `type` | Payload in `data` |
|--------|-------------------|
| `NEW_TRANSACTION` | Created transaction and optional account metadata |
| `REVERTED_TRANSACTION` | Original transaction ID and compensating transaction |
| `SET_METADATA` | Metadata target and values |
| `DELETE_METADATA` | Metadata target and key |
| `SET_METADATA_FIELD_TYPE` | Metadata field type assignment |
| `REMOVED_METADATA_FIELD_TYPE` | Metadata field type removal |
| `ORDER_SKIPPED` | Reason and optional context |
| `FILL_GAP` | Fill-gap payload |
| `CREATE_INDEX` | Index creation payload |
| `DROP_INDEX` | Index removal payload |
| `ADDED_ACCOUNT_TYPE` | Account type addition payload |
| `REMOVED_ACCOUNT_TYPE` | Account type removal payload |
| `UPDATED_DEFAULT_ENFORCEMENT_MODE` | Default enforcement mode update payload |

Metadata logs use `targetType` (`ACCOUNT` or `TRANSACTION`) and `targetId`: a
string account address or an unsigned integer transaction ID, including zero.
EN-1790 replaces the earlier unreleased v3 wrappers and shared
`SET_METADATA` fallback discriminator.

The alignment with v2 is limited to the ledger-log envelope and metadata target
shape. V3 retains typed metadata, colored volumes, and
`revertedTransactionId` plus `revertTransaction` for reversals. System logs and
events retain their enclosing global-log structure. These JSON changes do not
change protobuf messages, persisted data, or audit hashes.

This contract defines an output projection. The internal ledger-log type has
no custom JSON decoder. Metadata integers are emitted as JSON numbers,
and null-valued keys remain present. The emitted JSON does not retain
positive-integer signedness, the distinction between datetime values and
strings, or `NullValue.original`. It is not an audit replay or backup format.

## OpenAPI Documentation

The OpenAPI specification is available in `openapi.yml`. It can be used for:

- Generate client SDKs (using tools like openapi-generator)
- Generate interactive documentation
- Validate requests

### Visualization

Use a tool like Swagger UI or Redoc to visualize the API:

```bash
# With Swagger UI
docker run -p 8080:8080 -e SWAGGER_JSON=/openapi.yml -v $(pwd)/openapi.yml:/openapi.yml swaggerapi/swagger-ui

# With Redoc
npx @redocly/cli preview-docs openapi.yml
```

## Next Steps

To deepen your understanding:

1. [gRPC API](grpc-api.md) - Programmatic API for clients and CLI
2. [General Architecture](../../overview.md) - How the APIs integrate
3. [Data Flows](../../data-flows.md) - Detailed flows of requests
4. [Development](../../../contributing/development.md) - Add new endpoints
