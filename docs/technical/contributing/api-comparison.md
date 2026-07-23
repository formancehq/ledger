# API Comparison: ledger vs github.com/formancehq/ledger

This document compares the POC's API with the original Formance ledger API and documents missing features.

> **URL prefix.** All business endpoints in this POC are served under the
> `/v3` prefix (e.g. `POST /v3/{ledgerName}/transactions`). Ops endpoints
> (`/health`, `/livez`, `/readyz`, `/clusterz`, `/_info`, `/debug/pprof`) are
> intentionally unversioned. The original ledger's `/v2` is **not** preserved
> by this POC — there is no compatibility shim.

## Summary

| Feature | POC | Original | Notes |
|---------|-----|----------|-------|
| **Transactions (Write)** |
| Create transaction (postings) | ✅ | ✅ | |
| Create transaction (numscript) | ✅ | ✅ | |
| Revert transaction | ✅ | ✅ | |
| Revert with `force` | ✅ | ✅ | |
| Revert with `atEffectiveDate` | ✅ | ✅ | |
| Revert by reference | ❌ | ❌ | Not supported — only numeric `id` targets are accepted |
| Set/delete tx metadata by reference | ❌ | ❌ | Not supported — only numeric `targetId` is accepted |
| Create transaction with `force` | ✅ | ✅ | Bypasses balance checks |
| **Transactions (Read)** |
| Get transaction by ID | ✅ | ✅ | |
| List transactions | ⚠️ | ✅ | gRPC stream only (no HTTP handler); supports `source`/`destination` address filtering, `reference`, `startTime`/`endTime`, and `id` via prepared queries |
| **Metadata** |
| Save account metadata | ✅ | ✅ | |
| Delete account metadata | ✅ | ✅ | |
| Save transaction metadata | ✅ | ✅ | |
| Delete transaction metadata | ✅ | ✅ | |
| Save ledger metadata | ✅ | ❌ | New in v3 |
| Delete ledger metadata | ✅ | ❌ | New in v3 |
| **Bulk** |
| Bulk CREATE_TRANSACTION | ✅ | ✅ | |
| Bulk ADD_METADATA | ✅ | ✅ | |
| Bulk REVERT_TRANSACTION | ✅ | ✅ | |
| Bulk DELETE_METADATA | ✅ | ✅ | |
| Bulk atomic | ✅ | ✅ | System-level atomicity (cross-ledger) |
| Bulk continueOnFailure | ✅ | ✅ | |
| **Ledger** |
| Create ledger | ✅ | ✅ | HTTP + gRPC accept the full model: `initialSchema`, `accountTypes` (name/pattern/persistence/segmentTypes), `defaultEnforcementMode` |
| Create mirror ledger | ✅ | ❌ | HTTP or PostgreSQL source |
| Promote mirror ledger | ✅ | ❌ | Mirror → Normal mode |
| Delete ledger | ✅ | ✅ | |
| Get ledger | ✅ | ✅ | |
| List ledgers | ✅ | ✅ | |
| **Account Types** |
| Add account type | ✅ | ❌ | Full model over HTTP + gRPC: name, pattern, `persistence`, `segmentTypes` |
| List account types | ✅ | ❌ | List all types for a ledger (HTTP + gRPC; includes persistence + segmentTypes) |
| Get account type | ✅ | ❌ | Get details of a specific type |
| Remove account type | ✅ | ❌ | Remove a type from a ledger |
| **Accounts (Read)** |
| Get account | ✅ | ✅ | Includes volumes per asset |
| List accounts | ✅ | ✅ | Supports rich boolean filter (metadata equality/range/existence, address) with schema validation and cursor pagination |
| Get account balances | ⚠️ | ✅ | Included in account volumes |
| Get account volumes | ✅ | ✅ | Returns input/output/balance per asset |
| Analyze accounts | ✅ | ❌ | Suggest Chart of Accounts from address patterns |
| Aggregate volumes | ✅ | ✅ | Per-asset aggregated volumes for filtered accounts (direct RPC, no prepared query needed) |
| **Logs** |
| List logs | ✅ | ✅ | gRPC stream, supports `--filter 'ledger == "foo"'` for per-ledger listing (opt-in index) |
| **Import/Export** |
| Import logs | ⚠️ | ✅ | Interface defined but not implemented |
| Export logs | ⚠️ | ✅ | Interface defined but not implemented |
| **Idempotency** |
| Idempotency key | ✅ | ✅ | |
| **Reference Uniqueness** |
| Unique reference validation | ✅ | ✅ | Per-ledger uniqueness, HTTP 409 on conflict |
| Skip-on-conflict opt-in | ✅ | ❌ | Per-entry `skippableReasons: ["TRANSACTION_REFERENCE_CONFLICT"]` on the bulk endpoint → the entry's `BulkResult.data` carries an `OrderSkippedResponse` shape instead of the normal Transaction. The unitary POST endpoint intentionally does NOT expose the opt-in (a single-tx caller can catch the 4xx directly); gRPC clients can set `LedgerApplyRequest.skippable_reasons` on any Apply. |
| **Numscript Library** |
| Save numscript (versioned) | ✅ | ❌ | Per-ledger, immutable append-only, explicit full semver (e.g. "1.0.0"). Re-saving a version returns `NUMSCRIPT_VERSION_ALREADY_EXISTS` |
| Get numscript (by version) | ✅ | ❌ | Per-ledger, `?version=1.0.0`; empty/`latest` = greatest stored semver; partial selectors (`1`, `1.2`) allowed |
| List numscripts | ✅ | ❌ | Per-ledger, greatest version of each |
| List numscript versions | ✅ | ❌ | Per-ledger, `GET .../numscripts/{name}/versions`, current latest + every stored version |
| **Audit Log** |
| Audit log (success + failure) | ✅ | ❌ | Replicated via Raft, stored in Pebble |
| List audit entries | ✅ | ❌ | `GET /v3/_/audit-entries` (HTTP) + gRPC stream. Bucket-wide; `pageSize`/`after`/`reverse` + a bare-audit-field filter expression (`outcome`, `ledger`, `seq`, `proposal_id`, `timestamp`, `log_seq`, `caller_subject`, `order_type`, resolved against the audit query target — EN-1549 replaced the old `audit[...]` namespaced syntax; textual form only, audit has no structured JSON form — see [Filter input formats](#filter-input-formats-dual-format-contract-en-1511)) |
| Get audit entry by sequence | ✅ | ❌ | `GET /v3/_/audit-entries/{sequence}` (HTTP) + gRPC. Populates per-order `items` |
| Audit log disable/enable | ❌ | ❌ | Not implemented |
| **Error Handling** |
| Structured gRPC error codes | ✅ | ✅ | BusinessError with ErrorInfo details |
| **Security** |
| Request signing (Ed25519) | ✅ | ❌ | Envelope pattern with signed_payload |
| Dynamic key management | ✅ | ❌ | Register/revoke/list keys via gRPC API (bootstrap: first key unsigned) |
| Require signatures | ✅ | ❌ | Optional enforcement via `signing require` API call |
| **Maintenance** |
| Maintenance mode | ✅ | ❌ | Block all writes, Raft-replicated flag, dual check (admission + FSM) |
| **Store Operations** |
| Store metrics | ✅ | ❌ | Pebble storage metrics |
| Store integrity check | ✅ | ❌ | Hash chain + derived data verification |
| Store backup | ✅ | ❌ | Point-in-time Pebble backup as tar archive |
| Index status | ✅ | ❌ | Read index builder progress (lag, file size) |
| **Chapters** |
| Close chapter | ✅ | ❌ | Two-step close: CloseChapter → SealChapter |
| Seal chapter (background) | ✅ | ❌ | Background sealer computes BLAKE3 sealing hash |
| List chapters | ✅ | ❌ | gRPC streaming |
| Transaction receipts (JWT) | ✅ | ✅ | HMAC-SHA256 JWT receipts with chapter ID; surfaced on GetTransaction over both transports (`data.receipt`, empty when none) |
| Receipt-based revert | ✅ | ❌ | Revert using JWT receipt (avoids server-side lookup) |
| Chapter crash recovery | ✅ | ❌ | Automatic recovery for both crash windows |
| Archive chapter | ✅ | ❌ | Two-step archive: ArchiveChapter → ConfirmArchiveChapter with cold storage export |
| Store restore | ✅ | ❌ | Upload backup, validate, preview, finalize (--restore mode) |
| **Prepared Queries** |
| Create prepared query | ✅ | ❌ | Reusable parameterized filter queries |
| Update prepared query | ✅ | ❌ | |
| Delete prepared query | ✅ | ❌ | |
| List prepared queries | ✅ | ❌ | |
| Execute prepared query (list) | ✅ | ❌ | Returns matching entities with cursor pagination; validates filters against metadata schema |
| Execute prepared query (aggregate) | ✅ | ❌ | Returns aggregated volumes per asset; validates filters against metadata schema |
| **User-Configurable Indexes** |
| Create index | ✅ | ❌ | Opt-in address, metadata, reference, timestamp, inserted-at, or account-asset indexes per ledger. HTTP: `POST /v3/{ledger}/indexes`; gRPC: `Apply(CreateIndex)` |
| Drop index | ✅ | ❌ | Remove an index from a ledger. HTTP: `DELETE /v3/{ledger}/indexes/{canonicalId}`; gRPC: `Apply(DropIndex)` |
| Get index | ✅ | ❌ | Fetch a single registry entry. HTTP: `GET /v3/{ledger}/indexes/{canonicalId}`; gRPC: `GetIndex` |
| Get index status | ✅ | ❌ | Per-index backfill cursor + per-replica IndexVersionState. HTTP: `GET /v3/{ledger}/indexes/{canonicalId}/status`; gRPC: `GetIndexEntryStatus` |
| Inspect index | ✅ | ❌ | Explore values of a metadata index (distinct values, facets, summary). HTTP: `GET /v3/{ledger}/indexes/{canonicalId}/inspect`; gRPC: `InspectIndex` |
| List indexes | ✅ | ❌ | View all indexes with build status and backfill progress. HTTP: `GET /v3/{ledger}/indexes` (per-ledger), `GET /v3/_/indexes?scope=all\|bucket` (bucket-wide); gRPC: `BucketService.ListIndexes`, scoped `ALL` / `BUCKET` / `LEDGER` |
| Aggregated index status | ✅ | ❌ | Cluster-wide progress (LastIndexedSequence, LastLogSequence, Lag, IndexFileSize) + IndexEntry list. HTTP: `GET /v3/_/indexes/status?ledger=`; gRPC: `GetIndexStatus` |
| **Volumes (responses)** |
| postCommitVolumes | ✅ | ✅ | On every transaction (create, revert, get, list, prepared-query); immutable per-transaction snapshot |
| preCommitVolumes | ❌ | ✅ | Intentionally removed |
| postCommitEffectiveVolumes | ❌ | ✅ | Intentionally removed |
| preCommitEffectiveVolumes | ❌ | ✅ | Intentionally removed |

**Legend:** ✅ Implemented | ⚠️ Partially/Not implemented | ❌ Absent

---

## Features Implemented in POC

### 1. Transaction Creation

**Endpoint:** `POST /v3/{ledgerName}/transactions`

**Features:**
- ✅ Creation with direct postings
- ✅ Creation with Numscript script
- ✅ Numscript variables support
- ✅ Balance verification (insufficient funds)
- ✅ `force` option (bypass balance checks)
- ✅ Transaction metadata
- ✅ Account metadata in the same request
- ✅ Transaction reference
- ✅ Custom timestamp
- ✅ Idempotency key

**Numscript Experimental Features (available, require `#![feature(...)]` opt-in):**
- ✅ Account interpolation (dynamic addresses like `@escrow:$order_id`)
- ✅ Asset colors — promoted to first-class posting field. Postings carry
  `color: string` and balances are strictly segregated per
  `(account, asset, color)`. The empty color is the uncolored bucket and
  is itself segregated from every colored bucket. Color values match
  `^[A-Z]*$` and are immutable once carried by funds. See "Color of money
  semantics" below.
- ✅ `get_amount()` / `get_asset()` functions
- ✅ Mid-script function calls (balance queries during execution)
- ✅ `oneof` selector (conditional routing)
- ✅ `overdraft()` function (dynamic overdraft calculation)

**Color of money semantics (new in this POC):**
- `Posting.color` is exposed on every read/write path. Direct postings
  accept the new field as the optional fifth component of the
  `--posting source,destination,amount,asset[,color]` syntax on
  `ledgerctl transactions create`.
- `Account.volumes` is a deterministic sorted list of
  `{asset, color, volumes}` entries. The HTTP query parameter
  `?collapseColors=true` on `GET /{ledger}/accounts/{address}` sums every
  colored bucket of the same asset into a single entry under `color: ""`.
- `AggregatedVolume.color` is set on every entry returned by
  `GET /{ledger}/volumes`. The same `?collapseColors=true` flag collapses
  the result to one entry per `(asset, precision)`.
- The double-entry invariant holds per `(asset, color)` bucket: each
  segregated bucket is its own conservation universe.
- Numscript `source = @acc \ "RED"` produces a `Posting` with
  `Color = "RED"` and only draws from the matching bucket. Spending more
  than the bucket holds returns `ErrInsufficientFunds` even when other
  colored or uncolored buckets have plenty.

See [Numscript Guide](./numscript.md) for complete documentation.

### 2. Transaction Revert

**Endpoint:** `POST /v3/{ledgerName}/transactions/{transactionId}/revert`

**Features:**
- ✅ Standard revert
- ✅ `force` option (ignore insufficient balances)
- ✅ `atEffectiveDate` option (use original transaction timestamp)
- ✅ Revert metadata (typed values — string, integer, boolean — preserved losslessly; unsupported values rejected with `400 INVALID_REQUEST`)
- ✅ Verification that transaction is not already reverted

**Navigable revert relationship.** The revert link is a first-class part of the
transaction representation (`GET`/list), not metadata — the platform never writes
`com.formance.spec/*` keys. A transaction exposes:
- `reverted` (bool) and `revertedAt` (timestamp) — set on the reverted original.
- `revertedByTransactionId` — on the reverted original, the id of the compensating transaction.
- `revertsTransactionId` — on the compensating transaction, the id of the original it reverts.

`revertedAt` is the compensating transaction's effective timestamp (so under
`atEffectiveDate` it equals the original's timestamp). All three are derived from
the structural `TransactionState`, so they hold identically after replay/backfill
and on the mirror revert path.

Both `reverted` and `revertedAt` are queryable (upstream v2 parity). `reverted` is
always available (served from the reversion bitset, no index); `revertedAt` requires
the `reverted-at` builtin index. `revertedByTransactionId`/`revertsTransactionId` are
navigable in the representation but not queryable (v3-only, no parity baseline).

### 3. Metadata Management

**Endpoints:**
- `POST /v3/{ledgerName}/metadata` - Save ledger metadata (new in v3)
- `DELETE /v3/{ledgerName}/metadata/{key}` - Delete ledger metadata (new in v3)
- `POST /v3/{ledgerName}/accounts/{address}/metadata` - Save account metadata
- `DELETE /v3/{ledgerName}/accounts/{address}/metadata/{key}` - Delete account metadata
- `POST /v3/{ledgerName}/transactions/{transactionId}/metadata` - Save transaction metadata
- `DELETE /v3/{ledgerName}/transactions/{transactionId}/metadata/{key}` - Delete transaction metadata
- `GET /v3/{ledgerName}/metadata-schema` - Get metadata schema (per-field declared type)
- `PUT /v3/{ledgerName}/metadata-schema/{targetType}/{key}` - Set/change metadata field type
- `DELETE /v3/{ledgerName}/metadata-schema/{targetType}/{key}` - Remove metadata field type declaration

Ledger metadata is stored separately from ledger configuration (LedgerInfo) and is populated at read time when calling `GET /v3/{ledgerName}` or `GET /v3/` (list ledgers). It uses the same typed value system as account/transaction metadata.

### 4. Bulk Operations

**Endpoint:** `POST /v3/{ledgerName}/bulk`

**Supported actions:**
- ✅ `CREATE_TRANSACTION`
- ✅ `ADD_METADATA` (account and transaction)
- ✅ `REVERT_TRANSACTION`
- ✅ `DELETE_METADATA` (account and transaction)

**Options:**
- ✅ `continueOnFailure` - Continue even on error
- ✅ `atomic` - All operations or nothing (supports cross-ledger operations)

> **Note:** Unlike v2, v3 supports **system-level atomic bulk operations** that can span multiple ledgers. This is enabled by the [Global Log Architecture](../architecture/subsystems/consensus/global-log.md).

### 5. Ledger Management

**Endpoints:**
- `POST /v3/{ledgerName}` - Create a ledger. Optional body fields: `mode`, `mirrorSource`, `defaultEnforcementMode`, `initialSchema` (metadata field types), and `accountTypes` (full account-type model — name/pattern/persistence/segmentTypes). These mirror the gRPC `CreateLedgerRequest`.
- `DELETE /v3/{ledgerName}` - Delete a ledger
- `GET /v3/{ledgerName}` - Get ledger info (read)
- `GET /v3/` - List all ledgers (read)

### 5b. Account Types

**Endpoints:**
- `GET /v3/{ledgerName}/account-types` - List all account types for a ledger
- `GET /v3/{ledgerName}/account-types/{typeName}` - Get details of a specific account type
- `POST /v3/{ledgerName}/account-types` - Add a new account type (body: name, pattern, optional `persistence` and `segmentTypes`)
- `DELETE /v3/{ledgerName}/account-types/{typeName}` - Remove an account type

**Features:**
- ✅ Pattern-based account address validation (e.g., `users:{id}:checking`)
- ✅ Variable segments with optional regex constraints (e.g., `{iban:^[A-Z]{2}[0-9]{14}$}`)
- ✅ Ledger-level default enforcement mode: STRICT (reject) or AUDIT (warnings)
- ✅ Longest-match / highest-specificity resolution when multiple types match
- ✅ `world` account always passes validation
- ✅ Account types can be set at ledger creation time or added later
- ✅ Persistence modes: `NORMAL` (default), `EPHEMERAL` (purged when zero balance), `TRANSIENT` (never persisted, must be zero at end of batch)

### 6. Transaction Read

**Endpoint:** `GET /v3/{ledgerName}/transactions/{transactionId}`

**Features:**
- ✅ Get transaction by ID
- ✅ Returns transaction details (postings, metadata, timestamp, reference)
- ✅ Works from any node (leader or follower)
- ✅ Returns 404 for non-existent transactions

**Response includes:**
- Transaction ID
- Postings (source, destination, amount, asset)
- Metadata
- Timestamp
- Reference (if set)

**CLI command:**
```bash
ledgerctl transactions get --ledger <ledger-name> --id <transaction-id>
```

### 7. Chapters

Chapters partition a ledger's transaction history into discrete, sealed segments. See [Chapters Architecture](../architecture/subsystems/chapters/lifecycle.md) for full documentation.

**gRPC Methods:**
- `Apply(CloseChapterRequest)` - Close the current open chapter (write, leader-only)
- `Apply(SetChapterScheduleRequest)` - Set automatic chapter rotation schedule (write, leader-only)
- `Apply(DeleteChapterScheduleRequest)` - Delete automatic chapter rotation schedule (write, leader-only)
- `Apply(ArchiveChapterRequest)` - Archive a closed chapter to cold storage (write, leader-only)
- `GetChapterSchedule(GetChapterScheduleRequest)` - Get the current chapter rotation schedule (read, any node)
- `ListChapters(ListChaptersRequest)` - Stream all chapters (read, any node)

**Features:**
- ✅ Close current chapter (OPEN → CLOSING → CLOSED lifecycle)
- ✅ Background sealing with BLAKE3 hash (off Raft critical path)
- ✅ Automatic crash recovery for both crash windows
- ✅ Transaction receipts (HMAC-SHA256 JWT with chapter ID)
- ✅ List all chapters with status, timestamps, and sealing hashes
- ✅ Archive chapter (CLOSED → ARCHIVED with cold storage export and hot purge)
- ✅ Scheduled automatic chapter rotation (cron-based, leader-only, runtime-configurable)

**CLI commands:**
```bash
# Close the current open chapter
ledgerctl chapters close

# Set automatic chapter rotation (every day at midnight)
ledgerctl chapters set-schedule "0 0 * * *"

# Disable automatic rotation
ledgerctl chapters delete-schedule

# Show current schedule
ledgerctl chapters get-schedule

# Archive a closed chapter to cold storage
ledgerctl chapters archive 1

# List all chapters
ledgerctl chapters list
```

### 8. Mirror Ledgers

Mirror mode enables one-way synchronization from an existing v2 ledger into a v3 ledger. The mirror ledger is read-only until promoted to normal mode.

**Create a mirror ledger:** `POST /v3/{ledgerName}`

Request body includes `mode` (`"MIRROR"`) and a `mirrorSource` object specifying the source configuration.

**Source types:**
- **HTTP** (`type: "http"`) — Polls the v2 API endpoint `GET /v2/{ledger}/logs`. Fields: `baseUrl`, `oauth2ClientId`, `oauth2ClientSecret`, `oauth2TokenEndpoint`, `oauth2Scopes` (optional, for OAuth2 client credentials authentication).
- **PostgreSQL** (`type: "postgres"`) — Reads directly from the v2 ledger's PostgreSQL database. Fields: `dsn`. AWS RDS IAM authentication is provisioned via the operator `Ledger` CRD (`mirrorSource.postgres.awsIamAuth.region` + optional `assumeRoleArn` for cross-account / multi-tenant mirrors) — see `misc/operator/api/v1alpha1/ledger_crd_types.go`. The mirror pod mints SigV4 tokens per connection from the ambient AWS credential chain (IRSA on EKS).

If `type` is omitted, defaults to `"http"`.

**Write guard:** All direct write operations (create transaction, save metadata, delete metadata, revert transaction) are rejected on mirror-mode ledgers with HTTP 409 (`LEDGER_IN_MIRROR_MODE`) or gRPC `FailedPrecondition`.

**Sync progress:** `GET /v3/{ledgerName}` returns a `mirrorSyncProgress` object for mirror ledgers with:
- `state`: `SYNCING` (catching up with history) or `FOLLOWING` (up to date)
- `cursor`: Last ingested v2 log ID
- `sourceLogCount`: Latest known log ID in the v2 source
- `remainingLogs`: Number of logs remaining to sync (`sourceLogCount - cursor`)
- `error`: Most recent sync error (null if healthy)

**Sync behavior:** A background worker polls the source for v2 logs and replays them into the mirror ledger. Supported v2 log types:
- `NEW_TRANSACTION` — Creates a transaction with postings and optional account metadata
- `SET_METADATA` — Sets metadata on an account or transaction
- `REVERTED_TRANSACTION` — Replays a revert
- `DELETE_METADATA` — Deletes a metadata key
- Unknown log types are recorded as fill-gap entries (no-op for data, preserves log ID sequence)

**Promote a mirror ledger:** `POST /v3/{ledgerName}/promote`

Converts the mirror ledger to normal mode. After promotion:
- The mirror worker is stopped
- The ledger accepts direct writes
- The `mode` changes from `MIRROR` to `NORMAL`
- The `mirrorSource` configuration is cleared

Promoting a non-mirror ledger returns HTTP 400 (`LEDGER_NOT_IN_MIRROR_MODE`) or gRPC `FailedPrecondition`.

**gRPC:** Both create-mirror and promote operations go through the `Apply` method using `CreateLedgerRequest` (with `mode` and `mirror_source` fields) and `PromoteLedgerRequest`.

### 9. Numscript Library

The numscript library allows saving, retrieving, and managing reusable numscript programs with semver versioning.

**Endpoints:**
- `GET /v3/{ledgerName}/numscripts` - List all numscripts (greatest version of each)
- `GET /v3/{ledgerName}/numscripts/{name}?version=` - Get a numscript by name (optional version selector)
- `GET /v3/{ledgerName}/numscripts/{name}/usage` - Get invocation count and last-used timestamp for a template
- `GET /v3/{ledgerName}/numscripts/{name}/versions` - List version history (current latest + every stored version)
- `PUT /v3/{ledgerName}/numscripts/{name}` - Save an immutable version (explicit full semver)

**Versioning:**

Numscripts use **semantic versioning** (semver) with the format `major.minor.patch` (e.g. `"1.0.0"`).

When saving a numscript via `PUT /v3/{ledgerName}/numscripts/{name}`, the request body includes:
- `content` (required): The numscript source code
- `version` (optional): Controls versioning behavior:
  - A semver string (e.g. `"2.0.0"`) creates a new version. Fails with 409 if the version already exists.
  - The special value `"latest"` overwrites the content of the current latest version.
  - If omitted or empty, defaults to `"latest"`.

When retrieving a numscript via `GET /v3/{ledgerName}/numscripts/{name}`, the `version` query parameter selects which version to return. If omitted or empty, the latest version is returned.

**Response schema (NumscriptInfo):**
- `name` (string): Numscript name
- `content` (string): Numscript source code
- `version` (string): Semver version (e.g. `"1.0.0"`)
- `createdAt` (string, date-time): Timestamp

**Usage tracking (`GET /v3/{ledgerName}/numscripts/{name}/usage`):**

Returns per-template invocation counters and the timestamp of the most recent invocation. Populated asynchronously by the `usagebuilder` subsystem, which tails the FSM audit chain and writes to a dedicated secondary Pebble store (`<data-dir>/usage/`). Values are eventually consistent with the FSM and may lag by up to one usagebuilder tick interval (~100 ms).

A never-invoked template returns a zero-valued response (not 404), so clients handle "never used" uniformly:
- `count` (uint64): Number of times the template has been invoked. `0` means not yet invoked (or the usagebuilder has not caught up).
- `lastUsed` (string, date-time, nullable): Timestamp of the most recent invocation. Absent when count is 0.

On a fresh ledger the counter builds up organically from cursor=0. On an existing ledger whose audit chain has been partially archived to cold storage, only invocations still present in the primary Pebble store are counted.
### Filter input formats (dual-format contract, EN-1511)

Every filtered surface accepts one `filter` in either the textual `filterexpr`
grammar or the structured v2 JSON DSL; both compile to the same
`*commonpb.QueryFilter`. The full contract — parameter classification, the
textual/structured expressiveness asymmetries (address prefix, reference, audit),
date coercion (EN-1544), AND-combination, and the removed `prefix`/`reference`
aliases (EN-1540) — is documented once in
[query-filter.md](../architecture/subsystems/read-path/query-filter.md).

### 10. Prepared Queries and User-Configurable Indexes

Prepared queries are reusable, named filter queries stored per-ledger. They can be executed in two modes: `LIST` (returns matching entity IDs with cursor pagination) and `AGGREGATE_VOLUMES` (returns aggregated volumes per asset for matched accounts).

**Endpoints:**
- `POST /v3/{ledgerName}/prepared-queries` — Create
- `PUT /v3/{ledgerName}/prepared-queries/{name}` — Update filter
- `DELETE /v3/{ledgerName}/prepared-queries/{name}` — Delete
- `GET /v3/{ledgerName}/prepared-queries` — List
- `POST /v3/{ledgerName}/prepared-queries/{name}/execute` — Execute

**Supported filter types (`QueryFilter`):**

| Filter | Target | Requires index |
|--------|--------|----------------|
| `FieldCondition` — metadata string equality | accounts, transactions | yes (metadata index) |
| `AddressMatch` — prefix or exact address match | accounts | no |
| `AndFilter` / `OrFilter` / `NotFilter` | — | depends on sub-filters |
| `ReferenceCondition` — transaction reference exact match | transactions | yes (`reference` builtin index) |
| `BuiltinUintCondition` with `TIMESTAMP` — effective date range | transactions | yes (`timestamp` builtin index) |
| `BuiltinUintCondition` with `INSERTED_AT` — creation date range | transactions | yes (`inserted_at` builtin index) |
| `BuiltinUintCondition` with `REVERTED_AT` — revert date range | transactions | yes (`reverted_at` builtin index) |
| `BuiltinUintCondition` with `ID` — transaction ID range or equality | transactions | no (direct range scan) |
| `RevertedCondition` — transaction revert status (true/false) | transactions | no (reversion bitset) |
| `AccountHasAssetCondition` — accounts that have ever touched an asset | accounts | yes (`account-asset` index) |

**User-configurable indexes** control which filters are available. Each index has a lifecycle: BUILDING (backfill in progress) → READY (queries enabled).

| Index type | CLI flag | Enables |
|------------|----------|---------|
| `address` | `--type address` | `AddressMatch` on transaction queries |
| `source-address` | `--type source-address` | source-only address matching |
| `destination-address` | `--type destination-address` | destination-only address matching |
| `metadata` | `--type metadata --target … --key …` | `FieldCondition` on the specified field |
| `reference` | `--type reference` | `ReferenceCondition` |
| `timestamp` | `--type timestamp` | `BuiltinUintCondition(TIMESTAMP)` |
| `inserted-at` | `--type inserted-at` | `BuiltinUintCondition(INSERTED_AT)` |
| `reverted-at` | `--type reverted-at` | `BuiltinUintCondition(REVERTED_AT)` |
| `account-asset` | `--type account-asset` | `AccountHasAssetCondition` — `has asset <BASE>[/<PRECISION>]` filter on account queries |

> Filtering by transaction ID (`BuiltinUintCondition(ID)`) and by revert status
> (`RevertedCondition`) are always available with no index required — the latter is
> served from the per-ledger reversion bitset.

> **Revert querying (upstream v2 parity).** v2 queries transactions by both
> `reverted` and `reverted_at`; v3 matches this: `RevertedCondition` for the boolean
> and `BuiltinUintCondition(REVERTED_AT)` (behind the `reverted-at` index) for the
> date range. The v3-only navigable ids `revertedByTransactionId` /
> `revertsTransactionId` are exposed in the representation but are not queryable
> (no v2 baseline); adding them would follow the same builtin-index recipe.

**CLI commands:**
```bash
# Create and use a reference index
ledgerctl indexes create --ledger my-ledger --type reference
ledgerctl indexes list --ledger my-ledger

# Create a prepared query that filters by reference
# (done via gRPC / HTTP — no direct CLI for query creation)
```

---

## Missing Features

### 1. ❌ Log Import

**Description:** Import logs from another ledger for migration or synchronization.

**Current status:** 
- Interface defined in `Ledger.Import(ctx, stream chan *commonpb.Log) error`
- Implementation returns `ErrNotFound`

**To implement:**
- HTTP endpoint (probably `POST /v3/{ledgerName}/_import`)
- Log validation
- Sequential insertion with consistency verification
- Streaming support for large volumes

### 2. ❌ Log Export

**Description:** Export all logs from a ledger for backup or migration.

**Current status:**
- Interface defined in `Ledger.Export(ctx, w ExportWriter) error`
- Implementation returns `ErrNotFound`

**To implement:**
- HTTP endpoint (probably `GET /v3/{ledgerName}/_export`)
- Log streaming
- Output format (JSON lines, protobuf, etc.)
- Pagination/cursor for large volumes

### 3. ✅ Unique Reference Validation

**Description:** In the original ledger, transaction reference must be unique within a ledger.

**Current status:** Fully implemented.
- Transaction references are validated for uniqueness within a ledger using the attribute system
- References are stored as `TransactionReferenceValue` (containing the transaction ID) keyed by `[ledgerID][reference]`
- Duplicate references within the same ledger return HTTP 409 Conflict (`ErrTransactionReferenceConflict`)
- Empty references are allowed and not validated
- The same reference can exist in different ledgers

### 4. ✅ Ledger Metadata Update

**Description:** Ability to add/modify metadata on a ledger after creation.

**Current status:** Fully implemented.
- `POST /v3/{ledgerName}/metadata` - Add/modify metadata
- `DELETE /v3/{ledgerName}/metadata/{key}` - Delete a metadata key

These endpoints are documented in Section 3 (Metadata Management) above.

### 5. ❌ Ledger Configuration Update

**Description:** Modify certain ledger parameters after creation (e.g., maintenanceInterval).

**To implement:**
- `PATCH /v3/{ledgerName}` or `PUT /v3/{ledgerName}/config`

---

## Intentionally Removed Features

### 1. Post-Commit Volumes (Always Present) / Pre-Commit Volumes (Removed)

**Description:** In the original ledger, transaction creation responses include volumes before and after the commit:

- `postCommitVolumes` - Volumes after transaction application
- `preCommitVolumes` - Volumes before transaction application
- `postCommitEffectiveVolumes` - Effective volumes after application (with effective timestamp)
- `preCommitEffectiveVolumes` - Effective volumes before application (with effective timestamp)

**POC Status:**
- `postCommitVolumes` is part of **every** persisted transaction. It rides on the `transaction` object (not a sibling field), so create, revert, bulk, unitary get, list, and prepared-query results all expose the same value. It is an immutable historical snapshot captured at the transaction's sequence — the volumes (input/output) per `(account, asset, color)` tuple touched by the transaction, right after it applied — never recomputed from current balances.
- `preCommitVolumes`, `postCommitEffectiveVolumes`, and `preCommitEffectiveVolumes` remain **intentionally removed**.

**Response shape** (the `transaction` object carries the field):
```json
{
  "id": 1,
  "postings": [...],
  "postCommitVolumes": {
    "users:alice": [
      { "asset": "USD/2", "color": "", "input": "0", "output": "1000" }
    ],
    "users:bob": [
      { "asset": "USD/2", "color": "", "input": "1000", "output": "0" }
    ]
  }
}
```

**Impact on clients:**
- Post-commit volumes are always returned; there is no request flag to toggle them (the former `expandVolumes` flag is removed).
- Each account maps to a flat list of `(asset, color)` rows; the empty color is the uncolored bucket.
- `preCommitVolumes` and effective volumes remain removed; use dedicated read endpoints if needed.

---

## Behavior Differences to Note

### 1. Negative Balance Handling

**POC:** Strict balance verification by default. The `force` flag on transaction creation or revert bypasses balance checks, allowing accounts to go negative.

**Original:** Same behavior, but the original ledger has configuration options for "unbounded" accounts.

**Status:** ✅ Compliant (via `force` flag)

### 2. "world" Account

**POC:** The "world" account has infinite funds (universal source).

**Original:** Same behavior.

**Status:** ✅ Compliant

### 3. Posting Order in Revert

**POC:** Postings are reversed (source ↔ destination) AND the order is reversed.

**Original:** Same behavior.

**Status:** ✅ Compliant

### 4. Idempotency

**POC:**
- Supported via `Idempotency-Key` header (HTTP) or `ApplyBatch.idempotency_key` (gRPC) — keyed per atomic batch
- System-level scope (not per-ledger)
- Hash-based content verification (BLAKE3)
- Stored in generation-based cache and persisted to Pebble
- Maximum key length: 256 characters (validated at admission)

**Original:** Same mechanism.

**Status:** ✅ Compliant

See [Idempotency](../architecture/subsystems/admission/idempotency.md) for detailed documentation.

### 5. Index readiness and metadata schema retypes (EN-1323)

**POC:** Index "readiness" is a per-replica concept driven by the
local `IndexVersionState` (`current_version`, `pending_version`), not
by a cluster-wide flag.

- `CreateIndex` registers the index with `BuildStatus = BUILDING` and
  each replica starts a local backfill. When the backfill catches up
  to the global indexer cursor, the replica performs a local atomic
  switch (`current_version` 0 → 1) in a single Pebble batch. There is
  no cluster-wide `IndexReady` proposal — different replicas can be
  in different states at the same wall-clock moment.
- `BuildStatus` on the API is **informational only**. It is set to
  `BUILDING` at CreateIndex and never flipped to `READY` by the FSM.
  Clients that need to gate on "this replica is ready to query" must
  use `GetIndexStatus` and check `IndexEntry.current_version > 0`,
  or use `min_log_sequence` (below) to enforce ordering.
- `SetMetadataFieldType` (retype) bumps the cluster-wide
  `Index.forward_encoding_version`. Each replica then runs a local
  rewrite into the new versioned keyspace (`pending_version`), with
  live writes dual-writing into both `current_version` and
  `pending_version` until the local rewrite finishes and atomically
  switches. Queries served from the replica's `current_version` stay
  consistent throughout — no half-rewritten state is ever observable.

**Client synchronization:** Use `ReadOptions.min_log_sequence` to
require the read replica to have applied at least the given log
sequence. The gate is satisfied by `LastIndexedSequence >=
min_log_sequence` — it pins **log application** on this replica,
not rewrite completion. After a `SetMetadataFieldType` apply,
setting `min_log_sequence` to the retype's log sequence (returned
in `ApplyResponse.logs[].sequence`) guarantees:

  - the retype log has been processed locally,
  - `pending_version` has been bumped, and
  - the local schema-rewrite has been *scheduled*.

It does **not** guarantee that the rewrite has completed or that
the replica is serving the new encoding. The atomic switch
(`current_version ← pending_version`) is a separate per-replica
background event, gated internally on the read store catching up to
the FSM seq the rewrite observed. Queries against the replica
continue serving from `current_version` (the pre-retype encoding)
until that switch fires — see the previous bullet's "consistent
throughout" guarantee. Clients that need *post-switch* consistency
must poll `GetIndexStatus.IndexEntry.current_version` and wait for
it to reflect the bumped forward-encoding version, OR rely on the
eventual-consistency window (typically seconds) inherent to
background rewrites. No client primitive currently blocks until
that switch lands.

**Original:** No equivalent — the original ledger has no
per-replica versioning and reaches "ready" via a single
synchronously-applied schema migration.

**Status:** ⚠️ Different model — wire shape compatible (BuildStatus
still on the proto, populated, just ignored by query gating).

---

## Read Features

Read endpoints comparison with the original ledger:

| Endpoint | POC | Original | Notes |
|----------|-----|----------|-------|
| `GET /v3/{ledgerName}/transactions/{id}` | ✅ | ✅ | Get a transaction by ID |
| `GET /v3/{ledgerName}/transactions` | ⚠️ | ✅ | List transactions (gRPC stream only, no HTTP handler) |
| `GET /v3/{ledgerName}/accounts` | ✅ | ✅ | List accounts (rich boolean filter, cursor pagination) |
| `GET /v3/{ledgerName}/accounts/{address}` | ✅ | ✅ | Get an account |
| `GET /v3/{ledgerName}/accounts/{address}/balances` | ❌ | ✅ | Get account balances |
| `GET /v3/{ledgerName}/accounts/{address}/volumes` | ❌ | ✅ | Get account volumes |
| `GET /v3/{ledgerName}/volumes` | ✅ | ✅ | Aggregate volumes (per-asset, generic account `filter`) |
| `GET /v3/{ledgerName}/logs` | ✅ | ✅ | List per-ledger logs. Supports `?after=` for pagination |
| `GET /v3/{ledgerName}/stats` | ✅ | ✅ | Ledger usage statistics (transaction, volume, reference, posting, log, revert, Numscript-execution, ephemeral-evicted and transient-used counts) |
| `GET /v3/{ledgerName}` | ✅ | ✅ | Get ledger info |
| `POST /v3/{ledgerName}/promote` | ✅ | ❌ | Promote mirror ledger to normal mode |
| `GET /v3/` | ✅ | ✅ | List all ledgers |
| `GET /v3/{ledgerName}/metadata-schema` | ✅ | ❌ | Get metadata schema status |
| `GET /v3/{ledgerName}/analyze-accounts` | ✅ | ❌ | Analyze accounts and suggest Chart of Accounts |
| `GET /v3/{ledgerName}/analyze-transactions` | ✅ | ❌ | Analyze transaction flow patterns |
| `PUT /v3/{ledgerName}/metadata-schema/{targetType}/{key}` | ✅ | ❌ | Set metadata field type |
| `DELETE /v3/{ledgerName}/metadata-schema/{targetType}/{key}` | ✅ | ❌ | Remove metadata field type |
| `POST /v3/{ledgerName}/prepared-queries` | ✅ | ❌ | Create a prepared query |
| `PUT /v3/{ledgerName}/prepared-queries/{queryName}` | ✅ | ❌ | Update a prepared query |
| `DELETE /v3/{ledgerName}/prepared-queries/{queryName}` | ✅ | ❌ | Delete a prepared query |
| `GET /v3/{ledgerName}/prepared-queries` | ✅ | ❌ | List prepared queries |
| `POST /v3/{ledgerName}/prepared-queries/{queryName}/execute` | ✅ | ❌ | Execute a prepared query |
| `GET /v3/{ledgerName}/numscripts` | ✅ | ❌ | List all numscripts (greatest version of each) |
| `GET /v3/{ledgerName}/numscripts/{name}?version=` | ✅ | ❌ | Get numscript (version selector, empty/latest = greatest semver) |
| `GET /v3/{ledgerName}/numscripts/{name}/usage` | ✅ | ❌ | Get invocation count + last-used timestamp |
| `GET /v3/{ledgerName}/numscripts/{name}/versions` | ✅ | ❌ | List version history |
| `PUT /v3/{ledgerName}/numscripts/{name}` | ✅ | ❌ | Save an immutable version (explicit full semver) |
| `GET /v3/{ledgerName}/account-types` | ✅ | ❌ | List account types |
| `GET /v3/{ledgerName}/account-types/{typeName}` | ✅ | ❌ | Get account type |
| `POST /v3/{ledgerName}/account-types` | ✅ | ❌ | Add account type |
| `DELETE /v3/{ledgerName}/account-types/{typeName}` | ✅ | ❌ | Remove account type |
| `PUT /v3/{ledgerName}/account-types/default-enforcement-mode` | ✅ | ❌ | Set default enforcement mode (STRICT/AUDIT) |
| `GET /v3/{ledgerName}/transactions` | ✅ | ❌ | List transactions: cursor pagination, `startDate`/`endDate` range, and the generic `filter` (reference selection via `filter={"$match":{"reference":"..."}}`) |
| `GET /v3/_/logs/{sequence}` | ✅ | ❌ | Fetch a single system log by bucket-wide sequence |
| `GET /v3/_/chapters` | ✅ | ❌ | Stream chapters (audit-chain segments) |
| `GET /v3/_/chapter-schedule` | ✅ | ❌ | Get the auto-rotation cron for chapters |
| `GET /v3/_/events-sinks` | ✅ | ❌ | List configured event sinks with per-sink status (`{sinks, sinkStatuses}`, parity with gRPC `GetEventsSinks`) |
| `GET /v3/_/signing-keys` | ✅ | ❌ | List registered Ed25519 signing keys |
| `GET /v3/{ledgerName}/indexes` | ✅ | ❌ | List indexes registered on a ledger |
| `GET /v3/{ledgerName}/indexes/{canonicalId}` | ✅ | ❌ | Get a single Index registry entry |
| `GET /v3/{ledgerName}/indexes/{canonicalId}/status` | ✅ | ❌ | IndexEntry (backfill cursor + per-replica IndexVersionState) |
| `GET /v3/{ledgerName}/indexes/{canonicalId}/inspect` | ✅ | ❌ | Inspect metadata index (distinct values, facets, summary) — path unified with the DELETE/create routes |
| `POST /v3/{ledgerName}/indexes` | ✅ | ❌ | Create an index (body: `{"id": "<canonical>"}`) |
| `DELETE /v3/{ledgerName}/indexes/{canonicalId}` | ✅ | ❌ | Drop an index |
| `GET /v3/_/indexes?scope=all\|bucket` | ✅ | ❌ | List bucket-wide indexes (cluster-wide) |
| `GET /v3/_/indexes/status?ledger=` | ✅ | ❌ | Aggregate indexer progress + IndexEntry list |
| `GET /v3/_/indexes/{canonicalId}` | ✅ | ❌ | Get a single bucket-scoped Index entry |
| `GET /v3/_/indexes/{canonicalId}/status` | ✅ | ❌ | Bucket-scoped IndexEntry |
| `POST /v3/{ledgerName}/bulk` | ✅ | ❌ | Bulk operations (alternate path without underscore) |
| `GET /_info` | ✅ | ❌ | Server build info (`version`, `commit`, `buildDate`, `goVersion`); unauthenticated, flat JSON (no `data` envelope) |

---

## Priority Recommendations

### High Priority
1. **Import/Export** - Critical for migration and backups

### Low Priority
2. **Ledger config update** - Can be done manually via recreation

---

## Architecture Notes

The POC uses a different architecture with Raft for replication:
- A single Raft group manages all ledgers and their transactions
- Write operations go through the leader
- Logs are stored via the Store (Pebble)
- A global log provides system-wide ordering and enables cross-ledger atomic operations

This architecture impacts certain implementation decisions:
- **Bulk atomicity is handled at the Raft level** - All actions in a bulk request are submitted as a single Raft command, enabling system-level atomicity
- Import must respect log sequence
- Export can be done from any node (local read)

See [Global Log Architecture](../architecture/subsystems/consensus/global-log.md) for details on how the two-level log architecture enables cross-ledger atomic operations.

---

## gRPC API

The POC provides a gRPC API for internal service communication (Raft node forwarding to leader) and can be used by clients.

### BucketService Methods

| Method | Description | Status |
|--------|-------------|--------|
| `CreateLedger` | Create a new ledger | ✅ |
| `DeleteLedger` | Delete a ledger | ✅ |
| `ListLedgers` | Get all ledgers info | ✅ |
| `GetLedger` | Get ledger by name or ID | ✅ |
| `GetTransaction` | Get transaction by ID | ✅ |
| `ListTransactions` | Stream transactions for a ledger | ✅ |
| `ListAccounts` | Stream accounts for a ledger | ✅ |
| `GetAccount` | Get account by address | ✅ |
| `GetPrimaryMetrics` | Get primary Pebble store metrics | ✅ |
| `GetSecondaryMetrics` | Get secondary (read index) Pebble store metrics | ✅ |
| `CheckStore` | Verify store integrity (hash chain + derived data) | ✅ |
| `GetEventsSinks` | Get per-sink configurations and statuses | ✅ |
| `GetChapterSchedule` | Get current chapter rotation schedule | ✅ |
| `GetMetadataSchemaStatus` | Get the declared metadata schema for a ledger | ✅ |
| `AnalyzeTransactions` | Discover transaction flow patterns | ✅ |
| `CreatePreparedQuery` | Create a named prepared query | ✅ |
| `UpdatePreparedQuery` | Update an existing prepared query | ✅ |
| `DeletePreparedQuery` | Remove a prepared query | ✅ |
| `ListPreparedQueries` | List all prepared queries for a ledger | ✅ |
| `ExecutePreparedQuery` | Execute a prepared query against the read index | ✅ |
| `Barrier` | No-op Raft proposal to ensure all prior writes are applied | ✅ |
| `Apply` | Apply a ledger action (write operations) | ✅ |
| `Apply(CreateLedger)` with mirror mode | Create a mirror ledger | ✅ |
| `Apply(PromoteLedger)` | Promote mirror ledger to normal mode | ✅ |
| `Apply(SaveNumscript)` | Save an immutable numscript version (explicit full semver) | ✅ |
| `GetNumscript` | Get a numscript by name and version selector | ✅ |
| `ListNumscripts` | List the greatest version of each saved numscript | ✅ |
| `ListNumscriptVersions` | List the latest pointer and every stored version | ✅ |
| `Apply(CloseChapter)` | Close the current open chapter | ✅ |
| `ListChapters` | Stream all chapters | ✅ |
| `ListAuditEntries` | Stream audit log entries (success + failure). Request is `{ options }` only — no dedicated filter fields. Follows the shared `ListOptions` contract: cursor/page_size/reverse/checkpoint_id plus a bare-audit-field `QueryFilter` (outcome, ledger, caller_subject, order_type, seq, proposal_id, timestamp, log_seq — bare fields resolved against the audit query target, EN-1549 replacing the old `audit[...]` syntax) resolved through the audit secondary index. Ledger scope and outcome selection are expressed as filter conditions | ✅ |
| `GetAuditEntry` | Get a single audit entry by sequence number | ✅ |
| `ListLogs` | Stream system logs for a ledger (requires `ledger` field; supports `log_id` and date filters for pagination) | ✅ |
| `GetLog` | Get a single system log by sequence number | ✅ |
| `ListSigningKeys` | Stream all registered signing keys | ✅ |
| `Discovery` | Return server capabilities (response signing config) and build info (`ServerInfo`: version, commit, build date, Go version) | ✅ |
| `AnalyzeAccounts` | Analyze accounts and suggest Chart of Accounts | ✅ |
| `GetIndexStatus` | Read index builder progress (lag, file size) | ✅ |
| `GetLedgerStats` | Get aggregate usage statistics (transaction, volume, reference, posting, log, revert, Numscript-execution, ephemeral-evicted and transient-used counts) | ✅ |
| `AggregateVolumes` | Per-asset aggregated volumes for filtered accounts | ✅ |
| `InspectIndex` | Inspect metadata index (distinct values, facets, summary) | ✅ |

### Apply Method

The `Apply` method is the **single entry point for all ledger write operations**. It provides a unified way to apply any ledger action through a single gRPC call.

**Benefits:**
- Simplified API surface - single method for all write operations
- Consistent behavior across all action types
- Better for bulk operations executed in parallel
- Simplified client logic
- Efficient forwarding between Raft nodes

**Request:** `ApplyRequest` containing a `LedgerAction` with:
- `ledger_id`: Target ledger ID
- `idempotency_key`: Optional idempotency key (on the enclosing `ApplyBatch` — one per atomic batch)
- `skip_response`: When `true`, strips log payloads from the response (only `sequence` is returned per log). Useful for historical ingestion where the client does not need the full response on success.
- One of:
  - `create_transaction`: Create a new transaction
  - `add_metadata`: Add metadata to an account or transaction
  - `revert_transaction`: Revert a transaction
  - `delete_metadata`: Delete metadata from an account or transaction
  - `save_numscript`: Save an immutable numscript version (explicit full semver)

**Response:** `common.Log` - The log entry created by the action (stripped to `sequence` only when `skip_response` is set)

**Note:** Individual RPC methods like `CreateTransaction`, `RevertTransaction`, `SaveAccountMetadata`, etc. have been consolidated into the `Apply` method for a cleaner API.

### gRPC Error Mapping

Business errors from the processing layer are mapped to gRPC status codes with structured `ErrorInfo` details via the `Describable` contract in `internal/domain`. This allows clients to programmatically identify error types without parsing error messages. See `internal/domain/errors.go` for the canonical list — the `Reason()` method on each typed error returns the constant below, and `Metadata()` returns the keys listed.

Each error response includes a `google.rpc.ErrorInfo` detail with:
- **`reason`**: Machine-readable error reason constant (e.g., `LEDGER_ALREADY_EXISTS`)
- **`domain`**: Always `"ledger"`
- **`metadata`**: Error-specific key-value pairs with context

**Adding a new error**: define a typed error implementing `domain.Describable` (`Kind`, `Reason`, `Metadata`). The exhaustive `kindToGRPCCode` switch + reflection test enforce that no new error reaches the API without a mapping. No edit to this table required to keep it accurate — but please keep it in sync as a reference.

| Error | gRPC Code | Reason | Metadata |
|-------|-----------|--------|----------|
| Ledger already exists | `ALREADY_EXISTS` | `LEDGER_ALREADY_EXISTS` | `name` |
| Ledger not found | `NOT_FOUND` | `LEDGER_NOT_FOUND` | `name` |
| Ledger deleted | `FAILED_PRECONDITION` | `LEDGER_DELETED` | `name` |
| Ledger in mirror mode | `FAILED_PRECONDITION` | `LEDGER_IN_MIRROR_MODE` | `name` |
| Ledger not in mirror mode | `FAILED_PRECONDITION` | `LEDGER_NOT_IN_MIRROR_MODE` | `name` |
| Idempotency key conflict | `ALREADY_EXISTS` | `IDEMPOTENCY_KEY_CONFLICT` | `key` |
| Idempotency check failed | `INTERNAL` | `IDEMPOTENCY_CHECK_FAILED` | *(none)* |
| Transaction reference conflict | `ALREADY_EXISTS` | `TRANSACTION_REFERENCE_CONFLICT` | `ledger`, `reference` |
| Transaction not found | `NOT_FOUND` | `TRANSACTION_NOT_FOUND` | `transactionId` |
| Transaction already reverted | `FAILED_PRECONDITION` | `TRANSACTION_ALREADY_REVERTED` | `transactionId` |
| Transaction state inconsistent | `INTERNAL` | `TRANSACTION_STATE_INCONSISTENT` | `transactionId`, `operation` |
| Insufficient funds | `FAILED_PRECONDITION` | `INSUFFICIENT_FUNDS` | `account`, `asset`, `amount`, `balance` |
| Volume overflow | `FAILED_PRECONDITION` | `VOLUME_OVERFLOW` | `account`, `asset`, `side`, `amount`, `current` |
| Volume not materialized | `INTERNAL` | `VOLUME_NOT_MATERIALIZED` | `account`, `asset`, `side` |
| Balance not found | `FAILED_PRECONDITION` | `BALANCE_NOT_FOUND` | `account`, `asset` |
| Balance not preloaded | `FAILED_PRECONDITION` | `BALANCE_NOT_PRELOADED` | `account`, `asset` |
| Numscript parse error | `INVALID_ARGUMENT` | `NUMSCRIPT_PARSE_ERROR` | `details` |
| Numscript runtime error | `INTERNAL` | `NUMSCRIPT_RUNTIME` | `detail` |
| Numscript not found | `NOT_FOUND` | `NUMSCRIPT_NOT_FOUND` | `name` |
| Numscript invalid version | `INVALID_ARGUMENT` | `NUMSCRIPT_INVALID_VERSION` | `version` |
| Numscript version already exists | `ALREADY_EXISTS` | `NUMSCRIPT_VERSION_ALREADY_EXISTS` | `name`, `version` |
| Validation error (generic) | `INVALID_ARGUMENT` | `VALIDATION` | *(none — see message)* |
| Audit disabled | `FAILED_PRECONDITION` | `AUDIT_DISABLED` | *(none)* |
| Maintenance mode | `UNAVAILABLE` | `MAINTENANCE_MODE` | *(none)* |
| Stale proposal | `UNAVAILABLE` | `STALE_PROPOSAL` | *(none)* |
| Stale Numscript inputs resolution | `UNAVAILABLE` | `STALE_INPUTS_RESOLUTION` | *(none)* |
| Preload unavailable (discovery failed; forwarded for idempotent replay) | `UNAVAILABLE` | `PRELOAD_UNAVAILABLE` | *(none)* |
| Writes blocked — disk full | `RESOURCE_EXHAUSTED` | `WRITES_BLOCKED_DISK_FULL` | *(none)* |
| Writes blocked — clock skew | `UNAVAILABLE` | `WRITES_BLOCKED_CLOCK_SKEW` | *(none)* |
| Cold storage disabled | `FAILED_PRECONDITION` | `COLD_STORAGE_DISABLED` | *(none)* |
| No chapter open | `FAILED_PRECONDITION` | `NO_CHAPTER_OPEN` | *(none)* |
| Chapter not found | `NOT_FOUND` | `CHAPTER_NOT_FOUND` | `chapterId` |
| Chapter not closing | `FAILED_PRECONDITION` | `CHAPTER_NOT_CLOSING` | `chapterId` |
| Chapter not closed | `FAILED_PRECONDITION` | `CHAPTER_NOT_CLOSED` | `chapterId` |
| Chapter not archiving | `FAILED_PRECONDITION` | `CHAPTER_NOT_ARCHIVING` | `chapterId` |
| Metadata not found | `NOT_FOUND` | `METADATA_NOT_FOUND` | `target`, `key` |
| Metadata field not in schema | `FAILED_PRECONDITION` | `METADATA_FIELD_NOT_IN_SCHEMA` | `target`, `key` |
| Invalid receipt | `INVALID_ARGUMENT` | `INVALID_RECEIPT` | `reason` |
| Invalid cron expression | `INVALID_ARGUMENT` | `INVALID_CRON_EXPRESSION` | `expression`, `details` |
| Prepared query already exists | `ALREADY_EXISTS` | `PREPARED_QUERY_ALREADY_EXISTS` | `ledger`, `name` |
| Prepared query not found | `NOT_FOUND` | `PREPARED_QUERY_NOT_FOUND` | `ledger`, `name` |
| Filter compilation error | `INVALID_ARGUMENT` | `FILTER_COMPILATION_ERROR` | `detail` |
| Index not found | `FAILED_PRECONDITION` | `INDEX_NOT_FOUND` | `index` |
| Index building | `FAILED_PRECONDITION` | `INDEX_BUILDING` | `index` |
| Index inconsistent | `INTERNAL` | `INDEX_INCONSISTENT` | `index`, `detail` |
| Account not matching type | `FAILED_PRECONDITION` | `ACCOUNT_NOT_MATCHING_TYPE` | `address` |
| Account type not found | `NOT_FOUND` | `ACCOUNT_TYPE_NOT_FOUND` | `name` |
| Account type already exists | `ALREADY_EXISTS` | `ACCOUNT_TYPE_ALREADY_EXISTS` | `name` |
| Account type conflict | `FAILED_PRECONDITION` | `ACCOUNT_TYPE_CONFLICT` | `pattern`, `existingName`, `existingPattern` |
| Account type has accounts | `FAILED_PRECONDITION` | `ACCOUNT_TYPE_HAS_ACCOUNTS` | `name` |
| Invalid pattern | `INVALID_ARGUMENT` | `INVALID_PATTERN` | `pattern`, `details` |
| Transient account non-zero | `FAILED_PRECONDITION` | `TRANSIENT_ACCOUNT_NON_ZERO` | `account`, `asset` |
| Sink already exists | `ALREADY_EXISTS` | `SINK_ALREADY_EXISTS` | `name` |
| Sink not found | `NOT_FOUND` | `SINK_NOT_FOUND` | `name` |
| Sink batch size too large | `INVALID_ARGUMENT` | `SINK_BATCH_SIZE_TOO_LARGE` | `name`, `batchSize`, `max` |
| Invalid order type (protocol mismatch) | `INTERNAL` | `INVALID_ORDER_TYPE` | `typeName` |
| Invalid apply type (protocol mismatch) | `INTERNAL` | `INVALID_APPLY_TYPE` | `typeName` |
| Storage operation failed | `INTERNAL` | `STORAGE_OPERATION_FAILED` | `operation` |
| Checkpoint ID required | `INVALID_ARGUMENT` | `CHECKPOINT_ID_REQUIRED` | *(none)* |

### REST/HTTP Error Mapping

The REST adapter uses the same `Describable.Reason()` as the JSON `errorCode` field — wire contract is uniform with gRPC's `ErrorInfo.reason`. HTTP status code is derived from `Kind` via `kindToHTTPStatus`:

| ErrorKind | HTTP Status |
|-----------|-------------|
| `KindValidation` | 400 Bad Request |
| `KindNotFound` | 404 Not Found |
| `KindAlreadyExists` | 409 Conflict |
| `KindConflict` | 409 Conflict |
| `KindPrecondition` | 400 Bad Request |
| `KindUnavailable` | 503 Service Unavailable |
| `KindResourceExhausted` | 429 Too Many Requests |
| `KindUnauthenticated` | 401 Unauthorized |
| `KindPermissionDenied` | 403 Forbidden |
| `KindInternal` | 500 Internal Server Error |

**Breaking change in #432**: HTTP `errorCode` JSON field previously used HTTP-specific codes (`"CONFLICT"`, `"NOT_FOUND"`, `"SCRIPT_PARSE_ERROR"`, `"INSUFFICIENT_FUNDS"`, ...) that were sometimes the same as the gRPC Reason and sometimes different. After the Describable refactor (#432) it is uniformly `Reason()` from the table above — e.g. `"LEDGER_ALREADY_EXISTS"` (was `"CONFLICT"`), `"NUMSCRIPT_PARSE_ERROR"` (was `"SCRIPT_PARSE_ERROR"`). Update REST clients to widen pattern matching accordingly.

**Client-side Kind reconstruction is lossy — match on `Reason`, not `Kind`.** The server-side `Kind` enum has two values (`KindConflict` and `KindPrecondition`) that both serialise to `codes.FailedPrecondition` on the wire. Client SDKs that reconstruct a `RemoteError` from a gRPC status (see `cmd/ledgerctl/cmdutil`) conservatively pick `KindPrecondition` for every `FailedPrecondition` response — so a server-side `KindConflict` (e.g. ledger deleted, transaction already reverted) reads as `KindPrecondition` client-side. Branching on `RemoteError.Kind()` will therefore misclassify conflict responses. Match on `Reason()` (`LEDGER_DELETED`, `TRANSACTION_ALREADY_REVERTED`, etc.) instead — it is preserved end-to-end and is the reliable discriminator.

**Client-side usage (Go):**
```go
import (
    "google.golang.org/genproto/googleapis/rpc/errdetails"
    "google.golang.org/grpc/status"
)

st, ok := status.FromError(err)
if ok {
    for _, detail := range st.Details() {
        if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Domain == "ledger" {
            switch info.Reason {
            case "INSUFFICIENT_FUNDS":
                // Handle insufficient funds using info.Metadata
            case "LEDGER_NOT_FOUND":
                // Handle ledger not found
            }
        }
    }
}
```
