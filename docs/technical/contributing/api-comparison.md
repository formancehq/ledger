# API Comparison: ledger-v3-poc vs github.com/formancehq/ledger

This document compares the POC's API with the original Formance ledger API and documents missing features.

## Summary

| Feature | POC | Original | Notes |
|---------|-----|----------|-------|
| **Transactions (Write)** |
| Create transaction (postings) | ✅ | ✅ | |
| Create transaction (numscript) | ✅ | ✅ | |
| Revert transaction | ✅ | ✅ | |
| Revert with `force` | ✅ | ✅ | |
| Revert with `atEffectiveDate` | ✅ | ✅ | |
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
| Create ledger | ✅ | ✅ | |
| Create mirror ledger | ✅ | ❌ | HTTP or PostgreSQL source |
| Promote mirror ledger | ✅ | ❌ | Mirror → Normal mode |
| Delete ledger | ✅ | ✅ | |
| Get ledger | ✅ | ✅ | |
| List ledgers | ✅ | ✅ | |
| **Account Types** |
| Add account type | ✅ | ❌ | Pattern-based account validation |
| List account types | ✅ | ❌ | List all types for a ledger |
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
| **Numscript Library** |
| Save numscript (versioned) | ✅ | ❌ | Per-ledger, semver versioning (e.g. "1.0.0") |
| Get numscript (by version) | ✅ | ❌ | Per-ledger, query param `?version=1.0.0`, empty = latest |
| List numscripts | ✅ | ❌ | Per-ledger, lists all saved numscripts |
| Delete numscript | ✅ | ❌ | Per-ledger, deletes latest version entry |
| **Audit Log** |
| Audit log (success + failure) | ✅ | ❌ | Replicated via Raft, stored in Pebble |
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
| **Periods** |
| Close period | ✅ | ❌ | Two-step close: ClosePeriod → SealPeriod |
| Seal period (background) | ✅ | ❌ | Background sealer computes BLAKE3 sealing hash |
| List periods | ✅ | ❌ | gRPC streaming |
| Transaction receipts (JWT) | ✅ | ❌ | HMAC-SHA256 JWT receipts with period ID; available on GetTransaction |
| Receipt-based revert | ✅ | ❌ | Revert using JWT receipt (avoids server-side lookup) |
| Period crash recovery | ✅ | ❌ | Automatic recovery for both crash windows |
| Archive period | ✅ | ❌ | Two-step archive: ArchivePeriod → ConfirmArchivePeriod with cold storage export |
| Store restore | ✅ | ❌ | Upload backup, validate, preview, finalize (--restore mode) |
| **Prepared Queries** |
| Create prepared query | ✅ | ❌ | Reusable parameterized filter queries |
| Update prepared query | ✅ | ❌ | |
| Delete prepared query | ✅ | ❌ | |
| List prepared queries | ✅ | ❌ | |
| Execute prepared query (list) | ✅ | ❌ | Returns matching entities with cursor pagination; validates filters against metadata schema |
| Execute prepared query (aggregate) | ✅ | ❌ | Returns aggregated volumes per asset; validates filters against metadata schema |
| **User-Configurable Indexes** |
| Create index | ✅ | ❌ | Opt-in address, metadata, reference, timestamp, or inserted-at indexes per ledger |
| Drop index | ✅ | ❌ | Remove an index from a ledger |
| List indexes | ✅ | ❌ | View all indexes with build status and backfill progress (via GetLedger) |
| **Volumes (responses)** |
| postCommitVolumes | ✅ | ✅ | Opt-in via `expandVolumes` in request body |
| preCommitVolumes | ❌ | ✅ | Intentionally removed |
| postCommitEffectiveVolumes | ❌ | ✅ | Intentionally removed |
| preCommitEffectiveVolumes | ❌ | ✅ | Intentionally removed |

**Legend:** ✅ Implemented | ⚠️ Partially/Not implemented | ❌ Absent

---

## Features Implemented in POC

### 1. Transaction Creation

**Endpoint:** `POST /{ledgerName}/transactions`

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
- ✅ Asset colors (fund origin tracking)
- ✅ `get_amount()` / `get_asset()` functions
- ✅ Mid-script function calls (balance queries during execution)
- ✅ `oneof` selector (conditional routing)
- ✅ `overdraft()` function (dynamic overdraft calculation)

See [Numscript Guide](./numscript.md) for complete documentation.

### 2. Transaction Revert

**Endpoint:** `POST /{ledgerName}/transactions/{transactionId}/revert`

**Features:**
- ✅ Standard revert
- ✅ `force` option (ignore insufficient balances)
- ✅ `atEffectiveDate` option (use original transaction timestamp)
- ✅ Revert metadata
- ✅ Verification that transaction is not already reverted

### 3. Metadata Management

**Endpoints:**
- `POST /{ledgerName}/metadata` - Save ledger metadata (new in v3)
- `DELETE /{ledgerName}/metadata/{key}` - Delete ledger metadata (new in v3)
- `POST /{ledgerName}/accounts/{address}/metadata` - Save account metadata
- `DELETE /{ledgerName}/accounts/{address}/metadata/{key}` - Delete account metadata
- `POST /{ledgerName}/transactions/{transactionId}/metadata` - Save transaction metadata
- `DELETE /{ledgerName}/transactions/{transactionId}/metadata/{key}` - Delete transaction metadata
- `GET /{ledgerName}/metadata-schema` - Get metadata schema status (field types and conversion progress)
- `PUT /{ledgerName}/metadata-schema/{targetType}/{key}` - Set/change metadata field type
- `DELETE /{ledgerName}/metadata-schema/{targetType}/{key}` - Remove metadata field type declaration

Ledger metadata is stored separately from ledger configuration (LedgerInfo) and is populated at read time when calling `GET /{ledgerName}` or `GET /` (list ledgers). It uses the same typed value system as account/transaction metadata.

### 4. Bulk Operations

**Endpoint:** `POST /{ledgerName}/_bulk`

**Supported actions:**
- ✅ `CREATE_TRANSACTION`
- ✅ `ADD_METADATA` (account and transaction)
- ✅ `REVERT_TRANSACTION`
- ✅ `DELETE_METADATA` (account and transaction)

**Options:**
- ✅ `continueOnFailure` - Continue even on error
- ✅ `atomic` - All operations or nothing (supports cross-ledger operations)

> **Note:** Unlike v2, v3 supports **system-level atomic bulk operations** that can span multiple ledgers. This is enabled by the [Global Log Architecture](../architecture/core/global-log.md).

### 5. Ledger Management

**Endpoints:**
- `POST /{ledgerName}` - Create a ledger (supports optional `chartOfAccounts` and `enforcementMode` in body)
- `DELETE /{ledgerName}` - Delete a ledger
- `GET /{ledgerName}` - Get ledger info (read)
- `GET /` - List all ledgers (read)

### 5b. Account Types

**Endpoints:**
- `GET /{ledgerName}/account-types` - List all account types for a ledger
- `GET /{ledgerName}/account-types/{typeName}` - Get details of a specific account type
- `POST /{ledgerName}/account-types` - Add a new account type
- `DELETE /{ledgerName}/account-types/{typeName}` - Remove an account type

**Features:**
- ✅ Pattern-based account address validation (e.g., `users:{id}:checking`)
- ✅ Variable segments with optional regex constraints (e.g., `{iban:^[A-Z]{2}[0-9]{14}$}`)
- ✅ Ledger-level default enforcement mode: STRICT (reject) or AUDIT (warnings)
- ✅ Longest-match / highest-specificity resolution when multiple types match
- ✅ Account type lifecycle: ACTIVE → DEPRECATED
- ✅ `world` account always passes validation
- ✅ Account types can be set at ledger creation time or added later
- ✅ Persistence modes: `NORMAL` (default), `EPHEMERAL` (purged when zero balance), `TRANSIENT` (never persisted, must be zero at end of batch)

### 6. Transaction Read

**Endpoint:** `GET /{ledgerName}/transactions/{transactionId}`

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

### 7. Periods

Periods partition a ledger's transaction history into discrete, sealed segments. See [Periods Architecture](../architecture/data-model/periods.md) for full documentation.

**gRPC Methods:**
- `Apply(ClosePeriodRequest)` - Close the current open period (write, leader-only)
- `Apply(SetPeriodScheduleRequest)` - Set automatic period rotation schedule (write, leader-only)
- `Apply(DeletePeriodScheduleRequest)` - Delete automatic period rotation schedule (write, leader-only)
- `Apply(ArchivePeriodRequest)` - Archive a closed period to cold storage (write, leader-only)
- `GetPeriodSchedule(GetPeriodScheduleRequest)` - Get the current period rotation schedule (read, any node)
- `ListPeriods(ListPeriodsRequest)` - Stream all periods (read, any node)

**Features:**
- ✅ Close current period (OPEN → CLOSING → CLOSED lifecycle)
- ✅ Background sealing with BLAKE3 hash (off Raft critical path)
- ✅ Automatic crash recovery for both crash windows
- ✅ Transaction receipts (HMAC-SHA256 JWT with period ID)
- ✅ List all periods with status, timestamps, and sealing hashes
- ✅ Archive period (CLOSED → ARCHIVED with cold storage export and hot purge)
- ✅ Scheduled automatic period rotation (cron-based, leader-only, runtime-configurable)

**CLI commands:**
```bash
# Close the current open period
ledgerctl periods close

# Set automatic period rotation (every day at midnight)
ledgerctl periods set-schedule "0 0 * * *"

# Disable automatic rotation
ledgerctl periods delete-schedule

# Show current schedule
ledgerctl periods get-schedule

# Archive a closed period to cold storage
ledgerctl periods archive 1

# List all periods
ledgerctl periods list
```

### 8. Mirror Ledgers

Mirror mode enables one-way synchronization from an existing v2 ledger into a v3 ledger. The mirror ledger is read-only until promoted to normal mode.

**Create a mirror ledger:** `POST /{ledgerName}`

Request body includes `mode` (`"MIRROR"`) and a `mirrorSource` object specifying the source configuration.

**Source types:**
- **HTTP** (`type: "http"`) — Polls the v2 API endpoint `GET /v2/{ledger}/logs`. Fields: `baseUrl`, `oauth2ClientId`, `oauth2ClientSecret`, `oauth2TokenEndpoint`, `oauth2Scopes` (optional, for OAuth2 client credentials authentication).
- **PostgreSQL** (`type: "postgres"`) — Reads directly from the v2 ledger's PostgreSQL database. Fields: `dsn`.

If `type` is omitted, defaults to `"http"`.

**Write guard:** All direct write operations (create transaction, save metadata, delete metadata, revert transaction) are rejected on mirror-mode ledgers with HTTP 409 (`LEDGER_IN_MIRROR_MODE`) or gRPC `FailedPrecondition`.

**Sync progress:** `GET /{ledgerName}` returns a `mirrorSyncProgress` object for mirror ledgers with:
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

**Promote a mirror ledger:** `POST /{ledgerName}/promote`

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
- `GET /{ledgerName}/numscripts` - List all saved numscripts for a ledger
- `GET /{ledgerName}/numscripts/{name}?version=` - Get a numscript by name (optional version query param)
- `PUT /{ledgerName}/numscripts/{name}` - Save a numscript (create new version or overwrite latest)
- `DELETE /{ledgerName}/numscripts/{name}` - Delete a numscript

**Versioning:**

Numscripts use **semantic versioning** (semver) with the format `major.minor.patch` (e.g. `"1.0.0"`).

When saving a numscript via `PUT /{ledgerName}/numscripts/{name}`, the request body includes:
- `content` (required): The numscript source code
- `version` (optional): Controls versioning behavior:
  - A semver string (e.g. `"2.0.0"`) creates a new version. Fails with 409 if the version already exists.
  - The special value `"latest"` overwrites the content of the current latest version.
  - If omitted or empty, defaults to `"latest"`.

When retrieving a numscript via `GET /{ledgerName}/numscripts/{name}`, the `version` query parameter selects which version to return. If omitted or empty, the latest version is returned.

**Response schema (NumscriptInfo):**
- `name` (string): Numscript name
- `content` (string): Numscript source code
- `version` (string): Semver version (e.g. `"1.0.0"`)
- `createdAt` (string, date-time): Timestamp

### 10. Prepared Queries and User-Configurable Indexes

Prepared queries are reusable, named filter queries stored per-ledger. They can be executed in two modes: `LIST` (returns matching entity IDs with cursor pagination) and `AGGREGATE_VOLUMES` (returns aggregated volumes per asset for matched accounts).

**Endpoints:**
- `POST /{ledgerName}/prepared-queries` — Create
- `PUT /{ledgerName}/prepared-queries/{name}` — Update filter
- `DELETE /{ledgerName}/prepared-queries/{name}` — Delete
- `GET /{ledgerName}/prepared-queries` — List
- `POST /{ledgerName}/prepared-queries/{name}/execute` — Execute

**Supported filter types (`QueryFilter`):**

| Filter | Target | Requires index |
|--------|--------|----------------|
| `FieldCondition` — metadata string equality | accounts, transactions | yes (metadata index) |
| `AddressMatch` — prefix or exact address match | accounts | no |
| `AndFilter` / `OrFilter` / `NotFilter` | — | depends on sub-filters |
| `ReferenceCondition` — transaction reference exact match | transactions | yes (`reference` builtin index) |
| `BuiltinUintCondition` with `TIMESTAMP` — effective date range | transactions | yes (`timestamp` builtin index) |
| `BuiltinUintCondition` with `INSERTED_AT` — creation date range | transactions | yes (`inserted_at` builtin index) |
| `BuiltinUintCondition` with `ID` — transaction ID range or equality | transactions | no (direct range scan) |

**User-configurable indexes** control which filters are available. Each index has a lifecycle: BUILDING (backfill in progress) → READY (queries enabled).

| Index type | CLI flag | Enables |
|------------|----------|---------|
| `address` | `--type address` | `AddressMatch` on transaction queries |
| `source-address` | `--type source-address` | source-only address matching |
| `dest-address` | `--type dest-address` | destination-only address matching |
| `metadata` | `--type metadata --target … --key …` | `FieldCondition` on the specified field |
| `reference` | `--type reference` | `ReferenceCondition` |
| `timestamp` | `--type timestamp` | `BuiltinUintCondition(TIMESTAMP)` |
| `inserted-at` | `--type inserted-at` | `BuiltinUintCondition(INSERTED_AT)` |

> Filtering by transaction ID (`BuiltinUintCondition(ID)`) is always available with no index required.

**CLI commands:**
```bash
# Create and use a reference index
ledgerctl ledgers create-index --ledger my-ledger --type reference
ledgerctl ledgers list-indexes --ledger my-ledger

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
- HTTP endpoint (probably `POST /{ledgerName}/_import`)
- Log validation
- Sequential insertion with consistency verification
- Streaming support for large volumes

### 2. ❌ Log Export

**Description:** Export all logs from a ledger for backup or migration.

**Current status:**
- Interface defined in `Ledger.Export(ctx, w ExportWriter) error`
- Implementation returns `ErrNotFound`

**To implement:**
- HTTP endpoint (probably `GET /{ledgerName}/_export`)
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
- `POST /{ledgerName}/metadata` - Add/modify metadata
- `DELETE /{ledgerName}/metadata/{key}` - Delete a metadata key

These endpoints are documented in Section 3 (Metadata Management) above.

### 5. ❌ Ledger Configuration Update

**Description:** Modify certain ledger parameters after creation (e.g., maintenanceInterval).

**To implement:**
- `PATCH /{ledgerName}` or `PUT /{ledgerName}/config`

---

## Intentionally Removed Features

### 1. Post-Commit Volumes (Opt-in) / Pre-Commit Volumes (Removed)

**Description:** In the original ledger, transaction creation responses include volumes before and after the commit:

- `postCommitVolumes` - Volumes after transaction application
- `preCommitVolumes` - Volumes before transaction application
- `postCommitEffectiveVolumes` - Effective volumes after application (with effective timestamp)
- `preCommitEffectiveVolumes` - Effective volumes before application (with effective timestamp)

**POC Status:**
- `postCommitVolumes` is available **opt-in** via `expandVolumes: true` in the request body for both `createTransaction` and `revertTransaction`. When enabled, the response includes volumes (input/output) per account/asset after the transaction is applied.
- `preCommitVolumes`, `postCommitEffectiveVolumes`, and `preCommitEffectiveVolumes` remain **intentionally removed**.

**Usage:**
```json
POST /{ledgerName}/transactions
{
  "postings": [...],
  "expandVolumes": true
}
```

The response will include a `postCommitVolumes` field:
```json
{
  "postCommitVolumes": {
    "users:alice": {
      "USD/2": { "input": "0", "output": "1000" }
    },
    "users:bob": {
      "USD/2": { "input": "1000", "output": "0" }
    }
  }
}
```

**Impact on clients:**
- Clients that need post-commit volumes can opt-in with `expandVolumes: true`
- When `expandVolumes` is false (default), no volumes are returned (preserving the lightweight default)
- `preCommitVolumes` and effective volumes remain removed; use dedicated read endpoints if needed

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
- Supported via `Idempotency-Key` header (HTTP) or `idempotency_key` field (gRPC)
- System-level scope (not per-ledger)
- Hash-based content verification (BLAKE3)
- Stored in generation-based cache and persisted to Pebble
- Maximum key length: 256 characters (validated at admission)

**Original:** Same mechanism.

**Status:** ✅ Compliant

See [Idempotency](../architecture/data-model/idempotency.md) for detailed documentation.

---

## Read Features

Read endpoints comparison with the original ledger:

| Endpoint | POC | Original | Notes |
|----------|-----|----------|-------|
| `GET /{ledgerName}/transactions/{id}` | ✅ | ✅ | Get a transaction by ID |
| `GET /{ledgerName}/transactions` | ⚠️ | ✅ | List transactions (gRPC stream only, no HTTP handler) |
| `GET /{ledgerName}/accounts` | ✅ | ✅ | List accounts (rich boolean filter, cursor pagination) |
| `GET /{ledgerName}/accounts/{address}` | ✅ | ✅ | Get an account |
| `GET /{ledgerName}/accounts/{address}/balances` | ❌ | ✅ | Get account balances |
| `GET /{ledgerName}/accounts/{address}/volumes` | ❌ | ✅ | Get account volumes |
| `GET /{ledgerName}/volumes` | ✅ | ✅ | Aggregate volumes (per-asset, supports prefix filtering) |
| `GET /{ledgerName}/logs` | ✅ | ✅ | List per-ledger logs. Supports `?after=` for pagination |
| `GET /{ledgerName}/aggregate/balances` | ❌ | ✅ | Balance aggregation |
| `GET /{ledgerName}/stats` | ✅ | ✅ | Ledger statistics (account + transaction count) |
| `GET /{ledgerName}` | ✅ | ✅ | Get ledger info |
| `POST /{ledgerName}/promote` | ✅ | ❌ | Promote mirror ledger to normal mode |
| `GET /` | ✅ | ✅ | List all ledgers |
| `GET /{ledgerName}/metadata-schema` | ✅ | ❌ | Get metadata schema status |
| `GET /{ledgerName}/analyze-accounts` | ✅ | ❌ | Analyze accounts and suggest Chart of Accounts |
| `GET /{ledgerName}/analyze-transactions` | ✅ | ❌ | Analyze transaction flow patterns |
| `PUT /{ledgerName}/metadata-schema/{targetType}/{key}` | ✅ | ❌ | Set metadata field type |
| `DELETE /{ledgerName}/metadata-schema/{targetType}/{key}` | ✅ | ❌ | Remove metadata field type |
| `POST /{ledgerName}/prepared-queries` | ✅ | ❌ | Create a prepared query |
| `PUT /{ledgerName}/prepared-queries/{queryName}` | ✅ | ❌ | Update a prepared query |
| `DELETE /{ledgerName}/prepared-queries/{queryName}` | ✅ | ❌ | Delete a prepared query |
| `GET /{ledgerName}/prepared-queries` | ✅ | ❌ | List prepared queries |
| `POST /{ledgerName}/prepared-queries/{queryName}/execute` | ✅ | ❌ | Execute a prepared query |
| `GET /{ledgerName}/numscripts` | ✅ | ❌ | List all numscripts for a ledger |
| `GET /{ledgerName}/numscripts/{name}?version=` | ✅ | ❌ | Get numscript (semver version, empty = latest) |
| `PUT /{ledgerName}/numscripts/{name}` | ✅ | ❌ | Save numscript (semver versioned) |
| `DELETE /{ledgerName}/numscripts/{name}` | ✅ | ❌ | Delete numscript |
| `GET /{ledgerName}/account-types` | ✅ | ❌ | List account types |
| `GET /{ledgerName}/account-types/{typeName}` | ✅ | ❌ | Get account type |
| `POST /{ledgerName}/account-types` | ✅ | ❌ | Add account type |
| `DELETE /{ledgerName}/account-types/{typeName}` | ✅ | ❌ | Remove account type |
| `PUT /{ledgerName}/account-types/default-enforcement-mode` | ✅ | ❌ | Set default enforcement mode (STRICT/AUDIT) |
| `GET /{ledgerName}/indexes/{metadataKey}` | ✅ | ❌ | Inspect metadata index (distinct values, facets, summary) |
| `POST /{ledgerName}/bulk` | ✅ | ❌ | Bulk operations (alternate path without underscore) |

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

See [Global Log Architecture](../architecture/core/global-log.md) for details on how the two-level log architecture enables cross-ledger atomic operations.

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
| `GetPeriodSchedule` | Get current period rotation schedule | ✅ |
| `GetMetadataSchemaStatus` | Get metadata field conversion status | ✅ |
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
| `Apply(SaveNumscript)` | Save a numscript (semver versioned) | ✅ |
| `Apply(DeleteNumscript)` | Delete a numscript | ✅ |
| `GetNumscript` | Get a numscript by name and optional version | ✅ |
| `ListNumscripts` | List all saved numscripts | ✅ |
| `Apply(ClosePeriod)` | Close the current open period | ✅ |
| `ListPeriods` | Stream all periods | ✅ |
| `ListAuditEntries` | Stream audit log entries (success + failure) | ✅ |
| `GetAuditEntry` | Get a single audit entry by sequence number | ✅ |
| `ListLogs` | Stream system logs (supports `ledger` and `log_id` filters for per-ledger listing and pagination) | ✅ |
| `GetLog` | Get a single system log by sequence number | ✅ |
| `ListSigningKeys` | Stream all registered signing keys | ✅ |
| `Discovery` | Return server capabilities (response signing config) | ✅ |
| `AnalyzeAccounts` | Analyze accounts and suggest Chart of Accounts | ✅ |
| `GetIndexStatus` | Read index builder progress (lag, file size) | ✅ |
| `GetLedgerStats` | Get aggregate statistics (account + transaction count) | ✅ |
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
- `idempotency_key`: Optional idempotency key
- `skip_response`: When `true`, strips log payloads from the response (only `sequence` is returned per log). Useful for historical ingestion where the client does not need the full response on success.
- One of:
  - `create_transaction`: Create a new transaction
  - `add_metadata`: Add metadata to an account or transaction
  - `revert_transaction`: Revert a transaction
  - `delete_metadata`: Delete metadata from an account or transaction
  - `save_numscript`: Save a numscript (with semver version)
  - `delete_numscript`: Delete a numscript

**Response:** `common.Log` - The log entry created by the action (stripped to `sequence` only when `skip_response` is set)

**Note:** Individual RPC methods like `CreateTransaction`, `RevertTransaction`, `SaveAccountMetadata`, etc. have been consolidated into the `Apply` method for a cleaner API.

### gRPC Error Mapping

Business errors from the processing layer are mapped to proper gRPC status codes with structured `ErrorInfo` details. This allows clients to programmatically identify error types without parsing error messages.

Each error response includes a `google.rpc.ErrorInfo` detail with:
- **`reason`**: Machine-readable error reason constant (e.g., `LEDGER_ALREADY_EXISTS`)
- **`domain`**: Always `"ledger"`
- **`metadata`**: Error-specific key-value pairs with context (e.g., account name, asset, amount)

| Error | gRPC Code | Reason | Metadata |
|-------|-----------|--------|----------|
| Ledger already exists | `ALREADY_EXISTS` | `LEDGER_ALREADY_EXISTS` | `name` |
| Ledger not found | `NOT_FOUND` | `LEDGER_NOT_FOUND` | `name` |
| Idempotency key conflict | `ALREADY_EXISTS` | `IDEMPOTENCY_KEY_CONFLICT` | `key` |
| Transaction reference conflict | `ALREADY_EXISTS` | `TRANSACTION_REFERENCE_CONFLICT` | `ledgerId`, `reference` |
| Transaction not found | `NOT_FOUND` | `TRANSACTION_NOT_FOUND` | `transactionId` |
| Transaction already reverted | `FAILED_PRECONDITION` | `TRANSACTION_ALREADY_REVERTED` | `transactionId` |
| Insufficient funds | `FAILED_PRECONDITION` | `INSUFFICIENT_FUNDS` | `account`, `asset`, `amount`, `balance` |
| Balance not found | `FAILED_PRECONDITION` | `BALANCE_NOT_FOUND` | `account`, `asset` |
| Balance not preloaded | `FAILED_PRECONDITION` | `BALANCE_NOT_PRELOADED` | `account`, `asset` |
| Numscript parse error | `INVALID_ARGUMENT` | `NUMSCRIPT_PARSE_ERROR` | `details` |
| Numscript not found | `NOT_FOUND` | `NUMSCRIPT_NOT_FOUND` | `name` |
| Numscript invalid version | `INVALID_ARGUMENT` | `NUMSCRIPT_INVALID_VERSION` | `version` |
| Numscript version already exists | `ALREADY_EXISTS` | `NUMSCRIPT_VERSION_ALREADY_EXISTS` | `name`, `version` |
| Numscript no version exists | `FAILED_PRECONDITION` | `NUMSCRIPT_NO_VERSION_EXISTS` | `name` |
| Validation error | `INVALID_ARGUMENT` | `VALIDATION` | *(none)* |
| Audit disabled | `FAILED_PRECONDITION` | `AUDIT_DISABLED` | *(none)* |
| Ledger in mirror mode | `FAILED_PRECONDITION` | `LEDGER_IN_MIRROR_MODE` | `name` |
| Ledger not in mirror mode | `FAILED_PRECONDITION` | `LEDGER_NOT_IN_MIRROR_MODE` | `name` |
| Prepared query already exists | `ALREADY_EXISTS` | `PREPARED_QUERY_ALREADY_EXISTS` | `name` |
| Prepared query not found | `NOT_FOUND` | `PREPARED_QUERY_NOT_FOUND` | `name` |
| Account not matching type | `FAILED_PRECONDITION` | `ACCOUNT_NOT_MATCHING_TYPE` | `address` |
| Account type not found | `NOT_FOUND` | `ACCOUNT_TYPE_NOT_FOUND` | `name` |
| Account type already exists | `ALREADY_EXISTS` | `ACCOUNT_TYPE_ALREADY_EXISTS` | `name` |
| Invalid pattern | `INVALID_ARGUMENT` | `INVALID_PATTERN` | `pattern`, `details` |
| Account type has accounts | `FAILED_PRECONDITION` | `ACCOUNT_TYPE_HAS_ACCOUNTS` | `name` |

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
