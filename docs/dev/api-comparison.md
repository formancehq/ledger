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
| List transactions | ✅ | ✅ | gRPC stream with pagination |
| **Metadata** |
| Save account metadata | ✅ | ✅ | |
| Delete account metadata | ✅ | ✅ | |
| Save transaction metadata | ✅ | ✅ | |
| Delete transaction metadata | ✅ | ✅ | |
| **Bulk** |
| Bulk CREATE_TRANSACTION | ✅ | ✅ | |
| Bulk ADD_METADATA | ✅ | ✅ | |
| Bulk REVERT_TRANSACTION | ✅ | ✅ | |
| Bulk DELETE_METADATA | ✅ | ✅ | |
| Bulk atomic | ✅ | ✅ | System-level atomicity (cross-ledger) |
| Bulk continueOnFailure | ✅ | ✅ | |
| **Ledger** |
| Create ledger | ✅ | ✅ | |
| Delete ledger | ✅ | ✅ | |
| Get ledger | ✅ | ✅ | |
| List ledgers | ✅ | ✅ | |
| **Accounts (Read)** |
| Get account | ✅ | ✅ | Includes volumes per asset |
| List accounts | ✅ | ✅ | Supports prefix filter and cursor pagination |
| Get account balances | ⚠️ | ✅ | Included in account volumes |
| Get account volumes | ✅ | ✅ | Returns input/output/balance per asset |
| **Logs** |
| List logs | ✅ | ✅ | gRPC stream |
| **Import/Export** |
| Import logs | ⚠️ | ✅ | Interface defined but not implemented |
| Export logs | ⚠️ | ✅ | Interface defined but not implemented |
| **Idempotency** |
| Idempotency key | ✅ | ✅ | |
| **Reference Uniqueness** |
| Unique reference validation | ✅ | ✅ | Per-ledger uniqueness, HTTP 409 on conflict |
| **Audit Log** |
| Audit log (success + failure) | ✅ | ❌ | Replicated via Raft, stored in Pebble |
| Audit log disable/enable | ✅ | ❌ | `--audit-enabled` flag |
| **Error Handling** |
| Structured gRPC error codes | ✅ | ✅ | BusinessError with ErrorInfo details |
| **Security** |
| Request signing (Ed25519) | ✅ | ❌ | Envelope pattern with signed_payload |
| Dynamic key management | ✅ | ❌ | Register/revoke keys via gRPC API (bootstrap: first key unsigned) |
| Require signatures | ✅ | ❌ | Optional enforcement via `signing require` API call |
| **Maintenance** |
| Maintenance mode | ✅ | ❌ | Block all writes, Raft-replicated flag, dual check (admission + FSM) |
| **Store Operations** |
| Store metrics | ✅ | ❌ | Pebble storage metrics |
| Store integrity check | ✅ | ❌ | Hash chain + derived data verification |
| Store backup | ✅ | ❌ | Point-in-time Pebble backup as tar archive |
| **Periods** |
| Close period | ✅ | ❌ | Two-step close: ClosePeriod → SealPeriod |
| Seal period (background) | ✅ | ❌ | Background sealer computes BLAKE3 sealing hash |
| List periods | ✅ | ❌ | gRPC streaming |
| Transaction receipts (JWT) | ✅ | ❌ | HMAC-SHA256 JWT receipts with period ID; available on GetTransaction |
| Receipt-based revert | ✅ | ❌ | Revert using JWT receipt (avoids server-side lookup) |
| Period crash recovery | ✅ | ❌ | Automatic recovery for both crash windows |
| Archive period | ✅ | ❌ | Two-step archive: ArchivePeriod → ConfirmArchivePeriod with cold storage export |
| Store restore | ✅ | ❌ | Upload backup, validate, preview, finalize (--restore mode) |
| **Volumes (responses)** |
| postCommitVolumes | ❌ | ✅ | Intentionally removed |
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

**Numscript Experimental Features (all enabled by default):**
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
- `POST /{ledgerName}/accounts/{address}/metadata` - Save account metadata
- `DELETE /{ledgerName}/accounts/{address}/metadata/{key}` - Delete account metadata
- `POST /{ledgerName}/transactions/{transactionId}/metadata` - Save transaction metadata
- `DELETE /{ledgerName}/transactions/{transactionId}/metadata/{key}` - Delete transaction metadata

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

> **Note:** Unlike v2, v3 supports **system-level atomic bulk operations** that can span multiple ledgers. This is enabled by the [Global Log Architecture](./architecture/global-log.md).

### 5. Ledger Management

**Endpoints:**
- `POST /{ledgerName}` - Create a ledger
- `DELETE /{ledgerName}` - Delete a ledger
- `GET /{ledgerName}` - Get ledger info (read)
- `GET /` - List all ledgers (read)

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

Periods partition a ledger's transaction history into discrete, sealed segments. See [Periods Architecture](./architecture/periods.md) for full documentation.

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

### 4. ❌ Ledger Metadata Update

**Description:** Ability to add/modify metadata on a ledger after creation.

**Current status:**
- Metadata supported at creation
- No endpoint to modify ledger metadata after creation

**To implement:**
- `POST /{ledgerName}/metadata` - Add/modify metadata
- `DELETE /{ledgerName}/metadata/{key}` - Delete a metadata key

### 5. ❌ Ledger Configuration Update

**Description:** Modify certain ledger parameters after creation (e.g., snapshotThreshold).

**To implement:**
- `PATCH /{ledgerName}` or `PUT /{ledgerName}/config`

---

## Intentionally Removed Features

### 1. ❌ Pre/Post Commit Volumes in Transaction Responses

**Description:** In the original ledger, transaction creation responses include volumes before and after the commit:

- `postCommitVolumes` - Volumes after transaction application
- `preCommitVolumes` - Volumes before transaction application  
- `postCommitEffectiveVolumes` - Effective volumes after application (with effective timestamp)
- `preCommitEffectiveVolumes` - Effective volumes before application (with effective timestamp)

**POC Status:** These fields **no longer exist** in POC responses.

**Reason for removal:**
- **Architecture simplification**: Computing pre/post commit volumes adds complexity
- **Performance**: Avoids additional reads to compute volumes
- **Decoupling**: Volumes can be retrieved via dedicated read endpoints if needed
- **Raft consistency**: In a Raft architecture, volumes are computed by the FSM when applying the log, not when creating the command

**Impact on clients:**
- Clients that depend on these fields to display balances after transaction will need to make a separate request
- Integrations using these fields for reconciliation will need to be adapted

**Alternative in POC:**
- Use read endpoints to get account balances/volumes
- Balances are maintained in real-time in the runtime store

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

**Original:** Same mechanism.

**Status:** ✅ Compliant

See [Idempotency](./architecture/idempotency.md) for detailed documentation.

---

## Read Features

Read endpoints comparison with the original ledger:

| Endpoint | POC | Original | Notes |
|----------|-----|----------|-------|
| `GET /{ledgerName}/transactions/{id}` | ✅ | ✅ | Get a transaction by ID |
| `GET /{ledgerName}/transactions` | ✅ | ✅ | List transactions (gRPC stream) |
| `GET /{ledgerName}/accounts` | ✅ | ✅ | List accounts (prefix filter, cursor pagination) |
| `GET /{ledgerName}/accounts/{address}` | ✅ | ✅ | Get an account |
| `GET /{ledgerName}/accounts/{address}/balances` | ❌ | ✅ | Get account balances |
| `GET /{ledgerName}/accounts/{address}/volumes` | ❌ | ✅ | Get account volumes |
| `GET /{ledgerName}/logs` | ✅ | ✅ | List logs (gRPC stream) |
| `GET /{ledgerName}/aggregate/balances` | ❌ | ✅ | Balance aggregation |
| `GET /{ledgerName}/stats` | ❌ | ✅ | Ledger statistics |
| `GET /{ledgerName}` | ✅ | ✅ | Get ledger info |
| `GET /` | ✅ | ✅ | List all ledgers |

---

## Priority Recommendations

### High Priority
1. **Import/Export** - Critical for migration and backups

### Medium Priority
2. **Ledger metadata update** - Useful for ledger management

### Low Priority
4. **Ledger config update** - Can be done manually via recreation

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

See [Global Log Architecture](./architecture/global-log.md) for details on how the two-level log architecture enables cross-ledger atomic operations.

---

## gRPC API

The POC provides a gRPC API for internal service communication (Raft node forwarding to leader) and can be used by clients.

### LedgerService Methods

| Method | Description | Status |
|--------|-------------|--------|
| `CreateLedger` | Create a new ledger | ✅ |
| `DeleteLedger` | Delete a ledger | ✅ |
| `GetAllLedgersInfo` | Get all ledgers info | ✅ |
| `GetLedger` | Get ledger by name or ID | ✅ |
| `GetTransaction` | Get transaction by ID | ✅ |
| `StreamLogs` | Stream logs from a ledger | ✅ |
| `Apply` | Apply a ledger action (write operations) | ✅ |
| `Apply(ClosePeriod)` | Close the current open period | ✅ |
| `ListPeriods` | Stream all periods | ✅ |
| `ListAuditEntries` | Stream audit log entries (success + failure) | ✅ |
| `ListLogs` | Stream system logs (optional ledger filter) | ✅ |

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
- One of:
  - `create_transaction`: Create a new transaction
  - `add_metadata`: Add metadata to an account or transaction
  - `revert_transaction`: Revert a transaction
  - `delete_metadata`: Delete metadata from an account or transaction

**Response:** `common.Log` - The log entry created by the action

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
| Validation error | `INVALID_ARGUMENT` | `VALIDATION` | *(none)* |
| Audit disabled | `FAILED_PRECONDITION` | `AUDIT_DISABLED` | *(none)* |

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
