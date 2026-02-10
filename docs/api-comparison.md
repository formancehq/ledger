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
| List accounts | ❌ | ✅ | Not implemented |
| Get account balances | ⚠️ | ✅ | Included in account volumes |
| Get account volumes | ✅ | ✅ | Returns input/output/balance per asset |
| **Logs** |
| List logs | ❌ | ✅ | Not implemented |
| **Import/Export** |
| Import logs | ⚠️ | ✅ | Interface defined but not implemented |
| Export logs | ⚠️ | ✅ | Interface defined but not implemented |
| **Idempotency** |
| Idempotency key | ✅ | ✅ | |
| **Store Operations** |
| Store metrics | ✅ | ❌ | Pebble storage metrics |
| Store integrity check | ✅ | ❌ | Hash chain + derived data verification |
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

> **Note:** Unlike v2, v3 supports **system-level atomic bulk operations** that can span multiple ledgers. This is enabled by the [Global Log Architecture](./global-log.md).

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

### 3. ⚠️ Unique Reference Validation

**Description:** In the original ledger, transaction reference must be unique within a ledger.

**Current status:** 
- Lock on reference is implemented (`tx/references/{reference}`)
- But duplication check needs to be confirmed at store level

**To verify:**
- Ensure the runtime store checks reference uniqueness
- Return appropriate error if reference already exists

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
| `GET /{ledgerName}/accounts` | ❌ | ✅ | List accounts |
| `GET /{ledgerName}/accounts/{address}` | ✅ | ✅ | Get an account |
| `GET /{ledgerName}/accounts/{address}/balances` | ❌ | ✅ | Get account balances |
| `GET /{ledgerName}/accounts/{address}/volumes` | ❌ | ✅ | Get account volumes |
| `GET /{ledgerName}/logs` | ❌ | ✅ | List logs |
| `GET /{ledgerName}/aggregate/balances` | ❌ | ✅ | Balance aggregation |
| `GET /{ledgerName}/stats` | ❌ | ✅ | Ledger statistics |
| `GET /{ledgerName}` | ✅ | ✅ | Get ledger info |
| `GET /` | ✅ | ✅ | List all ledgers |

---

## Priority Recommendations

### High Priority
1. **Import/Export** - Critical for migration and backups

### Medium Priority
2. **Unique reference validation** - Verify and document behavior
3. **Ledger metadata update** - Useful for ledger management

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

See [Global Log Architecture](./global-log.md) for details on how the two-level log architecture enables cross-ledger atomic operations.

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
