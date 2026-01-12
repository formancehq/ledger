# Write API Comparison: ledger-v3-poc vs github.com/formancehq/ledger

This document compares the POC's write API with the original Formance ledger API and documents missing features.

## Summary

| Feature | POC | Original | Notes |
|---------|-----|----------|-------|
| **Transactions** |
| Create transaction (postings) | ✅ | ✅ | |
| Create transaction (numscript) | ✅ | ✅ | |
| Revert transaction | ✅ | ✅ | |
| Revert with `force` | ✅ | ✅ | |
| Revert with `atEffectiveDate` | ✅ | ✅ | |
| Create transaction with `force` | ❌ | ✅ | Not implemented |
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
| Bulk parallel | ✅ | ✅ | |
| Bulk atomic | ⚠️ | ✅ | Not implemented |
| Bulk continueOnFailure | ✅ | ✅ | |
| **Ledger** |
| Create ledger | ✅ | ✅ | |
| Delete ledger | ✅ | ✅ | |
| **Import/Export** |
| Import logs | ⚠️ | ✅ | Interface defined but not implemented |
| Export logs | ⚠️ | ✅ | Interface defined but not implemented |
| **Idempotency** |
| Idempotency key | ✅ | ✅ | |
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
- ✅ Transaction metadata
- ✅ Account metadata in the same request
- ✅ Transaction reference
- ✅ Custom timestamp
- ✅ Idempotency key

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
- ✅ `parallel` - Parallel execution
- ✅ `continueOnFailure` - Continue even on error
- ⚠️ `atomic` - **Not implemented** (returns an error)

### 5. Ledger Management

**Endpoints:**
- `POST /{ledgerName}` - Create a ledger
- `DELETE /{ledgerName}` - Delete a ledger
- `GET /{ledgerName}` - Get ledger info (read)
- `GET /` - List all ledgers (read)

---

## Missing Features

### 1. ❌ Bulk Atomic

**Description:** Execute all bulk operations in an atomic transaction. If one operation fails, all are rolled back.

**Current status:** The option exists in the API but returns an error "atomic bulk transactions are not yet supported".

**Implementation required:**
- Wrap all operations in a database transaction
- Rollback on failure
- Not compatible with `parallel`

### 2. ❌ Log Import

**Description:** Import logs from another ledger for migration or synchronization.

**Current status:** 
- Interface defined in `Ledger.Import(ctx, stream chan *ledgerpb.Log) error`
- Implementation returns `ErrNotFound`

**To implement:**
- HTTP endpoint (probably `POST /{ledgerName}/_import`)
- Log validation
- Sequential insertion with consistency verification
- Streaming support for large volumes

### 3. ❌ Log Export

**Description:** Export all logs from a ledger for backup or migration.

**Current status:**
- Interface defined in `Ledger.Export(ctx, w ExportWriter) error`
- Implementation returns `ErrNotFound`

**To implement:**
- HTTP endpoint (probably `GET /{ledgerName}/_export`)
- Log streaming
- Output format (JSON lines, protobuf, etc.)
- Pagination/cursor for large volumes

### 4. ⚠️ Unique Reference Validation

**Description:** In the original ledger, transaction reference must be unique within a ledger.

**Current status:** 
- Lock on reference is implemented (`tx/references/{reference}`)
- But duplication check needs to be confirmed at store level

**To verify:**
- Ensure the runtime store checks reference uniqueness
- Return appropriate error if reference already exists

### 5. ❌ Ledger Metadata Update

**Description:** Ability to add/modify metadata on a ledger after creation.

**Current status:**
- Metadata supported at creation
- No endpoint to modify ledger metadata after creation

**To implement:**
- `POST /{ledgerName}/metadata` - Add/modify metadata
- `DELETE /{ledgerName}/metadata/{key}` - Delete a metadata key

### 6. ❌ Ledger Configuration Update

**Description:** Modify certain ledger parameters after creation (e.g., snapshotThreshold).

**To implement:**
- `PATCH /{ledgerName}` or `PUT /{ledgerName}/config`

### 7. ❌ Force Parameter on Transaction Creation

**Description:** In the original ledger, the `force` parameter on transaction creation allows creating transactions even if source accounts have insufficient funds (bypasses balance check).

**Current status:**
- The `force` parameter exists for **revert transaction** only
- It does NOT exist for **create transaction**

**Use cases:**
- Creating transactions for accounts that can go negative (unbounded accounts)
- Forcing a transaction through when balance check should be bypassed
- Migration scenarios where balances may be temporarily inconsistent

**To implement:**
- Add `force` field to `CreateTransactionRequestPayload` in protobuf
- Regenerate protobuf code
- Update `createTransaction` logic to skip `checkBalances` when `force=true`
- Update OpenAPI specification
- Regenerate SDK

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

**POC:** Strict balance verification (except with `force` for revert).

**Original:** Same behavior, but the original ledger has configuration options for "unbounded" accounts.

**To consider:** Add unbounded accounts support if needed.

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
- Supported via `Idempotency-Key` header or field in body
- Stored in runtime store

**Original:** Same mechanism.

**Status:** ✅ Compliant

---

## Read Features (Out of Scope but Noted)

These read endpoints exist in the original ledger but are not documented here as the focus is on write operations:

- `GET /{ledgerName}/transactions` - List transactions
- `GET /{ledgerName}/transactions/{id}` - Get a transaction
- `GET /{ledgerName}/accounts` - List accounts
- `GET /{ledgerName}/accounts/{address}` - Get an account
- `GET /{ledgerName}/accounts/{address}/balances` - Get account balances
- `GET /{ledgerName}/accounts/{address}/volumes` - Get account volumes
- `GET /{ledgerName}/logs` - List logs
- `GET /{ledgerName}/aggregate/balances` - Balance aggregation
- `GET /{ledgerName}/stats` - Ledger statistics

---

## Priority Recommendations

### High Priority
1. **Import/Export** - Critical for migration and backups
2. **Bulk Atomic** - Important for complex transactional operations

### Medium Priority
3. **Force parameter on transaction creation** - Required for unbounded accounts and migration scenarios
4. **Unique reference validation** - Verify and document behavior
5. **Ledger metadata update** - Useful for ledger management

### Low Priority
6. **Ledger config update** - Can be done manually via recreation

---

## Architecture Notes

The POC uses a different architecture with Raft for replication:
- Each ledger has its own Raft group
- Write operations go through the leader
- Logs are stored via the LogStore (Pebble or SQLite)

This architecture impacts certain implementation decisions:
- Bulk atomicity requires handling at the Raft level
- Import must respect log sequence
- Export can be done from any node (local read)
