# RFC: Account Types & Pattern Migration

- **Status:** Draft
- **Target:** Ledger v3
- **Audience:** Ledger implementers, API consumers
- **Supersedes:** [Chart of Accounts RFC](chart-of-accounts.md) (replaces the monolithic tree model)
- **Scope:** Replaces the monolithic Chart of Accounts with a collection of first-class Account Types. Each type owns a pattern, a lifecycle status, and an enforcement mode. Migration between patterns becomes a natural status transition on an account type. Covers the data model, pattern engine, migration lifecycle, storage-level rewriting, read-side index updates, API surface, and protobuf changes.

---

## 0. Context and Motivation

### 0.1 The Problem

Clients using the ledger for extended periods need to change their account naming patterns for business reasons:

- **Rebranding:** `users:{id}:checking` → `clients:{id}:courant`
- **Restructuring:** `org:{id}:main` → `companies:{id}:treasury`
- **Simplification:** `platform:fees:incoming:card` → `fees:card`
- **Normalization:** `user-{id}` → `users:{id}`

The Raft log is immutable — postings in historical `CreatedTransaction` entries cannot be rewritten. The existing Chart of Accounts (a monolithic tree replaced as a whole) makes this harder than it should be.

### 0.2 Problems with the Monolithic Chart

The current `ChartOfAccounts` is a single tree:

```protobuf
message ChartOfAccounts {
  map<string, ChartSegment> roots = 1;
}
```

Updated via whole-chart replacement (`PUT /chart-of-accounts`). This causes three problems:

1. **Granularity too coarse.** To modify one pattern (e.g., `users:{id}:checking`), the entire tree must be re-pushed — including `banks`, `platform`, and every other subtree. Error-prone for large charts.

2. **No per-pattern lifecycle.** The enforcement mode (STRICT/AUDIT) is global to the ledger. During a migration, you want STRICT on the new pattern and AUDIT on the old one — independently.

3. **Migration is artificial.** A separate `AccountMigrationRule` concept is needed, with its own state machine, storage, and API — when the relationship "old pattern → new pattern" is naturally an attribute of the account type itself.

### 0.3 The Insight

The chart's `roots` map already has a natural decomposition point — each root key is an independent family. But the tree structure forces them into a single unit. If we decompose into independent **Account Types**, each owning its pattern and lifecycle:

- Adding/removing/modifying a single pattern is a targeted operation
- Migration becomes a status transition: `ACTIVE → MIGRATING → DEPRECATED`
- Per-type enforcement mode is natural
- The API surface is simpler (CRUD on types, not whole-tree replacement)

### 0.4 Requirements

1. **Immutability:** The Raft log must never be modified.
2. **Consistency:** After migration, volumes and metadata are accessible under the new address only.
3. **Auditability:** All changes (type creation, migration start/progress/completion) are recorded in the log chain.
4. **Cluster-safety:** Deterministic replay on all replicas.
5. **Resumability:** Leader crash mid-migration → new leader resumes.
6. **Non-blocking:** Normal transactions continue during migration.
7. **Pattern-level:** Migrations operate on patterns, not individual accounts.

### 0.5 What This RFC Does NOT Cover

- **Transaction address rewriting:** Historical transactions keep original addresses. Optional enrichment discussed in §12.
- **Cross-ledger migration.**
- **Chart of Accounts tree model:** This RFC replaces it entirely.

---

## 1. Data Model

### 1.1 Account Type

An Account Type is a first-class entity that defines a valid account address pattern for a ledger.

```protobuf
// AccountType defines a single account address pattern for a ledger.
message AccountType {
  string name = 1;                          // Unique identifier: "user-checking"
  string pattern = 2;                       // Address pattern: "users:{id}:checking"
  AccountTypeStatus status = 3;             // ACTIVE, MIGRATING, DEPRECATED
  ChartEnforcementMode enforcement_mode = 4; // Per-type enforcement
  string superseded_by = 5;                 // If MIGRATING/DEPRECATED: target type name
  MigrationProgress migration_progress = 6; // If MIGRATING: progress tracking
}

enum AccountTypeStatus {
  ACCOUNT_TYPE_ACTIVE = 0;       // Accepts new accounts, validated normally
  ACCOUNT_TYPE_MIGRATING = 1;    // Background worker actively rewriting accounts
  ACCOUNT_TYPE_DEPRECATED = 2;   // No new accounts; kept for chart-level backward compat
}

message MigrationProgress {
  uint64 total_accounts = 1;
  uint64 migrated_accounts = 2;
  Timestamp started_at = 3;
  Timestamp completed_at = 4;
}
```

### 1.2 Pattern Syntax

Patterns use a flat, human-readable syntax with inline variable constraints:

```
users:{id}:checking
banks:{iban:^[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}$}:main
platform:fees
org:{orgId}:departments:{deptId:^[0-9]+$}:main
```

**Grammar:**

```
pattern     = segment (":" segment)*
segment     = fixed | variable
fixed       = [a-zA-Z0-9_-]+
variable    = "{" name (":" regex)? "}"
name        = [a-zA-Z_][a-zA-Z0-9_]*
regex       = <valid Go regexp>
```

- **Fixed segments:** literal strings, must match `[a-zA-Z0-9_-]+`
- **Variable segments:** `{name}` or `{name:regex}` — capture a dynamic value, optionally constrained by regex
- Variable names must be unique within a pattern

**Examples:**

| Pattern | Matches | Does not match |
|---------|---------|----------------|
| `users:{id}:checking` | `users:alice:checking`, `users:123:checking` | `users:checking`, `users:alice:savings` |
| `banks:{iban:^[A-Z]{2}[0-9]{14}$}:main` | `banks:FR76300060000112345:main` | `banks:invalid:main` |
| `platform:fees` | `platform:fees` | `platform:revenue`, `platform:fees:extra` |
| `fees:{type}` | `fees:card`, `fees:wire` | `fees:card:extra` |

### 1.3 Storage on LedgerInfo

```protobuf
message LedgerInfo {
  // ... existing fields 1-9 ...
  // field 10-11 removed (old chart_of_accounts + enforcement_mode)
  map<string, AccountType> account_types = 12;  // NEW: name → type
}
```

The `ChartOfAccounts` message, `chart_of_accounts` field, and global `enforcement_mode` field on `LedgerInfo` are removed. All validation is driven by the `account_types` collection.

### 1.4 Validation Algorithm

Address validation changes from "walk the tree" to "match any active type, longest wins":

```go
func validateAddress(address string, types map[string]*AccountType) (matched *AccountType, ok bool) {
    if address == "world" {
        return nil, true
    }

    var best *AccountType
    bestLen := -1 // number of fixed segments matched

    for _, at := range types {
        if at.Status == ACCOUNT_TYPE_DEPRECATED {
            continue // deprecated types don't validate new addresses
        }
        segments, bindings, ok := matchPattern(address, at.Pattern)
        if !ok {
            continue
        }
        fixedCount := countFixed(segments, bindings)
        if fixedCount > bestLen {
            best = at
            bestLen = fixedCount
        }
    }

    if best == nil {
        return nil, false
    }
    return best, true
}
```

**Longest match:** When multiple patterns match the same address, the pattern with the most fixed (non-variable) segments wins. This is deterministic and analogous to HTTP route matching.

**Example:** Given types `fees:{type}` and `fees:card`:
- Address `fees:card` matches both, but `fees:card` (2 fixed segments) wins over `fees:{type}` (1 fixed + 1 variable)
- Address `fees:wire` matches only `fees:{type}`

**Enforcement:** Each matched type has its own `enforcement_mode`. When a type is in AUDIT mode, violations are logged as warnings but the transaction proceeds.

### 1.5 Special Cases

- **`world`:** Always valid, never needs to match a type.
- **No types defined:** If `account_types` is empty, no validation is performed (backward compatible with ledgers created before this feature).
- **MIGRATING types:** Still validate addresses (both old and new patterns are active during migration). The source type is MIGRATING, the target type is ACTIVE.

---

## 2. Account Type Lifecycle

### 2.1 State Machine

```
                  AddAccountType
                       │
                       ▼
                ┌─────────────┐
                │    ACTIVE    │ ◄─── normal state
                └──────┬──────┘
                       │
                MigrateAccountType
                (sets superseded_by,
                 target type must exist)
                       │
                       ▼
                ┌─────────────┐
                │  MIGRATING   │ ◄─── background worker running
                │              │      accounts being rewritten
                │  progress:   │
                │  42/1500     │
                └──────┬──────┘
                       │
            ┌──────────┼──────────┐
            │                     │
     (worker completes)    CancelMigration
            │                     │
            ▼                     ▼
     ┌─────────────┐      ┌─────────────┐
     │ DEPRECATED   │      │   ACTIVE     │ (rolled back)
     │              │      └─────────────┘
     │ superseded   │
     │ by: "new"    │
     └──────┬──────┘
            │
     RemoveAccountType
     (user decides when)
            │
            ▼
         (deleted)
```

### 2.2 Transitions

| From | To | Trigger | Conditions |
|------|----|---------|------------|
| — | ACTIVE | `AddAccountType` | Pattern valid, no overlap with existing types, name unique |
| ACTIVE | MIGRATING | `MigrateAccountType` | Target type exists and is ACTIVE, variables in target are subset of source, no address collisions |
| MIGRATING | DEPRECATED | `CompleteMigration` (automatic) | All accounts rewritten by background worker |
| MIGRATING | ACTIVE | `CancelMigration` | Stops worker, clears `superseded_by` (partial state: some accounts may have been migrated) |
| DEPRECATED | — | `RemoveAccountType` | No accounts remain under this pattern (verified by scan) |
| ACTIVE | — | `RemoveAccountType` | No accounts exist under this pattern |

### 2.3 Per-Type Enforcement

Each type has its own enforcement mode:

| Type Status | Default Enforcement | Behavior |
|-------------|-------------------|----------|
| ACTIVE | STRICT | Reject transactions with non-matching addresses |
| MIGRATING | AUDIT | Log violations but allow (transactions may still use old pattern during migration) |
| DEPRECATED | AUDIT | Log violations but allow (grace period before removal) |

The enforcement mode is configurable per type, overriding the defaults above. For example, a new ACTIVE type can start in AUDIT mode for gradual rollout.

---

## 3. Migration Lifecycle

### 3.1 Overview

Migration is a status transition on an account type, not a separate concept:

```
Step 1: Create the target type (if it doesn't exist)
  POST /ledger/account-types
  { "name": "client-courant", "pattern": "clients:{id}:courant" }

Step 2: Trigger migration on the source type
  POST /ledger/account-types/user-checking/migrate
  { "targetType": "client-courant" }

  → "user-checking" status: ACTIVE → MIGRATING
  → "user-checking" superseded_by: "client-courant"
  → Background worker starts

Step 3: (automatic) Background worker rewrites accounts
  → Scans Pebble for volumes/metadata matching source pattern
  → Rewrites keys in batches via Raft
  → Updates bbolt read index
  → Progress visible via API

Step 4: (automatic) Worker completes
  → "user-checking" status: MIGRATING → DEPRECATED

Step 5: (manual) Cleanup
  DELETE /ledger/account-types/user-checking
  → Removes the deprecated type
```

### 3.2 Validation at Migration Start

When `MigrateAccountType` is called:

1. **Source type** must be ACTIVE and exist.
2. **Target type** must be ACTIVE and exist.
3. **Variable binding:** All variables in the target pattern must appear in the source pattern (target can use a subset).
4. **Collision check:** For each account matching the source pattern, compute the target address and verify no existing account uses it.
5. **No active migration:** No other type in the ledger can be in MIGRATING status.

Steps 3-4 run in the admission layer (off the FSM hot path).

### 3.3 Background Worker

The `AccountMigrationWorker` follows the `MetadataConverter` architecture (`internal/infra/state/metadata_converter.go`):

- Runs on **all nodes**, only the **leader** scans and proposes
- **Followers** poll the type status and wait
- **Request channel** drains from FSM without back-pressure
- **Retry with backoff** on leader changes

**Scan algorithm:**

```
Open Pebble read snapshot

Pass 1: Count matching accounts (progress tracking)
  ForEachInPrefix(Volume, ledgerPrefix):
    Parse VolumeKey → extract account address
    Match against source pattern → count unique matches

Pass 2: Rewrite in batches
  ForEachInPrefix(Volume, ledgerPrefix):
    Parse VolumeKey → match source pattern → extract bindings
    Compute new address via target pattern
    Add {old_key, new_key} to batch
    Every batch_size entries → propose MigrateAccountBatchOrder via Raft

  ForEachInPrefix(Metadata, ledgerPrefix):
    Same logic for metadata keys

Propose CompleteMigrationOrder
```

### 3.4 FSM Processing

#### MigrateAccountBatchOrder

**Pebble (write-side):**

For each entry in the batch:
1. Read current value at old canonical key
2. Write value at new canonical key (via `SetBase`)
3. Delete old canonical key (via `Delete`)

Applied for both volume and metadata attributes.

**Design choice: keys-only in batch.** The Raft proposal only contains old/new canonical keys, not values. The FSM reads values from Pebble at apply time. This keeps proposals small. The MetadataConverter already does similar Pebble reads during apply.

#### CompleteMigrationOrder

1. Set source type status to `DEPRECATED`
2. Set `migration_progress.completed_at`
3. Emit `CompletedMigrationLog`

#### CancelMigrationOrder

1. Set source type status back to `ACTIVE`
2. Clear `superseded_by`
3. Worker stops on next staleness check
4. **Partial state:** Some accounts are under the new pattern, some under the old. Both types remain ACTIVE. The user can re-trigger migration to finish, or reverse-migrate.

### 3.5 Log Replay Correctness

On full log replay (new node, snapshot restore + catch-up):

1. `AddAccountTypeOrder` entries create types
2. `MigrateAccountTypeOrder` sets source type to MIGRATING
3. Historical `CreatedTransaction` entries create volumes under old addresses
4. `MigrateAccountBatchOrder` entries rewrite Pebble keys — deterministic, same result on all replicas
5. Post-migration `CreatedTransaction` entries create volumes under new addresses
6. `CompleteMigrationOrder` sets source type to DEPRECATED

Final state is identical on all replicas.

---

## 4. Pattern Engine

### 4.1 Parsing

```go
type PatternSegment struct {
    Kind    SegmentKind
    Value   string  // literal for Fixed, name for Variable
    Pattern string  // regex constraint for Variable (empty = match all)
}

type SegmentKind int

const (
    SegmentFixed SegmentKind = iota
    SegmentVariable
)

// ParsePattern parses "users:{id}:checking" into segments.
func ParsePattern(pattern string) ([]PatternSegment, error)
```

**Parsing rules:**
- Split on `:`
- `{name}` → Variable with no constraint
- `{name:regex}` → Variable with regex constraint
- Anything else → Fixed (must match `[a-zA-Z0-9_-]+`)

### 4.2 Address Matching

```go
// MatchAddress matches an address against a parsed pattern.
// Returns variable bindings if matched.
func MatchAddress(address string, segments []PatternSegment) (map[string]string, bool)
```

Algorithm:
1. Split address on `:` into parts
2. If len(parts) != len(segments) → no match
3. For each position:
   - Fixed: literal equality
   - Variable: check regex constraint (if any), capture value
4. Return bindings

### 4.3 Address Rewriting

```go
// RewriteAddress applies bindings to a target pattern.
func RewriteAddress(bindings map[string]string, target []PatternSegment) string
```

Algorithm:
1. For each target segment:
   - Fixed: emit literal
   - Variable: look up name in bindings, emit captured value
2. Join with `:`

### 4.4 Specificity Scoring

For longest-match resolution when multiple patterns match:

```go
// Specificity returns the number of fixed segments in a pattern.
func Specificity(segments []PatternSegment) int
```

Higher specificity wins. Tie-breaking: pattern with fewer total segments wins (more constrained). If still tied: lexicographic order on pattern string (deterministic).

### 4.5 Canonical Key Rewriting

The migration rewrites only the account portion of Pebble keys:

```
Volume key: [ledger]\x00[account]\x00[asset]
             ──────────  ───────  ─────
             preserved   REWRITE  preserved

Metadata key: [ledger]\x00[account]\x01[key]
               ──────────  ───────  ─────
               preserved   REWRITE  preserved
```

---

## 5. Raft Commands

### 5.1 Account Type Management

```protobuf
message LedgerApplyOrder {
  string ledger = 1;
  oneof data {
    // ... existing fields 2-14 ...
    AddAccountTypeOrder add_account_type = 15;
    UpdateAccountTypeOrder update_account_type = 16;
    RemoveAccountTypeOrder remove_account_type = 17;
    MigrateAccountTypeOrder migrate_account_type = 18;
    MigrateAccountBatchOrder migrate_account_batch = 19;
    CompleteMigrationOrder complete_migration = 20;
    CancelMigrationOrder cancel_migration = 21;
  }
}

// AddAccountTypeOrder adds a new account type to the ledger.
message AddAccountTypeOrder {
  AccountType account_type = 1;
}

// UpdateAccountTypeOrder updates an existing account type's enforcement mode.
// Pattern and status changes are not allowed via this order (use migration).
message UpdateAccountTypeOrder {
  string name = 1;
  ChartEnforcementMode enforcement_mode = 2;
}

// RemoveAccountTypeOrder removes an account type from the ledger.
message RemoveAccountTypeOrder {
  string name = 1;
}

// MigrateAccountTypeOrder triggers migration from source type to target type.
message MigrateAccountTypeOrder {
  string source_type = 1;    // Name of the type to migrate FROM
  string target_type = 2;    // Name of the type to migrate TO
  uint64 total_accounts = 3; // Pre-counted by admission layer
}

// MigrateAccountBatchOrder rewrites a batch of account keys.
message MigrateAccountBatchOrder {
  string source_type = 1;
  repeated MigrateAccountEntry volume_entries = 2;
  repeated MigrateAccountEntry metadata_entries = 3;
  uint64 migrated_accounts_so_far = 4;
}

message MigrateAccountEntry {
  bytes old_canonical_key = 1;
  bytes new_canonical_key = 2;
}

message CompleteMigrationOrder {
  string source_type = 1;
}

message CancelMigrationOrder {
  string source_type = 1;
}
```

### 5.2 Log Payloads

```protobuf
message LedgerLogPayload {
  oneof payload {
    // ... existing fields 1-10 ...
    AddedAccountTypeLog added_account_type = 11;
    UpdatedAccountTypeLog updated_account_type = 12;
    RemovedAccountTypeLog removed_account_type = 13;
    StartedMigrationLog started_migration = 14;
    MigratedAccountBatchLog migrated_account_batch = 15;
    CompletedMigrationLog completed_migration = 16;
    CancelledMigrationLog cancelled_migration = 17;
  }
}

message AddedAccountTypeLog {
  AccountType account_type = 1;
}

message UpdatedAccountTypeLog {
  string name = 1;
  ChartEnforcementMode enforcement_mode = 2;
}

message RemovedAccountTypeLog {
  string name = 1;
}

message StartedMigrationLog {
  string source_type = 1;
  string target_type = 2;
  string source_pattern = 3;  // Denormalized for auditability
  string target_pattern = 4;
  uint64 total_accounts = 5;
}

message MigratedAccountBatchLog {
  string source_type = 1;
  repeated MigratedAccount accounts = 2;
  uint64 migrated_accounts_so_far = 3;
}

message MigratedAccount {
  string old_address = 1;
  string new_address = 2;
}

message CompletedMigrationLog {
  string source_type = 1;
}

message CancelledMigrationLog {
  string source_type = 1;
}
```

---

## 6. Read Index Updates

### 6.1 Index Builder

When the index builder encounters a `MigratedAccountBatch` log entry:

**For each migrated account (old_address → new_address):**

1. **Account existence (BucketExistence):**
   - Add: `[ledger\x00][a:][new_address]`
   - Delete: `[ledger\x00][a:][old_address]`

2. **Metadata indexes (BucketMetadataIndex, BucketReverseMap, BucketEntityExists):**
   - For each metadata key (read from reverse map at old address):
     - Delete old forward index, existence index, and reverse map entries
     - Add new entries with new address

3. **Account-tx mappings (BucketAccountTx, BucketSourceAccountTx, BucketDestAccountTx):**
   - Scan `[ledger\x00][old_address\x00]*`
   - For each `[old_address\x00][txID]`: add `[new_address\x00][txID]`, delete old
   - Applies to all three buckets (any-role, source, destination)

### 6.2 Transaction Postings

Historical transactions keep their original addresses in postings (immutable record). Transaction search by address uses the updated account-tx index — searching for the new address finds historical transactions.

---

## 7. API Design

### 7.1 Account Type CRUD

**Add Account Type:**
```
POST /{ledgerName}/account-types
Content-Type: application/json

{
  "name": "user-checking",
  "pattern": "users:{id}:checking",
  "enforcementMode": "STRICT"
}
```

Response: `201 Created`

**Get Account Type:**
```
GET /{ledgerName}/account-types/{name}
```

Response:
```json
{
  "name": "user-checking",
  "pattern": "users:{id}:checking",
  "status": "ACTIVE",
  "enforcementMode": "STRICT"
}
```

**List Account Types:**
```
GET /{ledgerName}/account-types
```

Response:
```json
{
  "types": [
    {
      "name": "user-checking",
      "pattern": "users:{id}:checking",
      "status": "ACTIVE",
      "enforcementMode": "STRICT"
    },
    {
      "name": "bank-main",
      "pattern": "banks:{iban:^[A-Z]{2}[0-9]{14}$}:main",
      "status": "ACTIVE",
      "enforcementMode": "STRICT"
    }
  ]
}
```

**Update Account Type** (enforcement mode only):
```
PATCH /{ledgerName}/account-types/{name}
Content-Type: application/json

{
  "enforcementMode": "AUDIT"
}
```

**Remove Account Type:**
```
DELETE /{ledgerName}/account-types/{name}
```

Returns `409 Conflict` if accounts still exist under this pattern.

### 7.2 Migration

**Start Migration:**
```
POST /{ledgerName}/account-types/{name}/migrate
Content-Type: application/json

{
  "targetType": "client-courant"
}
```

Response `202 Accepted`:
```json
{
  "sourceType": "user-checking",
  "targetType": "client-courant",
  "sourcePattern": "users:{id}:checking",
  "targetPattern": "clients:{id}:courant",
  "totalAccounts": 15234,
  "preview": [
    {"oldAddress": "users:alice:checking", "newAddress": "clients:alice:courant"},
    {"oldAddress": "users:bob:checking", "newAddress": "clients:bob:courant"}
  ]
}
```

**Dry Run** (validate without starting):
```
POST /{ledgerName}/account-types/{name}/migrate?dryRun=true
Content-Type: application/json

{
  "targetType": "client-courant"
}
```

Same response format, but no Raft proposal — just validation and preview.

**Cancel Migration:**
```
POST /{ledgerName}/account-types/{name}/cancel-migration
```

**Get Migration Progress** (part of the account type response):
```
GET /{ledgerName}/account-types/{name}
```

Response when MIGRATING:
```json
{
  "name": "user-checking",
  "pattern": "users:{id}:checking",
  "status": "MIGRATING",
  "enforcementMode": "AUDIT",
  "supersededBy": "client-courant",
  "migrationProgress": {
    "totalAccounts": 15234,
    "migratedAccounts": 8721,
    "startedAt": "2026-03-05T10:00:00Z"
  }
}
```

### 7.3 CLI

```bash
# List account types
ledgerctl account-types list --ledger payments

# Add a type
ledgerctl account-types add --ledger payments \
    --name user-checking \
    --pattern "users:{id}:checking"

# Add the target type
ledgerctl account-types add --ledger payments \
    --name client-courant \
    --pattern "clients:{id}:courant"

# Dry-run migration
ledgerctl account-types migrate --ledger payments \
    --name user-checking \
    --target client-courant \
    --dry-run

# Start migration
ledgerctl account-types migrate --ledger payments \
    --name user-checking \
    --target client-courant

# Check progress
ledgerctl account-types get --ledger payments --name user-checking

# Cancel
ledgerctl account-types cancel-migration --ledger payments --name user-checking

# Remove deprecated type
ledgerctl account-types remove --ledger payments --name user-checking
```

### 7.4 Analyze → Suggest Account Types

The existing `analyze-accounts` endpoint is updated to suggest Account Types instead of a tree:

```
GET /{ledgerName}/analyze-accounts?variableThreshold=10
```

Response:
```json
{
  "suggestedTypes": [
    {
      "name": "user-checking",
      "pattern": "users:{id}:checking",
      "accountCount": 487,
      "assets": ["USD", "EUR"],
      "metadataKeys": ["kyc_verified", "tier"]
    },
    {
      "name": "user-savings",
      "pattern": "users:{id}:savings",
      "accountCount": 312,
      "assets": ["USD"],
      "metadataKeys": ["tier"]
    },
    {
      "name": "bank-main",
      "pattern": "banks:{iban}:main",
      "accountCount": 23,
      "assets": ["USD", "EUR", "GBP"],
      "metadataKeys": []
    }
  ],
  "totalAccounts": 822
}
```

Each leaf path in the old trie-based discovery becomes a suggested Account Type. The user can accept, rename, or adjust them.

---

## 8. Backward Compatibility & Migration from Tree Model

### 8.1 Conversion

The old tree model can be mechanically converted to Account Types by extracting every path where `account: true`:

**Old tree:**
```json
{
  "users": {
    "variable": {
      "name": "id",
      "account": true,
      "children": {
        "checking": { "account": true },
        "savings": { "account": true }
      }
    }
  },
  "platform": {
    "children": {
      "fees": { "account": true }
    }
  }
}
```

**Extracted Account Types:**

| Name | Pattern |
|------|---------|
| `user` | `users:{id}` |
| `user-checking` | `users:{id}:checking` |
| `user-savings` | `users:{id}:savings` |
| `platform-fees` | `platform:fees` |

### 8.2 Breaking Change

The old `ChartOfAccounts` proto message, `SetChartOfAccountsOrder`, and related API endpoints are removed. This is a clean break. Old ledgers that used the tree model must be migrated via a one-time conversion (extracting paths as above). This conversion can be automated in the upgrade path.

---

## 9. Edge Cases

### 9.1 Transactions During Migration

While migration is running:
- **Old pattern:** Allowed (source type is MIGRATING, not DEPRECATED). If the account was already migrated, volumes are created under the old address again. The worker handles stragglers.
- **New pattern:** Allowed (target type is ACTIVE).

**Recommendation:** Users should switch their applications to the new pattern before or during migration. Stragglers can be caught by re-running or a follow-up migration.

### 9.2 Concurrent Transactions on Migrated Accounts

If a transaction targets an old address after its volumes were moved to the new address:
- Volumes under old address are empty/zero
- Transaction may fail with insufficient balance (unless source is `world`)

**Mitigation:** Source type is automatically set to AUDIT mode during migration.

### 9.3 Reverting a Transaction Post-Migration

Revert creates postings with swapped src/dest using old addresses:
- Old addresses must be valid → source type is DEPRECATED (still in the chart)
- Small volume entry created under old address (nets to zero after revert)
- Cleanup: leave it or re-run migration

### 9.4 Overlapping Patterns

If `fees:{type}` and `fees:card` both exist:
- Address `fees:card` → matched by `fees:card` (specificity 2) over `fees:{type}` (specificity 1)
- Address `fees:wire` → matched by `fees:{type}` only
- This is deterministic and documented

### 9.5 Migration of `world`

The `world` account is always valid, never matches any pattern, and must never be migrated. The pattern engine explicitly excludes it.

### 9.6 Snapshot Restore

Snapshots capture the partially migrated Pebble state + `LedgerInfo` with type statuses. On restore, the worker resumes from where it left off.

### 9.7 Cancellation with Partial State

After cancellation, some accounts live under the new pattern, some under the old. Both types are ACTIVE. Options:
- **Re-trigger migration:** Worker skips already-migrated accounts
- **Reverse migration:** Create the old type as target, migrate from new back to old
- **Leave as-is:** Both patterns coexist permanently

---

## 10. Implementation Plan

### Phase 1: Pattern Engine (`internal/domain/accounttype/`)
1. `pattern.go` — ParsePattern, MatchAddress, RewriteAddress, Specificity
2. `pattern_test.go` — comprehensive unit tests
3. `validate.go` — pattern validation, overlap detection

### Phase 2: Proto Changes
1. New messages in `common.proto`: AccountType, AccountTypeStatus, MigrationProgress
2. Remove old ChartOfAccounts, ChartSegment, ChartVariable messages
3. New orders in `raft_cmd.proto`
4. New log payloads in `common.proto`
5. Update LedgerInfo
6. `just generate-proto`

### Phase 3: FSM Processing (`internal/domain/processing/`)
1. `processor_account_type.go` — Add, Update, Remove, Migrate handlers
2. `processor_account_migration.go` — Batch, Complete, Cancel handlers
3. Replace `processor_chart.go` validation with new matching logic
4. Unit tests

### Phase 4: Background Worker (`internal/infra/state/`)
1. `account_migration_worker.go` — following MetadataConverter pattern
2. Wire into Machine lifecycle
3. Request channel in Buffered
4. Unit tests with mock proposer

### Phase 5: Index Builder (`internal/application/indexbuilder/`)
1. Handle `MigratedAccountBatch` log entries
2. Update existence, metadata, and account-tx indexes
3. Integration tests

### Phase 6: API
1. gRPC handlers: AddAccountType, UpdateAccountType, RemoveAccountType, MigrateAccountType, CancelMigration
2. HTTP handlers (one file per handler)
3. Update analyze-accounts to return suggested types
4. CLI commands
5. Admission layer validation
6. E2E tests

### Phase 7: Documentation
1. Update `docs/dev/api-comparison.md`
2. Update `docs/ops/cli.md`
3. Update `openapi.yml`
4. Rewrite `docs/drafts/chart-of-accounts.md` → reference this RFC
5. Migration guide for existing chart users

---

## 11. Proto Changes Summary

### Removed

| Message/Field | Location |
|---------------|----------|
| `ChartOfAccounts` | `common.proto` |
| `ChartSegment` | `common.proto` |
| `ChartVariable` | `common.proto` |
| `SetChartOfAccountsLog` | `common.proto` |
| `SetChartEnforcementModeLog` | `common.proto` |
| `LedgerInfo.chart_of_accounts` | `common.proto` |
| `LedgerInfo.enforcement_mode` | `common.proto` |
| `SetChartOfAccountsOrder` | `raft_cmd.proto` |
| `SetChartEnforcementModeOrder` | `raft_cmd.proto` |

### Added

| Message/Enum | Location | Fields |
|-------------|----------|--------|
| `AccountType` | `common.proto` | name, pattern, status, enforcement_mode, superseded_by, migration_progress |
| `AccountTypeStatus` | `common.proto` | ACTIVE, MIGRATING, DEPRECATED |
| `MigrationProgress` | `common.proto` | total_accounts, migrated_accounts, started_at, completed_at |
| `LedgerInfo.account_types` | `common.proto` | `map<string, AccountType>` |
| `AddAccountTypeOrder` | `raft_cmd.proto` | account_type |
| `UpdateAccountTypeOrder` | `raft_cmd.proto` | name, enforcement_mode |
| `RemoveAccountTypeOrder` | `raft_cmd.proto` | name |
| `MigrateAccountTypeOrder` | `raft_cmd.proto` | source_type, target_type, total_accounts |
| `MigrateAccountBatchOrder` | `raft_cmd.proto` | source_type, volume_entries, metadata_entries, migrated_accounts_so_far |
| `MigrateAccountEntry` | `raft_cmd.proto` | old_canonical_key, new_canonical_key |
| `CompleteMigrationOrder` | `raft_cmd.proto` | source_type |
| `CancelMigrationOrder` | `raft_cmd.proto` | source_type |
| All corresponding Log payloads | `common.proto` | See §5.2 |

---

## 12. Future Extensions

### 12.1 Address Translation in Transaction Responses

Optional `canonicalSource`/`canonicalDestination` fields on Posting responses, applied at read time using migration history.

### 12.2 Backward-Compatible Address Lookup

`GetAccount` with old address returns `301 Moved` or a `migratedTo` field.

### 12.3 Multi-Type Migration

Migrate multiple source types to multiple targets in a single operation:

```bash
ledgerctl account-types migrate-batch --ledger payments \
    --mapping user-checking:client-courant \
    --mapping user-savings:client-epargne
```

### 12.4 Account Type Metadata Schema

Attach per-type metadata schemas (which metadata keys are expected on accounts of this type):

```json
{
  "name": "user-checking",
  "pattern": "users:{id}:checking",
  "metadataSchema": {
    "kyc_verified": "bool",
    "tier": "string"
  }
}
```

This replaces the global metadata schema with per-type schemas — a natural evolution.

### 12.5 Account Type Default Metadata

Revisit the default metadata feature (excluded from the original CoA RFC §4) now that types are independent entities with clear boundaries. Default metadata per type is more tractable than default metadata per tree node.

---

## 13. Comparison: Tree Model vs Account Types

| Aspect | Monolithic Tree | Account Types (this RFC) |
|--------|----------------|--------------------------|
| **Granularity** | Whole-tree replacement | Per-type CRUD |
| **Enforcement** | Global (STRICT or AUDIT for all) | Per-type |
| **Migration** | Separate concept, separate API | Status transition on a type |
| **Lifecycle** | No status per pattern | ACTIVE → MIGRATING → DEPRECATED |
| **API surface** | 2 endpoints (get/set chart) + migration API | CRUD on types + migrate verb |
| **Validation** | Tree walk (O(depth)) | Pattern matching (O(types × segments)) |
| **Overlap handling** | Impossible (tree is exclusive) | Longest match wins |
| **Discoverability** | Must parse tree to find valid patterns | Each type is self-documenting |
| **Proto complexity** | 3 recursive messages | 1 flat message + 1 enum |
| **Partial updates** | Not supported | Native |
| **Per-type metadata schema** | Not possible | Natural extension (§12.4) |

---

## 14. Open Questions

| # | Question | Notes |
|---|----------|-------|
| 1 | Should `RemoveAccountType` require zero accounts, or allow force-remove? | Force-remove would leave orphan accounts unvalidated. Safer to require zero accounts. |
| 2 | Should we support pattern overlap detection at add time? | Warn or reject when a new pattern overlaps an existing one? Longest-match handles it correctly, but overlaps may be confusing. |
| 3 | Should migration freeze the source pattern for writes? | Optional "freeze" mode to prevent new accounts under the old pattern during migration. Reduces race conditions but is more restrictive. |
| 4 | Should we auto-name types during analyze? | The analyze endpoint suggests type names. Should it use a convention like `{root}-{leaf}` or let the user name them? |
| 5 | Maximum number of account types per ledger? | Practical limit? 100 types × pattern matching at every transaction could become a concern. Consider indexing or compilation. |
| 6 | Should DEPRECATED types auto-remove after a configurable TTL? | e.g., "remove deprecated types after 30 days if zero accounts remain." |
