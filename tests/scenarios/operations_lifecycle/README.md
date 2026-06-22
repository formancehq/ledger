# Operations Lifecycle

## Use Case

Tests administrative and operational features of the ledger: maintenance mode, audit logging, chapter archiving, chapter scheduling, Ed25519 request signing, and ledger deletion. Focuses on ops tooling rather than financial logic.

## Ledger

Primary ledger: `ops-test`. Asset: `USD/2` (cents). A secondary `temp-ledger` is created and deleted during the test.

## Account Structure

| Account | Pattern | Enforcement | Role |
|---------|---------|-------------|------|
| `ops:{id}` | `ops:{id}` | STRICT | Test accounts (5 accounts, each funded with 10,000) |

## Numscripts

| Name | Version | Description |
|------|---------|-------------|
| `deposit` | 1.0.0 | `@world` -> `$account` |

## Business Flow

### Phase 1: Setup

- Create ledger with 1 account type (`ops:{id}`, STRICT)
- Save 1 Numscript (`deposit`)
- Create 5 deposit transactions:
  ```
  @world  --[USD/2 10,000]--> ops:{1..5}
  ```

### Phase 2: Maintenance Mode

1. **Enable**: `SetMaintenanceMode(true)`
2. **Verify rejection**: deposit to `ops:1` fails during maintenance
3. **Disable**: `SetMaintenanceMode(false)`
4. **Verify resumption**: deposit to `ops:1` succeeds

### Phase 3: Audit Config

1. **Enable audit logging**: `SetAuditConfig(true)`
2. **3 successful transactions**: deposits of USD/2 50 to `ops:{1..3}`
3. **1 failing transaction**: raw posting of 999,999,999 from `ops:1` (insufficient funds)
4. **ListAuditEntries(all)**: >= 4 entries (3 success + 1 failure)
5. **ListAuditEntries(failures only)**: >= 1 entry, each has failure outcome
6. **Disable audit logging**: `SetAuditConfig(false)`

### Phase 3b: GetAuditEntry

Fetch individual audit entries by sequence number. Verify each entry has an outcome (success or failure).

### Phase 3c: Archive Chapter + CheckStore (Regression)

Tests that `CheckStore` works correctly after log purging:

1. Create 3 more transactions (deposits to `ops:{1..3}`)
2. **Close chapter** -> CLOSED status
3. **Archive chapter** -> ARCHIVED status (logs purged from Pebble)
4. **CheckStore**: must pass with no errors. The hash chain must correctly skip purged log ranges by reading archived chapter metadata (`start_sequence`, `close_sequence`, `last_log_hash`).

### Phase 4: Chapter Schedule

1. **Set cron**: `0 0 * * *` (daily midnight)
2. **Verify**: `GetChapterSchedule` returns `"0 0 * * *"`
3. **Delete**: `DeleteChapterSchedule`
4. **Verify empty**: `GetChapterSchedule` returns `""`

### Phase 5: Request Signing (Ed25519 Key Lifecycle)

1. **Generate 2 keypairs**: `(pubKey1, privKey1)` and `(pubKey2, _)`
2. **Register key-1** (bootstrap, unsigned -- no existing keys)
3. **Register key-2** (signed by key-1's private key)
4. **Verify parent chain**: key-2's `parentKeyId` is `"key-1"`
5. **Revoke key-2** (signed by key-1)
6. **Verify removal**: only key-1 remains
7. **Signed transaction**: create a posting signed with key-1
8. **Verify signature in log**: log entry has `signature.keyId == "key-1"`

### Phase 6: Delete Ledger

1. **Create** `temp-ledger`
2. **Make 1 transaction**: force posting `@world` -> `user:1`
3. **Delete** `temp-ledger`
4. **Verify**: either removed from list or has `deleted_at` timestamp

### Phase 7: Final Invariants

- Double-entry balance on `ops-test`
- No negative balances (except `@world`)
- Account and transaction counts > 0

### Post-Test Phases

1. **StoreCheck** -- hash chain integrity (accounts for archived chapter)
2. **Backup** -- full backup
3. **Restart + Verify** -- re-verify double-entry + no negative balances
4. **Backup-Restore + Verify** -- restore on fresh node, re-verify

## Volume

~30 Apply calls, moderate operational complexity.
