# Marketplace Lifecycle

## Use Case

Models a high-volume e-commerce marketplace where customers buy from merchants and the platform takes a commission. Covers the full purchase lifecycle: deposits, purchases with fee splitting, reverts, merchant payouts, and final reconciliation.

## Ledger

Single ledger: `marketplace`. Asset: `USD/2` (cents).

## Account Structure

| Account | Pattern | Enforcement | Role |
|---------|---------|-------------|------|
| `customer:{id}` | `customer:{id}` | STRICT | Customer wallets (50 accounts), funded from `@world` |
| `merchant:{id}` | `merchant:{id}` | STRICT | Merchant settlement accounts (10 accounts) |
| `platform:fees` | `platform:fees` | STRICT | Accumulated 3% platform commissions |
| `platform:payouts` | `platform:payouts` | STRICT | Destination for merchant payouts and backdated transactions |

## Numscripts

| Name | Version | Description |
|------|---------|-------------|
| `deposit` | 1.0.0 | `@world` -> `$customer` |
| `purchase` | 1.0.0 | `$customer` -> 3/100 to `@platform:fees` + remaining to `$merchant` |
| `payout` | 1.0.0 | `$merchant` -> `@platform:payouts` |

## Business Flow

### Phase 1: Setup

- Create ledger with 4 account types (all STRICT)
- Register 3 Numscripts (`deposit`, `purchase`, `payout`)
- **Verify setup**: `GetLedger` (4 account types), `ListNumscripts` (3 scripts), `GetNumscript` for each

### Phase 1b: Account Type Lifecycle

- **Add** temporary type `temp-type` (pattern: `temp:{id}`, STRICT)
- **Update** enforcement mode to AUDIT
- **Remove** the temporary type
- **Violation test**: posting to `unknown:address` fails (STRICT rejection)

### Phase 2: Customer Deposits (50 Apply calls)

```
@world  --[USD/2 1,000,000]--> customer:{1..50}
```

All 50 deposits submitted in a single batch.

### Phase 3: Purchases with Fees (200 Apply calls)

For each of 200 purchases (rotating customer/merchant pairs, amounts 1000..20900):

```
customer:{id}  --[$amount]--> platform:fees  (3%)
customer:{id}  --[$amount]--> merchant:{id}  (97%)
```

- **Chapter close** every 60 transactions + double-entry check
- **Cold-account read** every 20 transactions (cache eviction exercise)
- **GetTransaction** on first purchase to verify structure

### Phase 3b: WithTimestamp + WithExpandVolumes

- 2 **antidated transactions** (`@world` -> `platform:payouts`) with past timestamps (-24h, -48h)
- 1 transaction with **ExpandVolumes** flag: response includes `PostCommitVolumes`

### Phase 4: Reverts (10 Apply calls)

10 evenly-spaced purchases reverted with `force=true`. For each revert:

```
(reverse) merchant:{id}  --[97%]--> customer:{id}
(reverse) platform:fees  --[3%]-->  customer:{id}
```

### Phase 5: Final Chapter Close

### Phase 6: Merchant Payouts

Each merchant's full balance paid out:

```
merchant:{id}  --[full balance]--> platform:payouts
```

All merchant balances verified at 0.

### Phase 7: Metadata Operations

- **Account metadata**: Set `tier=gold`, `kyc=verified` on `customer:1`; set `category=electronics` on `merchant:1`
- **Transaction metadata**: Set `flagged=true`, `reason=review` on first purchase
- **Delete account metadata key**: Remove `kyc` from `customer:1`, verify `tier` remains
- **Delete transaction metadata key**: Remove `reason` from first purchase

### Phase 8: Inline Numscript + Raw Postings

- **Inline Numscript** (not ScriptReference): `customer:1` -> `customer:2` for USD/2 100
- **Raw posting** (balance-checked): `customer:2` -> `customer:3` for USD/2 50
- **Insufficient funds**: raw posting of 999,999,999 from `customer:50` fails

### Phase 9: Revert (force=false, balance-checked)

- Deposit USD/2 500 to `customer:3`, then revert with `force=false`
- **Double-revert**: reverting the same transaction again fails

### Phase 10: Idempotency + References

- **Transaction reference**: `unique-ref-001` on a deposit; duplicate reference fails
- **Idempotency key**: `ik-deposit-001` on a deposit; replay with same content succeeds (returns original); different content with same key fails

### Phase 11: Numscript versioning

- Append immutable `temp_script` versions `1.0.0` then `2.0.0`; the latest pointer tracks the greatest

### Phase 11b: Prepared Queries

| Query | Target | Filter Type | Parameters | Expected Results |
|-------|--------|-------------|------------|------------------|
| `customer-query` | ACCOUNTS | Hardcoded address prefix `customer:` | none | paginated customer list |
| `accounts-by-prefix` | ACCOUNTS | **Parameterized** address prefix `$prefix` | `prefix: StringParam("customer:")` | 50 customers |
| | | | `prefix: StringParam("merchant:")` | 10 merchants |
| | | | `prefix: StringParam("platform:")` | >= 1 platform account |

- **Update**: change `customer-query` filter to `merchant:` prefix
- **Delete**: both queries removed, verified via `ListPreparedQueries`

### Phase 12: Final Invariants

- Double-entry balance (sum of all inputs == sum of all outputs per asset)
- No negative balances (except `@world`)
- Platform fees match tracked total
- All 50 customer balances match tracked values
- **AggregateVolumes**: USD/2 present with non-nil input/output
- **GetLedgerStats**: account and transaction counts > 0

### Phase 12b: List with Filters + GetLog

- **ListAccountsFiltered**: prefix `customer:` returns exactly 50
- **ListTransactionsFiltered**: page_size=10 returns <= 10
- **GetLog**: fetch first log by sequence, verify match

### Phase 13: Audit Trail

~270 transactions expected (50 deposits + 200 purchases + 10 reverts + 1 revert + 3 timestamp txs + 3 extra).

### Post-Test Phases

1. **StoreCheck** -- hash chain integrity
2. **Backup** -- full backup to memory
3. **Restart + Verify** -- WAL replay, re-verify all balances + audit trail
4. **Backup-Restore + Verify** -- restore from backup on fresh node, re-verify
5. **Regression**: idempotency key `ik-deposit-001` must survive snapshot restore

## Volume

~270 Apply calls, ~5 cache rotations (threshold=50).
