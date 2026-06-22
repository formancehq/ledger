# Lending Lifecycle

## Use Case

Models a consumer lending operation: loan disbursements from a funding pool, monthly repayment cycles with interest accrual, borrower defaults with provisions and write-offs, and early full repayment. Tests complex multi-account financial flows typical of banking/fintech lending products, plus prepared queries with typed parameters for loan status filtering.

## Ledger

Single ledger: `lending`. Asset: `USD/2` (cents).

## Account Structure

| Account | Pattern | Enforcement | Role |
|---------|---------|-------------|------|
| `funding:pool` | `funding:{type}` | STRICT | Bank's lending pool, funded from `@world` with total loan amount |
| `borrower:{id}:loan` | `borrower:{id}:loan` | STRICT | Outstanding principal per borrower (10 accounts) |
| `borrower:{id}:wallet` | `borrower:{id}:wallet` | STRICT | Borrower wallet, receives disbursement and pays from here |
| `revenue:interest` | `revenue:{type}` | STRICT | Accrued interest income |
| `expense:provision` | `expense:{type}` | STRICT | Provision for doubtful debts |
| `recovery:pool` | `recovery:{type}` | STRICT | Write-off contra account |

## Borrower Profiles

| Borrowers | Behavior | Months Active |
|-----------|----------|---------------|
| 1, 2, 4, 6, 8, 9, 10 | Normal repayment | 1-6 (full term) |
| 3, 7 | **Defaulters** -- pay months 1-2 only, then stop | 1-2 |
| 5 | **Early repayer** -- full repayment in month 2 | 1-2 |

Loan amount: USD/2 100,000 per borrower. Monthly interest rate: 2%.

## Numscripts

| Name | Version | Description |
|------|---------|-------------|
| `fund_pool` | 1.0.0 | `@world` -> `@funding:pool` |
| `disburse_loan` | 1.0.0 | Multi-send: `@funding:pool` -> `$borrower_loan` + `@world` -> `$borrower_wallet` |
| `repay_principal` | 1.0.0 | Multi-send: `$borrower_wallet` -> `@world` + `$borrower_loan` -> `@funding:pool` |
| `accrue_interest` | 1.0.0 | `$borrower_wallet` -> `@revenue:interest` |
| `provision` | 1.0.0 | `@world` -> `@expense:provision` |
| `write_off` | 1.0.0 | `$borrower_loan` -> `@recovery:pool` |

## Business Flow

### Phase 1: Setup

- Create ledger with 6 account types (all STRICT)
- Save 6 Numscripts
- Fund lending pool:
  ```
  @world  --[USD/2 1,000,000]--> funding:pool  (10 borrowers x 100,000)
  ```

### Phase 2: Loan Disbursements (10 Apply calls)

Each disbursement is a 2-block multi-send with unique reference `loan-disburse-{id}`:

```
funding:pool         --[USD/2 100,000]--> borrower:{id}:loan
@world               --[USD/2 100,000]--> borrower:{id}:wallet
```

Funding pool verified at 0 after all disbursements.

### Phase 3: Monthly Repayment Cycles (6 months)

For each active borrower each month:

**Interest payment** (2% of outstanding principal):
```
borrower:{id}:wallet  --[USD/2 interest]--> revenue:interest
```

**Principal repayment** (equal installments: 100,000 / 6):
```
borrower:{id}:wallet  --[USD/2 principal]--> @world
borrower:{id}:loan    --[USD/2 principal]--> funding:pool
```

Special cases:
- **Early repayer (borrower 5, month 2)**: pays interest then repays max principal capped by wallet balance
- **Defaulters (borrowers 3, 7, months 3-6)**: no payments, outstanding balance accrues
- **All borrowers**: principal capped to outstanding, total payment capped to wallet balance

Chapter close after each month.

### Phase 4: Provisions

Provision full outstanding balance of defaulters (borrowers 3, 7):

```
@world  --[USD/2 outstanding]--> expense:provision
```

Metadata: `borrower=borrower:{id}`, `reason=default`.

### Phase 5: Write-Offs

Write off defaulted loan balances:

```
borrower:{id}:loan  --[USD/2 outstanding]--> recovery:pool
```

Loan balance set to 0. Metadata: `reason=write-off-default`.

### Phase 6: Metadata Enrichment

| Account | Key | Value |
|---------|-----|-------|
| `borrower:3:loan` | `status` | `"written-off"` |
| `borrower:7:loan` | `status` | `"written-off"` |
| `borrower:5:loan` | `status` | `"repaid-early"` |

### Phase 7: Prepared Queries with Typed Parameters

Metadata index created: `status` (backfill awaited).

| Query | Target | Filter | Parameter | Type | Executions |
|-------|--------|--------|-----------|------|------------|
| `loans-by-status` | ACCOUNTS | ParamStringMetadata(`status`, `$status_value`) | `status_value` | StringParam | `"written-off"` -> 2 (borrowers 3, 7); `"repaid-early"` -> 1 (borrower 5); `"active"` -> 0 |
| `accounts-by-prefix` | ACCOUNTS | ParamAddressPrefix(`$prefix`) | `prefix` | StringParam | `"borrower:"` -> 20 (10 x 2 accounts); `"funding:"` -> 1 |

All queries deleted after verification.

### Phase 8: Final Invariants

- Double-entry balance
- No negative balances (except `@world`)
- `revenue:interest` is positive
- Normal borrowers (1,2,4,6,8,9,10): loan balance matches tracked value
- Defaulted borrowers (3,7): loan balance is 0 (written off)
- `recovery:pool` has positive USD/2 balance

### Post-Test Phases

1. **StoreCheck** -- hash chain integrity
2. **Backup** -- full backup
3. **Restart + Verify** -- re-verify double-entry + no negative balances
4. **Backup-Restore + Verify** -- restore on fresh node, re-verify

## Volume

~130 Apply calls, ~2 cache rotations (threshold=50).
