# Multi-Currency Treasury

## Use Case

Models a corporate treasury managing 3 currencies (USD, EUR, GBP) with foreign exchange operations through a clearing account, vendor payments in EUR and GBP, and analytics. Tests multi-asset accounting where a single clearing account holds positions in multiple currencies simultaneously.

## Ledger

Single ledger: `treasury`. Assets: `USD/2`, `EUR/2`, `GBP/2` (all in cents).

## Account Structure

| Account | Pattern | Enforcement | Role |
|---------|---------|-------------|------|
| `treasury:usd` | `treasury:{currency}` | STRICT | USD treasury (initial: 1,000,000) |
| `treasury:eur` | `treasury:{currency}` | STRICT | EUR treasury (initial: 500,000) |
| `treasury:gbp` | `treasury:{currency}` | STRICT | GBP treasury (initial: 300,000) |
| `fx:clearing` | `fx:clearing` | STRICT | FX clearing house -- holds multi-currency positions |
| `vendor:{name}` | `vendor:{name}` | STRICT | 9 vendors: acme, globex, initech, umbrella, stark (EUR) + brit-co, london-ltd, windsor, thames (GBP) |

## Numscripts

| Name | Version | Description |
|------|---------|-------------|
| `fund_account` | 1.0.0 | `@world` -> `$account` |
| `fx_convert` | 1.0.0 | `$source_account` -> `$clearing_account` (balance-checked leg of FX) |
| `vendor_payment` | 1.0.0 | `$treasury` -> `$vendor` |

## Business Flow

### Phase 1: Setup

- Create ledger with 3 account types (treasury, fx-clearing, vendor)
- Save 3 Numscripts
- Fund 3 treasury accounts from `@world`:
  ```
  @world --[USD/2 1,000,000]--> treasury:usd
  @world --[EUR/2 500,000]---> treasury:eur
  @world --[GBP/2 300,000]---> treasury:gbp
  ```

### Phase 2: FX Operations (40 Apply calls)

20 FX conversions across 5 currency pairs, each executed as 2 legs:

**Leg 1 (balance-checked)**: Source treasury -> fx:clearing in source currency
```
treasury:usd --[USD/2 10,000]--> fx:clearing
```

**Leg 2 (force transaction)**: fx:clearing -> target treasury in target currency
```
fx:clearing --[EUR/2 9,200]--> treasury:eur  (force=true, clearing doesn't have EUR)
```

Currency pairs and volume:
| Pair | Operations | Source Range | Target Range |
|------|-----------|--------------|--------------|
| USD -> EUR | 4 | 8,000 - 20,000 | 7,360 - 18,500 |
| EUR -> GBP | 4 | 3,000 - 12,000 | 2,580 - 10,320 |
| GBP -> USD | 4 | 2,000 - 6,000 | 2,540 - 7,620 |
| USD -> GBP | 4 | 5,000 - 11,000 | 3,950 - 8,690 |
| EUR -> USD | 4 | 4,000 - 10,000 | 4,360 - 10,900 |

Chapter close + double-entry check every 10 FX operations.

### Phase 2b: Force Script + Indexes

- **Force script transaction**: inline Numscript with `force=true` bypassing balance check on `fx:clearing`
- **Builtin transaction index**: create timestamp index, wait for READY, then drop it

### Phase 3: Vendor Payments (50 Apply calls)

30 EUR payments to 5 vendors (rotating, amounts 500 - 3,400):
```
treasury:eur --[EUR/2 amount]--> vendor:{name}
```

20 GBP payments to 4 vendors (rotating, amounts 400 - 1,920):
```
treasury:gbp --[GBP/2 amount]--> vendor:{name}
```

### Phase 3b: Analytics

- **AnalyzeAccounts**: address pattern discovery (segment depth 3)
- **AnalyzeTransactions**: flow pattern analysis

### Phase 3c: Prepared Queries with Typed Parameters

| Query | Target | Filter | Parameter | Type | Executions |
|-------|--------|--------|-----------|------|------------|
| `accounts-by-prefix` | ACCOUNTS | ParamAddressPrefix(`$prefix`) | `prefix` | StringParam | `"treasury:"` -> 3; `"vendor:"` -> 9; `"fx:"` -> 1 |
| `account-exact` | ACCOUNTS | ParamAddressExact(`$addr`) | `addr` | StringParam | `"vendor:acme"` -> 1; `"vendor:nonexistent"` -> 0 |
| `volumes-by-prefix` | ACCOUNTS | ParamAddressPrefix(`$prefix`) | `prefix` | StringParam | AGGREGATE_VOLUMES mode: `"treasury:"` -> volumes; `"vendor:"` -> volumes |

All 3 queries deleted after verification.

### Phase 4: Close + Reconciliation

- Final chapter close
- Double-entry balance check
- All tracked balances verified against actual (every account x every asset)
- No negative balances (except `@world` and `fx:clearing`)

### Audit Trail

94 transactions: 3 funds + 40 FX legs + 1 force script + 30 EUR payments + 20 GBP payments.

### Post-Test Phases

1. **StoreCheck** -- hash chain integrity
2. **Backup** -- full backup
3. **Restart + Verify** -- re-verify all balances + audit
4. **Backup-Restore + Verify** -- restore on fresh node, re-verify

## Volume

~100 Apply calls, ~2 cache rotations (threshold=50).
