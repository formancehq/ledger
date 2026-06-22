# Subscription Billing Cycle

## Use Case

Models a SaaS billing system with 50 subscribers across 3 tiers, running 3 monthly billing cycles. Tests revenue recognition (deferred -> recognized), under-funded payment failures, typed metadata schemas, and prepared queries with typed parameters.

## Ledger

Single ledger: `billing`. Asset: `USD/2` (cents).

## Account Structure

| Account | Pattern | Enforcement | Role |
|---------|---------|-------------|------|
| `subscriber:{id}` | `subscriber:{id}` | STRICT | Subscriber wallets (50 accounts) |
| `revenue:deferred` | `revenue:deferred` | STRICT | Charges land here before recognition |
| `revenue:recognized` | `revenue:recognized` | STRICT | Recognized revenue (transferred from deferred) |
| `revenue:adjustment` | `revenue:adjustment` | STRICT | Adjustments (refunds, corrections) |

## Subscriber Tiers

| Tier | Monthly Charge | Per-Cycle Funding | Subscribers |
|------|---------------|-------------------|-------------|
| basic | USD/2 1,000 ($10) | USD/2 5,000 ($50) | IDs 1, 4, 7, 10, ... |
| pro | USD/2 2,500 ($25) | USD/2 10,000 ($100) | IDs 2, 5, 8, 11, ... |
| enterprise | USD/2 5,000 ($50) | USD/2 20,000 ($200) | IDs 3, 6, 9, 12, ... |

Subscribers 1-5 are **under-funded**: their per-cycle funding is `charge / 4`, so they can never afford the monthly charge. Their billing transactions fail with insufficient funds every cycle.

## Metadata Schema

Declared at ledger creation and via `SetMetadataFieldType`:

| Target | Key | Type | Usage |
|--------|-----|------|-------|
| ACCOUNT | `subscriber_plan` | STRING | Subscriber tier name ("basic", "pro", "enterprise") |
| ACCOUNT | `billing_cycle` | INT64 | Billing cycle number |
| ACCOUNT | `retention_score` | INT64 | Customer retention score (0-100) |

## Numscripts

| Name | Version | Description |
|------|---------|-------------|
| `fund_wallet` | 1.0.0 | `@world` -> `$subscriber` |
| `charge_subscription` | 1.0.0 | `$subscriber` -> `@revenue:deferred` |
| `issue_credit` | 1.0.0 | `@world` -> `$subscriber` |
| `recognize_revenue` | 1.0.0 | `@revenue:deferred` (allowing unbounded overdraft) -> `@revenue:recognized` |
| `adjust_revenue` | 1.0.0 | `@revenue:deferred` (allowing unbounded overdraft) -> `@revenue:adjustment` |

## Business Flow

### Phase 1: Setup

- Create ledger with metadata schema (`subscriber_plan: STRING`, `billing_cycle: INT64`)
- Register 4 account types (all STRICT)
- Save 5 Numscripts

### Phase 1b: Add Extra Metadata Field

- `SetMetadataFieldType`: add `retention_score: INT64` to account metadata schema

### Phases 2-4: Billing Cycles (x3)

For each of 3 monthly cycles:

#### Fund Wallets (50 Apply calls)

```
@world  --[per-tier funding]--> subscriber:{1..50}
```

#### Monthly Billing (50 attempts, 5 failures)

```
subscriber:{id}  --[tier charge]--> revenue:deferred
```

- 45 succeed (sufficient funds)
- 5 fail with insufficient funds (subscribers 1-5)

#### Credits (2 Apply calls)

```
@world  --[USD/2 200]--> subscriber:{6+cycle}
@world  --[USD/2 300]--> subscriber:{10+cycle}
```

#### Typed Metadata (cycle 1 only)

Account metadata with explicit types:

| Account | Key | Type | Value |
|---------|-----|------|-------|
| `subscriber:6` | `subscriber_plan` | STRING | `"pro"` |
| `subscriber:6` | `billing_cycle` | INT64 | `1` |
| `subscriber:6` | `retention_score` | INT64 | `85` |
| `subscriber:7` | `subscriber_plan` | STRING | `"enterprise"` |
| `subscriber:7` | `retention_score` | INT64 | `92` |

Verified via `GetAccount`: type-preserving read (string stays string, int stays int).

#### Typed Transaction Metadata (cycle 1 only)

Set typed metadata on the first fund transaction:

| Transaction | Key | Type | Value |
|-------------|-----|------|-------|
| first fund tx | `billing_cycle` | INT64 | `1` |
| first fund tx | `subscriber_plan` | STRING | `"initial_fund"` |

Verified via `GetTransaction`.

#### Revenue Adjustment (1 Apply call)

```
revenue:deferred  --[USD/2 500]--> revenue:adjustment
```

Uses `allowing unbounded overdraft` (deferred may go negative).

#### Revenue Recognition (1 Apply call)

```
revenue:deferred  --[full balance]--> revenue:recognized
```

Balance of `revenue:deferred` verified at 0 after recognition.

#### Chapter Close

Close chapter + double-entry check after each cycle.

### Metadata Schema Verification

`GetMetadataSchemaStatus`: verify all 3 declared account fields (`subscriber_plan`, `billing_cycle`, `retention_score`) persist through billing cycles.

### Prepared Queries with Typed Parameters

Metadata indexes created before querying: `subscriber_plan` and `retention_score`. Index backfill awaited (lag=0, no progress).

| Query | Target | Filter | Parameter | Type | Executions |
|-------|--------|--------|-----------|------|------------|
| `accounts-by-prefix` | ACCOUNTS | ParamAddressPrefix(`$prefix`) | `prefix` | StringParam | `"subscriber:"` -> 50 results; `"revenue:"` -> >= 2 results |
| `by-plan` | ACCOUNTS | ParamStringMetadata(`subscriber_plan`, `$plan_value`) | `plan_value` | StringParam | `"pro"` -> >= 1; `"enterprise"` -> >= 1; `"nonexistent"` -> 0 |
| `high-retention` | ACCOUNTS | ParamInt64RangeMetadata(`retention_score`, `$min_score`, `$max_score`) | `min_score`, `max_score` | Int64Param | `90-100` -> >= 1 (sub:7 has 92); `80-90` -> >= 1 (sub:6 has 85); `0-50` -> 0 |

All 3 queries deleted after verification.

### Metadata Index Under Load (Regression)

`CreateAccountMetadataIndex("subscriber_plan")` after ~200 Apply calls (4+ cache rotations). Tests that `LedgerInfo` survives cache eviction. Index dropped after test.

### Final Invariants

- Double-entry balance
- `revenue:recognized` matches tracked total
- Under-funded subscribers (1-5) have `funding_per_cycle * 3` (never charged)

### Audit Trail

Per cycle: 50 funds + 45 charges + 2 credits + 1 adjust + 1 recognize = 99. Total: 297 transactions.

### Post-Test Phases

1. **StoreCheck** -- hash chain integrity
2. **Backup** -- full backup to memory
3. **Restart + Verify** -- WAL replay, re-verify balances + audit
4. **Backup-Restore + Verify** -- restore on fresh node, re-verify

## Volume

~200 Apply calls, ~4 cache rotations (threshold=50).
