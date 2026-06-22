# Multi-Ledger Payroll

## Use Case

Models a company with 3 departments (engineering, sales, operations), each with its own ledger, plus a central "clearing" ledger for inter-department transfers and payroll funding. Tests multi-ledger isolation: same account names in different ledgers must not interfere, and cross-ledger balance reconciliation works correctly.

## Ledgers

| Ledger | Purpose |
|--------|---------|
| `clearing` | Central clearing: company treasury, department allocation tracking |
| `dept-eng` | Engineering department: 15 employees |
| `dept-sales` | Sales department: 10 employees |
| `dept-ops` | Operations department: 5 employees |

All use asset `USD/2` (cents). Base salary: USD/2 500,000 per employee per month.

## Account Structure

### Clearing Ledger

| Account | Pattern | Enforcement | Role |
|---------|---------|-------------|------|
| `company:treasury` | `company:{type}` | STRICT | Company-wide treasury, funded from `@world` |
| `dept:{name}` | `dept:{name}` | STRICT | Per-department allocation tracking (engineering, sales, operations) |

### Department Ledgers (each)

| Account | Pattern | Enforcement | Role |
|---------|---------|-------------|------|
| `payroll:pool` | `payroll:{type}` | STRICT | Department payroll pool, funded from `@world` |
| `employee:{id}` | `employee:{id}` | STRICT | Individual employee accounts |
| `expense:{type}` | `expense:{type}` | STRICT | Department expenses (unused) |

## Numscripts (shared across all ledgers)

| Name | Version | Description |
|------|---------|-------------|
| `fund_clearing` | 1.0.0 | `@world` -> `@company:treasury` |
| `fund_dept` | 1.0.0 | `@company:treasury` -> `$dept_account` |
| `fund_payroll` | 1.0.0 | `@world` -> `@payroll:pool` |
| `pay_salary` | 1.0.0 | `@payroll:pool` -> `$employee` |
| `pay_salary` | 2.0.0 | Same as v1 (tests Numscript versioning) |
| `cost_allocation` | 1.0.0 | `$from_dept` -> `$to_dept` (in clearing ledger) |

## Business Flow

### Phase 1: Setup

- Create 4 ledgers with account types
- Save 5 shared Numscripts (global, not per-ledger)

### Phase 2: Monthly Payroll Cycles (x3)

For each of 3 months:

#### Step 1: Fund Clearing

```
@world  --[USD/2 total_needed]--> company:treasury  (in clearing ledger)
```

Total: 30 employees x 500,000 = 15,000,000 per month.

#### Step 2: Distribute to Departments

In clearing ledger:
```
company:treasury  --[USD/2 dept_amount]--> dept:engineering  (15 x 500,000)
company:treasury  --[USD/2 dept_amount]--> dept:sales        (10 x 500,000)
company:treasury  --[USD/2 dept_amount]--> dept:operations    (5 x 500,000)
```

#### Step 3: Fund Department Payroll Pools

In each department ledger (mirroring clearing allocation):
```
@world  --[USD/2 dept_amount]--> payroll:pool
```

#### Step 4: Pay Employees

In each department ledger:
```
payroll:pool  --[USD/2 500,000]--> employee:{1..N}
```

Metadata: `month={N}`, `type=salary`.

Chapter close after each monthly cycle.

### Phase 3: Bonuses (Engineering only)

10% bonus (50,000 per employee) for engineering department:

```
@world         --[USD/2 750,000]--> payroll:pool    (in dept-eng)
payroll:pool   --[USD/2 50,000]---> employee:{1..15} (in dept-eng)
```

### Phase 4: Inter-department Cost Allocation

In clearing ledger:

| From | To | Amount | Reason |
|------|----|--------|--------|
| `dept:operations` | `dept:engineering` | 50,000 | Cloud hosting |
| `dept:engineering` | `dept:sales` | 30,000 | Lead generation |
| `dept:sales` | `dept:operations` | 20,000 | Office supplies |

### Phase 5: Ledger Isolation Verification

| Check | Expected |
|-------|----------|
| `employee:` accounts in dept-eng | 15 |
| `employee:` accounts in dept-sales | 10 |
| `employee:` accounts in dept-ops | 5 |
| `payroll:pool` exists in each dept ledger | independently (same name, different ledger) |
| `dept:` accounts in clearing | 3 (engineering, sales, operations) |
| `employee:` accounts in clearing | 0 (employees don't exist in clearing) |

### Phase 6: Numscript Versioning

1. **Save v2** of `pay_salary` (same content, different version)
2. **Verify both versions**: `GetNumscript("pay_salary", "1.0.0")` and `GetNumscript("pay_salary", "2.0.0")`
3. **Use v2**: fund + pay using v2 in dept-sales
4. **Duplicate semver rejection**: saving `pay_salary` v1.0.0 again fails

### Phase 7: Final Invariants

- Double-entry balance in all 4 ledgers
- No negative balances in any ledger (except `@world`)
- All employee balances match tracked values
- Payroll pools match tracked values (should be 0 for months 1-3)

### Post-Test Phases

1. **StoreCheck** -- hash chain integrity (all 4 ledgers)
2. **Backup** -- full backup
3. **Restart + Verify** -- re-verify double-entry in all ledgers
4. **Backup-Restore + Verify** -- restore on fresh node, re-verify

## Volume

~150 Apply calls across 4 ledgers.
