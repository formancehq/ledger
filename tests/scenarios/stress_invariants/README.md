# Stress Invariants

## Use Case

Pure stress test focused on verifying invariant correctness under high transaction volume. Models a trading exchange with 100 trader accounts, 400 trading iterations with 1% fee splitting, periodic reverts, interleaved account reads, and monitoring RPCs.

## Ledger

Single ledger: `stress`. Asset: `USD/2` (cents).

## Account Structure

| Account | Pattern | Enforcement | Role |
|---------|---------|-------------|------|
| `trader:{id}` | `trader:{id}` | STRICT | 100 trader accounts, each funded with 1,000,000 |
| `exchange:fees` | `exchange:fees` | STRICT | Accumulated trading fees (1% per trade) |
| `exchange:withdrawals` | `exchange:withdrawals` | STRICT | Withdrawal destination (unused in this test) |

## Numscripts

| Name | Version | Description |
|------|---------|-------------|
| `deposit` | 1.0.0 | `@world` -> `$account` |
| `trade` | 1.0.0 | `$buyer` -> 1/100 to `@exchange:fees` + remaining to `$seller` |
| `withdraw` | 1.0.0 | `$account` -> `@exchange:withdrawals` |

## Business Flow

### Phase 1: Setup

- Create ledger with 3 account types (all STRICT)
- Save 3 Numscripts

### Phase 2: Bulk Deposits (100 Apply calls)

All 100 accounts funded in a single batch:

```
@world  --[USD/2 1,000,000]--> trader:{1..100}
```

Spot-check balances for traders 1, 25, 50, 75, 100.

### Phase 3: Trading Loop (400 Apply calls)

400 trades between rotating buyer/seller pairs (amounts 100-590):

```
trader:{buyer}  --[USD/2 amount]--> exchange:fees  (1%)
trader:{buyer}  --[USD/2 amount]--> trader:{seller}  (99%)
```

Interleaved operations during the loop:

| Trigger | Action |
|---------|--------|
| Every 20 trades | Read 5 accounts (pseudo-random selection, cache hit/miss exercise) |
| Every 40 trades | Revert a trade from 10 trades ago (`force=true`) |
| Every 80 trades | Period close + double-entry balance check |

### Phase 3b: Audit Entries

`ListAuditEntries` RPC verification (audit not explicitly enabled, so empty is acceptable).

### Phase 4: Final Invariants

- Double-entry balance
- No negative balances (except `@world`)
- `exchange:fees` has positive USD/2 balance
- **GetLedgerStats**: account and transaction counts > 0

### Phase 4b: Monitoring RPCs

| RPC | Verification |
|-----|-------------|
| `GetStoreMetrics` | available=true, metrics not nil |
| `GetReadIndexMetrics` | available=true |
| `GetIndexStatus` | lastIndexedSequence > 0 |

### Phase 4c: Discovery

`Discovery` RPC: response not nil (responseSigning may be nil if not configured).

### Phase 5: Audit Trail

~510 transactions: 100 deposits + 400 trades + ~10 reverts.

### Post-Test Phases

1. **StoreCheck** -- hash chain integrity
2. **Backup** -- full backup
3. **Restart + Verify** -- re-verify double-entry, no negative balances, fees positive, audit trail
4. **Backup-Restore + Verify** -- restore on fresh node, re-verify

## Volume

~530 Apply calls, ~10 cache rotations (threshold=50), ~53 snapshots (threshold=10).
