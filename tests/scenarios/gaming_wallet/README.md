# Gaming Wallet Lifecycle

## Use Case

Models a gaming platform with dual assets: real money (USD) and virtual currency (COINS). Players buy coins, spend them on in-game items, trade peer-to-peer, receive promotional credits, and face clawbacks on expired promotions. Tests dual-asset accounting, force transactions, multiple revert modes, account type enforcement switching, metadata lifecycle, and prepared queries with typed parameters.

## Ledger

Single ledger: `gaming`. Assets: `USD/2` (cents), `COINS` (integer).

## Account Structure

| Account | Pattern | Enforcement | Role |
|---------|---------|-------------|------|
| `player:{id}:usd` | `player:{id}:usd` | STRICT | Real money balance per player (20 players) |
| `player:{id}:coins` | `player:{id}:coins` | STRICT | Virtual currency balance per player |
| `platform:revenue` | `platform:{type}` | AUDIT | Revenue from coin purchases (USD) |
| `platform:promotions` | `platform:{type}` | AUDIT | Clawback destination for expired promos |
| `shop:items` | `shop:{type}` | STRICT | Item sales revenue (COINS) |
| `escrow:p2p` | `escrow:{type}` | STRICT | Peer-to-peer trade escrow |

## Numscripts

| Name | Version | Description |
|------|---------|-------------|
| `top_up` | 1.0.0 | 3-block multi-send: `@world` -> player USD, player USD -> `@platform:revenue`, `@world` -> player COINS |
| `buy_item` | 1.0.0 | `$player_coins` -> `@shop:items` |
| `p2p_transfer` | 1.0.0 | `$from_player` -> `$to_player` (COINS) |
| `clawback` | 1.0.0 | `$player_coins` -> `@platform:promotions` |

## Business Flow

### Phase 1: Setup

- Create ledger
- Register 5 account types: player-usd (STRICT), player-coins (STRICT), platform (AUDIT), shop (STRICT), escrow (STRICT)
- Save 4 Numscripts

### Phase 2: Top-Ups (20 Apply calls)

Each player buys coins with real money (3 sends per transaction):

```
@world              --[USD/2 5,000]---> player:{id}:usd
player:{id}:usd     --[USD/2 5,000]---> platform:revenue
@world              --[COINS 5,000]---> player:{id}:coins
```

Each transaction has reference `topup-initial-{id}` and metadata `type=initial-topup`.

### Phase 3: Promotional Credits (10 Apply calls, force=true)

First 10 players receive 500 free coins via **force transactions** (bypass balance check on `@world`):

```
world  --[COINS 500]--> player:{1..10}:coins  (force=true)
```

Metadata: `type=promotion`, `reason=welcome-bonus`. Period close after promotions.

### Phase 4: Item Purchases (3 rounds)

3 rounds with increasing costs (100, 250, 500 COINS). Each player buys if they have enough:

```
player:{id}:coins  --[COINS cost]--> shop:items
```

- Round 1: 100 COINS -- first 5 transaction IDs captured for revert tests
- Round 2: 250 COINS -- mid-way account reads (cache exercise)
- Round 3: 500 COINS -- only players with sufficient balance

Period close after purchases.

### Phase 5: Peer-to-Peer Trades (up to 10 Apply calls)

10 predefined trades between player pairs (amounts 25-150 COINS):

```
player:{from}:coins  --[COINS amount]--> player:{to}:coins
```

Trades skipped if sender has insufficient balance.

### Phase 6: Refunds (Reverts)

| Revert | Mode | Description |
|--------|------|-------------|
| Purchase 1 | `force=false` | Balance-checked revert. Metadata: `reason=refund` |
| Purchase 2 | `force=true` | Forced revert. Metadata: `reason=admin-refund` |
| Purchase 1 again | `force=false` | **Double-revert error** (expected failure) |
| Purchase 3 | `force=false` + `ExpandVolumes` | Revert with post-commit volumes in response |

### Phase 7: Insufficient Funds

Attempt to buy COINS 999,999,999 from last player -- fails with insufficient funds.

### Phase 8: Promotional Clawback

Reclaim expired promo coins from players 8-10. Capped to available balance:

```
player:{8..10}:coins  --[COINS min(500, balance)]--> platform:promotions
```

Metadata: `type=promo-clawback`, `reason=expired-welcome-bonus`. Period close after clawback.

### Phase 9: Metadata Lifecycle

On `player:1:coins`:
1. **Add**: `vip=true`, `tier=gold`, `joined=2025-01-01`
2. **Update**: `tier=platinum` (overwrites gold)
3. **Verify**: `tier` is `"platinum"`
4. **Delete key**: remove `joined`
5. **Verify deletion**: `joined` is nil, other keys remain

### Phase 10: Account Type Enforcement

1. **STRICT rejection**: posting to `invalid-address` fails
2. **Switch platform to STRICT**: posting to `platform:test` succeeds (valid pattern)
3. **Switch platform back to AUDIT**

### Phase 11: Prepared Queries with Typed Parameters

Metadata index created: `tier` (backfill awaited).

| Query | Target | Filter | Parameter | Type | Executions |
|-------|--------|--------|-----------|------|------------|
| `accounts-by-prefix` | ACCOUNTS | ParamAddressPrefix(`$prefix`) | `prefix` | StringParam | `"player:"` -> 40 (20 x 2); `"shop:"` -> >= 1; `"escrow:"` -> >= 0 |
| `account-exact` | ACCOUNTS | ParamAddressExact(`$addr`) | `addr` | StringParam | `"platform:revenue"` -> 1; `"nonexistent:account"` -> 0 |
| `by-tier` | ACCOUNTS | ParamStringMetadata(`tier`, `$tier_value`) | `tier_value` | StringParam | `"platinum"` -> >= 1; `"gold"` -> 0 (overwritten) |

All 3 queries deleted after verification.

### Phase 12: Final Invariants

- Double-entry balance
- No negative balances (except `@world`)
- All 20 player coin balances match tracked values
- `shop:items` COINS balance matches tracked total
- `platform:revenue` USD balance matches tracked total
- Transaction count > 100
- Audit trail: >= 100 transactions, correct revert count

### Post-Test Phases

1. **StoreCheck** -- hash chain integrity
2. **Backup** -- full backup
3. **Restart + Verify** -- re-verify double-entry + no negative balances
4. **Backup-Restore + Verify** -- restore on fresh node, re-verify

## Volume

~180 Apply calls, ~3 cache rotations (threshold=50).
