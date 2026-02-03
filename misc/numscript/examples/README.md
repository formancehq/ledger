# Numscript Examples

This directory contains example Numscript files that can be used with the `ledgerctl` CLI.

## Usage

```bash
ledgerctl transactions create --ledger <ledger-name> --script <script-file> [--var "name=value"]...
```

## Examples

| File | Description |
|------|-------------|
| `simple_transfer.num` | Basic transfer between two accounts |
| `world_funding.num` | Create money by funding from `@world` |
| `multi_destination.num` | Split payment to multiple destinations with percentages |
| `multi_source.num` | Pay from multiple sources (fallback pattern) |
| `bounded_overdraft.num` | Allow overdraft up to a specific limit |
| `unbounded_overdraft.num` | Allow unlimited overdraft |
| `payment_with_fees.num` | Payment with platform fee calculation |
| `escrow_funding.num` | Fund an escrow account with dynamic address |

## Variable Format

Variables are passed using the `--var` flag:

```bash
# Account variable
--var "source=@users:alice"

# Monetary variable (asset/precision amount)
--var "amount=USD/2 100"    # 100 cents = $1.00
--var "amount=EUR/2 5000"   # 5000 cents = €50.00

# String variable
--var "order_id=order123"
```

## Quick Start

```bash
# 1. Create a ledger
ledgerctl ledgers create --name demo

# 2. Fund an account from world
ledgerctl transactions create --ledger demo --script world_funding.num \
  --var "destination=@bank" --var "amount=USD/2 100000"

# 3. Transfer between accounts
ledgerctl transactions create --ledger demo --script simple_transfer.num \
  --var "source=@bank" --var "destination=@users:alice" \
  --var "amount=USD/2 1000"

# 4. Check account balance
ledgerctl accounts get bank --ledger demo
```
