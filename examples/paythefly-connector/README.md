# PayTheFly Connector for Formance Ledger

A connector service that receives [PayTheFly](https://pro.paythefly.com) crypto payment webhook notifications and creates corresponding double-entry transactions in Formance Ledger.

## Architecture

```
PayTheFly → Webhook → Connector → Formance Ledger API (v2)
```

### Transaction Mapping

| PayTheFly Event | Ledger Transaction |
|---|---|
| Payment (tx_type=1) | `world` → `paythefly:payments:{wallet}` |
| Withdrawal (tx_type=2) | `paythefly:treasury` → `paythefly:withdrawals:{wallet}` |

All transactions include metadata with PayTheFly serial_no, tx_hash, wallet, and value.

## Configuration

| Environment Variable | Description | Default |
|---|---|---|
| `PAYTHEFLY_PROJECT_KEY` | HMAC-SHA256 key for webhook verification | *required* |
| `LEDGER_URL` | Formance Ledger API endpoint | `http://localhost:3068` |
| `LEDGER_NAME` | Target ledger name | `paythefly` |
| `LISTEN_ADDR` | Webhook listener address | `:8081` |
| `PAYTHEFLY_DECIMALS` | Token decimals (BSC=18, TRON=6) | `18` |

## Quick Start

### With Docker Compose

```bash
export PAYTHEFLY_PROJECT_KEY="your-project-key"
docker-compose up -d
```

### Standalone

```bash
export PAYTHEFLY_PROJECT_KEY="your-project-key"
export LEDGER_URL="http://localhost:3068"
go run main.go
```

Then configure your PayTheFly project webhook URL to: `http://your-host:8081/webhook/paythefly`

## Webhook Verification

The connector verifies every webhook using:
- **HMAC-SHA256** signature: `HMAC-SHA256(data + "." + timestamp, projectKey)`
- **Timing-safe** comparison to prevent timing attacks
- **Timestamp** validation (rejects webhooks older than 5 minutes)

## PayTheFly API Reference

- **EIP-712 Domain**: `{ name: "PayTheFlyPro", version: "1" }`
- **Webhook payload fields**: `value`, `confirmed`, `serial_no`, `tx_hash`, `wallet`, `tx_type`
- **tx_type**: 1 = payment, 2 = withdrawal
- **Supported chains**: BSC (chainId=56, 18 decimals), TRON (chainId=728126428, 6 decimals)
- **Amount format**: Human-readable (e.g., "0.01"), NOT raw token units
