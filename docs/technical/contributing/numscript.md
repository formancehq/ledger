# Numscript Support

Numscript is a domain-specific language (DSL) for expressing financial transactions. It provides a safe, declarative way to describe complex money movements with built-in support for multi-source/multi-destination transfers, overdrafts, and fee calculations.

## Overview

This ledger implementation uses the official Numscript interpreter from `github.com/formancehq/numscript`. All experimental features are **available** (the server imposes no restrictions), but each script must **explicitly opt in** using the `#![feature("...")]` pragma.

### Key Capabilities

- **Safe money movements**: Prevents accidental money creation (except from `@world`)
- **Multi-source/destination**: Split payments or collect from multiple accounts
- **Overdraft support**: Bounded or unbounded overdrafts per account
- **Percentage-based splits**: Distribute amounts using percentages
- **Variable substitution**: Parameterize scripts with runtime variables
- **Metadata operations**: Set transaction and account metadata within scripts

## Enabled Features

All experimental Numscript features are available but must be explicitly opted into per script via the `#![feature("...")]` pragma syntax (e.g., `#![feature("experimental-account-interpolation")]`).

| Feature Flag | Description |
|--------------|-------------|
| `experimental-account-interpolation` | Dynamic account addresses using variable interpolation |
| `experimental-asset-colors` | Asset coloring for tracking fund origins |
| `experimental-get-amount-function` | `get_amount()` function to extract amount from monetary values |
| `experimental-get-asset-function` | `get_asset()` function to extract asset from monetary values |
| `experimental-mid-script-function-call` | Mid-script function calls (e.g., balance queries during execution) |
| `experimental-oneof` | `oneof` source/destination selector for conditional routing |
| `experimental-overdraft-function` | `overdraft()` function for dynamic overdraft calculation |

## Basic Syntax

### Simple Transfer

```numscript
send [USD/2 1000] (
  source = @users:alice
  destination = @users:bob
)
```

### With Variables

```numscript
vars {
  account $source
  account $destination
  monetary $amount
}

send $amount (
  source = $source
  destination = $destination
)
```

### Multiple Postings

```numscript
send [USD/2 500] (
  source = @users:alice
  destination = @merchant
)

send [USD/2 50] (
  source = @users:alice
  destination = @platform:fees
)
```

### Setting Transaction Metadata

Use `set_tx_meta` to attach metadata to the transaction being created:

```numscript
set_tx_meta("type", "payment")
set_tx_meta("category", "purchase")
set_tx_meta("order_id", "order-12345")

send [USD/2 1000] (
  source = @users:alice
  destination = @merchant
)
```

The metadata will be stored on the created transaction and can be retrieved via the API.

### Setting Account Metadata

Use `set_account_meta` to attach metadata to accounts involved in the transaction:

```numscript
vars {
  account $destination
  monetary $amount
}

set_account_meta($destination, "account_type", "savings")
set_account_meta($destination, "created_by", "numscript")

send $amount (
  source = @world
  destination = $destination
)
```

This is useful for:
- Setting default account properties when first funding an account
- Tracking account categories or types
- Storing audit information about account creation

## Feature Examples

### 1. Account Interpolation (Dynamic Addresses)

Create dynamic account addresses using variable interpolation:

```numscript
vars {
  account $buyer
  string $order_id
  monetary $amount
}

send $amount (
  source = $buyer
  destination = @escrow:$order_id
)
```

**Usage:**
```bash
ledgerctl transactions create --ledger demo \
  --script escrow.num \
  --var "buyer=users:alice" \
  --var "order_id=order-12345" \
  --var "amount=USD/2 500"
```

This creates a transaction sending funds to `escrow:order-12345`.

### 2. Asset Colors

Track the origin of funds using asset coloring:

```numscript
#![feature("experimental-asset-colors")]

// Send specifically colored funds: the color qualifies the source bucket.
// Colors are uppercase (validated against ^[A-Z]*$).
send [USD/2 100] (
  source = @marketing:budget \ "PROMO"
  destination = @users:alice
)
```

Asset colors allow tracking where funds originated, useful for:
- Promotional credits
- Restricted fund usage
- Compliance tracking

### 3. Get Amount Function

Extract the amount from a monetary value:

```numscript
vars {
  monetary $payment
}

// Get just the numeric amount
amount = get_amount($payment)

send $payment (
  source = @treasury
  destination = @users:alice
)
```

### 4. Get Asset Function

Extract the asset from a monetary value:

```numscript
vars {
  monetary $payment
}

// Get just the asset (e.g., "USD/2")
asset = get_asset($payment)

send $payment (
  source = @treasury
  destination = @users:alice
)
```

### 5. Mid-Script Function Calls

Query balances during script execution:

```numscript
vars {
  account $user
}

// Query balance mid-script
current_balance = balance($user, USD/2)

send [USD/2 100] (
  source = $user
  destination = @treasury
)
```

### 6. OneOf Selector

Conditional routing based on fund availability:

```numscript
vars {
  monetary $amount
}

send $amount (
  source = oneof {
    @users:alice:primary
    @users:alice:secondary
    @users:alice:backup
  }
  destination = @merchant
)
```

The `oneof` selector tries each source in order until one has sufficient funds.

### 7. Overdraft Function

Calculate available overdraft dynamically:

```numscript
vars {
  account $user
}

// Calculate how much overdraft is available
available = overdraft($user, USD/2)

send [USD/2 1000] (
  source = $user allowing overdraft up to [USD/2 500]
  destination = @merchant
)
```

## Overdraft Patterns

### Bounded Overdraft

Allow overdraft up to a specific limit:

```numscript
vars {
  account $source
  account $destination
  monetary $amount
  monetary $max_overdraft
}

send $amount (
  source = $source allowing overdraft up to $max_overdraft
  destination = $destination
)
```

### Unbounded Overdraft

Allow unlimited overdraft (use with caution):

```numscript
vars {
  account $source
  account $destination
  monetary $amount
}

send $amount (
  source = $source allowing unbounded overdraft
  destination = $destination
)
```

## Multi-Source and Multi-Destination

### Multiple Sources (Fallback Pattern)

Draw from multiple accounts in order:

```numscript
vars {
  monetary $amount
}

send $amount (
  source = {
    @users:alice:checking
    @users:alice:savings
    @users:alice:credit allowing overdraft up to [USD/2 1000]
  }
  destination = @merchant
)
```

### Multiple Destinations (Split Payment)

Split a payment across multiple recipients:

```numscript
vars {
  monetary $total
}

send $total (
  source = @users:alice
  destination = {
    90% to @merchant
    10% to @platform:fees
  }
)
```

### Remaining Amount

Send whatever is left after specific amounts:

```numscript
send [USD/2 1000] (
  source = @treasury
  destination = {
    [USD/2 100] to @platform:fees
    remaining to @merchant
  }
)
```

## World Account

The special `@world` account represents an unlimited source of funds (money creation):

```numscript
// Fund an account from world (money creation)
send [USD/2 10000] (
  source = @world
  destination = @treasury
)
```

**Note:** Only `@world` can create money. All other accounts must have sufficient balance (or overdraft allowance).

## Variable Types

| Type | Declaration | Example Value |
|------|-------------|---------------|
| `account` | `account $name` | `users:alice` |
| `monetary` | `monetary $name` | `USD/2 100` |
| `string` | `string $name` | `order-123` |
| `number` | `number $name` | `42` |
| `portion` | `portion $name` | `1/4` or `25%` |

## CLI Usage

### Create Transaction with Numscript

```bash
ledgerctl transactions create --ledger demo \
  --script path/to/script.num \
  --var "source=users:alice" \
  --var "destination=users:bob" \
  --var "amount=USD/2 1000"
```

### Using Example Scripts

```bash
# Fund from world
ledgerctl transactions create --ledger demo \
  --script numscript/examples/world_funding.num \
  --var "destination=treasury" \
  --var "amount=USD/2 100000"

# Simple transfer
ledgerctl transactions create --ledger demo \
  --script numscript/examples/simple_transfer.num \
  --var "source=treasury" \
  --var "destination=users:alice" \
  --var "amount=USD/2 1000"

# Payment with fees
ledgerctl transactions create --ledger demo \
  --script numscript/examples/payment_with_fees.num \
  --var "source=users:alice" \
  --var "merchant=merchants:shop" \
  --var "platform=platform:fees" \
  --var "amount=USD/2 1000" \
  --var "fee_percent=10%"
```

## HTTP API Usage

### Create Transaction with Script

```bash
curl -X POST "http://localhost:8080/demo/transactions" \
  -H "Content-Type: application/json" \
  -d '{
    "script": {
      "plain": "vars { account $src account $dst monetary $amt } send $amt ( source = $src destination = $dst )",
      "vars": {
        "src": "users:alice",
        "dst": "users:bob",
        "amt": "USD/2 1000"
      }
    }
  }'
```

## Error Handling

### Insufficient Funds

If a source account doesn't have enough balance (and no overdraft is allowed):

```json
{
  "errorCode": "INSUFFICIENT_FUND",
  "errorMessage": "account users:alice has insufficient funds for asset USD/2"
}
```

### Parse Errors

If the Numscript syntax is invalid:

```json
{
  "errorCode": "NUMSCRIPT_PARSE_ERROR",
  "errorMessage": "numscript parse error: unexpected token at line 3"
}
```

## Volume Preloading (Numscript Emulation)

When a Numscript transaction is submitted, account balances must be preloaded at admission time. However, the accounts involved are determined dynamically by the script at runtime, creating a chicken-and-egg problem: we need balances before execution, but we don't know which accounts until after execution.

### How It Works

As a temporary workaround, the admission layer runs the Numscript once with a "discovery store" that returns infinite balances (`2^256`) for every account queried. This emulation run discovers which accounts and assets the script needs, without modifying any real state. The discovered volume keys are then preloaded normally from storage.

The emulation is implemented in `internal/domain/processing/numscript/emulate.go`:

1. **Parse** the script using `cache.GetOrParse(script)` — the NumscriptCache is used for parsing during discovery
2. **Execute** with a discovery store that records queried account/asset pairs and returns infinite balances
3. **Collect** volume keys from both balance queries (sources) and result postings (sources + destinations)
4. **Preload** the discovered volumes from the store as usual

### Determinism Constraint

Scripts must be deterministic: the discovery store enforces that `GetBalances` may be called **at most once** during discovery. A second call indicates a non-deterministic script (e.g., mid-script balance queries that depend on earlier execution results) which cannot be reliably preloaded. `GetAccountsMetadata` always returns `ErrMetaNotSupported` — `meta()` calls are fully rejected, not constrained.

If a script violates the single `GetBalances` constraint, `DiscoverNumscriptDependencies` returns `ErrNonDeterministicScript` with the offending method name. If a script uses `meta()`, it returns `ErrMetaNotSupported`. The admission layer rejects these dependency discovery failures as business validation errors before proposing the command, because proposing without complete preloads would produce a doomed Raft apply.

### Known Limitations

- **`oneof` selectors**: With infinite balances, `oneof` may only query the first source account, since the first source always has sufficient funds. Other sources in the `oneof` list may not be discovered.
- **Discovery errors**: If dependency discovery fails, admission rejects the transaction before proposal instead of skipping volume discovery.
- **Shared parsing cache**: Both discovery and real execution use `cache.GetOrParse(script)`, so the script is parsed only once and cached for both paths.
- **Non-deterministic scripts**: Scripts with multiple `send` statements that trigger separate `GetBalances` calls are rejected during discovery. Such scripts cannot have their volumes reliably preloaded.

### Long-term Solution

The emulation approach is a temporary workaround. The long-term solution is static analysis of Numscript scripts to declare all required inputs at parse time. See the [Static Inputs RFC](../../drafts/numscript/numscript-static-inputs-rfc.md) for details.

## Performance Considerations

### Script Caching

Parsed Numscript programs are cached using a blake3 hash of the script content. This avoids re-parsing the same script multiple times, significantly improving performance for repeated transactions with the same script.

Cache metrics are exposed via OpenTelemetry:
- `numscript.cache.size` - Number of scripts in cache

### Best Practices

1. **Reuse scripts**: Use variables to parameterize scripts rather than generating unique scripts
2. **Keep scripts simple**: Complex scripts with many sources/destinations take longer to execute
3. **Minimize balance queries**: Each balance query requires storage access

## Related Documentation

- [CLI Reference](../../ops/cli.md) - CLI usage and examples
- [Numscript Examples](../../../misc/numscript/examples/README.md) - Ready-to-use example scripts
- [API Comparison](./api-comparison.md) - Feature parity with original ledger
- [Static Inputs RFC](../../drafts/numscript/numscript-static-inputs-rfc.md) - RFC for static input declaration
