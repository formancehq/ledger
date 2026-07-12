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
// Send specifically colored funds
send [USD/2#promo 100] (
  source = @marketing:budget
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

## Volume Preloading (Dependency Resolution)

When a Numscript transaction is submitted, the accounts and metadata it touches must be preloaded into the FSM cache at admission time. However, which accounts a script touches can depend on the script's inputs (a `meta()`-resolved account, a `balance()`-derived amount), creating a chicken-and-egg problem: we need to preload before apply, but the touched set can depend on current state.

Since EN-1406 this is solved by the upstream library's static dependency-resolution API rather than by emulating a full execution.

### How It Works

Admission calls `ParseResult.ResolveDependencies(ctx, vars, store)`, which walks the script statically, evaluating var origins and posting selectors against a `Store` backed by admission-time reads (a Pebble snapshot — **not** FSM hot-path reads). It returns a richer dependency model than the old emulation:

- **account reads** — balances the resolution consulted (bounded sources, capped/allotment amounts, `balance()`/`overdraft()` in vars or selectors);
- **account writes** — accounts the script credits or debits (sources — including unbounded ones — and destinations);
- **account metadata reads** — `meta()` lookups;
- **account metadata writes** — `set_account_meta` targets;
- **transaction metadata writes** — `set_tx_meta` keys.

The wrapper lives in `internal/domain/processing/numscript/discover.go` (`DiscoverNumscriptDependencies`), the `Store` adapter and value-recording layer in `internal/domain/processing/numscript/store.go`:

1. **Parse** the script using `cache.GetOrParse(script)` — the NumscriptCache is shared with real execution.
2. **Resolve** dependencies against a `RecordingStore` wrapping the admission-time value source.
3. **Map** the resolved account/metadata dependencies to Ledger volume/metadata preload keys `(ledger, account, asset)` / `(ledger, account, key)` — Ledger does not partition volumes or metadata by color/scope.
4. **Preload** the union of read and write keys so every FSM read/mutate resolves from cache.

`meta()`, `oneof` (all branches), and multiple `send` blocks are all handled by the resolver — none is rejected. `ErrMetaNotSupported` and `ErrNonDeterministicScript` no longer exist.

#### Color/scope-qualified balance reads are rejected

Ledger volumes and account metadata have **no** color/scope dimension: every color/scope view of `(account, asset)` resolves to the *same* underlying volume, and every scope-qualified metadata read collapses to the same `(account, key)` entry. Rather than silently collapsing a qualified query onto the unqualified key — which would hand the script a full-balance view *per color* and let it spend the same funds once per color — the `Store` (`internal/domain/processing/numscript/store.go`) **rejects** any balance or metadata read that carries a non-empty `Color` or `Scope`, returning `domain.ErrColoredBalanceUnsupported` (a validation sentinel, so admission fails the transaction as a business error before proposing).

The rejection triggers only when a color/scope-qualified read is actually *performed*. An **unbounded colored source** such as `send [COIN *] (source = @world \ "RED" ...)` reads no balance — an unbounded source draws whatever the destination requires without consulting a balance — so it is **not** rejected. Only bounded/capped colored sources and colored `balance()`/`overdraft()` reads, which must consult a per-color balance, hit the guard.

#### Intra-batch effect accumulation

When several orders arrive in one atomic batch, the FSM applies them **sequentially** against a single mutated `WriteSet`, so a later order sees the balances and metadata written by earlier orders in the same batch. Admission mirrors this during discovery: each order is resolved against the pre-batch Pebble snapshot **with the accumulated effects of every preceding order layered on top** (`internal/application/admission/numscript_source.go`, `batchEffects`). Concretely, the balance/metadata effect of every preceding **mutating** order is folded — not just `CreateTransaction`:

- **`CreateTransaction`** — script/postings net `(input − output)` **balance deltas** per `(ledger, account, asset)`, plus `set_account_meta` writes; the caller-supplied `AccountMetadata` is folded **after** the script's writes because the FSM merges it with precedence over `set_account_meta` for the same key;
- **`RevertTransaction`** — the reversed-posting balance deltas (original destination → source, original source → destination), the inverse of the original postings, matching `processRevertTransaction`;
- **`AddMetadata`** (account target) — the metadata value it writes;
- **`DeleteMetadata`** (account target) — a **tombstone** for the key, so a later `meta()` resolves *absent* even if the pre-batch snapshot still holds a value.

The next order's `admissionValueSource` then reads a balance as `snapshot value + accumulated delta`, and a metadata key as the batch's last write if any — a set returns its value as present, a tombstone returns absent (last-writer-wins) — so it resolves against exactly the state the FSM will present it.

Without this, a batch where order N spends funds order N−1 credits (or reads metadata order N−1 wrote or deleted) would resolve N against a stale (pre-batch) state and mis-predict its outcome — a permanent `STALE_INPUTS_RESOLUTION` on every retry. Layering the effects keeps admission's resolution — and therefore the preload key set and the `inputs_resolution_hash` — consistent with sequential FSM apply.

### Stale-inputs Detection

When resolution's outcome depends on a current value (a `meta()`-resolved account, a `balance()`-derived amount), the preloaded key set is correct only for the *admission-time* state. If that value changes before the FSM applies the transaction, the preload set may be wrong.

To catch this, the `RecordingStore` records every balance/metadata value the resolution actually read and produces a deterministic BLAKE3 `Hash()` over the sorted records. Admission stores the hash on the order (`CreateTransactionOrder.inputs_resolution_hash`). At apply time the FSM re-runs `ResolveDependencies` against a `Store` backed **only** by preloaded cache values (via the coverage-gated `Scope` — no Pebble reads, respecting the hot-path invariant), recomputes the hash, and compares. A mismatch (or a resolution error on a script admission already resolved) returns `ErrStaleInputsResolution` (`Kind=Unavailable`, `codes.Unavailable`, retryable): the client retries, the second admission re-resolves against the new values and re-preloads.

The hash covers only values that *determined* the resolution — not every preloaded balance. A plain bounded source is a read *dependency* (it must be preloaded so the apply-path balance check can run) but its value does not change which keys are discovered, so it is not part of the hash. Fully-static scripts read nothing at resolution time and carry no hash; the FSM then skips the check.

### Notes

- **Shared parsing cache**: dependency resolution, the FSM-time stale re-resolution, and real execution all use `cache.GetOrParse(script)`, so a script is parsed once and cached for every path.
- **`force` mode**: with the transaction's `force` flag, the resolver's `Store` returns unlimited balances, so bounded sources still resolve but no real balance is consulted.
- **Resolution errors**: if resolution fails at admission (e.g. a `meta()` chain that cannot resolve), admission rejects the transaction as a business validation error before proposing — proposing without complete preloads would produce a doomed Raft apply.

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
