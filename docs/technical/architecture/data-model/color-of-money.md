# Color of money

Color is a per-posting segregation key. The ledger tracks balances per
`(account, asset, color)` triple. Two postings on the same `(account, asset)`
but different colors operate on strictly isolated balances.

## Invariants

1. **Color is immutable.** Once funds are emitted under a color, no operation
   ever changes that color. To "convert" color X to color Y the operator must
   compose two transactions: send `X` to a clearing account, then mint `Y`
   from `@world` back to the original holder.

2. **Empty color is a bucket.** `Color == ""` is the *uncolored* bucket and
   participates in segregation just like any colored bucket. A posting with
   `Color == ""` cannot pull funds from a colored bucket, and vice versa.

3. **Color charset is `^[A-Z]*$`.** Enforced both by the upstream
   numscript interpreter and at the ledger admission boundary. The empty
   string remains valid (the uncolored bucket).

4. **Double-entry holds per bucket.** For every `(asset, color)`, the sum of
   all account balances is zero. Each bucket is its own conservation universe.

## Wire shapes

- `commonpb.Posting.color: string` — primary carrier of the field; rides on
  every read and write path.
- `commonpb.AccountVolume{asset, color, volumes}` — `Account.volumes` is a
  list sorted by `(asset, color)` ascending. Deterministic serialization is
  required for stable JSON / snapshot tests.
- `commonpb.VolumeEntry{asset, color, volumes}` — same shape, used inside
  `PostCommitVolumes.volumes_by_account[account].volumes`.
- `commonpb.AggregatedVolume.color: string` — set on every entry returned by
  `AggregateVolumes`. By default one entry per `(asset, color)` tuple;
  `collapse_colors` flag sums across colors.

## Storage layout

`domain.VolumeKey` carries `Color string`. Canonical bytes:

```
[ledgerID BE 4B] [account] \x00 [color] \x00 [asset_base] [precision 1B]
```

The color sits between account and asset so prefix scans behave naturally:

- `[ledgerID][account]\x00` returns every `(color, asset)` for the account.
- `[ledgerID][account]\x00[color]\x00` returns every asset for that color.
- The trailing `precision` byte can be `0x00` (e.g. `"EUR"`) without
  encoding ambiguity because nothing follows it.

## Numscript contract

The numscript interpreter (`github.com/formancehq/numscript`, branch
`feat/color-on-posting`, see PR
[formancehq/numscript#139](https://github.com/formancehq/numscript/pull/139))
exposes color as a first-class property of the output `Posting`. The
in-memory shapes are:

```go
type AssetColor   struct { Asset string; Color string }
type BalanceQuery map[string][]AssetColor
type ColorBalance map[string]*big.Int       // color -> amount
type AccountBalance map[string]ColorBalance // asset -> ColorBalance
type Balances map[string]AccountBalance     // account -> AccountBalance
```

The Numscript syntax `source = @alice \ "GRANTS"` produces a posting with
`Color = "GRANTS"` and only draws from the matching bucket. The previous
`USD_GRANTS` asset-suffix encoding is gone — color is never folded into the
asset string anywhere in the contract.

## Collapse modes

For consumers that want a per-asset summary (totals ignoring color), the
read API exposes opt-in collapse flags:

- `GET /{ledger}/accounts/{address}?collapseColors=true` — `Account.volumes`
  returns one entry per asset with `color = ""` and amounts summed.
- `GET /{ledger}/volumes?collapseColors=true` — `AggregatedVolume` entries
  collapse to one per `(asset, precision)` with `color = ""`.

Collapse is always opt-in: the default keeps the segregation visible so
clients cannot accidentally aggregate across buckets that should stay
isolated.

## What this does NOT do

- **No color filtering on list queries.** This iteration does not surface a
  native `WHERE color = "GRANTS"` filter on `ListAccounts` / `ListTransactions`.
  Filter client-side, or rely on the generic `filterexpr` engine.
- **No re-coloring primitive.** Numscript does not (yet) expose a syntax to
  mutate the color of funds in-place. The composition pattern (clearing
  account + mint from `@world`) is the supported path.
- **No automatic propagation from account metadata to color.** Color is
  carried by the posting, not derived from the account's metadata.
