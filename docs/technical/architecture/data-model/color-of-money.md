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
[ledgerName padded 64B] [account] \x00 [color] \x00 [asset_base] [precision 1B]
```

The ledger prefix is the ledger name written as a fixed-size, zero-padded
64-byte block (`dal.LedgerNameFixedSize`) by `appendLedgerName`/`readLedgerName`
in `internal/domain/keys.go` — not a numeric ledger ID.

The color sits between account and asset so prefix scans behave naturally:

- `[ledgerName][account]\x00` returns every `(color, asset)` for the account.
- `[ledgerName][account]\x00[color]\x00` returns every asset for that color.
- The trailing `precision` byte can be `0x00` (e.g. `"EUR"`) without
  encoding ambiguity because nothing follows it.

## Numscript contract

The numscript interpreter (`github.com/formancehq/numscript`, pinned in
`go.mod`) exposes color as a first-class property of the output `Posting` and
carries it per row on the balance query/response contract. The user-facing
shapes are row-based (not nested maps) — color is a column on each query item
and each returned row:

```go
// BalanceQueryItem is one (account, asset, color) the script needs; a
// BalanceQuery is the flat batch the store must answer.
type BalanceQueryItem struct {
	Account string
	Asset   string
	Color   string
}
type BalanceQuery []BalanceQueryItem

// BalanceRow is one materialized (account, asset, color) balance; Balances is
// the flat result set. Color is omitted from JSON when empty (uncolored).
type BalanceRow struct {
	Account string   `json:"account"`
	Asset   string   `json:"asset"`
	Amount  *big.Int `json:"amount"`
	Color   string   `json:"color,omitempty"`
}
type Balances []BalanceRow

// Posting carries the resolved color of the moved funds.
type Posting struct {
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Amount      *big.Int `json:"amount"`
	Asset       string   `json:"asset"`
	Color       string   `json:"color,omitempty"`
}
```

The Numscript syntax `source = @alice \ "GRANTS"` produces a posting with
`Color = "GRANTS"` and only draws from the matching bucket. The previous
`USD_GRANTS` asset-suffix encoding is gone — color is never folded into the
asset string anywhere in the contract.

### Insufficient-funds color is unresolved on the Numscript path

When a colored Numscript spend runs short, the interpreter raises
`numscriptlib.MissingFundsErr`, which carries only `{Asset, Needed, Available,
parser.Range}` — it does **not** expose the failing account or the color of the
bucket that ran short. A single asset can be sourced from several
`(account, color)` buckets in one script, and `Range` points into the source
text, not a resolved bucket, so the color cannot be recovered from the error
alone. `convertNumscriptError` therefore builds `ErrInsufficientFunds` with an
empty, *unresolved* color.

Because `Color == ""` is a first-class bucket everywhere else, the error carries
a `ColorKnown` flag to keep the two meanings apart on the wire:

- **Direct-posting failures** resolve the exact source bucket (`ColorKnown =
  true`). An empty color there is the genuine uncolored bucket, and the `color`
  key is always present in the error metadata (as `""`).
- **Numscript failures** cannot resolve the color (`ColorKnown = false`). The
  `color` key is **omitted** from the error metadata entirely, so a client reads
  its absence as "unknown" rather than mistaking `color: ""` for a definite hit
  on the uncolored bucket.

When a future numscript bump attaches the resolved `(account, color)` to
`MissingFundsErr`, the conversion path sets the real color with `ColorKnown =
true` and this asymmetry disappears.

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
