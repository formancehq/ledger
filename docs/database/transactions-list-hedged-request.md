# Hedged request for the transactions-list query

Listing transactions with a JSONB filter (`metadata[source_wallet_id]`, `account`,
`source`, `destination`) and `ORDER BY id DESC LIMIT n` can take tens of seconds
for a *sparse* wallet — one whose matching rows are few and old.

Postgres satisfies the ordering with an Index Scan Backward on `id` and discovers
the filter selectivity only mid-scan, so it walks a large fraction of the table
before collecting `n` rows (~50 s observed at production scale). For a *dense*
wallet — recent rows, most of them matching — that same backward scan is the fast
plan, and forcing the GIN bitmap path instead would make it slower. There is no
single plan that serves both.

## What the hedge does

When enabled, each paginated transactions-list query races two attempts:

1. **Original** — fires immediately, no timeout, no plan override.
2. **Chaser** — fires only if the original is still running after
   `--tx-list-chaser-delay-ms`, with `SET LOCAL enable_indexscan = off` (forcing
   the GIN path) and its own `SET LOCAL statement_timeout`.

Whichever returns first wins; the other is cancelled. Both run in their own
read-only transaction, so the `SET LOCAL` statements cannot leak onto other
queries sharing the pooled connection.

Dense wallets finish well inside the delay, so no chaser fires and the only
overhead is the explicit transaction wrapping. Sparse wallets get rescued by the
chaser without killing and restarting a query that may be nearly done.

## Enabling it

It is **off by default** and enabled per deployment — only the deployments
observed to hit the timeout should pay for a second connection and a second
running query.

| Flag | Default | Meaning |
|------|---------|---------|
| `--tx-list-adaptive-fallback` | `false` | Enable the hedge |
| `--tx-list-chaser-delay-ms` | `5000` | How long the original gets before a chaser fires |
| `--tx-list-chaser-timeout-ms` | `40000` | `statement_timeout` for the chaser; the original has none |

Enable it for one deployment by setting `--tx-list-adaptive-fallback=true` on the
`serve` command (or the equivalent environment variable). The delay and timeout
already carry sensible values, so turning it on is a single boolean.

Set the delay above the p99 of the *healthy* case: a delay under normal query
latency makes every request run twice.

## What to watch after enabling

Two counters report the behaviour:

- `store.tx_list_chaser_fired_total` — a chaser was launched, i.e. the original
  exceeded the delay. Should stay near zero on a healthy workload.
- `store.tx_list_chaser_won_total` — the chaser beat the original. A high ratio of
  won/fired means the GIN plan is genuinely the better one for that workload.

If `fired` is high and `won` is low, the delay is too short: the original was
about to return anyway and the second query is pure waste.

## Scope and lifetime

This is a stopgap. The real fix is an index that serves the filter and the `id`
ordering in one scan, so the planner has a cheap ordered path and no race is
needed. Treat the flag as something to turn *off* again once that lands.

Only queries carrying a JSONB predicate are hedged. An unfiltered list, or one
filtered on `id`/`timestamp`/`reference` only, is already served well by the `id`
index and takes the plain path regardless of the flag.
