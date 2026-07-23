# INDEXED_METADATA_KEYS — operator guide

## What it does

By default, metadata filters on transactions use a JSONB containment predicate:

```sql
metadata @> '{"source_wallet_id": "abc"}'
```

Postgres satisfies this with a GIN index, but GIN bitmap scans are incompatible
with `ORDER BY id DESC LIMIT N` — the planner falls back to an index scan
backward on `id`, which walks the entire table for sparse wallets.

When a key is added to `INDEXED_METADATA_KEYS`, Ledger rewrites the predicate to:

```sql
metadata ->> 'source_wallet_id' = 'abc'
```

This form is eligible for a BTree functional index and allows the planner to use
an index-only scan that also covers the `ORDER BY id DESC`, eliminating the
table walk entirely.

**The rewrite only takes effect when a matching functional index is confirmed to
exist.** Ledger checks `pg_indexes` at startup (per ledger). If the index is
absent, the query falls back to the standard `@>` form.

---

## Lifecycle

### 1. Create the index

Create a partial composite functional index on the target ledger's bucket:

```sql
-- Replace <bucket>, <ledger>, and <key> with your values.
CREATE INDEX CONCURRENTLY IF NOT EXISTS transactions_metadata_<key>_<ledger>_idx
ON "<bucket>".transactions ((metadata->>'<key>'), id DESC)
WHERE ledger = '<ledger>';
```

The `CONCURRENTLY` option builds the index without blocking reads or writes.
Wait for `CREATE INDEX` to complete before proceeding.

**Index design notes:**
- The composite `((metadata->>'<key>'), id DESC)` covers both the equality
  filter and the `ORDER BY id DESC` in a single scan, avoiding a sort step.
- The `WHERE ledger = '<ledger>'` partial condition keeps the index small —
  it only covers rows for that ledger.
- For multiple keys, create one index per key.

### 2. Enable the feature flag

Set `INDEXED_METADATA_KEYS` in the ledger's feature configuration at creation
time:

```
POST /v2/{ledger}
Content-Type: application/json

{
  "features": {
    "INDEXED_METADATA_KEYS": "source_wallet_id,destination_wallet_id"
  }
}
```

On startup, Ledger checks `pg_indexes` for each key and silently disables the
rewrite for any key whose functional index is missing (falling back to the
standard `@>` containment predicate). A warning is logged for each missing index.

Feature flags are immutable after ledger creation. To change the key list,
recreate the ledger or update the configuration and restart.

### 3. Deactivation

To deactivate, recreate the ledger without the key in the flag, or simply drop
the index — Ledger's startup check detects the missing index and disables the
rewrite automatically:

```sql
DROP INDEX CONCURRENTLY IF EXISTS "<bucket>".transactions_metadata_<key>_<ledger>_idx;
```

After dropping the index, restart Ledger (or wait for the next store open) so
`ResolveIndexedMetadataKeys` re-checks `pg_indexes` and reverts to the `@>`
containment predicate for that key.

---

## Key naming constraints

Key names in `INDEXED_METADATA_KEYS` must match `[a-zA-Z0-9_]+`. Keys
containing other characters (dots, hyphens, spaces) are rejected at flag-set
time. This constraint exists because the key name is embedded as a literal in
the generated SQL to enable functional-index matching.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Flag set but queries still use `@>` | Index does not exist or has a different expression | Check `pg_indexes` for the exact index expression; restart Ledger to re-run `ResolveIndexedMetadataKeys` |
| Warning logged: "no functional index found for key" | Index was not yet created, or the expression/partial condition does not match | Create the index, then restart Ledger |
| Slow queries after dropping the index | Flag still references the dropped key | Restart Ledger to trigger the startup check, which auto-disables the rewrite |
