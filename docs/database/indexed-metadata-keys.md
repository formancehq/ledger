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
exist.** Ledger checks `pg_index` (via `pg_get_expr`) at store-open time (per
ledger, per request). If the index is absent, the query falls back to the
standard `@>` form.

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

Features are set at ledger creation time. To create a ledger with indexed
metadata keys:

```http
POST /v2/ledgers/<ledger>
Content-Type: application/json

{"features": {"INDEXED_METADATA_KEYS": "source_wallet_id,destination_wallet_id"}}
```

Key names are validated against `[a-zA-Z0-9_]+` at creation time.

For existing ledgers, update the `features` column directly:

```sql
UPDATE _system.ledgers
SET features = jsonb_set(features, '{INDEXED_METADATA_KEYS}', '"source_wallet_id,destination_wallet_id"')
WHERE name = '<ledger>';
```

Index existence is verified at store-open time (the start of each request): if
no matching functional index is found for a given key, that key falls back to
the `@>` containment form and an INFO message is logged.

After the change, new requests will use the functional index for confirmed keys.

### 3. Deactivation

To deactivate, clear the flag **before** dropping the index:

```sql
UPDATE _system.ledgers
SET features = jsonb_set(features, '{INDEXED_METADATA_KEYS}', '""')
WHERE name = '<ledger>';
```

Once the flag is cleared, Ledger reverts to `@>` for all metadata filters and
the index can be safely dropped.

```sql
DROP INDEX CONCURRENTLY IF EXISTS "<bucket>".transactions_metadata_<key>_<ledger>_idx;
```

Dropping the index before clearing the flag is safe (Ledger's startup check
detects the missing index and disables the rewrite automatically), but clearing
the flag first avoids any window where the rewrite is attempted without an
index.

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
| Flag accepted but queries still use `@>` | Index does not exist or expression does not match | Check `pg_index` via `pg_get_expr` for the exact expression; INFO log at startup shows unconfirmed keys |
| Key listed in flag but not confirmed (INFO at startup) | Index not yet created, or expression / partial condition differs | Create the index (`CONCURRENTLY`), then wait for the next request open to re-confirm |
| Slow queries after dropping the index | Flag still references the dropped key | Clear the flag so the next store-open falls back to `@>` |
