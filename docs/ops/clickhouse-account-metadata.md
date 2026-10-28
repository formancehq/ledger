# Preserve existing ClickHouse account metadata

The event sink now creates `data.accountMetadata` as
`Map(String, Map(String, String))`, matching account address → metadata key →
string value. Existing tables retain their old `Map(String, String)` hint:
`CREATE TABLE IF NOT EXISTS` does not change their schema. Sink startup does
not alter customer-owned tables.

Use an explicit migration before resuming delivery to an old table. This
procedure copies existing rows into a new table and retains the source. It
changes only the external analytical projection, not Ledger storage, audit,
Raft entries, protocol revision, or the sink cursor. Complete the normal
[rolling server upgrade](upgrades.md) separately; old and new binaries emit the
same nested event payload and can publish to the corrected table.

## Prepare and stop delivery

1. Record the ClickHouse server version, `SHOW CREATE TABLE ledger_events`,
   database engine, configured sink name/table, and sink cursor. Replace the
   example identifiers below with the actual database-qualified identifiers.
   Take and verify a backup of the existing table.
2. Quiesce every writer to this ClickHouse table. An operator can temporarily
   deny the sink account `INSERT` permission using a separate administrative
   account, and let existing inserts finish. Keep the sink definition, name,
   and cursor. Failed deliveries retain the cursor and retry later; removing
   and re-adding a sink is unnecessary. Ensure event retention covers the
   migration interval. Pause other consumers that require an uninterrupted
   table identity during cutover.
3. Confirm there are no in-flight writes before copying. Ledger admission
   maintenance alone does not stop an event emitter from draining its backlog.
   Keep writers quiesced through verification and cutover.

A migration requires enough free disk for both table copies and both local
export files. Preserve custom table
settings, indexes, partitioning, TTLs, grants, and dependent views explicitly;
the example below covers the sink's default schema. Stop for a deployment-specific
migration design when the existing table differs materially.

## Create an empty destination

Enable the same settings used by the sink:

```sql
SET allow_experimental_json_type = 1;
SET allow_experimental_variant_type = 1;
SET output_format_json_quote_64bit_integers = 0;
SET date_time_input_format = 'best_effort';
```

Create a new table using the target binary's `ClickHouseCreateTableDDL`, with a
fresh destination name such as `ledger_events_nested`. For the default schema,
use the existing `SHOW CREATE TABLE` output, change the table name and change
only `accountMetadata Map(String, String)` to
`accountMetadata Map(String, Map(String, String))`. Execute that new `CREATE`
statement with a fresh table UUID if the output includes an explicit `UUID`;
never reuse the source UUID. Do not execute an `ALTER` against the source.
Verify the destination is empty and its type with `SHOW CREATE TABLE ledger_events_nested`.

The JSON type was introduced in ClickHouse 24.8 and declared production ready
in 25.3. The integration fixture uses the floating `24-alpine` image, so record
its actual `SELECT version()` result when validating a release. Rehearse this
procedure on a copy with the same ClickHouse version and settings; current JSON
`ALTER` capabilities are not evidence of support on older 24.x versions.
See the official [JSON documentation](https://clickhouse.com/docs/reference/data-types/newjson)
and [24.8 examples](https://presentations.clickhouse.com/2024-release-24.8/).

## Validate and copy

Run [the migration statements](clickhouse-account-metadata.sql) in order,
**stopping on the first error**, in the prepared session. Replace both table
identifiers first. Run every preflight before the copy; do not invoke the
export independently. Keep the source unchanged for the entire operation.
Save the final `SELECT` result as `legacy.jsonl` using `clickhouse-client`.
The export includes `data` as a JSON string, not a parsed JSON object.

Rows with missing or empty account metadata need no reconstruction. For
non-empty legacy maps, the statements accept only values containing valid JSON
objects with unique keys and string values. They decode those stringified
objects into nested maps. Malformed JSON, arrays, scalar values, non-string
members, and duplicate keys stop the procedure before copying. If legacy
coercion already lost information, this procedure cannot recover it: preserve
the source and stop for a separate recovery plan. Do not substitute empty maps
for unrepresentable values.

From the repository root, use the pinned Go environment to convert the export:

```bash
nix develop --command go run ./scripts/clickhouse-account-metadata < legacy.jsonl > nested.jsonl
```

Continue only if the command exits successfully. Retain both files for
verification; a failed conversion can leave partial output, which must never
be imported. The converter validates legacy objects and unique string-valued
keys again, decodes `accountMetadata`, and retains other JSON tokens without
floating-point conversion. It uses memory proportional to one exported row.

ClickHouse 24.x's generic JSON parser cannot preserve arbitrary `UInt256`
numbers through whole-document `JSONExtractKeysAndValuesRaw`, even though the
function is named "Raw". Do not use it on the complete event. The converter
quotes only `transaction.postings[].amount` and
`revertTransaction.postings[].amount` for transport: the destination's explicit
`UInt256` hints restore their numeric types and exact values on import. Other
numeric tokens retain their original representation, including unknown fields.
This also avoids the nested JSON parser's unquoted-large-integer limitation
when constructing source fixtures or importing into ClickHouse 24.x.
See the [24.x JSON function implementation](https://github.com/ClickHouse/ClickHouse/blob/v24.12.6.70-stable/src/Functions/FunctionsJSON.cpp)
for the generic parser and reserialization used by raw extraction.

After successful conversion, recheck that the destination is empty and import
with the same session settings used above:

```bash
clickhouse-client --allow_experimental_json_type=1 --allow_experimental_variant_type=1 \
  --date_time_input_format=best_effort \
  --query 'INSERT INTO ledger_events_nested FORMAT JSONEachRow' < nested.jsonl
```

Use the actual database-qualified destination identifier and your existing
ClickHouse connection configuration. `clickhouse-client` is the operational
client for the deployed server; Go is provided by the pinned development shell.

Preflight and conversion errors leave the source and empty destination unchanged.
A failed copy can leave a partial destination; the source remains intact. Do
not retry into that destination: retain it for diagnosis and prepare a fresh
empty destination before rerunning all preflights. No step deletes the source.

## Verify and cut over

Compare the logical `(ledger, log_sequence)` sets and every outer event field
using `FINAL` on both tables, because `ReplacingMergeTree` can merge physical
duplicate rows at any time. Compare JSON fields other than `accountMetadata`
with their original values, and verify nested metadata for every converted
account. Check representative `UInt256` amounts and microsecond timestamps.
For example:

```sql
SELECT log_sequence, data.accountMetadata['users:001']['name']
FROM ledger_events_nested FINAL;
```

Do not cut over if any row, field, or metadata value differs unexpectedly.
Retain the old table and its backup. In an `Atomic` database, atomically exchange
the two table names after verification:

```sql
EXCHANGE TABLES ledger_events AND ledger_events_nested;
```

The original table now remains at `ledger_events_nested`; label it clearly as
the retained legacy copy in the operational record. ClickHouse documents
[`EXCHANGE`](https://clickhouse.com/docs/reference/statements/exchange) for
`Atomic` and `Shared` database engines. A multi-table `RENAME` is not atomic;
other database engines require a separately validated cutover procedure.
Review dependent views and grants before exchanging names.

Restore the writer permissions and resume delivery to the original configured
table name. Verify a new event's nested metadata and normal cursor advancement.
Before resuming writers, exchanging the names back restores the original table.
After delivery resumes, the legacy copy lacks new events: do not exchange back
without preserving and reconciling those new rows. Do not downgrade the nested
schema merely to roll back the Ledger binary; its published event shape is
unchanged.
