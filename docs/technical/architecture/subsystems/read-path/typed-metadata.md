# Typed Metadata

## Contract

`MetadataValue` is a self-describing protobuf `oneof`. Account, transaction,
and ledger metadata can carry strings, signed or unsigned integers, booleans,
datetimes, or `NullValue`. The value persisted in the primary store is the
client-written business value. Changing a metadata field declaration never
rewrites that primary value, and API entity reads return it verbatim.

A per-ledger `MetadataSchema` declares a type for individual keys. A declaration
is an **index and query contract**, not a storage-conversion contract:

- metadata index rows are encoded under the type bound to their local index
  version;
- query conditions are checked against the type bound to the version being
  served;
- schema changes are O(1) in the FSM and any attached index is re-encoded by
  each replica's indexbuilder;
- no field-level conversion state exists.

Account and transaction metadata indexes are supported. Ledger metadata can be
declared and is returned by the schema API, but `indexes.MetadataID` currently
does not expose a ledger-metadata index target.

## Types and representation

| Declared type | `MetadataValue` representation | Index encoding |
|---|---|---|
| `string` | `string_value` | string tag + terminated bytes |
| `int8`, `int16`, `int32`, `int64` | `int_value` (`int64`) | order-preserving signed integer |
| `uint8`, `uint16`, `uint32`, `uint64` | `uint_value` (`uint64`) | big-endian unsigned integer |
| `bool` | `bool_value` | boolean tag + byte |
| `datetime` | `datetime_value` (`int64` microseconds since Unix epoch) | signed integer encoding |
| conversion failure | `null_value` with the original string form | null tag + original bytes |

Sub-width integer declarations use the 64-bit protobuf branch and enforce the
declared range during index coercion. Datetime accepts RFC3339/RFC3339Nano
strings, supports pre-1970 values, truncates sub-microsecond precision through
`UnixMicro`, and renders as UTC RFC3339Nano. Float, byte-array, object, and array
metadata values are intentionally unsupported.

`commonpb.ConvertMetadataValue` implements the deterministic conversion matrix:

- strings parse into the requested numeric, boolean, or datetime type;
- signed and unsigned integers convert across numeric domains when in range;
- integers convert to booleans using zero/non-zero semantics;
- datetimes convert to strings or in-range integer values, but not booleans;
- booleans convert to strings and `0`/`1` integers, but not datetimes;
- `NullValue` retries conversion from its preserved `original` string;
- an impossible or out-of-range conversion returns another `NullValue` rather
  than failing the indexer.

This conversion is used to derive index keys. It does not mutate the source
metadata or change entity-read responses.

## Schema lifecycle

`CreateLedgerRequest.initial_schema` installs declarations when the ledger is
created. Later changes use one-key operations:

- `SetMetadataFieldType` adds or replaces a declaration. The FSM updates
  `LedgerInfo.metadata_schema`, bumps an attached index's
  `forward_encoding_version`, and emits `SetMetadataFieldTypeLog`.
- `RemoveMetadataFieldType` removes a declaration, drops an attached index from
  the registry, and emits `RemovedMetadataFieldTypeLog` with the dropped index
  identity when applicable.

Both operations leave existing account, transaction, and ledger metadata values
untouched. Removing a declaration does not convert values to strings.

The apply path remains deterministic and O(1): it updates cached primary
projections and emits audit evidence, but performs no Pebble scan and starts no
leader-side converter. `Proposal` has no metadata-conversion batch or completion
payloads.

## Index versioning and schema rewrite

The indexbuilder consumes the schema log independently on every replica. The
cluster-wide registry version records that a rewrite is required; the read
store's `IndexVersionState` determines the local keyspace served by queries.

For an already served metadata index, a retype performs this lifecycle:

1. allocate a single-use pending version above the local high-water mark and
   bind it to the new declared type;
2. keep queries on `CurrentVersion` and its old `CurrentType`;
3. scan current reverse-map entries, fetch each raw value from the canonical
   primary store, coerce it to `PendingType`, and write the pending forward,
   existence, and reverse-map rows;
4. dual-write live metadata changes into current and pending versions, using
   each version's own type binding;
5. after the scan and log-alignment gate complete, atomically promote pending
   to current and garbage-collect the old version.

If a retype arrives during an initial index backfill, the builder abandons the
partially populated pending version, allocates a fresh `HighWater+1` version,
and resets the persisted `BackfillKey` cursor to replay from the beginning.
Reusing the partial keyspace would mix encodings at identical event sequences.
The new pending state and the cursor deletion are written into the fold batch
that ingested the schema log, so a single commit makes both durable together;
a failure to stage either one aborts that batch instead of committing half the
reset. After restart, `CurrentVersion == 0` keeps this state owned exclusively
by the historical-log backfill; only a state with non-zero current and pending
versions is resumed as a reverse-map schema rewrite. Nothing else would fill
the prefix below a surviving cursor, which is why the reset cannot be a
separate direct write.

`IndexVersionState.RewriteProgress` remains an encoded opaque tail but is not
currently mutated by the indexbuilder. Both initial-backfill and schema-rewrite
cursors live under `BackfillKey`.

See [indexer.md](../indexer/indexer.md) for the full rewrite, dual-write,
known-absent insert, switch, and recovery mechanics.

## Write and read semantics

### Entity writes and reads

The schema does not coerce metadata at admission or FSM apply. gRPC values keep
their selected `MetadataValue` branch; the HTTP layer maps supported JSON
scalars to a branch. JSON `null` deletes a metadata key, and floats, objects,
and arrays are rejected.

Account, transaction, and ledger reads return the primary values verbatim. A
field declared `int64` can therefore still be returned as `string_value` if the
client stored a string. `NullValue` normally appears in derived index inspection
when coercion failed; it is not written back into the entity.

Numscript remains string-oriented. Its metadata operations write strings and
read string representations at the language boundary; declarations still do
not rewrite the stored values.

### Query compilation

Metadata filters require both a declared schema field and a registered, locally
ready index. The compiler resolves the local `CurrentVersion` and validates the
condition against that version's bound type, not blindly against the newest
schema declaration. During a rewrite, old-type queries therefore continue to
scan the complete old keyspace. The new type becomes visible atomically with
the version switch.

Signed conditions are accepted for signed and datetime fields. A non-negative
signed condition can be coerced to the unsigned condition form for an unsigned
field. String and boolean conditions require matching bound types. Existence
conditions are valid for every declared type.

### Index inspection

`InspectIndex` pins a read-store snapshot, rejects `CurrentVersion == 0`, and
scans only the locally served version. Distinct values, facets, and summary
statistics therefore describe one consistent encoding. The HTTP adapter uses
the declared type as a rendering hint so signed datetime index values are shown
as RFC3339 strings.

### Progress and barriers

Indexed reads automatically wait for the projection's Raft certificate at the
fixed main-snapshot horizon. That certificate does not wait for a schema rewrite
to switch versions. `PendingVersion != 0` is the local rewrite signal exposed
through index status; there is no field-level conversion status or
schema-rewrite barrier.

## API surface

| Layer | Operation | Current response/behavior |
|---|---|---|
| gRPC | `GetMetadataSchemaStatus` | Maps account, transaction, and ledger keys to `declared_type` only. The historical method name remains, but no status field exists. |
| gRPC | `SetMetadataFieldType` / `RemoveMetadataFieldType` | Change one declaration without rewriting primary values. |
| HTTP | `GET /v3/{ledgerName}/metadata-schema` | Returns camelCase `declaredType` entries. |
| HTTP | `PUT` / `DELETE /v3/{ledgerName}/metadata-schema/{targetType}/{key}` | Set or remove one declaration. |
| CLI | `ledgerctl ledgers get-schema` | Displays key and declared type columns only. |
| CLI | `ledgerctl ledgers set-metadata-type` / `remove-metadata-type` | Manage declarations. |

## Key files

| File | Responsibility |
|---|---|
| `misc/proto/common.proto` | Typed values, declared types, and schema maps. |
| `misc/proto/bucket.proto` | Schema RPC requests and declared-type response. |
| `misc/proto/raft_cmd.proto` | Set/remove schema orders. |
| `internal/domain/processing/processor_metadata_schema.go` | O(1) schema and index-registry updates. |
| `internal/proto/commonpb/metadata_convert.go` | Deterministic index coercion matrix. |
| `internal/application/indexbuilder/schema_resolver.go` | Resolves raw values and coercion for a bound version. |
| `internal/application/indexbuilder/backfill.go` | Pending-version schema rewrite and atomic switch. |
| `internal/query/compile.go` | Bound-type condition validation and encoding. |
| `internal/application/ctrl/controller_default.go` | Declared-type response and inspect readiness gate. |
