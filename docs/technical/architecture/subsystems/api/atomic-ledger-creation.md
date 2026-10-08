# Atomic ledger creation metadata (EN-2686)

## Requirement and API contract

Ledger v2 accepts ledger metadata during creation. V3 previously advertised the
same field but discarded it in HTTP decoding. Creation must acknowledge both
the new ledger and its initial metadata as one business operation; invalid
metadata must not leave an observable ledger or partially stored values.

`POST /v3/{ledgerName}` accepts optional `metadata` alongside the existing
creation fields. The service `CreateLedgerRequest.metadata` carries the same
typed values. The Go `actions.CreateLedgerAction` helper also forwards its
existing string metadata argument. `ledger:LedgerWrite` authorizes the complete creation operation,
including initial metadata, schema and account types. Subsequent metadata saves
still require `ledger:MetadataWrite`. HTTP, gRPC and unsigned bulk use the same
creation scope decision.

Normal and mirror ledgers both accept initial ledger metadata. This is a
provisioning operation; it does not change restrictions on subsequent writes or
turn mirror ingestion into a source of ledger metadata.

The shared HTTP metadata converter preserves exact integer values, infers
supported scalar types, rejects fractional numbers, objects and arrays, and
omits JSON null entries. Omitted, empty and null metadata produce no metadata
writes. Explicit protobuf `NullValue` follows the existing typed-value contract.
Initial schema declarations remain index/query hints: creation stores supported
values verbatim even when their supplied type differs from the declared type.

The response remains one created ledger in the existing response envelope with
HTTP 201, now including its initial metadata. A subsequent ledger read enriches
LedgerInfo from the canonical metadata keyspace. Metadata is not duplicated in
the persisted LedgerInfo row.

## Execution, audit, and restore

Admission forwards the map into a single `CreateLedgerOrder`. The shared
metadata walker validates storage-safe keys/value shapes and applies the
replicated per-entity and per-proposal limits. Creation rechecks the current
committed size policy before allocating the ledger ID or staging writes. It
then stages the ledger row, boundaries, and ledger metadata in the same proposal
write set. Rejection discards staged business effects; a committed FSM rejection
still has its failure audit record. The existing merge/commit boundary makes
successful effects durable together. A fatal merge/commit failure retains the
existing applier failure semantics rather than promising post-commit rollback.

The order-owned map is never modified. One `CreatedLedgerLog` contains the
initial metadata, so audit replay reconstructs the same creation output and
idempotent retries retain one frozen outcome. A second SaveLedgerMetadata order
would change authorization and log cardinality and is unnecessary.

Initial ledger metadata is **rebuilt** during incremental restore: creation logs
after the checkpoint assign their values into the ledger metadata keyspace.
Later save/delete logs and ledger deletion retain their existing ordered folds.
The checker independently folds successful chain-verified creation, save,
metadata deletion and ledger deletion orders. It compares stored values and
presence in both directions, so a mutable log/projection cannot justify itself.
An already reported broken audit chain suppresses comparison against its
incomplete prefix.

Protocol revision 20 adds creation request and log semantics. A revision-19
server would ignore the field, so communicating gRPC clients and servers must
be rebuilt with the matching revision.

## Cost and evidence

Creation validation costs O(n log n) for sorted key validation and O(n) measured
byte accounting, where n is the submitted metadata entry count. Replicated
policy bounds entries and bytes. Storage/cache writes and audit/WAL bytes grow
with the accepted map. Empty creation adds no metadata preload or scan. Creation
still preloads only its ledger row; metadata writes use the scoped accessor and
need no old-value reads. No lifecycle lock or extra network round trip is added.

Offline checking adds one scan of ledger metadata and keeps current expected
values plus per-ledger map overhead. Go maps can retain their historical
high-water capacity after key deletions; memory is bounded by the folded
history's peak maps rather than only the final key count. Per-ledger expectation
maps allow ledger deletion to discard that ledger's expectation without scanning
unrelated ledgers. This cost is in the checker, not admission or FSM apply.

Evidence includes the HTTP loss regression, real HTTP-to-storage creation and
rejection cases, precise-number and unsupported-value cases, shared shape/limit and proposal-budget tests, constant creation preload
cost, idempotent audit replay, and normal/mirror checkpoint-plus-nonempty-delta
restore compared with live reads. The restore fixture requires healthy checker
results and then verifies detection of a tampered restored metadata value.
The independent oracle folds initial metadata without schema coercion, and the
model workload generates populated normal/mirror creation maps and verifies
the returned creation log. Disabling the creation rebuild fold makes the restore
regression fail. Checker
lifecycle tests cover subsequent saves, key and ledger deletion, missing/injected
values and truncated-chain suppression.
