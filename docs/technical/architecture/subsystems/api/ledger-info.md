# LedgerInfo read contract (EN-2780 / EN-1635)

Clients need to explore ledgers through generated SDKs before the v3 RC without
losing configuration, metadata or synchronization progress. Read-scoped callers
must not recover reusable mirror credentials. The HTTP contract therefore uses
an explicit projection, and both external adapters use a detached sensitive
projection before returning current or checkpoint ledger values.

## HTTP representation

`GET /v3/` and `GET /v3/{ledgerName}` retain their existing `data` envelopes.
LedgerInfo uses `createdAt`, `deletedAt`, `metadataSchema`, `mirrorSource`,
`mirrorSyncProgress`, `accountTypes`, `defaultEnforcementMode` and `metadata`.
Optional fields are omitted when absent. Creation/deletion/error timestamps use
RFC3339. Metadata reuses the typed metadata value contract. Schema maps use
`accountFields`, `transactionFields`, `ledgerFields` and short `type` tokens,
including the default `string` declaration.

Mode is always `NORMAL` or `MIRROR`. Default enforcement is always `STRICT` or
`AUDIT`. Account types use `NORMAL`, `EPHEMERAL` or `TRANSIENT` persistence and
the same REST segment discriminators as account-type reads: `regex`, `uuid`,
`uint64`, `bytes`; null denotes an unconstrained non-empty segment.

Mirror progress always contains `state` (`SYNCING` or `FOLLOWING`), `cursor`,
`sourceLogCount` and `remainingLogs`, including zero values. Each counter is a
canonical unsigned decimal JSON string, described by OpenAPI as `string` with
`format: bigint`. This preserves all uint64 values while generated TypeScript
clients expose native bigint values. The numeric alternative (`integer/bigint`)
generated a TypeScript number and silently rounded 2^53+1 and uint64 maximum in
an operation-level Speakeasy experiment. Ordinary JSON.parse consumers of the
string representation also remain exact. Clients using JSON.stringify on a
decoded native bigint must explicitly convert it to a decimal string first.

Read configuration uses `ledgerName` and exactly one nested `http` or `postgres`
source, with optional `batchSize` and `rewriteRules`. HTTP sources include nested
`oauth2ClientCredentials`. PostgreSQL sources retain `dsn` and supported
`awsIamAuth` fields (`region`, `assumeRoleArn`). `MirrorSourceRead` is a separate
schema from the flat `MirrorSourceConfig` accepted at ledger creation: correcting
read responses does not change creation input or add HTTP creation features.

Marshal failures propagate. Get buffers with `writeOKChecked`; list buffers with `writePageOK` before
writing success headers so invalid values return a sanitized HTTP 500 instead
of a successful, incomplete representation.

## Sensitive projection and ownership

`internal/pkg/sensitive` reuses the annotation/deep-clone projection contract
introduced by EN-1632 / PR #1977. EN-1635 integrates it at HTTP get/list and gRPC
GetLedger/ListLedgers boundaries. gRPC projection runs after current/checkpoint
selection and pagination, and preserves stream headers/trailers and errors.

HTTP source and OAuth token-endpoint URL userinfo/query credentials are masked
while host/path and non-secret settings are preserved. OAuth client secrets
become `[redacted]` presence markers. PostgreSQL passwords
in URL userinfo/query settings or keyword/value DSNs become `xxxxx`. Non-secret
connection metadata remains visible; malformed connection strings are fully
redacted. Empty secrets remain empty/omitted. Detached protobuf views discard
unknown wire fields, whose confidentiality cannot be established by the schema.
The original object, nested maps/slices and stored bytes remain unchanged.

Do not apply this projection to controller/worker configuration, persistence,
accepted orders, signed bytes, or audit-chain material. This change does not
close EN-1634 (historical audit/log credentials) or EN-1632 (sink read boundaries).
Service protocol revision 26 identifies the changed external gRPC read semantics.

## Snapshot and cost boundary

Get and list mirror progress use the same main-store read handle as LedgerInfo
and metadata. Listing adds three point reads per consumed mirror; normal ledgers
do not take that path. The cursor owns its snapshot through Close and propagates
mirror read errors. Existing ledger/metadata scanning is unchanged. No new
persisted projection, WAL write, FSM capability or Raft work is introduced.
External cloning/redaction is linear in each returned configuration's size.
Progress enrichment precedes transport pagination. HTTP consumes the full list;
gRPC may consume skipped rows, reverse traversal and a lookahead row. Each
consumed mirror adds three point reads, including rows outside the returned page.


## v2 comparison and validation

v2 has no mirror-progress counterpart. Its stats/log-ID JSON uses numbers with
`integer/int64` or `integer/bigint` schemas; the Go SDK maps bigint to big.Int.
That schema alone does not guarantee lossless TypeScript decoding. The v3
mirror-specific counter strings deliberately differ for precision. Existing
REST camelCase properties, timestamps, metadata and envelopes are preserved.

Codec regressions cover defaults, timestamps, typed metadata/schema, segment
constraints, zero/large counters and invalid values. Routed HTTP tests check
get/list, credential canaries, non-mutation, OpenAPI conformance and clean marshal
failures. gRPC tests exercise actual current/checkpoint controller reads and
streaming list output, and verify that unprojected internal queries retain the
credentials. Shared projection tests cover URL/keyword DSNs and detached slices.
Temporary generated SDK list/get calls verify operation-level decoding from
these HTTP representations. Downstream platform-ui regeneration/delivery is
separately owned and is not changed by this Ledger PR.
