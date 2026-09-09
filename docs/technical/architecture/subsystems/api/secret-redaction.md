# Credentials in read responses

## Requirement and boundary (EN-1632, EN-1634, EN-1635)

Read scopes must not grant the reusable credentials used to connect to event
sinks or mirror sources. The original configuration also appears in historical
creation logs and accepted orders, including the opaque bytes of signed batches.
Redacting only live configuration would leave those same credentials readable
through history after rotation or removal. EN-1796 duplicates the gRPC part of
EN-1632; the earlier HTTP DTO proposal was closed without being merged.

`internal/adapter/readprojection` supplies one policy for HTTP and gRPC. The
adapters project deep clones immediately before serialization or streaming:

- `GetEventsSinks`: configurations and sink status diagnostics;
- `GetLedger` / `ListLedgers`: mirror configuration and mirror diagnostics;
- `GetLog` / `ListLogs`: sink and mirror creation payloads;
- `GetAuditEntry` / `ListAuditEntries`: serialized orders and signed batch
  payloads (the list includes the signed payload even when items are absent).

The projection runs after controller/checkpoint selection and on every serving
node, including a follower forwarding a response. It is idempotent. gRPC list
projection occurs at `Send`, preserving the source cursor and its continuation
trailer. Sequences, pagination, caller identity, error presence/timestamps, and
non-secret configuration remain usable.

Controllers, query/store readers, and internal workers retain the original
values. There is no mutation of accepted orders, FSM behavior, persisted bytes,
idempotency hashes, audit capture, or checker inputs. Backup/checkpoint and
incremental restore still preserve the same original credentials; this change
creates no persisted effect to restore. EN-1945 removed archived/cold history;
all current history resides permanently in the main store.

## Field policy

Nonempty opaque credentials become `[redacted]`; empty credentials remain empty.
That covers Kafka SASL passwords, HTTP webhook HMAC keys, Databricks PAT/OAuth
client secrets, and HTTP mirror OAuth client secrets. Names, auth modes, topics,
batch settings, OAuth client IDs/scopes, and mirror state remain visible.

Credential-bearing URLs are display values, not connection strings to reuse:

- NATS server URLs mask token/password userinfo, including server lists.
- HTTP sink endpoints and mirror base/token URLs mask userinfo and query
  values. Query authentication names are arbitrary, so preserving them by a
  password-key heuristic cannot guarantee confidentiality.
- ClickHouse DSNs mask password material and credentials in nested proxy URLs.
- PostgreSQL URI and keyword DSNs mask password material while preserving
  non-secret host/database/connection settings.
- Malformed credential containers fail closed to an opaque redaction marker.

Sink and mirror transport errors can repeat credentials from request URLs or
driver responses, including values no longer present in the live configuration.
Their public status message becomes `[redacted diagnostic]`, retaining the
presence of the error and its timestamp. These read APIs are for status, not
raw connection diagnostics. Protected operator diagnostics and persisted
technical state retain their existing behavior. Business `AuditFailure` reason,
message, and context retain their existing authoritative semantics (EN-1623).

This is a field-scoped credential policy, not a scrubber for caller-defined
ledger metadata or business payloads. Credentials must not be stored in those
fields, URL paths, identifiers, or other non-credential configuration metadata.
Write responses may echo the credentials the writer submitted; the read policy
does not change write admission or response-signing behavior.

## Audit and signature verification

The stored audit chain is the authoritative evidence. The returned `hash`
still identifies that original record; it is not a hash of the display
projection. If an order or signed payload contains a redacted credential, a
client **cannot recompute the original hash from the read response**.

For a changed `SignedApplyBatch.payload`, retain the signing key ID and redacted
payload, but omit the cryptographic signature: it does not authenticate the
replacement bytes. The same rule applies if a read contains a `SignedLog`
envelope. Malformed encoded containers fail the read rather than leaking
unchecked bytes. Before decoding, a schema-aware wire guard rejects duplicate
singular/oneof occurrences that shadow credential material. Ordinary protobuf
decoding alone can discard an earlier secret field while retaining it in the
original signed bytes. Duplicate non-secret fields and field ordering retain
their original encoding. A nested `SignedLog` envelope is an invariant failure because
the response signer excludes its own envelope from the signed preimage.

When no field is redacted, preserve the exact original serialized order,
payload, and signature bytes. Do not normalize a signed payload merely because
it was read. The checker continues to verify original store records. Consumers
needing original evidence must use the protected original store/backup workflow;
these read endpoints do not offer a scope-dependent raw bypass.

This changes public response semantics and increments service protocol revision
5 to 6. Deploy matching service clients and servers.

## Validation

Deterministic tests exercise all sink/mirror variants, URI/query/keyword
credentials, duplicate projection, original-object preservation, both HTTP and
gRPC read routes, and pagination. Audit tests decode the binary containers
(HTTP order bytes are hex, signed payloads are base64), prove signature
preservation for unchanged data and invalidation for changed data, and reject
malformed payloads. Current-HEAD qualification also reproduces an HTTP transport
failure that copies an endpoint credential into the sink status message.
