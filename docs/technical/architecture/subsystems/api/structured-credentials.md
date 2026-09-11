# Structured credentials and public audit views

## Requirement and decision

EN-1632, EN-1634 and EN-1635 require configuration reads to retain useful
connection information without disclosing sink or mirror credentials. The
alternative to PR #1963 moves parsing from each read-time redactor to the
construction of operational configurations. Public audit reads expose typed
business details, not opaque binary proof envelopes that can contain secrets.

Input APIs continue accepting connection URLs and DSNs. Their protobuf input
messages are distinct from operational configuration messages. The latter store
independent connection components, credentials and options. There is no original
DSN fallback and no flag recording which syntax the client used. Response fields
are structured; only driver adapters serialize connection strings for libraries
that require them. HTTP, NATS and proxy URL fragments are rejected with a static
diagnostic because
the structured model cannot preserve them; silently dropping a fragment can
change paths composed by a mirror adapter. Escaped `#` characters in paths or
query values remain supported.

A pure normalizer in `internal/domain/connectionconfig` derives the operational
configuration from accepted input. It performs no environment, file, network,
clock or storage access. Applying the same audited input therefore gives the
same normalized projection on every node. Driver initialization is a separate
runtime boundary: in particular pgx still performs its existing environment and
external-file resolution when opening connections. Normalization does not claim
to freeze those external runtime resources.

IAM mirrors accept URI and libpq keyword DSNs after parsing explicit TLS options;
quoted values cannot substitute for an `sslmode` setting. The runtime driver
configuration is checked again before connecting. Admission distinguishes a
malformed IAM connection from a parsed connection with insufficient TLS, using
static diagnostics that do not echo credentials.

## Extending sink normalization

Each sink's normalizer lives in its own file in
`internal/domain/connectionconfig`. A typed registration helper associates its
input and operational protobuf payloads with a registry entry keyed by the input
oneof field number. The shared `Sink` function copies common fields and dispatches
through that registry; adding a sink does not require extending a central type
switch. Registration happens only during package initialization. Duplicate
registrations and incompatible protobuf types are programming errors and fail
loudly.

All normalizers are compiled into every build, without importing driver
libraries or depending on sink build tags. Admission, deterministic apply and
public audit projection must derive the same operational configuration even
when a node cannot run that sink. The separate runtime factory registry in
`internal/application/events` may use build-tagged driver adapters; it controls
which connections the process can open, not how audited input is normalized.

To add a sink, define its input and operational protobuf payloads, annotate
credential fields on the operational or shared payloads, and add its normalizer
and initialization-time registration.
Extend tests to cover every input oneof arm, reject missing or incompatible
registrations, and verify the new sink's parameter semantics and input
immutability. Run normalization tests without optional driver tags as well as
the relevant driver checks. A runtime factory alone does not supply a normalizer.

## Persistence and evidence

Processors normalize into fresh objects before mutating their working state.
The sink-added and ledger-created logs carry operational configuration; the
existing write set persists the corresponding primary-store projections. The
accepted order is not rewritten, including through aliases. Original serialized
business orders and signed request bytes remain in the internal audit chain.
Backups retain that original evidence. A structured public display is never
written back or substituted into a signed payload.

Configuration rows are primary-store projections of successful audited orders.
The audit-first checker work in PR #1912 must derive their expected values with
the same pure normalizer and compare both emitted logs and final configurations,
folding sink removal and ledger promotion/deletion as well as creation. This
change does not claim that the existing log-driven checker already provides that
full coverage. The checker pivot remains owned by #1912.

Incremental restore preserves checkpoint configurations and rebuilds changes
from exported operational logs. Sink removal must remove the restored
configuration; mirror promotion clears the source. See the
[incremental restore contract](../backup/incremental-restore-contract.md).

## Public projection

`common.sensitive` is a protobuf field option. `internal/pkg/sensitive.Clone`
builds a detached copy, replaces nonempty annotated strings with `[redacted]`,
and clears annotated non-string fields. It recursively visits messages, oneofs,
repeated messages and message-valued maps. Empty secrets stay empty. Unknown
wire fields are discarded from the public copy because their confidentiality
cannot be established from the schema. Original values remain unchanged.

Sensitivity is declared on credential fields and mirror/audit diagnostics that may
embed credentials. Sink adapters sanitize their errors before persistence;
`SinkError.message` remains visible so clients retain useful failure diagnostics.
Types used exclusively as inputs carry no sensitivity annotations:
public reads normalize them into operational messages before masking. Shared
input/output types, such as Kafka and Databricks configurations, retain their
annotations because they are also exposed in public views. URL query values and
unrecognized database option values are
sensitive by default. Known operational options remain visible, including
node-local file paths (`passfile`, `sslkey`, `sslcert`, `sslrootcert`); the
normalizer never reads or exposes the files themselves. Passwords and private-key
passwords remain sensitive. Nested proxy
credentials follow the same annotations. Callers cannot choose their own
sensitivity classification through the input schema.

Ledger, sink and log reads apply the generic projector after snapshot selection.
Pagination keys and transport metadata remain unchanged. Read-side log copies
omit the entire server-signature envelope; returning a signature alongside an
altered payload would misrepresent what it authenticates. Write-response signing
and stored log evidence remain separate from this read projection.

`publicauditpb.AuditEntry` is a separate public contract. It contains typed orders,
outcome, identity, ordering and caller information. It has no `serializedOrder`,
raw signed batch payload or technical execution metadata. Signature information
identifies the original key only; it does not assert that this display was
verified or signed. The hash identifies the original evidence and cannot be
recomputed from the redacted response.

A rejected order can contain a configuration that cannot be normalized. Its
public item retains the other business fields, omits that configuration and sets
`configurationUnavailable`. It never falls back to the raw URL, DSN or parser
error. A missing audit entry or missing/nil outcome is an integrity error and
fails the read; it is never projected as a successful but incomplete response.
Internal evidence remains available to the server's integrity machinery.

## Validation requirements

- Every supported input format retains effective connection parameters,
  precedence, escaping and ordering where operationally significant.
- Normalization is deterministic and leaves accepted input and signed bytes
  unchanged; live logs and stored configs contain structured values.
- Binary gRPC, HTTP and CLI structured output mask annotated secrets, including
  nested options, mirror/audit diagnostics and checkpoint reads. Sanitized sink
  diagnostics remain visible.
- Public audit schemas have no path to raw input configurations or signed bytes;
  malformed rejected configuration remains inspectable without leaking it.
- A non-empty post-checkpoint delta proves logical live/restore parity, including
  sink removal, and passes the current checker without claiming its future scope.

NATS server entries with a scheme but no host (for example `nats://`) are
rejected with a static configuration error. An empty server list continues to
select the driver default; an explicit malformed entry is not rewritten into
a different destination.
