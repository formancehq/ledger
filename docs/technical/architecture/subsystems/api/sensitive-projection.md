# Sensitive protobuf projection

Public configuration reads must remove credentials while preserving supported
connection hosts, paths and harmless options. `internal/pkg/sensitive.Redact`
builds a detached public view before wire encoding. It is used by HTTP ledger
get/list and sink reads, service gRPC ledger get/list (including checkpoints)
and sink reads, and the CLI HTTP sink configuration renderer.

## Descriptor traversal and ownership

`common.sensitive` replaces non-empty annotated strings with `[redacted]` and
clears annotated fields of other kinds. `common.sensitive_url` selects a
connection-format strategy for scalar strings and clears other field kinds.
Both annotations act only on a deep clone. Traversal visits nested messages,
oneofs, repeated messages and message-valued maps, and drops unknown wire fields
whose confidentiality cannot be established from the schema. Empty strings stay
empty. Public response-signature envelopes retain their existing schema policy.

The source remains unchanged, including authoritative signed bytes and worker or
sink configuration. Never use the projector to persist or sign records: a public
projection is not audit evidence. Annotations do not change serialization by
themselves; callers must explicitly project at a read boundary. Diagnostic
sanitization and CLI connector-specific formatting retain their own contracts.

## Explicit connection formats

The private static field registry in `internal/pkg/sensitive/strategies.go`
selects these policies; there is no runtime registration or plugin loading:

| Protobuf field | Format |
| --- | --- |
| `common.PostgresMirrorSourceConfig.dsn` | PostgreSQL URL or keyword DSN |
| `common.NatsSinkConfig.url` | NATS/TLS/WebSocket server URL list |
| `common.ClickHouseSinkConfig.dsn` | ClickHouse native or HTTP(S) DSN |
| `common.HttpSinkConfig.endpoint` | HTTP(S) |
| `common.HttpMirrorSourceConfig.base_url` | HTTP(S) |
| `common.OAuth2ClientCredentials.token_endpoint` | HTTP(S) |

A newly annotated field without a registered format is fully masked. Descriptor
inventory tests require each declaration to have a representative safe output.
This explicit boundary prevents PostgreSQL syntax from being inferred for every
annotated URL and keeps connector policy outside generic protobuf traversal.
A protobuf format option is unnecessary for the current six fields.

PostgreSQL alone uses the lexical keyword scanner, preserving harmless quoting
and escaping while masking every occurrence of a credential key, including
`password` and `sslpassword`. URL form supports empty-host/socket connections.
No driver parser loads ambient credentials, service files or TLS keys.
NATS splits server lists before parsing and masks passwordless token userinfo
on every server. ClickHouse keeps comma-separated hosts in one authority.
HTTP(S), including ClickHouse HTTP transport and proxy URLs, masks passwordless
userinfo tokens. Password-bearing userinfo retains its username and replaces
the password with `xxxxx`; native ClickHouse and PostgreSQL retain non-secret
passwordless usernames.

Shared URL mechanics mask recognized credential query keys case-insensitively
with dash normalization, including duplicate values. Known `http_proxy` and
`https_proxy` values are projected as HTTP(S) URLs. Each query layer is decoded
once; proxy recursion beyond eight nested edges is replaced with `xxxxx`.
Malformed queries are replaced wholesale with `xxxxx`; unparseable or unsupported
connection formats become `[redacted]`. A malformed NATS server entry becomes
`[redacted]` while valid sibling entries retain their safe connection details.
The classifier is not an arbitrary secret detector for unknown option names,
path contents or fragments.

Supported safe public output is preserved. Unsupported cross-format values may
become more opaque: for example, a persisted `nats://` value in an HTTP mirror
field is fully masked. Inputs, routes, envelopes, admission, status codes and
worker behavior are unchanged. No protobuf/wire or persisted format changes;
protocol revision 40 remains unchanged and no migration is required. During a
rolling upgrade, public reads can differ in opacity for unsupported formats.

Projection work is bounded by returned configuration size: deep cloning and
traversal remain proportional to message size, field selection is constant,
URL/keyword scanning follows string size, and query encoding sorts parameter
keys. Proxy nesting has a fixed depth bound. No storage reads, network calls,
Raft/FSM work or background lifecycle is added.

Sink status messages are sanitized by their producing adaptor and remain
visible. `SinkError.message` deliberately has no sensitive annotation so useful
operational diagnostics survive; mirror and audit diagnostics have independent
annotation contracts.
