# Service gRPC protocol compatibility

## Requirement and decision (EN-1851)

[EN-1851](https://formance-team.atlassian.net/browse/EN-1851) requires detecting
an incompatible `ledgerctl` before it executes a business operation. Ledger v3
is unreleased: its protobuf fields may be renumbered between development
revisions, and a mismatched request can decode with a different meaning rather
than fail to decode. Build versions and commit IDs do not establish protocol
compatibility.

The approved decision is to **block requests with a different or missing
protocol revision**, using a revision independent of the software release.
Each server supports exactly one revision. There is no negotiation, historical
format support, or bypass flag. An advisory warning would not enforce the
requirement; exact commit matching would also reject builds whose service
contract has not changed. Release SemVer does not currently encode the
compatibility of development revisions.

## Wire contract and failure behavior

`pkg/grpcprotocol.Version` is the compiled service protocol revision, currently
`"5"`. `pkg/grpcprotocol.MetadataKey` is `ledger-protocol-version`. Clients send
exactly one value for this metadata key on every RPC. The Go
`grpcprotocol.ClientOption()` dial option supplies the local revision for unary
and streaming calls. Local `dev` builds carry the same constant without release
ldflags.

The ServiceServer checks the metadata before invoking service handlers,
including `BucketService`, `ClusterService`, and `RestoreService`. Missing,
invalid, duplicate, or different values return gRPC `FailedPrecondition` with a
diagnostic identifying the required revision and metadata key. The error does
not echo client-controlled metadata. No business handler
runs on rejection. Repeating the request with the same client does not fix this
failure: install or build a client implementing the server's protocol.

The check is per RPC, including stream establishment; a prior discovery result
is not authorization for subsequent calls. A connection or backend change does
not reuse a successful compatibility handshake. gRPC's generated unary handler
decodes the request before entering interceptors, so malformed protobuf can
return a decoding error before the compatibility diagnostic. The business
handler still does not execute.

`BucketService.Discovery`, gRPC health, and server reflection are exempt so
clients can diagnose a mismatch. Discovery's `ServerInfo.protocol_version`
and HTTP `GET /_info`'s `protocolVersion` report the server revision.
`ledgerctl version` reports the local revision without connecting to a server.
Restore mode does not register Discovery: its service gate provides the
compatibility error without requiring a preliminary Discovery call, while
health and reflection remain exempt.

Revision 5 (EN-1623) removes internal index/coverage details from public
error messages and ErrorInfo metadata while retaining their reasons and status
codes. Internal read failures in restore validation retain `Internal` with a
sanitized correlation message. AuditFailure records retain their original
diagnostic message and context.

Credential read projections (EN-1632, EN-1634, EN-1635) retain revision 5:
the exposed protobuf messages and RPC definitions remain compatible. They mask
sink/mirror credentials and transport status diagnostics in public reads.
Redacted audit payloads no longer constitute verifiable original evidence; unmodified envelopes retain
their original bytes. See [credential read projections](secret-redaction.md).

## Client and deployment scope

Enforcement starts with servers implementing EN-1851: they reject old clients
that omit the declaration. Servers predating this gate ignore the new metadata.
The new CLI sends its declaration but performs no compatibility preflight, so
it cannot protect an operation sent to an older, ungated server. Adopt the
updated server and CLI together, and update other service clients as part of
that deployment. This is not a guarantee of compatibility with historical
servers or support for mixed wire-format upgrades.

Every consumer of the service gRPC endpoint must declare its protocol,
including SDKs, automation, `grpcurl`, and internal requests forwarded to a
leader. For example, with a schema implementing revision 5:

```bash
grpcurl -plaintext -H 'ledger-protocol-version: 5' \
  localhost:8888 cluster.ClusterService.GetClusterState
```

Use the revision implemented by the client schema and semantics. Merely copying
the server's advertised value into an incompatible client defeats the check;
metadata is a compatibility declaration, not authentication. JWT, TLS,
authorization, and request-signature requirements still apply independently.

The CLI distributed with the server build is a straightforward way to obtain a
matching client. Different software releases or commits may also work when they
implement the same protocol revision. `ledgerctl upgrade` selects the latest
release in its channel, which may not match the deployed server; it is not a
server-specific compatibility repair.

HTTP consumers do not send this gRPC metadata. Internal forwarding from HTTP
uses the server's service client and is subject to the same gate at the peer.
Nodes with different service revisions can therefore fail forwarded requests;
this mechanism does not establish support for mixed-version clusters or rolling
wire-format upgrades. The separate Raft transport and snapshot server retain
their own contracts.

## Maintaining the revision

The revision primarily protects against breaking changes to exposed protobuf
messages and RPC definitions. Increment `pkg/grpcprotocol.Version` by one in the
same PR when old and new peers cannot safely exchange those messages: for
example, field renumbering/reuse, incompatible field types or oneof layouts,
removed RPCs, or changed request/response message types. Rebuild the clients and
servers that will communicate. A textual `.proto` change alone is not proof of
incompatibility: compatible additions do not automatically require a bump.

Do not use this counter as a general API behavior version. Changes to response
values, credential redaction, sanitized diagnostics, validation bug fixes, and
compatible additions do not automatically require rejecting every older client.
Document observable behavior changes and their client impact in the relevant
API contract and PR. In particular, credential read projections retain the
revision even though consumers of redacted audit evidence must account for the
loss of direct hash/signature verification.

A change without a schema break may exceptionally require a bump if peers can
no longer safely communicate under the existing protocol (for example, changing
the unit of a numeric wire field, or introducing a mandatory handshake). The PR
must identify the affected RPC/field, a concrete old-client/new-server or
new-client/old-server failure, and why rejecting older clients is necessary.
A generic claim of changed semantics is insufficient. Reviewers check this
assessment; revisions are not inferred from release tags or schema hashes.

This obligation applies to AI agents throughout pre-release development, even
though older Ledger versions and storage formats are not supported. Before
publication, and again after a rebase or target-branch update, compare the
revision with the target branch. If another PR has already consumed the planned
number, increment from the target branch's current value so a newly incompatible
contract does not reuse that number. Keep examples of the current revision in
sync. State the compatibility assessment and the old/new revision in the PR;
when no increment is needed, explain why. Documentation-only changes do not
increment the counter.

Compatible additions need not bump the revision only when both directions remain
compatible under the declared service contract. A change confined to a persisted
or Raft message does not automatically change the service protocol; inspect any
shared types exposed through the service API. See
[protobuf contributor rules](../../../contributing/protobuf.md).

The gate is a transport admission check. Its metadata and revision are not
persisted, included in audited business intent, or consulted during Raft apply.
It introduces no storage migration or incremental-restore state. It does not
validate whether a backup's persisted format matches the restore binary.

## Validation contract

Tests must prove that missing, malformed, duplicate, and different revisions
cannot enter unary or streaming business handlers, while the matching revision
does. Keep diagnostic exemptions usable without a revision. Exercise the real
client/server paths, restoration without Discovery, and internal service
forwarding so the gate cannot make the repository's own clients incompatible.
