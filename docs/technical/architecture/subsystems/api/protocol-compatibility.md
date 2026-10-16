# Service gRPC protocol compatibility

## Requirement and decision (EN-1851)

[EN-1851](https://formance-team.atlassian.net/browse/EN-1851) requires detecting
an incompatible `ledgerctl` before it executes a business operation. Before
`v3.0.0-rc.1`, protobuf fields could be renumbered between development revisions,
and a mismatched request could decode with a different meaning rather than fail
to decode. Build versions and commit IDs do not establish protocol compatibility.

The approved decision is to **block requests with a different or missing
protocol revision**, using a revision independent of the software release.
Each server supports exactly one revision. There is no negotiation, historical
format support, or bypass flag. An advisory warning would not enforce the
requirement; exact commit matching would also reject builds whose service
contract has not changed. Release SemVer does not currently encode the
compatibility of development revisions. During the RC line, revision 40 is
frozen and the published gRPC wire contract must remain compatible.

## Wire contract and failure behavior

`pkg/grpcprotocol.Version` is the compiled service protocol revision, currently
`"40"`. `pkg/grpcprotocol.MetadataKey` is `ledger-protocol-version`. Clients send
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

Revision 6 (EN-2009) makes fresh `CreateIndex` requests strict: an existing
ledger/canonical index identity returns `INDEX_ALREADY_EXISTS` (`AlreadyExists`)
instead of overwriting the registry. Retained batch idempotency retries still
return the frozen outcome. This incompatible semantic change requires matching
clients and servers; apply semantics must agree across every replica.

Revision 5 (EN-1623) removes internal index/coverage details from public
error messages and ErrorInfo metadata while retaining their reasons and status
codes. Internal read failures in restore validation retain `Internal` with a
sanitized correlation message. AuditFailure records retained their diagnostic
message and context until revision 30.

Revision 8 (EN-1771) removes `CreatedIndexLog.initial` and renumbers the
remaining exposed fields. Clients and servers built against revision 7 would
therefore decode the same varint fields with different meanings.

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
leader. For example, with a schema implementing revision 40:

```bash
grpcurl -plaintext -H 'ledger-protocol-version: 40' \
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
the revision-40 wire contract alone does not prove a rolling upgrade safe. RC
updates must also preserve peer communication, Raft apply, and persisted-state
compatibility while old and new servers coexist. The separate Raft transport and
snapshot server retain their own contracts. See [server upgrades](../../../../ops/upgrades.md).

## Apply execution provenance (revision 7)

Every successful `BucketService.Apply` response carries exactly one
`ledger-apply-replayed` trailer, either `true` or `false`. The server derives it
from the FSM's idempotency outcome and preserves it through admission and the
controller. The forwarding client requires this trailer from its leader and
rejects an absent, duplicate, or invalid value. Incoming request metadata is
never used as execution provenance. This is response transport metadata, not a
client-controlled request field, a signed business log, or persisted state.

The follower uses this provenance to skip checkpoint materialization waits for
historical outcomes, including when the first call is still materializing or
the checkpoint has been deleted. New creations retain the local readiness wait
unless superseded by deletion. See the [checkpoint contract](../read-path/query-checkpoints.md#readiness-and-error-contract).
Revision 7 is incompatible with peers that omit or ignore this distinction;
all communicating service clients and nodes must use the matching revision.

## Metadata size limits (revision 8)

Revision 8 adds the replicated metadata ceilings and `METADATA_LIMIT_EXCEEDED`
service error contract. It follows revision 7's Apply execution provenance
changes; clients and servers must use the matching current revision.

## Empty conjunction semantics (revision 9)

Revision 9 reads a `QueryFilter` carrying an `AndFilter` with no children as
vacuously true: it selects the target universe, the same rows as no filter at
all. The `.proto` text is unchanged, so the difference is invisible to a schema
comparison — a revision-8 peer answers the identical payload with an empty page.
`OrFilter` with no children is the empty set at both revisions. See
[query filtering](../read-path/query-filter.md#5-combination-semantics).

## Deleted-ledger write rejection (revision 10)

Revision 10 closes a soft-deleted ledger to every write the service exposes.
`SaveLedgerMetadata`, `DeleteLedgerMetadata`, `SaveNumscript`, the prepared-query
create, update and delete, and `PromoteLedger` answer `LEDGER_DELETED` where a
revision-9 peer applied them and returned success. The `.proto` text is
unchanged and `LEDGER_DELETED` was already part of the error contract, so the
difference is invisible to a schema comparison: only which request produces it
changed. See [deleted ledger data retention](../../../../ops/disk-space.md#deleted-ledger-data-retention).

## Revert-target rejection and its retained outcome (revision 11)

Revision 11 changes what a batch that reverts a transaction it also creates
returns, and what that outcome leaves behind under an idempotency key.

A revision-10 peer answered `COVERAGE_MISS` (`Internal`), because admission
could not declare the volume coverage apply would need and the coverage gate
rejected the order. `KindInternal` failures are never frozen, so the key stayed
free and a later retry could execute.

Revision 11 answers `REVERT_TARGET_CREATED_IN_BATCH` (`InvalidArgument`). That
kind **is** freezable, so the rejection is retained against the key and a retry
replays it instead of re-executing — including a retry issued after the target
transaction was committed independently, which a revision-10 peer could still
execute successfully. The same batch therefore has a different retained outcome
on the two revisions.

The service `.proto` text gains only the `ERROR_REASON_REVERT_TARGET_CREATED_IN_BATCH`
enum value, which is a compatible addition on its own; the incompatibility is the
changed retained outcome, invisible to a schema comparison. The revision also adds
`OrderTechnical.revert_target_digest` to `raft_cmd.proto`. That message is not
part of the service contract — no service RPC carries it; it travels only inside
Raft entries between replicas — so it does not bear on this revision's client
compatibility; apply semantics must still agree across every replica. A stale admission observation of a target that predates the
batch and is otherwise revertable answers the retryable
`STALE_INPUTS_RESOLUTION`, which is not frozen and is unchanged in kind from the
surrounding contract; a target that is unknown or already reverted keeps
answering `TRANSACTION_NOT_FOUND` or `TRANSACTION_ALREADY_REVERTED` as it did on
revision 10, because those checks run first. See
[the revert-target observation](../admission/README.md#revert-target-observation).

## Prepared-query execution errors (revision 12)

Revision 12 classifies invalid prepared-query execution requests as
`InvalidArgument` without structured error information. In particular,
aggregate requests for non-account targets and requests with an unsupported
query mode no longer surface as sanitized `Unknown` failures. A revision-11
client can interpret those status codes differently for retry and operation
handling, so clients and servers must use the matching revision.

## Total caller attribution (revision 13)

Revision 13 replaces the nullable caller identity fields exposed by audit and
forwarding messages with a principal union. Authenticated, anonymous, system,
and authentication-disabled actions now have distinct wire representations,
and authenticated authorization state moved under its principal variant.
Revision 12 clients and servers would decode these field numbers with different
types and must not communicate with revision 13 peers.

## Required caller attribution (revision 14)

Revision 14 makes caller attribution mandatory at the common Apply boundary.
Followers freeze and forward a validated principal, leaders reject missing or
malformed attribution before preload or proposal, and every FSM replica repeats
the same validation for replicated writes before mutation. Direct clients cannot provide
the peer-only forwarding field. This semantic tightening requires revision 14
clients and servers to communicate together.

## Missing-ledger errors (revision 15)

Revision 15 (EN-1568) changes missing-ledger responses from `GetLedgerStats`
and `GetTemplateUsage` to the structured `LEDGER_NOT_FOUND` error. Both still
return gRPC `NotFound`, but now include `ErrorInfo.Reason=LEDGER_NOT_FOUND`
and the ledger `name` metadata; revision 14 returned `NotFound` without
`ErrorInfo` for these RPCs.

This is an incompatible response-semantic change even though the service
protobuf schema is unchanged: revision-14 clients may interpret the structured
reason and metadata differently. All communicating service clients and servers
must be rebuilt with the matching revision. HTTP missing-ledger responses remain 404.

## Filterless prepared-query updates (revision 16)

Revision 16 changes a nil `Apply(UpdatePreparedQuery)` filter from a validation
failure into an explicit removal of the stored filter. The query then matches
every entity in its immutable target. The protobuf wire shape is unchanged, but
a revision-15 peer interprets the identical request differently, so clients,
servers, and every Raft replica must agree on revision 16 semantics.

## Prepared-query cursor without `previous` (revision 17)

Revision 17 removes `PreparedQueryCursor.previous`, which only echoed the
request cursor, and renumbers `next`, `account_data`, `transaction_data` and
`log_data` to 3–6. A revision-16 peer decodes those fields under the wrong
numbers, so clients and servers must agree on revision 17.

## Reverse prepared-query execution (revision 18)

Revision 18 adds `ExecutePreparedQueryRequest.reverse`, which pages `LIST`
results in descending entity order and is rejected with `InvalidArgument` in
`AGGREGATE_VOLUMES` mode. A revision-17 server ignores the unknown field and
answers in ascending order, so a reverse request would silently return the
wrong page; clients and servers must use the matching revision.

## Superuser authentication terminology (revision 19)

Revision 19 renames the privileged JWT claim to `superuser`, the CLI flag to
`--superuser`, and the authenticated caller field to `superuser`. Authorization
semantics and protobuf field number 3 are unchanged, but tokens and credential
configuration must use the new name. Older claims and flags have no aliases;
update clients, servers, OIDC claim mappings, and operator Credentials together.

## Atomic creation metadata (revision 20)

Revision 20 adds typed initial metadata to CreateLedgerRequest and its creation
log. A revision-19 server ignores that request field and acknowledges creation
without the supplied values. Clients, servers and replicas must agree on the
atomic creation semantics; rebuild communicating service binaries together.

## Prepared-query filter shape validation (revision 21)

Revision 21 rejects `Apply(CreatePreparedQuery)` and `Apply(UpdatePreparedQuery)`
with `InvalidArgument` / `FILTER_COMPILATION_ERROR` when a filter leaf has a shape
that never compiles, such as a missing condition value, a missing field reference,
or a builtin field the condition does not serve. A revision-20 server stores such
a query and fails every execution, so peers on the same revision must agree on
which writes are rejected.

## Typed arbitrary-precision volumes (revision 22)

Revision 22 replaces the decimal `string` fields in `Volumes` and
`VolumesWithBalance` with typed arbitrary-precision integers. Non-negative
input/output totals use a canonical minimal unsigned big-endian magnitude;
balances use a sign plus that magnitude and reject negative zero. The HTTP JSON
projection remains exact decimal strings, while protobuf clients must implement
the new typed messages. The representation is unbounded because account color
collapse may sum several independently bounded `Uint256` buckets beyond 256
bits.

## Numscript metadata rendering and VM execution (revision 23)

Revision 23 changed account-typed Numscript metadata from `@merchants:acme`
to `merchants:acme` and introduced VM execution. It also moved static compile
failures from FSM apply to admission, reported as
`ERROR_REASON_NUMSCRIPT_COMPILE_ERROR`. These are exposed semantic changes,
so the service revision increment was required. At that pre-release stage,
admission also attached VM bytecode to the internal Raft order. That internal
representation has since been removed; see
[Numscript library upgrades](../../../../ops/deployment.md#numscript-library-upgrades).

## Omitting already-cached Numscript bytecode (revision 24)

Revision 24 was a pre-release intermediate representation: admission could
send a program hash instead of its bytes after its own cache had compiled the
script. Older binaries interpreted that shape differently, so this revision
incremented the service gate. The current internal Raft order carries no
compiled program, program hash, encoded variables, or script hash. These
historical fields are absent from the schema and generated types.

## Two-way page tokens (revision 25)

Revision 25 replaces every list cursor with a page token from
`pkg/pagecursor`: base64url (unpadded) of `{"key": <string>, "back": <bool>}`.
A revision-24 cursor was the raw key itself (a decimal id, an address, a
name, or base64url value bytes for index inspection), which a revision-25
server rejects with `InvalidArgument`, and a revision-25 token is not a raw
key a revision-24 server can parse.

Paged list RPCs publish `x-previous-cursor` beside `x-next-cursor`, and
`PreparedQueryCursor`, `InspectDistinctValues` and `InspectFacets` gain a
previous token (`previous` is `PreparedQueryCursor` field 4, renumbering its
data fields to 5–7). `ListLogs` accepts `ListOptions.reverse`. A back token
serves the page ending strictly before its key in the requested order, so it
is not meaningful to a peer that does not implement it; clients and servers
must use the matching revision.

## Private cluster policy command (revision 26)

Revision 26 removes `Request.set_cluster_policy` from the public
`BucketService.Apply` envelope. Cluster policy reconciliation is now admitted
only through the server-internal `Admission.AdmitClusterPolicy` entry point,
which still commits the internal Raft `SetClusterPolicyOrder` and audited
`SetClusterPolicy` log payload.

A revision-25 client can still encode field 28 in `Request.type`, but a
revision-26 server no longer interprets that field as a valid Apply request.
Clients and servers must therefore use the matching revision.

## Current Numscript Raft order (internal pre-release change)

The internal `OrderTechnical` compiled fields were removed. Admission still
validates and predicts using its local VM; FSM apply and audit replay compile
the committed script text or resolved library reference and business variables
with the running binary. This changes only the internal Raft order
representation, not an exposed service request, response, or operation
contract, so it needs no additional service protocol revision.
Semantic changes in a later Numscript library update still need their own
service-compatibility assessment and an upgrade plan for in-flight Raft
entries; the absence of committed bytecode does not make rolling upgrades
safe across such changes.

## Required member identity on AddLearner (revision 29)

Revision 29 makes the target node's 16-byte `instance_id` mandatory on
administrative `ClusterService.AddLearner` requests. A revision-29 server
rejects requests that omit `instance_id` with `InvalidArgument` at the
handler boundary. In practice, a client implementing a different revision will
first be rejected by the protocol-version gate (lines 29-35 above) with
`FailedPrecondition` before reaching identity validation. Clients and servers
must implement the same revision; there is no compatibility fallback.

## Maintaining the revision

Ledger v3 is in its release-candidate phase. The revision is frozen at `"40"`:
do not increment `pkg/grpcprotocol.Version`. Preserve published gRPC service
and method names, request and response types, protobuf field numbers, wire
types, field identity, and enum numbers so existing clients can call and decode
the service. The revision identifies this wire contract; it does not freeze
every behavior or configuration setting for the release-candidate phase.

The author of a service change must identify its effect on published clients.
Removing or renaming a published RPC, changing its request or response type,
or removing, renumbering, or repurposing a published field or enum value is not
allowed during the RC. Behavior changes, such as a bounded page with a next
cursor, and configuration changes can ship when their effect and upgrade steps
are documented in the PR and user guidance. Reviewers must check whether a
client can still decode the response and how it must handle any changed result.
Do not bump the revision to avoid this assessment. Documentation-only changes
need no client assessment.

An additive service field must preserve decoding in both directions. A change
confined to a persisted or Raft message does not automatically change the service
protocol; inspect any shared types exposed through the service API. Existing
persisted and audited data must remain readable, or have a safe migration. See
[protobuf contributor rules](../../../contributing/protobuf.md).

### Automated enforcement

`PROTOCOL_BASE_SHA=<commit> just check-protocol-version` enforces the wire part
of the freeze.
The required `Dirty` check and the `invariants` check run it on pull requests
and merge-queue entries against the target tip, including after a PR changes
base. `agent-check-pr` runs it when `misc/proto/**` or
`pkg/grpcprotocol/**` changes.

1. The check reads `Version` at the base and in the worktree. Any change fails.
2. The check runs `buf breaking` between the base and the worktree with the
   rules in `misc/proto/buf.yaml` (`WIRE`, `SERVICE_NO_DELETE`,
   `RPC_NO_DELETE`). Deleting, renumbering, or retyping a field is reported;
   adding a field or a wire-compatible widening such as `int32` to `int64` is
   not. `buf` is used only for this classification; code generation stays on
   `protoc`.
3. The check keeps a finding only when its innermost enclosing declaration is
   a gated service (`BucketService`, `ClusterService`, `RestoreService`) or a
   message or enum transitively reachable from one of their RPCs. It uses source
   line and column to distinguish nested declarations on the same line. A
   finding it cannot attribute to a declaration counts when its file declares a
   gated type. Raft-only and persisted-only messages are not counted.
4. Any kept finding fails the check. A revision bump does not excuse it.

The check does not detect semantic changes that keep the `.proto` text
unchanged; those remain the author's and reviewer's classification. When a
gated service is added or renamed, update `gatedServices()` in the same PR: the
check fails if a listed service no longer exists.

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

## Credential-safe LedgerInfo reads (revision 27)

EN-1635 / EN-2780 changes GetLedger and ListLedgers responses, including
checkpoint reads, to detached credential-safe projections. Original mirror
configuration remains available internally. This external read-semantic change
requires matching clients and servers. See [LedgerInfo](ledger-info.md).

## Numscript execution errors (revision 28)

Revision 28 adds `ERROR_REASON_NUMSCRIPT_EXECUTION_ERROR` (`ErrNumscriptExecution`,
`KindPrecondition`, detail in the `detail` metadata key). A script that runs
and fails on its own terms — a negative amount, an allotment that does not sum
to 1, a send-all from an unbounded source, a posting amount wider than 256
bits, and the other script-caused VM errors — returns gRPC `FailedPrecondition`
(HTTP 400) with that reason. Revision 27 returned `Internal` with
`NUMSCRIPT_RUNTIME` for the same requests. The failure is now freezable under
an idempotency key, like `INSUFFICIENT_FUNDS`. Dependency-resolution failures
caused by the script, its vars, or the state it read (an invalid account name,
a missing or mistyped var, mismatched currencies, a bad portion, a division by
zero, a missing or malformed metadata value, a negative balance) return the same
reason. `NUMSCRIPT_RUNTIME` remains for genuine server faults: recovered library
panics, numscript internal errors, and unmapped library errors.

This is an incompatible response-semantic change: a revision-27 client sees a
status code and reason it does not know. Clients and servers must use the
matching revision.

## Flat backup object keys (revision 29)

Backup object keys drop the hardcoded `backups/` segment. The manifest moves
from `<bucketID>/backups/manifest.json` to `<bucketID>/manifest.json`, and
checkpoint and export objects move from `<bucketID>/backups/{data,exports}/` to
`<bucketID>/{data,exports}/`.

The protobuf contract is untouched: no field is added, removed, renumbered, or
retyped. What changes is the interpretation of `bucket_id`. A byte-identical
`Backup`, `IncrementalBackup`, or `StartDownloadBackup` request writes and reads
a different set of object locations against a revision 28 and a revision 29
server. A revision 28 client that passed the gate against a revision 29 server
would publish or look for a manifest the other never reads, so an incremental
backup fails for want of a checkpoint, a restore 404s, or — if both layouts are
present in one bucket — either side silently selects the wrong chain.

Ledger v3 is unreleased and no backup written under the old layout is
supported, so there is no migration: operators re-run a full backup under the
new layout.

## Structured audited failures (revision 30)

Revision 30 replaces the persisted `AuditFailure.message/context`,
`IdempotencyFailure.message/metadata`, and `OrderSkippedLog.context` fields
with a stable `reason`, `code`, and `facts` tuple. Audit hashing and
idempotency comparison use that tuple. Read and API paths render presentation
text from it; Numscript execution and compiler diagnostics are not persisted
as failure messages or copied into their public error details. A skipped order
exposes its code and facts in gRPC, HTTP bulk, and
event payloads. This changes exposed log and audit responses, so service
clients and servers must use the same revision.

## Stable public failure details (revision 31)

Revision 31 renders local business error messages and `ErrorInfo.metadata` from
the same explicit stable facts used by the audit chain. Diagnostic `Error()`
text does not enter the public response. Errors
that have no declared reason receive a generic message for their error kind.
The status code and broad reason remain stable. Clients that parsed former
free-form text must switch to the reason and stable fact keys. No persisted
failure format changes between revisions 30 and 31.

## Typed audited failure details (revision 32)

Revision 32 replaces the persisted and service-visible `facts` string maps in
`AuditFailure`, `IdempotencyFailure`, and `OrderSkippedLog` with one shared
`FailureDetails` protobuf oneof. Each arm names a concrete stable failure code
and carries presence-tracked fields with explicit protobuf types; numeric
correlators are `uint64` instead of decimal text. At revision 32, the
containing reason had to match an arm annotation and the code had to name that
arm. Ledger rejects missing arms,
absent required facts, unknown fields, and malformed values
when reading audited failures. HTTP and public
`ErrorInfo.metadata` still render the named facts as strings at their API
boundaries.

The audit hash pre-image now binds the selected arm number alongside the
sorted rendered fact keys and values. Persisted failures from revision 31
cannot be read or verified under revision 32. Ledger v3 is unreleased, so
deployments must start with fresh state; mixed revisions cannot share a
cluster or service connection.

## Contextual metadata validation failures (revision 33)

Revision 33 requires the `key` fact for `METADATA_KEY_INVALID_CHAR` and
`METADATA_VALUE_CONTAINS_NULL_BYTE`. `METADATA_KEY_EMPTY` is a distinct code
whose key is necessarily empty, so it carries no redundant `key` fact. The
validation producer retains the offending key for the other variants. Their
primitive validation sentinels remain available for `errors.Is`, but cannot
be serialized without the key. Account metadata failures also carry `account`;
ledger and transaction metadata failures do not
have an account. The public gRPC error metadata and audited failure details now
include the key on paths that previously returned a bare sentinel. The schema
also removes generic `key` and `account` fields from failure variants whose
producers never supply metadata context. Remaining conditional fields reflect
distinct producer paths. Previously persisted failures with the removed fields
or without a now-required key cannot be read under revision 33.

## Deterministic failure fact shapes (revision 34)

Revision 34 removes all conditionally present fields from `FailureDetails`.
Every scalar fact in a selected variant is required, including empty strings
and zero IDs when those are the actual values. `proto3 optional` remains on
these required fields solely to distinguish an explicitly supplied zero value
from an absent field during validation. The validator requires presence of
every scalar in the selected variant; no per-field required annotation is
needed. Removing `optional` would make a missing fact indistinguishable from
an explicit zero or empty value in proto3.

Distinct variants now represent account-scoped metadata failures, metadata
limits with a known key, direct posting versus Numscript insufficient funds,
Numscript failures with a known numerator or scope, and an unknown script name
versus an unknown exact version. The coarse `ErrorReason` stays the same for
each pair; the stable detail code identifies its precise fact shape. An
unconfigured metadata policy has no key context because validation stops
before selecting an entry. Transaction reference conflicts always include
`existingTransactionId`, even when its value is zero. The changed detail arms
and field numbering require fresh state and a uniform revision 34 cluster.

## Audited versus public failures (revision 35)

Revision 35 removes detail variants for failures that cannot be written to an
audited order outcome. Read aggregation overflow, admission dependency discovery,
execution-plan size, idempotency lookup, write health gates, and audit availability
remain public errors with their existing reason, code, status and fact presentation.
A malformed caller attribution and a maintenance gate can also reject a committed
proposal in the FSM, but both return before the order-audit path; they are not
`AuditFailure` records. `PRELOAD_UNAVAILABLE` also returns before order
processing when no earlier idempotency outcome exists: it is a retryable
preparation interruption with no audit entry or frozen result.
`STALE_PROPOSAL` and `INVALID_EXECUTION_PLAN` remain typed audited outcomes.

The `FailureDetails` oneof now contains only variants eligible for the audited
projection. Removed arms are renumbered sequentially because Ledger v3 is
unreleased. This also removes the unreachable bare `validation` arm: it had no
allowed failure reason or producer; concrete validation failures retain their
own typed arms. The service revision changes to 35 because this shared protobuf type
is exposed by gRPC. Existing revision-34 state must be discarded; clients and
servers must use the same revision. Public `ErrorInfo` identifiers and metadata
are unchanged.

## Uncommitted internal proposal failures (revision 36)

Revision 36 removes the `cluster_policy_invalid`,
`cluster_policy_revision_conflict`, and `coverage_miss` arms from the audited
`FailureDetails` oneof. These errors indicate an invalid internal policy
proposal or an admission coverage contract violation. An affected proposal
returns its typed error without an `AuditFailure` entry or frozen idempotency
result; staged order writes are discarded. A corrected proposal can be retried.
Successful cluster-policy updates still produce audited orders and the policy
projection remains checker-verified. The removed arms are renumbered because
v3 is unreleased. This shared gRPC change requires revision 36 on both sides.

## Technical failures outside the audited outcome (revision 37)

Revision 37 removes 22 technical, transient, invariant, and unclassified
variants from the `FailureDetails` oneof. Storage failures, stale admission
observations, missing preload coverage, sequence exhaustion, malformed internal
plans, and inconsistent materialized state now return without an `AuditFailure`
entry or frozen idempotency result. Mirror source sequence gaps are also
retryable after correcting the source. Read-side index and checkpoint readiness
errors retain their public reasons but have no audited detail variant.

Known Numscript compiler and execution errors keep their specific audited
variants. An unclassified compiler failure and an unknown Numscript runtime
failure are not business verdicts. The generic compile and execution fallback
variants are removed; public responses retain their coarse reasons. An
unconfigured committed metadata policy is an internal failure, not a rejection
attributed to the business command. Successful orders still produce audit
entries. This shared protobuf change requires fresh v3 state and revision 37
clients and servers.

## Derive stored failure codes from typed details (revision 38)

Revision 38 removes the redundant `code` field from persisted `AuditFailure`,
`IdempotencyFailure`, and `OrderSkippedLog`. The selected `FailureDetails` oneof
arm is the sole stored source of the stable subcode. Audit hashing binds the
arm number, the explicit facts, and unknown field bytes; checker validation
rejects missing or invalid arms. Public HTTP, bulk skip, audit JSON, and event
responses still render the same `code` from the arm. gRPC `ErrorInfo` continues
to expose the coarse reason and the facts as metadata.
The fields are removed and renumbered because v3 is unreleased. This shared
protobuf change requires fresh v3 state and revision 38 clients and servers.

## Derive stored failure reasons from typed details (revision 39)

Revision 39 removes the redundant `reason` field from `AuditFailure`,
`IdempotencyFailure`, and `OrderSkippedLog`. Each `FailureDetails` oneof arm now
declares exactly one `failure_reason` enum value; the selected arm is the sole
stored identity for the failure. The FSM validates that a producer's `Reason()`
matches the arm before persistence. Audit hashing still binds the derived reason,
arm number, and facts. The checker rejects missing or invalid details and
compares idempotency projections against the hash-bound audit outcome.

The request's `skippable_reasons` whitelist remains a list of coarse reasons.
FSM order processing checks the producer's reason against that whitelist, then
validates that it matches the typed details before emitting an `OrderSkippedLog`.
Public HTTP and JSON responses continue to render the same reason, code, and
facts; gRPC clients reading `OrderSkippedLog` derive its reason from `details`.
The removed and renumbered protobuf fields change the service wire format.
Ledger v3 is unreleased, so deployments start with fresh state and use revision
39 clients and servers together.

## Retry writes while the health verdict is unknown (revision 40)

After a leadership transition, the write gate has no current-term verdict until
the first health poll completes. Revision 40 returns gRPC `Unavailable` without
an `ErrorInfo.reason` during that interval. Clients using the standard retry
policy retry the request. A measured disk block still returns
`ResourceExhausted` with `WRITES_BLOCKED_DISK_FULL`, and a measured clock-skew
block retains its existing response. Admission checks the gate before proposing
an order, so a retry after `Unavailable` cannot duplicate a committed write.
This status change requires revision 40 clients and servers together.

## Capped index inspection page size

EN-2946 caps `InspectIndexRequest.page_size` at 10000 in the read store, which
HTTP and gRPC share. A request for more than 10000 values or facets returns
10000, `has_more`, and a next cursor. Earlier builds returned the full requested
page. The protobuf text changes only by a comment. Clients that previously
treated the requested page as complete must now follow the cursor to read all
results. This response behavior changes under revision 40 by the RC policy;
the revision is frozen, and the client impact must be documented.
