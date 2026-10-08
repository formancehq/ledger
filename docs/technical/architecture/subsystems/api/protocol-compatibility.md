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
`"24"`. `pkg/grpcprotocol.MetadataKey` is `ledger-protocol-version`. Clients send
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
sanitized correlation message. AuditFailure records retain their original
diagnostic message and context.

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
leader. For example, with a schema implementing revision 24:

```bash
grpcurl -plaintext -H 'ledger-protocol-version: 23' \
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

Revision 22 stores and returns an account-typed Numscript metadata value
(`set_tx_meta("k", @merchants:acme)` and its `set_account_meta` counterpart) as
the bare account name, `merchants:acme`, where revision 22 returned
`@merchants:acme`. The rendering now comes from the Numscript library itself,
identically on both of its engines, and the bare name is the form a later
`meta()` read can resolve as an account again — the `@`-prefixed form could
not. Scalar values are unchanged: strings and numbers stay verbatim, monetary
stays `ASSET amount`, portions and assets keep their canonical forms. The
`.proto` text of the exposed metadata messages is unchanged, so the difference
is invisible to a schema comparison; a revision-22 client would read the same
Apply request back with different metadata bytes.

Revision 22 also changes apply semantics: admission compiles each resolvable
script to Numscript VM bytecode and binds it to the order's technical
sub-message, and the FSM executes that artifact instead of re-interpreting the
script text. The VM is the only engine: a script it cannot compile is rejected
at admission, and a scripted order reaching the FSM without an artifact is
recompiled from its text rather than interpreted. Like revision 6, apply semantics must agree across every replica:
a binary predating these fields silently drops them and interprets with the
older Numscript library, so a mixed-binary cluster applying the same committed
entry writes divergent transaction and audit bytes. Deploy this revision with
all nodes stopped — see
[Upgrading across the Numscript VM execution change](../../../../ops/deployment.md#upgrading-across-the-numscript-vm-execution-change-revision-23).
The artifact is bound to its script text by an XXH3-128 hash
(`compiled_script_hash`), which also keys the FSM's script caches.
The artifact itself carries the Numscript library's bytecode version
(major.minor). The FSM executes it when the bundled library can read that
version: the same major and a minor no newer for a stable major, or exactly
the same version for an unstable `0.x`. A version it cannot read means
another library version produced the artifact; the FSM then derives program
and vars from the script text with its own library instead, exactly as the
store checker's audit replay derives every order (see revision 24 below), so
foreign bytecode is never run. A malformed artifact — one the library reads
but cannot decode or verify, a partial one, or one whose script hash does not
match the resolved text — fails the order with a Numscript runtime error and
is never repaired from the text.

Revision 22 also moves where and how a statically invalid script fails.
`Parse` checks syntax only; the Numscript typechecker runs inside the
compiler. Every static-semantics failure the compiler catches — a type
mismatch, an undeclared variable, an unknown function or var type, `oneof` or
a mid-script `balance()` without its feature flag, a send-all from an
unbounded-overdraft source — used to pass admission, reach apply, and fail
there as `ERROR_REASON_NUMSCRIPT_RUNTIME` (`KindInternal`). It now fails
at admission as `ErrNumscriptCompile` with the new
`ERROR_REASON_NUMSCRIPT_COMPILE_ERROR` (`KindValidation`, detail in the
`details` metadata key, like `NUMSCRIPT_PARSE_ERROR`), and such an order no longer produces a proposal, a failure
log, or an audit entry. The one exception is a `latest` script reference under
an idempotency key: admission forwards it as preload-unavailable (see
[admission idempotency](../admission/idempotency.md)), so the FSM replays the
key's frozen outcome or rejects with `ERROR_REASON_PRELOAD_UNAVAILABLE`.
Neither revision freezes the compile failure itself under an idempotency key:
revision 22's apply failure was `KindInternal`, which is not freezable, and
revision 23's rejection happens before apply.

## Omitting already-cached Numscript bytecode (revision 24)

Revision 23 lets admission send `OrderTechnical.compiled_program` by
reference: a new field, `compiled_program_hash` (the XXH3-128 of the bytes),
replaces the bytes once admission's own compile cache has compiled the script
before (`CompiledScript.AlreadyCompiled`, backed by `lruEntry.compileParsed`
on admission's own `NumscriptCache` instance), so the bytecode travels once
per script per admission instance. `compiled_vars` and `compiled_script_hash`
remain mandatory for every scripted order exactly as before, and exactly one
of `compiled_program` and `compiled_program_hash` accompanies them; any other
combination fails the order loudly. The signal describes what this instance
has sent, never what any replica has cached — admission and the FSM apply
path each construct their own `NumscriptCache` instance and share no state —
and the FSM tolerates it being wrong either way.

The FSM runs a committed artifact when its own library can use it — by value
when it reads the bytecode version, by reference when it holds or reproduces
bytes with the committed hash — and otherwise derives program and vars from
the script text with its own library (`numscript.SafeExecCommitted`), so
replicas on different library versions apply the same entry without failing
it; see
[Omitting already-cached Numscript bytecode](../../../../ops/deployment.md#omitting-already-cached-numscript-bytecode-revision-24)
for the contract that keeps their outcomes equal. A revision-23 binary does
not know the new field and treats a by-reference order as a partial
artifact, failing the order a revision-24 binary applies — replicated-state
divergence, not merely an availability difference — which is why the revision
changes.

## Maintaining the revision

The author of a service contract change must determine whether an existing
client or server would interpret requests, responses, or operations differently.
Increment the decimal counter `pkg/grpcprotocol.Version` by one in the same PR
for an incompatible wire or semantic change (for example, `"1"` to `"2"`), and
rebuild every service client and server that will communicate. This includes
renumbering or changing the interpretation of exposed
protobuf fields, and incompatible semantics even when the `.proto` text stays
unchanged. Reviewers must check this classification; the revision is not inferred
from a release tag or automatically negotiated from a schema hash.

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
