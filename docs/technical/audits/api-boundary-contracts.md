# API boundary audit evidence and ownership

The [manifest](api-boundary-contracts.json) scopes a reusable audit of how HTTP,
service gRPC and `ledgerctl` translate inputs and expose results. Existing domains
cover the correctness of the resulting business operations and durable state;
they do not provide a focused inventory of transport presence, codecs, limits,
schemas and stream framing. This domain adds that inventory to the native audit
workflow without adding a runner or changing product guarantees.

## Evidence contract

For each hypothesis, identify a registered entry point, its middleware or
interceptors, the original input, the exact conversion chain, the downstream
request and the observable response. Cite the specific authoritative contract
and inspect guards and callers before asserting a violation. The API subsystem
README identifies gRPC as the primary service contract and HTTP as a compatibility
surface; that does not promise identical features, defaults or encodings.

Use `http-api.md` for response encoders and error presentation,
`protocol-compatibility.md` for the service gate, `api-comparison.md` and
`openapi.yml` for endpoint contracts, and the relevant section of `docs/ops/cli.md`
for CLI behavior. Reconcile these sources with registered routes, protobuf
presence, command flags, implementation comments and tests at the audited SHA.
A stale table is not proof of a missing route. Conflicting sources or unspecified
null/unknown-field/trailing-input policies become **questions**, not assumed bugs.

Prepared-query `LIST` page-size evidence must reach the registered HTTP execute
route and service gRPC handler. Capture downstream requests for 0, 1000, 1001
and uint32 maximum in both directions: zero selects 100, oversized values select
1000, and the response reports the effective size. Supplied HTTP query values
override the body, including empty/zero; overflowing uint32 values reject before
the backend. Preserve caller-owned inputs and unchanged `AGGREGATE_VOLUMES`
requests. This domain owns adapter normalization and response presentation;
`query-semantic-equivalence` owns executor enforcement before ID collection and
enrichment, with exact fixed-state windows and continuation. Neither claims
snapshot stability across live requests.

Establish which error tier a failure belongs to before asserting that its
presentation is wrong. The three contracts in `internal/domain` — `Classifiable`,
`Describable`, `SerializableError` — are described in the "Three Error Contract
Tiers" section of `http-api.md`. An error that declares only a kind owns no wire
reason by design: the absence of an `ErrorInfo` on gRPC, and a coarse
`errorCode` such as `INVALID_REQUEST` on HTTP, are the contract rather than a
lost identifier. A reason that is declared and then dropped, changed or
renamed across the boundary remains a defect.

A reproduction plan must name the input, entry path, guard crossed, failure point
and distinguishing observation: exact outgoing value, status, header, body,
terminal stream error or trailer. Reject helper-only states that production
validation cannot admit. For severity, use the native P0–P3 standard and actual
impact: a cosmetic schema difference cannot establish corruption or authorization
bypass. P0/P1/P2 need a concrete failure path. Every first-pass finding remains a
hypothesis until an independent challenge tries to disprove it.

The output projection need not round-trip internal types. In particular,
LedgerLog JSON loses some typed metadata distinctions by design and is not a
backup/replay representation. JSON number precision must be assessed using exact
values, not a test oracle that has already rounded through `float64`.

For generated SDK evidence (EN-2685, EN-2781), bind the specification, generator and
dependency versions. Exercise actual LedgerLog and nested SystemLog decoders
with nonempty operation payloads; explicit additional properties must prevent
empty-object stripping. Reconcile the SystemLog variant fixtures with the live
LogPayload descriptor and JSON codec. Verify the actual built SDK externally:
single-log, list-log and prepared-query LOGS response decoders
must retain representative nested non-apply payloads, every current variant and
an unknown future sibling. SystemLog.payload is open while apply.log stays typed
and LedgerLog.data remains complete. Credential-safe projections remain EN-1634;
SDK field stripping is not a security boundary. The repository Go regression
and its `testdata/` inventory guard the schema and actual JSON codec. Exercise
actual prepared-query create/update serializers
with structured, nested, textual, null and omitted filters. A standalone helper
or OpenAPI lint result alone cannot establish request corruption or preservation.
The OpenAPI 3.0 filter union uses a QueryFilter reference and an inline nullable
string, without a dummy null-only object or ignored nullable `$ref` sibling.
Check `TestOpenAPISpec_NoBareObjects` for inline and component schema coverage:
opaque ledger metadata, transaction account metadata, audit/signature fields,
event sinks/statuses, signing keys and index responses explicitly allow
additional properties. Only the intentionally empty `DropAction` is exempt.
SDK metadata keys are raw inputs; direct HTTP clients encode a single segment.

For bulk SDK evidence, preserve both processing envelopes and early
errors at every rolled-up status. `BulkErrorResponse` uses non-exclusive `anyOf`
because its optional bulk error properties overlap ordinary ErrorResponse.
JWT rejection can be plain-text 401, while missing/insufficient element scopes
produce JSON 401/403. `TestHandleBulk_*Contract` validates real router responses
and can export them for `tests/sdk/bulk-contract.mjs`; the generated operation
must retain every successful, failed and aborted element's fields, 503 headers,
and default retry behavior. Typecheck `tests/sdk/bulk-idempotency.ts` and prove
the typed key emits the real header (including omission). Sequential elements
retain wire field `ik`; the batch header applies only to atomic HTTP batches
for the URL ledger. Bind the exact schema, generator configuration/version and
dependency lock. These boundary probes do not prove durable keyed replay,
atomic rollback or safe unkeyed retry; those belong to the neighboring
idempotency domain.

For Speakeasy's mixed JSON/text error limitation, apply
`tests/sdk/bulk-generation-overlay.yml` only to generation input. Keep both 401
media types in the canonical schema and verify typed JSON plus raw invalid-token
text through the generated operation, as described in `tests/sdk/README.md`.

Compare documented write scopes with both the granular route guard and default
aggregate mapping. Creation metadata remains EN-2686's separate server contract.

HTTP large-integer negotiation preserves the v2 monetary default and extends it
by type: every response `Uint256`, `BigUint` and `SignedBigInt` uses exact decimal
JSON number tokens by default and canonical strings with case-insensitive
`true`, `yes`, `y` or `1` in `Formance-Bigint-As-String`, without trimming.
Include standalone values and nonmonetary fields inside structs, maps, slices
and custom public projections. Trace shared streaming, checked, paged and bulk
writers from the request through option-aware nested codecs; dedicated protojson
projections must contain none of these types, including in nested descriptors.
Preserve null/zero/omission shapes, escaping, streaming newlines and checked
failure buffering. Concurrent mixed-header requests must remain isolated.
Ordinary integer IDs/counts, metadata and explicitly string-encoded counters
retain their current representation. CLI, protobuf, event and storage codecs
also keep their existing representations; response negotiation does not expand
request decoding. Posting input still accepts canonical unsigned decimal strings
or integer tokens within uint256; volumes retain arbitrary precision and signed
balances.

Use exact raw-token assertions at zero, 2^53 boundaries, Uint256 maximum,
arbitrary precision beyond 256 bits and negative signed values. Preserve
validation failures for invalid magnitudes and negative zero. The generated SDK
integration in `tests/sdk` exercises the 12 operations currently exposing these
types; future fields inherit the same type-based rule without a new business
classification. Bind the actual schema/generator/dependencies and prove exact
opt-in strings through generated operations. A number/string schema union or a
helper-only probe cannot prove routed SDK preservation.

## Boundaries

For index inspection (EN-2905), HTTP omission selects summary and preserves
the backend's page-size default of 100 for paginated modes. Accept `summary`,
`distinctValues`, its `distinct-values` alias, and `facets`. Reject explicitly
empty or unsupported modes with `400`/`INVALID_REQUEST`; reject supplied page
sizes outside 1..10000, empty/malformed values and uint32 overflow with
`400`/`INVALID_PAGE_SIZE`, including for summary. The routed
`TestHandleInspectIndex_InvalidModeRouting`, `InvalidPageSizeRouting`, and
`QueryOptionsRouting` cases use a valid metadata canonical, prove rejection
without backend invocation, and capture exact accepted modes, sizes, identity
and cursor, including omission and boundary controls. Retain canonical-ID and
builtin rejection coverage. The HTTP range alone is not evidence of a service
gRPC cap; proto3 zero is unspecified and the omitted enum selects distinct
values. Service scan policy and query correctness belong to the read path.

EN-2783 pins HTTP auth response media types and reachable write-exhaustion
declarations. Use `TestOpenAPI_NumscriptAuthentication` and
`TestOpenAPI_WriteResourceExhaustion` as registered-router oracles, including
anonymous/disabled authentication, local and reconstructed domain errors, and
the 24 non-bulk Apply paths. Require actual regenerated SDK operation fixtures
for bearer transmission and preserved failure details. Primitive plain-text
auth bodies and JSON controller bodies are different media types; a generator
limitation must be disclosed rather than hidden with an inaccurate schema.
Disk gating can clear; sequence exhaustion is permanent. Neither 429 supplies
`Retry-After`. Reads, prepared-query execution and terminal audit-sequence
failure do not establish ordinary request-level 429 reachability. Bulk envelope
corrections remain EN-2782's separate scope; authentication internals remain
excluded from this audit.

For unary peer-connection interruption (EN-2212), the observable contract is
specific: `grpcerr.Conn.Invoke` converts the exact bare grpc-go close status
only when the local connection is shut down and the caller context is live.
Check actual pool removal/replacement, caller cancellation, a peer-authored
lookalike on a live connection, and structured/unknown statuses separately.
The wrapped connection must expose the state of the connection used for
`Invoke` through `GetState()`; the production call site passes a raw
`*grpc.ClientConn`. A wrapper without that state receives reconstruction only.
The post-invocation shutdown check is
not atomic with status delivery: an identical bare peer-authored close status
received just before local closure is also normalized. Preserve the deterministic
`TestConn_ServerStatusBeforeLocalShutdown` control for this attribution limit;
do not interpret shutdown as proof of status origin.
`internal/adapter/grpcerr/conn_cancellation_test.go` provides these controls.
`tests/antithesis/workload/internal/client_transport_test.go` adds a real Ledger
commit before response loss, preserving the native retry control and separately
testing the current `NewGRPCConn` factory. Its maintenance case, a retry-forever
client, retries through the window rather than reporting the rejection, so the committed write surfaces
as the success it earned once the gate reopens. The actual
forwarded close error must satisfy the workload's ambiguity predicate, pinning
both boundaries without deriving the expected wire text from production code. The companion
`client_transport_controls_test.go` checks terminal statuses and cancellation
through that factory. These checks establish keyed recovery, not non-commit or
safe unkeyed replay; those identities remain owned by
`idempotency-retries-partial-failures`.

The broad adapter and CLI globs locate request builders, shared encoders and
existing tests. They do not authorize auditing every subsystem reachable from
those directories. Bucket, Cluster and Restore service request/response adapters
are in scope; snapshot/file transfer, restore execution, membership algorithms,
CLI self-upgrade, local credential stores and deployment orchestration are not.
Follow an outside caller only far enough to establish boundary reachability or
disprove a hypothesis, loading its routed documentation when needed.

| Neighbor or workstream | Ownership and deduplication |
| --- | --- |
| `accounting-invariants` | Owns amounts, balancing and business outcomes. This domain owns faithful conversion of supported values, without re-proving the accounting rules. |
| `idempotency-retries-partial-failures` | Owns keys, attempt identity, commit/acknowledgement ambiguity, bulk atomicity and retry safety. This domain owns error/status presentation, including the shape of bulk errors. Cancellation never proves noncommit. |
| `read-consistency-projections` | Owns consistency-mode propagation, barriers, query semantics and pagination under concurrent writes. This domain owns syntactic list options, stream completion and the page-token trailers (`x-next-cursor`, `x-previous-cursor`) a paged stream derives from its delivered rows; route a lost consistency mode to the read domain. |
| `concurrency-lifecycle-shutdown` | Owns goroutine/resource ownership and shutdown. This domain owns propagation of an existing request context and the observable stream terminal contract. |
| `raft-membership-leadership`, `process-boundary-recovery`, `persistence-restore-replay` | Own consensus, process recovery and persisted formats/restore. A Cluster/Restore RPC conversion may be in scope; the operation's safety, file transfer and recovery are excluded. |
| `operator-reconciliation-durability` | Owns operator parsing of CLI output and reconciliation consequences. Trace a CLI representation change to establish its consumer contract, but deduplicate the resulting controller failure with that domain. |
| `test-reachability-enforcement` | Owns repository-wide test wiring and vacuity. This domain must still show its own probes reach the intended adapter. |
| Proposed `configuration-startup-contracts` ([PR #1972](https://github.com/formancehq/ledger/pull/1972)) | Owns server parameter sources, defaults, startup validation and consumer wiring. This domain owns request/response behavior under the installed adapter limits, not how startup selects those limits. |
| Authentication and credentials | Authentication internals, JWT trust/audience, credential storage, rotation and redaction are excluded. Use route/interceptor guards to establish reachability without re-auditing them. The protocol-revision declaration is independently in scope. |

## Existing work to check before reporting

This is a discovery snapshot from 2026-09-10 at base
`50dc4dcdca24a61d000294e7664c4172df721d81`, not a permanent claim about PR status.
Refresh the target, open PRs and available prior audit results in the trusted
outer preparation before a later run. Pass relevant deduplication context to
the read-only worker; a provider must not launch orchestration or publish Jira.

- [EN-1980 / PR #1953](https://github.com/formancehq/ledger/pull/1953): active
  forwarded error boundary work, including lazy cursor/status behavior. Do not
  restate that implementation as a new defect.
- [EN-1790 / PR #1947](https://github.com/formancehq/ledger/pull/1947): already
  merged LedgerLog JSON alignment. Use the merged output contract, not the old
  wrapper shape; any later finding needs a distinct remaining mechanism.
- [EN-1926 / PR #1962](https://github.com/formancehq/ledger/pull/1962): active JWT
  audience validation, excluded with authentication internals.
- [PR #1963](https://github.com/formancehq/ledger/pull/1963) and
  [PR #1970](https://github.com/formancehq/ledger/pull/1970): active credential
  redaction/typed projection work, excluded from this audit campaign.
- [EN-1829 / PR #1957](https://github.com/formancehq/ledger/pull/1957) and
  [EN-1954 / PR #1952](https://github.com/formancehq/ledger/pull/1952): active
  metadata limits and mutation-RPC reshaping. Check their current scope before
  reporting a boundary-limit or endpoint mismatch.

The historical duplicate `400` mapping under `/v3/{ledgerName}/promote` is a
requalification question. Inspect raw YAML, the actual loader's behavior and
client/schema consequences, then history and active work. Do not label it a
confirmed runtime defect or fix it as part of preparing this manifest.

Use stable ids `api-boundary-contracts/<root-cause-name>` and deduplicate by the
mechanism and required correction, not by transport or symptom. Where ownership
belongs elsewhere, record the handoff/overlap without publishing a second
finding. If backlog/history is unavailable, state that deduplication limitation;
do not claim it was checked.

## Validation and later execution

`TestAPIBoundaryManifestContract` uses the actual launcher's manifest validation
in a deliberately dirty disposable fixture. It must reach the clean-HEAD gate,
then stop before any provider runs; a malformed manifest must fail earlier.
The same test checks that scope globs match files and related documents exist.
It validates the manifest contract, not product correctness.

A later authorized campaign runs `bash scripts/ai-audit api-boundary-contracts`
at a clean exact HEAD, followed separately by
`bash scripts/ai-audit-challenge <audit-result>` at that same HEAD. Jira handling
and reproducer/product changes remain separate authorized work. Dynamic checks
in this manifest are proposals: record which actually ran and their limitations.
Use existing disposable fixtures and the pinned validation/cache workflow, with
no live user services or cleanup of another task's artifacts.

## Atomic creation metadata (EN-2686)

Initial creation metadata is one CreateLedger operation, returned in the HTTP 201 ledger envelope; supported scalar values retain exact integer precision, HTTP null entries are omitted, and schema declarations govern query indexing rather than write acceptance.

Does initial metadata survive HTTP decoding, the service request and order unchanged, including integers beyond JSON float precision, omitted/null inputs and a schema type mismatch? Do malformed keys/scalars reject before any ledger is created?

See [the creation contract](../architecture/subsystems/api/atomic-ledger-creation.md) for the authorized semantics and regression evidence. Treat HTTP response/forwarding, actual FSM readback, keyed replay, nonempty checkpoint-plus-delta restore, and primary-projection tampering as separate evidence oracles; a helper-only test does not prove every boundary.

## HTTP event sink mutations

Trace POST and DELETE under `/v3/_/events-sinks` through the registered
`ledger:WriteOps` route guards and unsigned Apply requests. POST accepts a
direct protobuf JSON configuration with a name and one sink variant; verify
full-body limits, strict JSON decoding and field preservation for all five
variants. Its acknowledgement includes only the committed name. GET retains
its existing envelope and config/status shape. DELETE decodes the path segment
once and forwards optional `controllerId` exactly as a gRPC removal
precondition. An omitted identity remains unconditional removal. Check
not-found, duplicate-name, batch-size and controller-mismatch status/reason
presentation with local and forwarded errors. Handler tests and the route-scope
exhaustiveness probes establish conversion and authorization; HTTP sink E2E
tests establish committed lifecycle outcomes. Retained-key replay belongs to
`idempotency-retries-partial-failures`; verify header transmission here without
inventing a new retry identity or an external-delivery guarantee.

## LedgerInfo wire and SDK oracle (EN-2780)

Require camelCase keys, short explicit enum defaults, typed metadata/schema, REST
segment discriminators and unconstrained null, RFC3339 timestamps, and populated
mirror state/counters at zero. Counter wire values are decimal strings with
OpenAPI string/bigint; a generated TypeScript number for integer/bigint can round
large counters despite successful decoding. Exercise actual generated list/get
operations with >2^53 and uint64 maximum, alongside schema validation and the
codec/routed HTTP fixtures. Read mirror schemas are nested, while flat create
fixtures must still be accepted. Rewrite-rule read models must expose CEL
`valueExpr` on transaction and account metadata actions across all supported
scopes, accept protojson's omitted empty replacements (both rewrite-address and
account-metadata replacement entries), and preserve explicit empty literal
oneofs. Get/list fixtures in `internal/adapter/http/testdata/ledger_info_rewrite_*`
are shared routed-response and generated SDK operation inputs. Marshal failure must return sanitized 500 before
headers. The confidentiality oracle belongs to sensitive-data-exposure-boundaries.
See [LedgerInfo](../architecture/subsystems/api/ledger-info.md).

## Cluster node identities

Cluster state projections, targeted peer requests, per-node enrichment,
leadership transfer and primary/secondary store-metrics targets must preserve
uint64 IDs, including colliding low 32 bits
(`1` and `4294967297`). Verify the real Node.GetClusterState projection against
nested Raft status and capture actual adapter and Cobra RPC targets. Include
small-ID controls, uint64 maximum, and pre-dial rejection of invalid, zero or
overflowing transfer arguments. Metrics target zero remains local; unknown
targets must not fall back to a colliding low-ID peer. Capture both metrics RPCs
through Cobra and the forwarding adapter, including uint64 maximum. The six
original cluster fields and two metrics target fields retain names,
numbers and unsigned-varint encoding without a parallel compatibility field.
This domain owns projection, parsing and routing fidelity; election correctness
and member persistence remain in raft-membership-leadership. Varint decoding
alone does not establish full-range support in old uint32 readers. Require the
client/server rollout limitation documented in the service protocol contract.
