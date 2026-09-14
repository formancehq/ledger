# Mirror ingestion audit evidence and ownership

The [manifest](mirror-ingestion-contracts.json) scopes the conversion of Ledger
v2 history into a v3 mirror ledger and the transition to normal writes. Existing
audits cover the correctness of accepted accounting operations, durable replay
and worker lifetimes; they do not provide a focused inventory of source paging,
translation and configured filtering. This domain adds that inventory to the
native workflow without changing product guarantees, schemas or runners.

## Evidence contract

For each hypothesis, establish the exact audited SHA and all of the following:

1. The supported source version/schema, source ledger/bucket, initial applied
   boundary and exact rewrite configuration. Translation/progression hypotheses
   need nonempty source history; observed-zero/idle reporting checks may require
   an empty source. Distinguish source log IDs, transaction IDs and destination
   log IDs.
2. The real source request/query and returned pages, including continuation,
   ordering, errors and any deliberate source gaps. Mocked pages prove only the
   behavior they model; they do not establish the external v2 API contract.
3. The expected field/variant mapping before and after the configured rewrite,
   with a specific authoritative contract, implementation symbol and relevant
   test. Record intentional omissions and filtering alongside retained fields.
4. The actual translation, proposal and apply path, guards crossed, speculative
   mutations, confirmation point and observable destination effect. State the
   residual state after a failure and whether a fresh attempt repairs it.
5. The violated promise, concrete impact, counterevidence and existing test gap.
   Supply a reproduction plan with exact inputs and a distinguishing assertion;
   do not treat an unexecuted plan as test evidence.

Use the native P0–P3 severity scale. P0/P1/P2 require a concrete reachable failure
path, not a missing test or scary-looking conversion. Ambiguous support for a
source version, unknown log variant, optional field or cutover guarantee becomes
an **audit question**, not a new product requirement. Every first-pass finding
remains a hypothesis until the independent challenge pass attempts to disprove
it at the same exact HEAD.

Fidelity is relative to the configured projection. A rule may rename accounts,
replace or remove metadata, coerce types or drop transactions. Source/destination
balance inequality after such a transformation does not establish a defect.
Even without rewriting, the mirror does not promise source-hash reconciliation.
Separate unsupported source data from supported data changed silently; establish
both the expected conversion and the production path before claiming corruption.

## Source map and contract qualifications

All paths below exist at the preparation base. Resolve symbols again at the
audited SHA; historical line numbers in subsystem docs are not stable evidence.

| Surface | Current source and evidence seeds | Contract to establish |
| --- | --- | --- |
| Source interface and HTTP | `internal/adapter/v2/source.go`, `source_http.go`, `source_http_test.go`, `types.go` in that directory | `FetchLogs` continuation and `GetLatestLogID` head observation; HTTP `after`, page size, returned order and `hasMore`. |
| PostgreSQL | `internal/adapter/v2/source_postgres.go`, `source_postgres_test.go` | `_system.ledgers` bucket discovery, per-ledger filtering, strict `id > afterID`, ascending order, lookahead and typed date normalization. Authentication and pool shutdown are excluded. |
| Translation | `internal/adapter/v2/translator.go`, `translator_test.go`, `translator_rewrite_test.go` | Four supported business variants; exact amount/target/metadata conversion, source dates, synthesized gaps and configured rewrite output. |
| CEL | `internal/adapter/v2/celrewrite/`, `internal/application/admission/validate_order.go`, `misc/proto/common.proto` | Rule compilation, scope/action oneofs, order/stop, address and metadata actions, drop, expression failure policy and typed coercion. |
| Worker | `internal/application/mirror/worker.go`, `worker_test.go`, `worker_rotation_test.go`, `manager.go` | Applied-boundary resume, speculative translation/prefetch, confirmation and error invalidation; manager is context for the mode transition only. |
| Apply and promotion | `internal/domain/processing/processor_mirror.go`, `processor_mirror_test.go`, `processor_apply.go`, `misc/proto/raft_cmd.proto` | Mirror mode gate, source prefix, skipped transaction IDs, imported fields, promotion and the maintenance whitelist in `isMirrorSafeApply`. |
| Progress | `internal/query/mirror.go`, `mirror_test.go`, `internal/infra/state/machine_technical_updates.go` | Applied cursor versus observed head and error reporting; reporting does not authorize consumption. |
| Destination integration | `tests/e2e/cluster/mirror_test.go` | Existing HTTP fixture, import, rewrite/drop, source insertion dates and promotion followed by normal writes. |

Read [mirror.md](../architecture/subsystems/events-mirror/mirror.md) and
[cel-rewrite.md](../architecture/subsystems/events-mirror/cel-rewrite.md)
alongside these sources. At preparation time, several passages need qualification:

- `FetchLogs` promises oldest-first rows, while `GetLatestLogID` describes the
  default first HTTP page as newest-first. Neither that pair of comments nor
  ordered HTTP test fixtures proves actual v2 pagination. Establish the supported
  upstream contract before treating a first-page mismatch as a product finding.
- The mirror doc's protobuf example still contains a removed `reserved` slot
  and an old rewrite field number. Read `misc/proto/common.proto`; historical v3
  wire compatibility is not an audit requirement during pre-release.
- CEL prose saying actions never evaluate CEL predates `value_expr`. The current
  implementation in `value_expr.go` and its tests establishes expression
  evaluation during leader-side translation, before proposal, not follower apply.
- `match` runtime errors skip a rule; `value_expr` runtime errors fail a batch.
  Declared type conversion can intentionally produce a null retaining the source
  string. `TestApply_MatchErrorDoesNotStallBatch`,
  `TestValueExpr_RuntimeErrorFailsBatch` and
  `TestTypedMetadata_UnparsableProducesNull` are distinct evidence seeds.
- CEL documentation forbids depending on proto-map iteration order. Repeated
  leader-side translation and deterministic replay of already-rewritten accepted
  bytes are different claims. Do not infer replica divergence merely from an
  unstable user expression.
- Source head is the observed highest source ID, although some names say
  "count". It need not equal physical row count. The current progress reader
  keeps a zero-head source in `SYNCING`; do not invent an empty-source
  `FOLLOWING` guarantee or interpret status as source quiescence.

These qualifications record evidence limits, not confirmed product defects or
authorization to fix adjacent documentation in this manifest PR.

## Translation, progression and cutover

`TranslateBatch` converts `NEW_TRANSACTION`, `SET_METADATA`,
`REVERTED_TRANSACTION` and `DELETE_METADATA`. Missing source log IDs are explicit
synthetic `FillGap` orders; unknown log types currently also become `FillGap`
(`TestTranslateBatch_UnknownLogType_FillGap`). A configured drop preserves the
source log ID and carries the created/compensating transaction ID in
`SkippedTransactionIds`. The FSM advances `NextTransactionId` past those values,
not by the number of skipped entries. Do not equate log gaps with transaction
gaps or assume that dropping a transaction automatically drops later metadata
or reverts referring to it. If that relationship is unspecified, ask a question.

Created/reverted entries preserve source log date separately from business
timestamp. Their resulting transaction `insertedAt` and `updatedAt` use the
source date; the enclosing v3 ledger log still uses its apply scope date.
Business timestamp fallback has its own implementation in the transaction
handlers. Metadata updates and fields absent from a particular variant require
their own contract; this domain does not assert lossless copying of every v2
field into every v3 record.

The worker initially reads both `LastMirrorV2LogId` and `NextTransactionId` from
`LedgerBoundaries`. It sets its speculative `nextTxID` after translation, before
building/proposing, and may fetch the next page while awaiting confirmation.
Only after Raft acceptance and successful FSM application does it advance
`lastAppliedV2LogID` and retain that prefetch. On a batch error, the outer
`processLogs` path reports the error and marks the boundary snapshot unloaded;
the next attempt rereads authority. A helper-only call to `processBatch` does
not exercise that retry path. A cancelled wait does not prove noncommit.

The FSM guards the contiguous applied source prefix before dispatching a fresh
entry: replay is intentionally a no-op, zero/gaps reject, and successful fresh
entries advance the boundary through the normal proposal writes. Source-head
and status technical updates are reporting projections. A standalone idle
publication can clear an error without new source logs; it cannot advance the
ingestion boundary. Use
[audit versus technical state](../architecture/audit-vs-technical-state.md)
to distinguish these roles, and load routed FSM/storage documentation if a
hypothesis requires tracing rollback or durable writes beyond this boundary.

Promotion sets the ledger to `NORMAL`, clears its source and emits the named
promotion log. Subsequent mirror ingest rejects at the mode gate; normal writes
continue from retained boundaries. The manager reconciles the committed change.
No source-freeze, zero-lag admission check or atomic v2/v3 handoff is established
by `processPromoteLedger`. Manual cutover coordination is not an automatic
product guarantee. Likewise, "mirror read-only" does not exclude the explicit
schema/index/account-type maintenance operations allowed by `isMirrorSafeApply`.

## Ownership and deduplication

The path globs locate evidence; they do not authorize auditing all code in shared
files. Follow outside calls only to establish reachability or counterevidence,
loading their routed docs. Classify by the mechanism requiring correction:

| Neighbor or workstream | Boundary |
| --- | --- |
| `accounting-invariants` | Owns conservation, balance/volume arithmetic, general sequence safety and deterministic business application. Mirror owns source-to-order field/identity mapping and configured transformation. Correct import can still expose an accounting defect; balanced results do not prove import fidelity. |
| `persistence-restore-replay` | Owns loss or misreconstruction after an accepted correct order, checkpoints, restart and restore, including reconstructed mirror boundaries. Inspect `tests/e2e/cluster/restore_mirror_test.go` and the incremental restore contract as counterevidence; do not run a restore campaign here. |
| `idempotency-retries-partial-failures` | Owns generic proposal/ack ambiguity, atomicity and retry mechanisms, including existing mirror replay protection. This domain uses those protections to evaluate source continuation and speculative worker state; a shared guard defect is one finding under its owner. |
| `concurrency-lifecycle-shutdown` | Owns worker Stop/join, resource lifetime, leadership fencing and stale generation callbacks, including EN-1997. This domain owns ordered ingestion/promotion effects, not shutdown timing. |
| `api-boundary-contracts` | Owns public HTTP/gRPC/CLI codecs, create/promote request building, statuses and wire envelopes. This domain owns consuming the external v2 source and translating it into mirror orders. |
| `configuration-startup-contracts` | Owns static server parameter selection, defaults, validation and wiring. This domain owns the semantics of persisted, selected mirror source and rewrite rules during ingestion, including rewrite compilation. |
| `operator-reconciliation-durability` | Owns Ledger CR reconciliation, pod-exec, status/spec identity and managed indexes. Merely creating a mirror through the operator does not move that work here. |
| `read-consistency-projections` | Owns query/index consistency and client pagination. External source pagination is here; a correct imported value served incorrectly by the readstore belongs there. |
| `raft-membership-leadership`, `process-boundary-recovery` | Own consensus, leader activation and process crash/reopen mechanisms. They remain assumptions to establish, not extra campaigns in this domain. |
| Parallel `event-delivery-contracts` and `authentication-authorization-boundaries` preparation | Outbound delivery, authentication, authorization and credential handling are excluded. Do not require these proposed manifests to exist as dependencies. |

Credential storage, redaction, OAuth2/IAM issuance, TLS authentication and secret
projection are excluded even when they share a source adapter file. Also exclude
generic CDC support, source-hash verification, performance-only tuning, invented
resource budgets, historical v3 format compatibility and coverage-only findings.

### Existing work snapshot

Preparation checked the manifest directory, open Ledger PRs and Jira mirror
ticket summaries/statuses on 2026-09-10 at base
`f76499637a5af0d92a2cfabbdacf8247256fa4bb`. No equivalent mirror manifest was
present. This snapshot is not a permanent statement of backlog or branch status:

- [EN-1997](https://formance-team.atlassian.net/browse/EN-1997), PostgreSQL mirror
  initialization blocking shutdown, was in Backlog and is explicitly excluded.
- [EN-1513](https://formance-team.atlassian.net/browse/EN-1513), applied-boundary
  resume; [EN-1550](https://formance-team.atlassian.net/browse/EN-1550), replay
  guard; [EN-1854](https://formance-team.atlassian.net/browse/EN-1854), source
  insertion dates; and [EN-1773](https://formance-team.atlassian.net/browse/EN-1773),
  restore-resume/lifecycle coverage, were Done. Current code/tests are required
  counterevidence, not a reason to presume every related scenario correct.
- [EN-1776](https://formance-team.atlassian.net/browse/EN-1776), rebuilding the
  mirror boundary during restore, was Done and belongs to persistence/restore.
- [EN-1251](https://formance-team.atlassian.net/browse/EN-1251), historical
  mirror preload coverage, was Cancelled. Inspect current declared coverage
  before reviving the old claim; do not infer a defect from the title.
- The EN-1632 credential work had open
  [PR #1963](https://github.com/formancehq/ledger/pull/1963),
  [#1970](https://github.com/formancehq/ledger/pull/1970),
  [#1977](https://github.com/formancehq/ledger/pull/1977) and
  [#1979](https://github.com/formancehq/ledger/pull/1979);
  [EN-1634](https://formance-team.atlassian.net/browse/EN-1634) and
  [EN-1635](https://formance-team.atlassian.net/browse/EN-1635) were in review.
  This domain must not duplicate their credential/projection scope.
- [PR #1957](https://github.com/formancehq/ledger/pull/1957) (EN-1829 metadata
  limits) and [PR #1952](https://github.com/formancehq/ledger/pull/1952)
  (EN-1954 mutation RPC reshaping) were open. Refresh their scope before claiming
  an ingestion validation or entry-path discrepancy.

The trusted outer preparation must refresh PRs, tickets and available previous
raw/qualified artifacts before a later audit. This preparation did not review
every historical report. Record inaccessible history as a deduplication limit.
Use stable IDs `mirror-ingestion-contracts/<root-cause-name>` and compare cause
and required correction, not just title, affected variant or source transport.
Record an overlap/handoff in inspected areas or residual risk instead of emitting
a second finding whose corrective mechanism belongs to another domain.

## Diagnostics and later execution

Manifest preparation validates shape and actual paths without an audit provider.
The existing `TestAPIBoundaryManifestContract` pattern validates a copied manifest
in a deliberately dirty disposable fixture: a valid manifest reaches the clean
HEAD gate, a malformed one fails earlier, and neither reaches a provider or
creates an audit artifact. Scope globs must match files and related docs must
exist. Reusing that test through a temporary Go overlay with only the audit ID
changed exercises this manifest without changing a runner or a shared test file.

Dynamic checks in the manifest are options for a later authorized campaign.
Providers may run existing tests and allowed temporary diagnostics; they must
not edit tracked files, implement reproducers, access live customer sources or
launch audit/challenge/Jira workflows. Use pinned Nix tools and the repository's
managed validation cache; do not purge shared caches or modify other worktrees.
Inspect test build tags and external dependencies before running a selection.
In particular, the PostgreSQL configuration e2e does not prove actual database
ingestion, and a helper SQL-string assertion does not prove row iteration.

After separate authorization, the trusted outer workflow selects a clean exact
HEAD, runs `bash scripts/ai-audit mirror-ingestion-contracts`, and separately
runs `bash scripts/ai-audit-challenge <audit-result>` at that same HEAD. Provider
leaf boundaries and challenge qualification remain those of the native
contracts. Jira preview/publication, reproducer implementation and product
corrections are separate decisions. Preparing or merging this manifest does
not authorize those actions.
