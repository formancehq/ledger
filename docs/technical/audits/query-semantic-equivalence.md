# Query semantic equivalence: boundaries and evidence

The [native manifest](query-semantic-equivalence.json) establishes a reusable
query-engine correctness scope. The operational need is reliable query results
when an execution strategy changes. The observable requirement is equality to
an independently established result for the same supported logical query and
fixed input state. Existing temporal and adapter domains do not isolate this
property; a separate manifest makes its oracle and exclusions explicit without
adding a runner, framework or product test suite.

## Comparison preconditions and production anchors

Record ledger, target, predicate, bound parameters, mode, effective ordering,
page window, projection options, primary horizon and served index version/type
before comparing results. Freeze writes and index transitions or use an
established frozen snapshot pair. Two separately opened live requests are not
proof of the same state. ACCOUNTS folded membership has documented cross-store
exceptions; remove that difference from a semantic comparison rather than
assuming exact historical account membership from a certificate alone.

| Contract | Current production anchors and falsifiable observation |
|---|---|
| Supported plans | `internal/application/ctrl/list_entities.go` dispatches main-store-only, ascending, unfiltered descending and compiled descending reads. `internal/query/compile.go` and `compile_reverse.go` implement target guards and compilation. Identify the caller/branch actually reached; equal empty outputs do not establish reachability. |
| Boolean and range semantics | `compile.go` implements universes, AND/OR/NOT, signed/unsigned bounds and existence; `compile_and_merge.go` coalesces hardcoded same-field numeric ranges after served-type validation, against the one version a compilation resolves per index, and intentionally leaves parameterized bounds and rejected leaves unmerged. Compare both forms with independently evaluated endpoint membership and acceptance, not with the merge helper as oracle. |
| Metadata | `resolveFieldMetadataCtx`, `validateAndCoerceCondition`, `requireIndexReady` and `compileExistsCondition` in `compile.go`, plus `internal/proto/commonpb/metadata_convert.go`, anchor the served-type and null/existence rules. Primary values remain raw; derive expected indexed values independently for each finite fixture. `includeNull` includes null-derived entries, not missing keys. HTTP JSON null deletion is a separate adapter contract. |
| Iterator ordering | `internal/storage/readstore/combinator_{and,or,not}.go`, `iterator_bounded_entity.go`, `iterator_address.go` and direction-specific leaves implement sorted identities, absolute seek and union/intersection. Compare exact windows, duplicates, terminal errors and reseeks with a separately sorted expected sequence. |
| Prepared/direct | `internal/query/executor.go` loads the stored predicate, binds parameters through compilation and selects LIST or account aggregation. Direct callers normalize their own ordering: transaction list defaults cannot be equated with prepared traversal defaults. Compare effective ordering or full membership first, then verify each ordered window against its own documented direction. |
| Aggregation strategies | `executor.go` routes exactly nil filters through `AggregateAllVolumes`; non-nil filters enumerate candidates for `AggregateVolumes` in `aggregate.go`. Compare equivalent options and complete bucket identities/values, including metadata-only accounts and zero buckets. Do not assume a non-nil empty filter takes the nil shortcut. |

Prepared-query `LIST` executor inputs use the shared page-size policy: zero
selects 100 and values above 1000 clamp to 1000 before ID collection, enrichment
and response materialization. The cursor reports the effective size. Prove this
through real `query.Execute` calls, including direct/controller callers, with
more than 1000 known matches and sizes 0, 1000, 1001 and uint32 maximum. Assert
exact independently ordered windows, bounded enrichment and next/previous
continuations in both directions. Aggregation must retain all matching accounts
regardless of page size. Adapter normalization and HTTP body/query precedence
belong to `api-boundary-contracts`; this domain owns the executor bound and
fixed-state traversal after canonical typed input. Performance alone remains
excluded, but enforcing the established result-size policy is a correctness
requirement.

Metadata filtering requires a declared field and a registered locally ready
index. There is no general index-free metadata filter plan. An independent
scan/evaluator is an **oracle**, not a promised server fallback. LOGS also uses
the read projection for its universe. Compare index/no-index server strategies
only when current call paths actually support both logical forms. Unavailable
indexes, invalid targets and incompatible modes belong in the acceptance matrix,
not in a successful-result comparison.

The typed-metadata contract binds compilation to the served version's type,
including during a rewrite. Do not use the latest declaration as the oracle for
an older served version. Preparation found older lifecycle wording in
`prepared-queries.md` (FSM compilation and blanket mid-rewrite refusal); inspect
`internal/domain/processing/processor_prepared_query.go`, current validation,
`requireIndexReady` and typed-metadata documentation before relying on those
claims. This note neither fixes that adjacent documentation nor declares a
product defect from the discrepancy.

## Shared cursor and generated acceptance contracts

The scope explicitly includes `pkg/pagecursor/**` and
`internal/pkg/cursor/**`, not only their query callers. Start from canonical
decoded cursors and a frozen independently sorted sparse sequence, for example
IDs `2, 5, 9, 14, 20`, plus string-key witnesses. Derive expected windows by
slicing that sequence without calling `Page`, `Links`, `ReadReverse`, or the
production skip predicate. Trace the actual controller/executor caller and
wrapper chain; helper-only parity cannot establish consumer correctness.

Cross both query orders with forward/backward navigation, size one, exact and
partial final pages, keys equal to/between/outside matches, and a backward
cursor with no key. Require exclusive continuation, unchanged query order,
complete traversal and page round trips. An empty resumed forward page links
back to the tail; an empty resumed backward page links to the head. Follow
those links to verify the recovery window. Check one-row lookahead and an
upstream source capped exactly at page size with `MoreReporter` evidence;
trace whether each real wrapper preserves the evidence its consumer needs.
Do not treat a source cap as clean complete exhaustion.

Inject iteration failure before the first row, after a partial page and during
lookahead through supported `FilteredCursor`, `SkipWhileCursor`, `Reverse`,
`Collect` and production consumers. A non-EOF error must not become a falsely
successful complete result. This domain owns resulting fixed-state omission
or duplication; cursor resource lifetime belongs to concurrency lifecycle,
and token decoding/encoding, error mapping and stream framing belong to API
boundaries. Concurrent changes between requests remain a temporal question;
this oracle does not promise retained snapshots across page tokens.

`tools/protoc-gen-queryfilter-validity/**`, its descriptor inputs in
`misc/proto/common.proto` and generated validity code are acceptance evidence.
Independently enumerate every `QueryFilter` arm and `QueryTarget`, then compare
annotations, emitted kinds/table and typed compiler verdicts with a separately
justified expected target matrix. Include nested predicates and deliberately
valid-on-none arms. Shared generated output can make all consumers agree on a
wrong annotation, so exhaustive generation is not an independent semantic
oracle. Temporary descriptor fixtures must reject missing declarations,
contradictory declarations and unknown target numbers; explicit valid-on-none
remains distinct from an accidentally absent declaration. Transport parser
acceptance stays with `api-boundary-contracts`; this domain owns canonical
engine acceptance. See [protobuf generation](../contributing/protobuf.md).

These are proposed audit checks, not checks executed by editing this contract.

## Ownership and exclusions

Assign a root cause by the violated contract and required correction, not by
shared filenames. Carry cross-domain evidence as a handoff with one finding
identity, never duplicate the same correction across domains.

| Overlap | Owner and boundary |
|---|---|
| Fixed-state predicate selection, compiler rewrites, iterator membership/order, parameter binding | `query-semantic-equivalence` owns the engine-semantic mismatch after equivalent inputs/state are established. |
| ReadIndex, stale/strong modes, snapshot selection, projection lag, event visibility, rebuild/readiness publication, concurrent pages | `read-consistency-projections` owns temporal correctness. Here readiness is a precondition and query rejection is checked; incorrect index population/certification belongs there. |
| HTTP/text/JSON/protobuf/CLI parsing, presence, numeric representation, error codes, stream framing | `api-boundary-contracts` owns translation before engine input and presentation after output. Here compare canonical typed engine inputs and internal result/error preservation. Textual and structured filter expressiveness need not be identical. |
| Conservation, amount arithmetic/overflow, authoritative balances and volumes | `accounting-invariants` owns arithmetic and business truth. Here own wrong candidate selection, bucket omission/duplication or differing result shape attributable to a query plan. |
| Index switch durability, checkpoint/restart recovery | `persistence-restore-replay` owns durable state loss; temporal certification remains with read consistency. No recovery campaign here. |
| Cancellation/shutdown/resource ownership | `concurrency-lifecycle-shutdown` owns lifecycle defects. Local iterator error propagation is in scope only when it produces a falsely successful semantic result. |
| Model-driver generation, reachability metrics and coverage expansion | The existing model-driver work owns implementation and coverage. Tests are evidence leads, not a second campaign to rewrite that harness. |

Performance alone, new query operators, undocumented SQL semantics, cross-ledger
query design, prepared registry mutation durability, wire equivalence and
accounting recomputation are excluded. Unsupported or ambiguous product
expectations remain audit questions, not bugs presumed by this manifest.

The log-date streaming leaf relies on a domain ordering guarantee: the FSM
assigns each per-ledger log an increasing ID and its monotone effective date,
including mirror ingest and skip logs. Audit evidence must check equal-date
ties, both directions, absolute reseeks and composition against independently
expected IDs. Mirror transaction `inserted_at` instead preserves the source
v2 date; it remains on the general materializing path. Do not infer its
ordering from the current ledger mode.

## Independent evidence and deduplication

The shared leaf resolvers in `resolve_address_match.go` and
`resolve_leaf_conditions.go` own direction-independent address, reference,
asset, metadata point/existence and ID preparation. The public compiler tests
`compile_leaf_resolution_test.go`, `compile_metadata_resolution_test.go` and
`compile_id_resolution_test.go` assert independent identities for these
families. Independent refusal assertions in these files cover address and
reference parameter failures, readiness precedence and address-role gates;
the metadata and ID files assert identities only. The separate
`compile_error_parity_test.go` checks both directions against expected refusal
substrings and identical messages for target/shape/depth, metadata schema/type
and index-readiness failures; it does not establish comprehensive metadata or
ID parameter-refusal coverage. Preserve readiness-before-parameter error order,
role-specific address gates, served metadata versions, asset pin exclusion,
missing point results and half-open ID bounds when challenging these helpers.
Directional iterator construction and page oracles remain separate evidence.

Exact transaction address matching requires current account attributes and uses
retained role mappings for resident accounts. Require independently expected
funding/draining IDs after a partial asset/color purge or a drain that leaves
metadata, for ANY, SOURCE and DESTINATION in both directions, including
parameters, boolean composition and page-size-one traversal. Fully absent
accounts must return empty exact and prefix address results. Keep reference and
indexed transaction-metadata controls to demonstrate that transaction history
itself remains retained. The independent oracle must distinguish historical
mappings from current asset exclusions: `tests/oracle/model_test.go` and the
workload `queries_test.go` cover resident EPHEMERAL cells, steady TRANSIENT
negatives, grandfathered TRANSIENT drains, and the current-account gate.
Index population and repair readiness belong to
`read-consistency-projections`.

For each tested family, enumerate a non-empty target universe with positive and
negative witnesses and the exact expected identities/cardinalities. Include
intentional empty results only alongside witnesses proving the leaf was seeded.
The oracle must not delegate predicate truth, coercion, range normalization or
ordering to the production helpers under audit. Comparing two plans which share
those helpers is supplementary evidence only. Verify target and branch selection;
for pagination compare every page, continuation and the complete traversal.
A same-total aggregate comparison is insufficient without bucket identities.

Existing `internal/query/compile_reverse_parity_test.go`,
`compile_error_parity_test.go`, `compile_bounds_test.go`,
`compile_and_merge_test.go`, `params_test.go`, `executor_page_size_test.go`,
`executor_aggregate_test.go` and
`internal/storage/readstore/iterator_conformance_test.go` are starting points,
not evidence that the whole matrix ran. A future report records exact commands,
SHA, fixture/state, plan selection, expected/actual output and limitations. Label
proposed checks as unexecuted. Missing tests alone are not findings.

Preparation inspected the manifest inventory and open Ledger PRs on 2026-09-11
at base `12f48ef84e7a82c2a9e96784dd069bd9bf320f36`:

- [#1659](https://github.com/formancehq/ledger/pull/1659) owns model-driver query
  coverage, filters, indexes and typed metadata. Do not edit, manage or duplicate
  that PR. Its historical server fixes are regression leads, not fresh findings.
- [#1964](https://github.com/formancehq/ledger/pull/1964), stacked on #1659, owns
  prepared-query model coverage and includes query error/index-builder changes.
  Inspect current commits before attributing a new defect; do not assume its
  proposed changes exist on this audit's base.
- [#1931](https://github.com/formancehq/ledger/pull/1931) concerns index promotion
  durability; [#1954](https://github.com/formancehq/ledger/pull/1954) concerns
  query-checkpoint materialization. Those corrections belong to neighboring
  domains, even when the symptom is an incorrect query result.
- [#1981](https://github.com/formancehq/ledger/pull/1981) concerns HTTP metadata
  integer precision and [#1957](https://github.com/formancehq/ledger/pull/1957)
  metadata size limits; neither is an engine-plan equivalence campaign.

This is a dated ownership inventory, not proof of PR correctness or merge state.
The trusted outer workflow must refresh open work and supply accessible qualified
reports/backlog history before the later campaign. No Jira/history completeness
is claimed here. The leaf auditor never launches orchestration or publishes
issues. Use stable `query-semantic-equivalence/<root-cause-name>` ids; preserve
historical rejected/fixed hypotheses for deduplication and prove current-SHA
reachability before calling a historical defect a new finding.

## Manifest validation and later campaign

Use the existing launcher predicate and verify all scope globs/document paths.
The documented `TestAPIBoundaryManifestContract` pattern in
`scripts/aiaudit/manifest_test.go` may be run from an untracked temporary test
copy with only its test name and audit ID changed: the valid copied manifest
must reach the dirty-HEAD guard and malformed input must fail validation,
without invoking a provider. Remove that temporary copy afterwards. Run
`bash scripts/agent-check` and
`AI_REVIEW_BASE_SHA=<exact-base-sha> bash scripts/agent-check-pr` with the managed
shared cache environment. Record results in the PR; these validate the artifact,
not query-engine correctness.

Only a separately authorized task after merge runs
`bash scripts/ai-audit query-semantic-equivalence` at a clean exact HEAD, followed
by `bash scripts/ai-audit-challenge <audit-result>` at the same HEAD. First-pass
findings remain hypotheses. Product fixes, reproducer implementation and Jira
publication are separate authorized phases. Preparing this manifest runs no
product audit or dynamic query campaign.
