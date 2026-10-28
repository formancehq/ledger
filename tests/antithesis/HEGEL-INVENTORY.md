# Hegel applicability inventory

Status: implementation inventory, inspected on 2026-10-22. All entries below
have a classified generation boundary. See [validation evidence and remaining
limits](HEGEL.md) for test, runtime and review results. Classification alone does
not prove cloud coverage or that every reachability probe has been satisfied.

Scope: all **59 main Composer entrypoints**, the **1 model Composer entrypoint**,
and all **26 registered scenario blocks**. Paths below are relative to the
repository root. Each main table row names the exact directory and `main.go`;
the row itself is the directory checklist entry. The Dockerfile discovers
`bin/cmds/<template>/<command>` directories, not a hand-maintained driver list.
`main` and `model` are separate Composer templates and must remain separate.

## Classification and common requirements

- **S — structured business input:** use Hegel at the actual business/scenario
  choices, including deliberate invalid shapes and state-dependent selections.
  One Hegel seed feeding an opaque PRNG does not cover these choices.
- **W — witness-only:** the driver writes or selects data to arm a particular
  oracle or recovery check. Use shared Hegel generators for variable witness
  payloads where applicable; retain fixed markers and relational probes. Do not
  replace the specialized oracle with a generic generated-transaction property.
- **O — operational/oracle-only:** no generated business payload is required.
  Keep the existing scheduling, topology, fault execution, and independent
  checks. Hegel may describe an existing bounded operational menu, but must not
  take over chaos control. Each O row states why a business generator adds no
  relevant coverage.

These classes describe responsibility, not migration status. An S or W row can
still contain legacy draws; an O row can legitimately use Hegel for a menu.
Technical unique run IDs, dedicated ledger prefixes, reference/key identity
allocation, connection choice, and deadlines remain distinct from business
choices. Keep intentionally small shared collision pools small. Keep private
namespaces private. Shrinking a unique identity toward a shared constant must
not manufacture unrelated interference.

Preserve every existing Antithesis assertion message/identity, kind, polarity,
registration, hit/non-hit emission, reachability condition, and failure path.
Keep transport errors, definitive business rejections, ambiguous commits,
cancellation, EOF, readiness lag, and concurrent deletion distinct as the source
does. A skipped/inconclusive observation must not become a passing observation.
Do not strengthen an existing weak check in this migration and then claim it was
already covered: e.g. the simple query drivers currently check response presence,
not every returned row's filter membership.

### Domains and relationships that must survive

1. `internal/random.go` and `internal/generated.go`: the common amount domain is
   **all uint64 values, including 0 and MaxUint64**, converted losslessly to
   `big.Int`/Uint256. Retain values beyond signed int64 and exact JSON integers.
   Postings have 1–2 entries; addresses are `world` or `users:0..31`; assets are
   `USD/2`, `EUR/2`, `COIN`. Self-transfers, repeated accounts and zero amounts
   remain possible. Metadata has 0–2 attempted entries with decimal key/value
   draws in 0..998; duplicate keys can reduce the final map size.
2. `parallel_driver_transactions`: keep single/bulk and postings/Numscript
   branches. Bulk sizes remain **1..5000**, with the existing truncated geometric
   transform (`p=0.001`); Hegel bias may change frequencies, but a default short
   list strategy must not erase large-bulk stress. Keep per-request log count,
   volume checks, and the sampled committed transaction readback.
3. Deliberate 256-bit failures remain separate from ordinary uint64 traffic:
   `parallel_driver_definitive_errors` uses 1, 2^63, and **2^256−1** against an
   unfunded source; model `overflowTransaction` uses **two 2^255 postings** to
   overflow the running Uint256 volume. Do not cap either case to uint64 or
   remove it by generating only valid inputs.
4. Stale-read probes remain **unit increments** with a unique reference per
   attempt. Their sound bound is `0 <= balance <= attempted`, not `acked`;
   `input == balance`, `output == 0`. Arbitrary generated amounts invalidate
   this count-based oracle. Round counts may vary; the increment is fixed.
5. Reference and idempotency conflicts reuse the same identity and must retain
   their intended payload relationship. Idempotent replay sends identical
   business bytes; conflict sends a **guaranteed different** payload, derived
   with a disjoint destination/asset or an explicit safe mutation. Two independent
   draws are not proof of inequality. Preserve original/retry/duplicate/fresh
   logical operation keys and the acknowledged predecessor that arms each check.
6. Preserve fixed scaling sequences, waits and cooldowns. In particular
   `scaling_structured` documents 3 → 5 → 3 → 7 → 3, with the source's
   `scalingCycle = {5,3,7,3}` targets. The function does not itself enforce
   an initial replica count of 3; keep this source distinction explicit. No generic list draw may
   replace it. Rapid scaling still has a chaos phase followed by recovery.
7. Model expectations stay independent in `tests/oracle` and the model
   validators. A generated input may be rejected. Never narrow generation merely
   because a legal server outcome is inconvenient to explain; never use server
   balances, returned postings, or response-derived expected state as the
   accounting oracle. Scenario balance reads are admission heuristics, not proof
   of the expected accounting result.
8. A Hegel case/source has one bounded owner and cannot escape its callback or
   be shared across workers. Pre-generate immutable inputs before concurrent RPCs
   or give each episode its own source. Cloud episodes must not repeat RPCs,
   Kubernetes mutations, backups, or key registration during shrinking/failure
   replay. Local resettable/pure properties can shrink. The current
   `generation.RunOnce` has an at-most-once callback guard; its presence alone
   does not prove native runtime or entropy integration.

## Main Composer directory checklist (59/59)

All entries are under `tests/antithesis/workload/bin/cmds/main/`. A checked box
means **classified from source**, not implemented or tested. The final column
identifies the source behavior to preserve and the Hegel boundary.

| Checked exact entrypoint | Class | Required domains, relationships, and preserved behavior |
|---|---|---|
| [x] `tests/antithesis/workload/bin/cmds/main/eventually_correct/main.go` | O | No new business input. Keep two-barrier quiescence, bounded full re-reads, per-(asset,color) double-entry and typed volumes, list/get account and metadata comparisons. Hegel must not sample away the oracle's existing account traversal. |
| [x] `tests/antithesis/workload/bin/cmds/main/eventually_cross_node_identity/main.go` | O | No business generator: compare existing state at an exact common durable index and normal sync status, including learners. Preserve node-specific routing, proto equality, recent audit sequence/hash/version comparisons and eventual-only isolation. Current `waitNodeAtIndex` uses `last_persisted_index`; introductory comments mentioning applied index are not a substitute for this implementation. |
| [x] `tests/antithesis/workload/bin/cmds/main/eventually_quiesce_read/main.go` | W | The acknowledged single-write sequence arms a lost-wakeup read. Hegel generates the address and a strictly positive full-width uint64 amount, which materializes the destination; preserve idle menu {2,5,8}s, stabilization, write retries and a separately bounded linearizable read. Never add background writes that could wake the stuck waiter. |
| [x] `tests/antithesis/workload/bin/cmds/main/eventually_stats_consistency/main.go` | O | No payload generation: preserve per-replica discovery, independent usage witness, qualified fixed source horizon, retry budget, exact convergence and separate qualification/liveness verdict. |
| [x] `tests/antithesis/workload/bin/cmds/main/first_default_ledger/main.go` | O | Fixed creation of `default` is Composer setup, not a variable business case. Preserve name and first-command behavior; no Hegel needed. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_account_type_violations/main.go` | S | Customer type suffix 0..9999, matching ID 0..999 and disjoint invalid prefix. Keep dedicated ledger, strict valid/invalid probes then AUDIT acceptance; preserve actual conditional violation reachability rather than inventing unconditional strict rejection. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_account_types/main.go` | S | Related name/pattern/matching address; ID 0..999. Keep add/get, typed transaction, AUDIT, remove and STRICT cleanup. Type identity may remain technical entropy; never independently draw the address prefix. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_audit/main.go` | W | Hegel applies to the existing full uint64 seed amount. Preserve acknowledged transaction/log precondition, audit stream checks and clean completion; do not replace audit evidence with a generated expected response. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_barrier_monotonic/main.go` | O | Empty Barrier requests have no business fields. Preserve iteration menu {1,2,8,32}, optional leader observation, positive/nondecreasing indices and cross-invocation locked high-water file. Torn-file absence weakens the floor, never fabricates one. Operational menus may use Hegel without changing barrier ordering. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_bulk/main.go` | S | Two transactions with related metadata address plus one metadata save in one Apply. Amounts 1..1000; USD/2 and EUR/2 remain distinct; preserve >=3 logs, post-commit volumes and metadata readback. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_bulk_atomicity/main.go` | S | Size menu {2,5,10}; valid prefix and final guaranteed unfunded non-Force overdraft. Keep owned references/accounts, separate helper-ledger marker, no partial transaction/account effects, permanent failure audit, <=1 setup success and one failure per ProposalId, not per retry invocation. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_check_store/main.go` | O | Empty CheckStore request; no business generation. Keep streamed integrity errors, progress and successful completion, with stream errors distinct from clean EOF. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_compaction/main.go` | O | Primary/secondary choice is operational. Preserve both compaction branches, subsequent stats/world readability and fault handling. No business generator needed; do not invent a size-after <= size-before assertion. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_concurrent_ledger_delete/main.go` | W | Generate bounded writer destinations before launching workers. Keep five attempts, fixed 100-unit writes racing deletion, acknowledged-delete gate and final 1-unit rejection probe. Hegel must not serialize writer/deleter or share a mutable case across them. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_definitive_errors/main.go` | S | Eight engineered attempts with {1,2^63,2^256−1}, unfunded private source and malformed asset every fourth attempt. Preserve definitive-code set excluding AlreadyExists, acknowledged marker and absence checks only for conclusively rejected references. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_delete_ledger/main.go` | W | Seed a nonempty private ledger using full uint64 amount, then delete and prove absence in a successful ListLedgers traversal. Keep deletion lifecycle and ambiguity handling. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_event_sinks/main.go` | S | Sink topic pool `ledger.events.0..9`, unique sink identity and generated posting witness. Preserve NATS URL, JSON format, batch size 32, add/list/remove/list. Event write is best effort; source does not prove event delivery. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_idempotency/main.go` | S | Shared full-domain postings generated once; replay exact request/key, compare transaction IDs. Hegel must not redraw the second payload. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_idempotency_conflict/main.go` | S | Same key with guaranteed different business payload: retain users:0/100/USD versus users:1/999/EUR as a fixed regression branch, alongside generated full-width amounts and amount-only conflicts with all other payload fields identical. The conflicting amount is guaranteed unequal to the original, including at uint64 boundaries. Keep acknowledged original and exact IDEMPOTENCY_KEY_CONFLICT assertion; variation must preserve inequality. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_indexes/main.go` | S | Metadata key pool 0..49; declare STRING before create, then status and drop of the identical account metadata index. Preserve AlreadyExists and failed-declaration gates. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_insufficient_funds/main.go` | S | Fund amount 100..10099 in private ledger/account; exact drain without Force, then 1-unit overdraft from zero. Derive all dependent amounts; preserve per-step idempotency keys, the linearizable zero-balance precondition, INSUFFICIENT_FUNDS, successful predecessor requirements and the forced-overdraft follow-up. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_invariants/main.go` | O | Aggregate existing ledger volumes; no generated payload. Preserve input==output per returned bucket and unexpected-error handling. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_ledger_create/main.go` | S | Preserve bounded 1,000,000-name namespace and collisions in creation, distinct from unique private oracle ledgers. Hegel can choose the bounded name suffix; keep canonical CreateLedger assertion. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_ledger_recreate/main.go` | S | Write count {3,5}; related old/new ledger names, accounts, references and distinct operation keys. Preserve permanent tombstone, point/list absence, cross-ledger isolation, reference reuse in another ledger and old-name reservation after reuse. Only acknowledged predecessors arm checks. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_list_completeness/main.go` | W | Confirmed write menu {3,8,20}, page sizes {1,2,5}, unit amounts, unique references and destination i%4. Preserve acked subset inclusion, exactly-once IDs and increasing order only after complete clean pagination; ambiguous extra rows remain allowed. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_list_transactions/main.go` | W | Shared generated transaction witness; retain all-page search for its acknowledged ID, forward page size 50, reverse size 10 and nonempty reverse check. Keep stream errors visible. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_logs/main.go` | W | Generated posting witness; preserve nonempty committed log precondition, ListLogs page size 20, aligned read and GetLog sequence identity. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_maintenance/main.go` | W | Generated write witnesses before/after maintenance gating on a dedicated ledger. Preserve enable → rejected write → disable → accepted write and Unavailable classification. Do not make gate execution a shrinkable repeated side effect. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_metadata/main.go` | S | Address in shared pool, metadata key/value and exact saved/deleted key relationship. Preserve linearizable read-after-write and read-after-delete; names may be technical unique entropy but values are business data. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_numscript_lifecycle/main.go` | S | Related script name, first and later versions/content. Preserve append-only versions, latest-version pointer and complete ListNumscripts traversal. The fixed 1.0.0 -> 2.0.0 branch remains; generated pairs derive a strictly later major, minor or patch version. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_numscripts/main.go` | S | Version major 1..10; saved script and execution reference agree. Generate account/monetary vars through structured draws, retain readable script identity and transaction checks. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_projection_alignment/main.go` | W | Keep fixed USD/2 unit probe and matching AccountHasAsset query in private ledger, asset index readiness, acknowledged log floor and probe membership in successful aligned response. Generated variants select an asset and precision shared by the posting and filter; the fixed USD/2 branch remains. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_queries/main.go` | S | Query-name collision pool 0..99; preserve prepared create/execute/delete and the existing query shape/page size 10. Concurrent NotFound is allowed as source specifies. Preserve response-presence assertion without claiming full filter validation. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_query_checkpoints/main.go` | O | No variable business payload: create/list/info/delete uses acknowledged server ID/maxSequence. Preserve bounded shared pool/capacity observation and owned cleanup, never reclaim foreign IDs or retry an ambiguous logical delete from defer. Hegel generation is unnecessary for empty creation request. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_reference_conflict/main.go` | S | Generate postings for first claim and guaranteed-different duplicate with same reference. Keep acknowledged original, conflict reason and reachability; do not rely on probabilistic inequality. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_reference_conflict_skip/main.go` | S | First claim, duplicate with opt-in skippable reason, then guaranteed fresh reference with opt-in. Preserve separate stable operation keys, exact OrderSkipped shape/reason/reference/ledger/existing ID and normal CreatedTransaction on fresh claim. Shared posting generator is applicable; skip must not swallow unrelated errors. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_reference_race/main.go` | S | Concurrency {2,4,8}, filler {60,120,250}, same-reference race plus aged reuse. Preserve distinct contenders, start synchronization, unique fillers, acknowledged original, rotation control point 60 versus >100 stress, marker-aligned list oracle and <=1 explicit OK/committed ID. Generate before goroutines. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_revert_concurrent/main.go` | S | Generate original postings once; two concurrent reverts target the same acknowledged transaction. Keep actual race, <=1 conclusive success, already-reverted reason and transient exclusions. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_revert_idempotent/main.go` | S | Generated original, successful first revert, volume/readback/reverted checks, second revert must return TRANSACTION_ALREADY_REVERTED. Preserve exact target and step order. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_revert_sequential/main.go` | S | Generated original followed by revert of its ID; retain post-commit volume and reverted readback checks. Hegel owns payload, not server ID allocation. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_stale_reads/main.go` | W | Rounds {5,15}; fixed 1-unit increments, unique references and stale consistency header. Preserve attempted/acked distinction, prefix bounds, untorn input/output relation and actual lag reachability. No arbitrary amount draw. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_stats_consistency/main.go` | O | No business generation: preserve main-store logCount>=txCount and aggregate input/output equality under concurrent writes. Async usage convergence belongs to the eventual driver, not a fabricated synchronous expectation here. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_timestamp_order/main.go` | W | Write sizes {3,8,20}, page sizes {1,2,5}, unit witnesses and every-third backdated user timestamp. Preserve ledger-local cursor/global sequence distinction and nondecreasing HLC log dates, allowing equal dates within a proposal. User timestamps do not control HLC. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_transactions/main.go` | S | Shared amounts/postings/metadata plus inline Numscript account/amount vars; single/bulk and postings/script choices, geometric 1..5000 bulk, exact log count and sampled readback. See mandatory domains above. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_transfer_leadership/main.go` | O | Select a real nonleader voter from current topology; preserve transfer target/newLeader and bounded post-transfer functionality checks. No generated business payload; Hegel must not invent node IDs or change election handling. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_transient_accounts/main.go` | S | Type suffix 0..9999, first ID 0..999, disjoint second ID 1000..1999, amount 100..1099. Keep equal fund/drain in same Apply versus positive unbalanced funding and TRANSIENT_ACCOUNT_NON_ZERO reason. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_tx_metadata/main.go` | S | Generated transaction, exact acknowledged ID plus metadata key/value lifecycle. Preserve save/read/delete/read and matching key identity. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_typed_metadata/main.go` | S | INT64 value 0..999; declaration/index/save/range-query relationship. Keep type migration sequence, schema observation, metadata survival and uncoercible literal-string case. Source checks response presence and no crash/classified errors; do not claim row correctness or rebuild exclusion is proven. |
| [x] `tests/antithesis/workload/bin/cmds/main/parallel_driver_update_query/main.go` | S | Related prepared name and users-prefix → world-prefix update, then execute/page size 100 and cleanup. Preserve fixed different filter stages; source currently asserts nonnil response, not full membership correctness. |
| [x] `tests/antithesis/workload/bin/cmds/main/singleton_driver_backup/main.go` | O | Fixed MinIO destination and backup operation; no business generator. Keep nonzero file assertion and failure classification; file production alone is not restore validation. |
| [x] `tests/antithesis/workload/bin/cmds/main/singleton_driver_config_change/main.go` | W | Sentinel data can use shared generation; configuration menu is operational: rotation {25,50,100,200}, expectedKeys {500000,1000000,2000000}, fpRate {0.001,0.01,0.05} across existing bloom categories. Preserve one-field patch, rollout/readiness/leader convergence, best-effort follower reachability and sentinel readback. |
| [x] `tests/antithesis/workload/bin/cmds/main/singleton_driver_incremental_backup/main.go` | O | Full checkpoint prerequisite, then two incremental exports to same fixed destination. No business input needed. Preserve missing-checkpoint concurrent race handling and nonregressing log/audit sequences; this alone does not prove nonempty-delta restore parity. |
| [x] `tests/antithesis/workload/bin/cmds/main/singleton_driver_quorum_recovery/main.go` | W | Sentinel is the payload boundary. Preserve 3 replicas, simultaneous killing of both nonleaders, forced scale to 1, recovery to 3, latency coverage, cooldown and deferred replica restoration on every exit. No Hegel control over destructive fault execution/retry. |
| [x] `tests/antithesis/workload/bin/cmds/main/singleton_driver_rolling_restart/main.go` | W | Generate sentinel/burst payloads per owned episode. Keep descending pod order, transfer-away, hard delete/new UID readiness, voter convergence, cooldown and Sometimes mid-restart commits. Preserve background concurrency without sharing a case. |
| [x] `tests/antithesis/workload/bin/cmds/main/singleton_driver_scaling/main.go` | O | Target menu is existing `OddReplicas` ({3,5,7}); keep maxStep clamp/odd intermediate steps, patch and voter convergence. No business generation; operational menu may use Hegel without replacing scale controller. |
| [x] `tests/antithesis/workload/bin/cmds/main/singleton_driver_scaling_chaos/main.go` | O | Keep 2–4 rapid odd-target patches, 3..10s inter-patch pauses, last successful target, recovery wait and round cooldown. Hegel may describe this menu; preserve deliberate absence of convergence waits between patches. No business input. |
| [x] `tests/antithesis/workload/bin/cmds/main/singleton_driver_scaling_structured/main.go` | W | Fixed 3→5→3→7→3 scenario, sentinel and fresh full-uint64 transaction witness per stable window. Generate witness amounts only; preserve targets, waits, post-commit volume checks and readback after each move. |
| [x] `tests/antithesis/workload/bin/cmds/main/singleton_driver_scenario_blocks/main.go` | S | Select among all 26 registered blocks with structured draws; preserve SCENARIO empty/all/comma-separated filtering, six setup groups and existing retry/error rules. All per-block requirements follow below. |
| [x] `tests/antithesis/workload/bin/cmds/main/singleton_driver_signing_keys/main.go` | S | Generate 32 seed bytes and derive matching Ed25519 private/public key; keep unique technical key ID, register/list, signed revocation and complete paginated list absence. Do not independently generate public/private keys or replay registration/revocation to shrink. |

## Model Composer directory checklist (1/1)

- [x] `tests/antithesis/workload/bin/cmds/model/singleton_driver_model/main.go`
  — **S**, with W and O sub-surfaces below. The model owns the entire timeline.
  Preserve concurrent workers, immutable published GlobalState snapshots, one
  response reorder buffer, ordered processor drain, read/in-flight ticket gates,
  pending/unknown candidate enumeration, retry/idempotency handling and bounded
  state growth. All model reads remain linearizable; the stale-read driver has
  a different oracle. Hegel must neither serialize the workload nor change the
  legal candidate-state search to make generated cases pass.

The following are generation families, not additional Composer commands. Source
names are relative to
`tests/antithesis/workload/bin/cmds/model/singleton_driver_model/`.

| Family and actual source | Class | Required generation and preserved relationship |
|---|---|---|
| Worker dispatch and episode ownership — `main.go`, `generation.go` | S | Preserve read/write dispatch and all existing read slots, checkpoint mutation branch and ordinary bulk branch. Keep one source per callback/worker episode, no source shared with processor, pollers, restore goroutine or asynchronous recovery. Slot weights are exploration controls; changing primitive bias is not proof of equivalent reach. |
| Bulk shape and ledger selection — `actions.go` (`generateBulk`, `pickLedgers`, `bulkSize`) | S | Preserve single-/multi-ledger choices, distinct ledger samples, small bulk menu 1..5 and branch precedence/back-pressure. This model bulk domain is separate from the main driver's 1..5000 stress domain. Preserve whole-bulk atomicity and productive metadata fallback when transaction caps engage. |
| Ledger lifecycle — `actions.go` (`generateLifecycle`) | S | Create/delete, deleted-name recreate rejection, mirror/promotion and maintenance transitions; retain live target, active/deleted sets, bounded growth and unique creation identity. Do not discard tombstone/absent/mirror-invalid outcomes. |
| Posting transactions and colors — `actions.go` (`generateTransaction`, `randomColor`) | S | 1–4 ordered postings, full uint64 including zero, world/pool sources, shared type-prefix/address collision pools, asset choices, Force and account/transaction metadata. Preserve same-cell repeated postings and running balance dependence. Keep the full-width trailing-zero color transform, empty color and rare tail buckets, rather than only a short fixed color list. Preserve optional backdated user timestamps. |
| Deliberate rejection and drainage — `actions.go` (`generateRejectedTransaction`, `overflowTransaction`, `generateDrainTransaction`) | S | Empty transaction, committed-reference duplicate and two 2^255 postings; retain genuinely invalid branches and known balances/cells for drain operations. No validity filter that deletes overflow or insufficient-funds coverage. |
| Chart and enforcement — `actions.go`, `enforcement.go` | S | Shared type names/patterns and addresses, all existing persistence choices, add/remove races and modes. Preserve small colliding pools and related matching-address construction, including transient chart behavior; mode choice cannot change the expected-state engine. |
| Transient balanced/unbalanced bulks — `actions.go` | S | Select existing transient type/cell, fund and drain equal quantities in one proposal versus nonzero residual. Preserve restrictions to a single ledger and transaction back-pressure. Do not let an independent second amount draw erase the zero-net case. |
| Metadata and schema lifecycle — `actions.go`, `metadata_filters.go` | S | Account/transaction/ledger save/delete, target identity, small key pools, typed values, nulls and map shapes; schema set/remove and migration. Preserve declaration/value/type relations and absence probes. Existing conversion/evaluation helpers remain as implemented; generation must not turn observed response values into expected metadata. |
| Apply aliases and skippable reasons — `apply_surface.go` | S | Preserve top-level versus ledger-action routes, independent whitelist, matching opt-ins, deliberately disallowed opt-in and duplicate/fresh reference branches. Keep successor-after-skip probes and trailing skips. Expected skip context and continuation remain oracle checks, not generation assumptions. |
| Idempotency replay/conflict — `actions.go`, `replay.go` | S | Retain tracked original key/outcome, later identical replay, and guaranteed different conflict bulk. Snapshot/copy before mutation; do not alter the frozen original via aliases. Technical ephemeral keys remain stable through transport retry. |
| Account, ledger, schema and transaction point reads — `reads.go` | S | Existing/absent accounts, cells, ledgers and transaction IDs; retain target selection separate from validation. Related expected state comes from candidate bases, not from the generation snapshot frozen as an unconditional expectation. |
| Account/transaction query generation — `queries.go`, `metadata_filters.go` | S | Indexed/free filter trees, boolean AND/OR/NOT and child arities, address roles/exact/prefix, reference, reverted, asset membership, metadata fields, ID/user-date ranges, open/exclusive/extreme bounds, forward/reverse paging and page-size request/effective distinction. Preserve unsupported builtin probes and index lifecycle legality. |
| Invalid/general filter shapes — `filter_shapes.go`, `rejected_filter.go` | S | Mutations, deep chains, nil/missing oneofs/conditions, unknown enum values, malformed fields, wrong target and parameter shapes. Preserve invalid cases as observable outcomes and existing depth/budget controls; no blanket valid-filter filter. `rejected_filter.go` classifies outcomes; do not replace it with Hegel expectations. |
| Log queries and GetLog — `queries_logs.go`, `getlog.go` | S | Log ID/date filters, complete/directional windows, malformed cursors, known global sequences and deliberately unassigned sequence. Preserve ledger-local ID versus global sequence, immutable log fields, learned/absent distinction and candidate-aware index legality. |
| Audit generation/readback — `audit.go` | S | Audit filter trees, sequence/range/cursor choices, existing/absent entry targets, sampled identities and refused-filter probes. Preserve success/failure/skipped evidence and audit context; sampling may guide inputs but cannot define the independent truth of a returned entry. |
| Index mutations, asset queries and introspection — `indexes.go`, `index_introspect.go`, `index_inspect.go` | S | Declared/builtin and absent index selection, create/drop, readiness, list/fleet/get/entry status, InspectIndex mode/key/page choices. Preserve index lifecycle errors and exact model summary counters. Existing InspectIndex paged-value contract checks shape/counters, not every encoded value's order; do not claim more. |
| Prepared query registry, parameters and execution — `prepared_queries.go`, `prepared_queries_read.go` | S | Create/update/delete with shared names and target/filter relationship, parameterization/binding across supported value kinds, missing/wrong parameter cases, execute/list and next-page request generation. Keep stored definition in candidate state, frozen bound parameters and cursor reuse contract. |
| Cursor shapes and continuation — `cursors.go` | S | Keep valid continuation tied to target/filter/direction and malformed cursor menu. Do not make malformed inputs valid by reconstruction, or carry a Hegel source into a later callback to finish a page. |
| Ledger listings — `ledgers_list.go` | S | Live/deleted fleet, optional filters, order/page/cursor choices and invalid filter cases; preserve whole LedgerInfo comparison and candidate-aware exact windows. |
| Aggregate queries — `aggregates.go` | S | Filter, (asset,color) buckets, grouping prefixes, ordering/paging choices and index requirements. Preserve independent fold over candidate model state and exact result semantics. |
| Checkpoint mutations/schedule — `checkpoints.go` | S | Singleton create/delete/schedule bulks; existing, zero and future/absent IDs; valid long-interval cron menu and deliberately invalid cron. Keep pool limit 10 baseline, predecessor maxSequence, deletion history and audited mutations in the ordered model stream. Long intervals prevent unobserved scheduler writes; do not shorten them casually. |
| Checkpoint point/list/query/aggregate reads — `checkpoint_reads.go` | S | Choose node, known/deleted/absent snapshot and existing read shapes; retain frozen checkpoint model state, replica availability/deletion explanations and bounded reads. Live candidates may explain deletion, not different historical business contents. |
| Secondary read dispatch — `main.go`, `stats.go`, `aggregates.go`, `audit.go`, `getlog.go`, `index_inspect.go`, `index_introspect.go`, `ledgers_list.go` | S | Preserve the secondary read dispatcher and every reachable arm, not only the top-level 16 read slots. Generate request choices; keep validators, model stats/counts and independent content comparisons intact. |
| Restore cycles — `restore.go` | O | Jitter is operational (currently a separate generated episode); preserve quiesce/drain/pause, restore/reconnect and resume synchronization. No Hegel shrinking/replay of backup/restore effects. Snapshot/recovery verification must still use independent model state. |
| Processor, search, validation, readiness/recovery and coverage — `checker.go`, `processor.go`, `search.go`, `validate.go`, `coverage_stats.go` and existing lifecycle helpers | O | No business generator belongs in the oracle machinery. Preserve all legal serializations, observation tickets, immutability, bounded search and registered assertion/reachability counters. Generators must not import server processing to manufacture expected outcomes. |

This classification does not claim every helper is migrated. The inspected
working tree already contains `runGeneration`/`generation.Source` in many of
these families and still changes concurrently. The parent must verify all
reachable generation paths, ownership, domain parity and dispatch coverage in
the final candidate, including new helpers added after this inventory.

## Registered block checklist (26/26)

Registration adapters are
`tests/antithesis/workload/internal/block/{marketplace,gaming,lending,subscription,multicurrency,payroll}/*.go`.
They register six `pkg/scenario` BlockGroups. The exact source paths and block
names follow. All blocks are **S** as scenario selection/business input, except
the fixed payroll funding payload marked **W**. Even that block remains eligible
for Hegel selection. Fixed formulas and constants are not missing random draws.

| Checked exact block name | Actual source and function | Class | Required preserved domain/relationship |
|---|---|---|---|
| [x] `marketplace/deposit` | `pkg/scenario/marketplace.go`: `marketplaceDeposit` | S | Customer 1..50; deposit 100000..1099999 USD/2; selected address and script vars agree. |
| [x] `marketplace/purchase` | `pkg/scenario/marketplace.go`: `marketplacePurchase` | S | Customer 1..50, merchant 1..10; skip below 1000; amount 1000..min(balance,50000). Keep existing purchase script/fee allocation. |
| [x] `marketplace/revert` | `pkg/scenario/marketplace.go`: `marketplaceRevert` | S | Select from observed nonreverted transaction candidates using shared helper; skip when none; revert actual ID with original Force/flags. No synthetic ID unrelated to the selected transaction. |
| [x] `marketplace/payout` | `pkg/scenario/marketplace.go`: `marketplacePayout` | S | Merchant 1..10; positive-balance prerequisite; payout amount is full observed balance, not an independent draw. |
| [x] `marketplace/metadata` | `pkg/scenario/marketplace.go`: `marketplaceMetadata` | S | Customer/merchant branch with respective pools; tier 0..4; full uint64 decimal last_seen; optional 1-in-3 delete of that exact key/address. |
| [x] `gaming/topup` | `pkg/scenario/gaming_wallet.go`: `gamingTopUp` | S | Player 1..20; USD 1000..10999 and equal COINS conversion; same player across asset-specific addresses. |
| [x] `gaming/buy_item` | `pkg/scenario/gaming_wallet.go`: `gamingBuyItem` | S | Player 1..20; costs {100,250,500}, fallback 100 when affordable; skip insufficient balance; item 0..99. |
| [x] `gaming/p2p_trade` | `pkg/scenario/gaming_wallet.go`: `gamingP2PTrade` | S | Distinct players 1..20 with deterministic same-player correction; amount 10..balance, COINS; preserve skip below 10. |
| [x] `gaming/promotion` | `pkg/scenario/gaming_wallet.go`: `gamingPromotion` | S | Player 1..20, forced world credit 100..1099 COINS, promotion metadata. |
| [x] `gaming/clawback` | `pkg/scenario/gaming_wallet.go`: `gamingClawback` | S | Player 1..20; skip below 50; clawback 50..balance COINS with existing metadata/script. |
| [x] `lending/fund_pool` | `pkg/scenario/lending_lifecycle.go`: `lendingFundPool` | S | Funding is 100000 × multiplier 1..5, USD/2. |
| [x] `lending/disburse` | `pkg/scenario/lending_lifecycle.go`: `lendingDisburse` | S | Pool >=100000; borrower 1..10; same borrower wallet/loan addresses, fixed loan 100000. |
| [x] `lending/repay` | `pkg/scenario/lending_lifecycle.go`: `lendingRepay` | S | Borrower 1..10 with positive loan/wallet; interest floor(loan×2/100), skip if unaffordable; principal min(loan,wallet−interest,100000/6), positive. Interest then principal in same Apply, omit zero interest. |
| [x] `lending/provision` | `pkg/scenario/lending_lifecycle.go`: `lendingProvision` | S | Borrower 1..10; positive loan; provision full outstanding balance and matching borrower/default metadata. |
| [x] `lending/write_off` | `pkg/scenario/lending_lifecycle.go`: `lendingWriteOff` | S | Borrower 1..10; positive loan; write off exactly that balance, same loan address and reason. |
| [x] `subscription/fund` | `pkg/scenario/subscription.go`: `subscriptionFund` | S | Subscriber 1..50; tier choice basic/pro/enterprise with funds {5000,10000,20000}. Keep tuple linkage. |
| [x] `subscription/charge` | `pkg/scenario/subscription.go`: `subscriptionCharge` | S | Subscriber 1..50; tier derived from (ID−1)%3; charges {1000,2500,5000}; skip if balance insufficient. Do not redraw tier independently. |
| [x] `subscription/recognize` | `pkg/scenario/subscription.go`: `subscriptionRecognize` | S | Positive deferred revenue; integer portion 25..100%; floor(balance×portion/100), skip zero. Preserve integer rounding and USD/2. |
| [x] `subscription/credit` | `pkg/scenario/subscription.go`: `subscriptionCredit` | S | Subscriber 1..50; credit 100..599 USD/2. |
| [x] `multicurrency/fund` | `pkg/scenario/multi_currency.go`: `multiCurrencyFund` | S | Currency tuple USD/2→treasury:usd, EUR/2→treasury:eur, GBP/2→treasury:gbp; amount 100000..999999. |
| [x] `multicurrency/fx` | `pkg/scenario/multi_currency.go`: `multiCurrencyFX` | S | Distinct source/destination currencies via offset; source balance >=5000; draw 5000..balance then clamp at 50000; destination floor(source×92/100). Preserve two separate RPC legs, clearing account and Force only on second leg; do not describe this as an atomic batch. |
| [x] `multicurrency/vendor_pay` | `pkg/scenario/multi_currency.go`: `multiCurrencyVendorPay` | S | Same currency/treasury tuple; six vendor names from source; balance >=400; amount 400 + draw[0,min(balance−399,10000)), up to min(balance,10399). Do not silently replace with a different 10000-total cap. |
| [x] `payroll/fund_clearing` | `pkg/scenario/multi_ledger_payroll.go`: `payrollFundClearing` | W | No per-payload random draw needed: total = (15+10+5) employees × 500000 = 15000000 USD/2. Keep fixed setup-derived funding; Hegel selects the block, not arbitrary independent total. |
| [x] `payroll/distribute` | `pkg/scenario/multi_ledger_payroll.go`: `payrollDistribute` | S | Select department tuple engineering/dept-eng/15, sales/dept-sales/10, operations/dept-ops/5; amount employees×500000, check clearing balance. Preserve account/department relation. |
| [x] `payroll/pay_salary` | `pkg/scenario/multi_ledger_payroll.go`: `payrollPaySalary` | S | Select department then employee 1..department count; fund and pay 500000 each in the same department Apply. Transient payroll:pool must net zero. |
| [x] `payroll/cost_alloc` | `pkg/scenario/multi_ledger_payroll.go`: `payrollCostAlloc` | S | Select whole tuple operations→engineering/50000/cloud-hosting, engineering→sales/30000/lead-generation, sales→operations/20000/office-supplies. Check source balance; never draw tuple fields independently. |

The six setup functions define ledgers, account types, schema and Numscript
libraries. Preserve their script names/versions, matching addresses, assets,
transient types, splits and all existing setup actions; Hegel adoption must not
replace the scenario families with raw generic postings. The block loop must
still register both `block <name> succeeded` (must-hit Reachable) and
`block <name> failed` (Unreachable), emit matching runtime IDs and check
post-commit volumes. Preserve ErrSkip, Unavailable and expected
FailedPrecondition handling exactly. Setup retries must not become Hegel replay.

`pkg/scenario/block.go` provides `Random`, bounded draw adapters, balance reads
and nonreverted-transaction selection. Hegel belongs in the workload adapter;
shared scenario code may consume the typed random interface without importing
Hegel. Keep `ForScenario` filtering and setup grouping. Standalone scenario
runners and the additional operations/stress scenarios in `pkg/scenario` are
not extra Composer blocks in this registry; do not count them toward 26 or
replace their existing behavior as a side effect of adapting the block API.

## Source-current implementation observations

These observations identify handoff work, not missing inventory classifications:

- `parallel_driver_reference_conflict/main.go` uses separate posting draws,
  plus metadata only on the second request. The metadata marker guarantees
  different business payloads even when both posting draws coincide.
- `replay.go` already guards its model conflict key reuse with
  `!oracle.RequestsEqual(...)`; preserve that check. Do not replace it with an
  assumption that independently generated bulks differ.
- `singleton_driver_scaling_structured/main.go` actually iterates
  `{5,3,7,3}`. Its introductory scenario is 3→5→3→7→3; `runCycle` does not reset
  the cluster to 3 before the first target. The inventory requires preservation
  of that fixed target list and explicitly does not claim an initial-3 gate.
- Several shared helpers, drivers and model families already use generated
  sources while concurrent agents are editing them. This inventory does not
  certify those migrations, compile status or runtime execution. Recount and
  review the final diff after those edits settle.

## Internal SUT assertions and non-driver surfaces

**No Hegel in production FSM/apply, cache, storage, admission, or query execution.**
The SDK's assertion/instrumentation path remains in place. Input generation is a
workload responsibility; committed behavior must remain deterministic and must
not depend on a Hegel case, seed, local config or generation profile.

Source inspection found SDK assertion imports in these production families;
these are **O**, not missing generator integrations:

- `internal/infra/state/{machine,scope,write_set,sentinel,cache_snapshotter}.go`
  and `internal/infra/plan/{builder,coverage}.go`: retain coverage, mutation,
  cache/restore and invariant checks; no random decision in committed apply.
- `internal/infra/node/{node,applier}.go`,
  `internal/storage/dal/batch.go`,
  `internal/storage/wal/{wal_default,snapshotter}.go`: retain durable ordering,
  Raft/apply and storage assertions under external workload/fault stimulation.
- `internal/domain/processing/{processor,processor_revert_transaction,skip_safe_scope}.go`
  and `internal/application/admission/{admission,overlay}.go`: retain business,
  revert, skip and order-boundary assertions without changing their contract.
- `internal/application/check/reverse_map_orphans.go`,
  `internal/application/ctrl/controller_default.go`,
  `internal/infra/health/healthcheck.go`,
  `internal/query/{aligned_snapshot,profile}.go`, and
  `internal/adapter/http/{unitary_log,middleware_query_profile}.go`: preserve
  integrity, alignment, health and profiling assertions; these are observations,
  not generator ownership sites.

Workload `internal/checks.go`, `query_oracle.go`, per-node helpers and sentinel
verification remain independent assertion/oracle infrastructure. Hegel may
construct the sentinel payload; it must not substitute the readback verdict.
`internal/ledger.go` and common generated helpers must retain owned ledger
prefix exclusions and shared-pool selection rules so other drivers cannot
invalidate private-oracle premises. `bin/init`, client retry/transport controls,
`restore-orchestrator.sh`, Kubernetes manifests, Dockerfile and Composer
instrumentation/discovery are operational plumbing, not additional business
drivers. Preserve their operation and arming; this inventory authorizes no
cloud run, image publication or production dependency.

## Coverage proof and handoff limits

- [x] Every main Composer directory has one exact-path classification row: 59.
- [x] Every model Composer directory has an exact-path checklist entry: 1.
- [x] Both discovered template directories are covered: `main`, `model`.
- [x] All six block adapters and their source BlockGroups are covered.
- [x] All 26 distinct registered block names have one classification row:
  marketplace 5, gaming 5, lending 5, subscription 4, multicurrency 3, payroll 4.
- [x] Model generation families and operational/oracle boundaries are classified.
- [x] Internal SUT assertions retain SDK ownership; production Hegel is excluded.
- [ ] Parent integration has verified final-source adoption and domain parity.
- [ ] Parent integration has completed local/native Linux/instrumented validation.
- [ ] Remote Antithesis evidence exists for the final integration (separate scope).

The following read-only source/directory check proves inventory membership and
uniqueness without executing any driver, test, build or cloud action. Run from
the repository root after concurrent source edits settle:

```python
from pathlib import Path
from collections import Counter
import re

root = Path('tests/antithesis/workload/bin/cmds')
text = Path('tests/antithesis/HEGEL-INVENTORY.md').read_text()
for template, expected in [('main', 59), ('model', 1)]:
    directories = sorted(p for p in (root / template).iterdir() if p.is_dir())
    paths = [str(p / 'main.go') for p in directories]
    missing_entrypoints = [p for p in paths if not Path(p).is_file()]
    missing = [p for p in paths if text.count('`' + p + '`') != 1]
    print(template, len(paths), 'missing entrypoints', missing_entrypoints,
          'missing/duplicate classifications', missing)
    assert len(paths) == expected and not missing_entrypoints and not missing
assert {p.name for p in root.iterdir() if p.is_dir()} == {'main', 'model'}
blocks = []
for path in Path('pkg/scenario').glob('*.go'):
    if not path.name.endswith('_test.go'):
        blocks += re.findall(r'Name:\s*"([a-z]+/[a-z0-9_]+)"\s*,\s*Run:', path.read_text())
counts = Counter(blocks)
missing = [name for name in blocks if text.count('| [x] `' + name + '` |') != 1]
print('blocks', len(blocks), 'distinct', len(counts),
      'missing/duplicate classifications', missing)
assert len(blocks) == len(counts) == 26 and not missing
```

Inventory validation result at authoring: **59 main + 1 model directories;
26/26 distinct block names; missing classifications: none**. This establishes
inventory coverage only. No tests/builds, commits, pushes, cloud runs or delegated
work were performed for this documentation-only task. Only this inventory is
owned by this task; `HEGEL.md` and concurrent implementation files remain owned
by the parent/other agents.
