# CLAUDE.md - AI Agent Instructions

This document contains rules and conventions for AI agents working on this codebase. Detailed documentation lives in `docs/` - see [docs/README.md](docs/README.md) for navigation.

## Invariants

**CRITICAL**: These rules are non-negotiable and must never be violated.

### Architecture

1. **Cache is the source of authority** — The in-memory cache must NEVER diverge between nodes. Every node must see identical cache state for the same applied index.
2. **FSM must be deterministic** — The finite state machine (Raft apply path) must produce identical results on every node for the same input. No randomness, no time-dependent logic, no node-local state.
3. **No Pebble reads in FSM / hot path** — The FSM apply path must never read from Pebble. All data needed for apply must come from the cache or the command itself. Pebble is write-only on the hot path. The hot path receives a `dal.WriteSessionFactory` parameter and opens a `*dal.WriteSession` — this type deliberately has no `Get`/`NewIter`, so the invariant is compiler-enforced for any code that holds a session. The hot-path FSM `Machine` itself holds NO Pebble read capability and NO `*dal.Store`: boot/recovery reads live on `state.Recovery` (which owns the only `dal.RecoveryReader`), follower-sync coordination lives on `state.Synchronizer` (which owns the only `dal.IncomingRestoreFactory`). Post-commit sentinel checks (debug mode) read through `dal.SentinelFactory.Run(fn)` — a scoped callback so the reader never escapes the check.
4. **Pebble writes only from the hot path or declared lifecycle paths** — `*dal.Store.OpenWriteSession()` is the only producer of `*dal.WriteSession`. Outside the FSM hot path, only declared lifecycle paths may call it: `internal/bootstrap/config_validation.go`, `internal/infra/backup/`, `internal/infra/attributes/prepare.go`. This is enforced by a `forbidigo` rule in `.golangci.yaml`; new call sites must be added to the exclusions block with a justification.
5. **Never delete cache entries outside of rotations** — Cache entries must only be evicted during generation rotations (Gen0 → Gen1 → discard). Deleting individual entries breaks the cache prediction mechanism (bloom filters, tombstones).
6. **Every FSM `Registry.X.Get(...)` must have a matching preload, declared by the component that proposes the command** — The FSM apply path reads from the in-memory cache; a cache miss turns the read into a silent no-op. Each component that emits a proposal (the metadata converter, the index builder, the cluster-config reconciler, the idempotency-eviction scheduler, the mirror worker, admission) is responsible for declaring its own `preload.Needs` covering every key its apply path will read. There is NO central proposal→needs registry — coupling the preload package to every proposal type creates a single point that easily falls behind. The component knows what it reads; the component declares it. The shared `proposeTechnical` helper takes a `*preload.Needs` parameter the caller fills in (pass nil or an empty `Needs` when the apply path has no cache-keyed reads, e.g. cluster config / idempotency eviction). The preload populates the cache via `MirrorPreload` with a fresh value read at propose time (Pebble fallback on cache miss), and `PredictedIndex` catches mutations between propose and apply.

    **Volume preload is load-bearing for the `LedgerLog.new_kept_volumes` / `ephemeral_volumes` split.** Admission MUST preload the current value of every `(account, asset)` volume touched by an order, including postings resolved from numscript execution. The FSM classifies a volume as "newly created" by checking `Old.IsDefined()` (or the zero-placeholder via `isVolumePreloadZero`) at merge time — a silent cache miss on an existing volume produces a false-new classification, mis-routes the tuple between the `new_kept_volumes` / `ephemeral_volumes` / `purged_volumes` per-log lists, and inflates `usagestore.CounterVolume`. Because `usagestore` is a rebuildable peer side-store held **outside** checker scope (invariant #8 verifies primary-Pebble-store projections, plus one narrow readstore exception that does not extend to `usagestore` — see scope refinement (a)), no checker pass would surface the drift; it persists until an explicit usage rebuild. That absence of a backstop makes the preload correctness even more load-bearing, not less. This is not opportunistic (unlike the metadata preload that was removed after the indexbuilder stopped needing it): balance checks, Uint256 arithmetic and numscript resolution all require the current volume value, so the preload is structurally mandatory. If a future refactor considers alleviating volume preload, `VolumeCount` must be re-hosted first (via computed-on-read or a subsystem-side seen-keys table) — see EN-1422 for the design rationale.
7. **Never silently skip a "should not happen" branch** — A branch that is reachable only if an invariant is violated (nil where the contract says non-nil, a state we believe unreachable, a cache miss after a guaranteed preload, etc.) MUST surface a loud signal: `return fmt.Errorf("invariant: ...")` so it bubbles up, or `assert.Unreachable(...)` for SUT-level invariants exercised under antithesis. A silent `return nil` / `continue` on these branches hides real bugs — particularly catastrophic in the FSM apply path, where a no-op desyncs nodes from each other. Branches that represent genuine runtime conditions (cache miss as an expected outcome, stale proposal, deleted entity) keep their soft `return nil`. The distinction is whether the case is *expected* (soft skip OK) or *impossible by design* (must fail loudly). The comment must say *why* the case is impossible so a reader can decide whether to add a hard fail or relax the rule.

8. **The audit log is the only source of truth — every other persisted dataset is a projection and must be verified by the checker** — Only `AuditEntry` (zone `Cold`, sub `Audit`) is cryptographically bound, via the hash chain that `state.BuildHashedHeaderPayload` + `processing.HashGenerator` produce and `checker.verifyAuditHashChain` verifies on every Check() run. Everything else stored in Pebble — `Log`, `AuditItem`, `AppliedProposal`, `LedgerLog.PurgedVolumes`, attribute caches (`Volume`, `Metadata`, `Transaction`, `Reference`, `Boundary`, etc.), reversion bitsets, idempotency keys, mirror cursors, chapters, bloom filters, signing keys, the read-side index — is a *projection* of orders that already live in the audit chain. Projections are rebuildable from the audit on demand, so we deliberately do NOT extend the hash chain to cover them (refactor over hash binding — see `feedback_audit_is_source_of_truth`). In exchange, **`internal/application/check/checker.go` MUST verify every projection it persists**: re-derive the value the projection should hold by replaying the audit (`ReplayLedgerLog`, `SimulateEphemeralPurge`, `partitionVolumes`, etc.) and compare to what is stored, emitting the matching `CHECK_STORE_ERROR_TYPE_*` event on divergence. A projection that the checker does not verify is a tampering vector — adding a new persisted projection without a matching compare* / collect* pass in the checker is the violation. Two scope refinements: (a) this applies to the **primary FSM store** — the single store `Check()` opens and walks; **peer secondary stores** (the `readstore` inverted index today; the `usagestore` counters forthcoming with EN-1334) are out of *main-store checker* scope as a rule, with **exactly one narrow, deliberate exception**: the readstore **reverse map** (`0x03`), verified by `compareReverseMapOrphans` (EN-1458). `Check()` therefore *does* open the peer readstore — read-only, one snapshot, for that single pass — so the old "*by construction*, `Check()` never opens them" formulation no longer holds and must not be re-asserted. The exception is justified narrowly: `0x03` is the **only** read-index limb that cannot be range-deleted by field (its metadata key sits *after* a fixed-width version block, see `readstore/keys.go`), so field removal must scan and point-delete row by row, and a row that scan misses is a permanent divergence with no other detector — unlike the `0x01`/`0x02` limbs, whose range-delete is atomic and self-healing. Everything else about peer stores stays out: `usagestore` entirely, and readstore index **contents** (tracked under `EN-1514` / `EN-1323`). The pass is skipped — loudly, via an INFO log, never reported as a clean result — when no readstore handle is available (restore / CLI validate a staged main store that has no peer readstore). It is **not** skipped on an empty audit: the read index folds *from* the log stream, so a reverse-map row over a zero-log store is unaudited by definition, and returning clean there would hide exactly the tamper classes this pass reports. Do **not** widen this exception to new peer-store data without the same argument: a projection that no other mechanism can detect a divergence in. That scope carve-out is **not** a claim they are integrity-safe: the readstore serves READY indexes directly to business-visible queries with no scan fallback, and its automated detect/drop/rebuild is **not yet wired** (tracked under `EN-1323`), so a corrupted or tampered index is a **current open integrity gap on the peer-store side** — a per-replica rebuild-health concern of the index builder, not an invariant-#8 main-store concern. Out of the main-store checker's mandate, not out of every integrity concern. (b) A primary-store projection may be exempted from a dedicated pass only when it is either (i) deterministically rebuildable from still-retained verified state through a real, *wired* rebuild path — not rebuildable merely in principle — or (ii) purely informational and intentionally carried across restore (cf. `BuildStatus`). These are distinct bases: of the *Known projection gaps* below, `DefaultEnforcementMode` and `SubAttrLedgerMetadata` qualify under (i) via `RebuildDelta` (`BuildStatus` on the index registry is the canonical (ii) example). The current passes are `compareVolumes`, `compareMetadata`, `compareTransactions` (per-transaction state; baseline-seeded lazily under archiving via `newLazyTxSeedWriter`, so a post-archive metadata/revert delta merges onto the pre-archive base whose create log is purged — the tx merge operator defers ops as a `txOpBatch` on a partial merge and never collapses a metadata delete away), `compareTransactionPostCommitVolumes` (the immutable `Transaction.post_commit_volumes` snapshot vs the replayed volume state — baseline checkpoint + replayed pre-purge delta — at the transaction's log sequence; run inline in the replay loop before the buffered ephemeral purge flushes, so the comparison sees the same state the FSM captured; missing/extra/duplicate/divergent rows emit `CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH`), `compareExclusionProjections` (AppliedProposal.TransientVolumes + LedgerLog.PurgedVolumes), `checkReversionInvariants`, `verifySealingHash`, `compareIdempotencyOutcomes` (frozen idempotency outcomes in SubIdempKeys vs the hash-chained AuditFailure/AuditSuccess that wrote them — the failure kind is re-derived from the chain-bound reason via `domain.KindForReason`, never stored), and `compareIndexes` (SubAttrIndex registry vs CreateIndex/DropIndex/RemovedMetadataFieldType/DeleteLedger logs — covers presence + identity; BuildStatus is intentionally excluded because it is purely informational on the cluster-wide registry entry — queries gate on the per-replica `IndexVersionState.CurrentVersion`, not on BuildStatus), and `compareMirrorV2LogID` (stored `LedgerBoundaries.last_mirror_v2_log_id` **==** `max(audited MirrorIngest.v2_log_id)` per ledger — a full equality check, `CHECK_STORE_ERROR_TYPE_MIRROR_V2LOGID_MISMATCH` on ANY divergence; the FSM enforces a contiguous applied prefix so at rest the two must be exactly equal, and the baseline floor is seeded from archived `Boundary` rows now included in the compact baseline snapshot; pre-field clusters are unsupported, no backfill leniency), and `compareSchema` (per-ledger `LedgerInfo.MetadataSchema` vs the replayed CreateLedger.initial_schema + SetMetadataFieldType/RemovedMetadataFieldType logs; under archived chapters the boundary schema is seeded from the baseline checkpoint, which carries LedgerInfo for this purpose), and `compareAccountTypes` (per-ledger `LedgerInfo.AccountTypes` vs the replayed AddAccountType/RemoveAccountType logs, baseline-seeded under archiving like the schema), and `compareLedgerPresence` (the live ledger set in the store must match the audit-derived set both ways: every audit-live ledger — CreateLedger with no later DeleteLedger, or a non-deleted baseline ledger under archiving — must have a *live* stored `LedgerInfo` else `MISSING_LEDGER` (covers an entry deleted outright OR tampered to a soft-deleted tombstone, which the compareSchema/compareAccountTypes loops skip), and every live stored `LedgerInfo` must be audit-backed else `UNAUDITED_LEDGER` (an injected empty ledger row carries no schema/types for the projection passes to flag). The audit-derived set is NEVER seeded from the live store — under archiving it comes from the baseline checkpoint, keeping the check independent of the data it verifies), and `compareReferences` (the SubAttrReference reference→txID uniqueness index vs the references replayed from CreatedTransaction/RevertedTransaction logs, baseline-seeded under archiving; verified both ways — missing, unaudited, and retargeted rows are all flagged; rows for deleted or cleanup-pending ledgers are skipped since they legitimately linger until a covering purge runs deleteLedgerData), and `compareBoundaries` (per-ledger `LedgerBoundaries` vs the checker’s re-derivation: only NextTransactionId/NextLogId are verified here — derived from the replayed logs plus the chain-bound AuditItem order effects (mirror fill-gap advances live on the orders, not the ledger-log stream), baseline-seeded under archiving; the mirror high-water `last_mirror_v2_log_id` is verified separately by `compareMirrorV2LogID`. The per-ledger usage counters — `PostingCount`, `RevertCount`, `NumscriptExecutionCount`, `VolumeCount`, `MetadataCount`, `ReferenceCount`, `EphemeralEvictedCount`, `TransientUsedCount` — no longer live on `LedgerBoundaries`: `LedgerBoundaries` now reserves tags 3-10 for them and they moved to the `usagestore` peer secondary store (EN-1334 / EN-1420), out of main-store checker scope **by construction** (per the peer-store carve-out in scope refinement (a) above), their integrity being a peer-store rebuild-health concern — reconverged by the online usagebuilder fold + `usagestore.Reset()` on rollback — rather than an invariant-#8 main-store concern), and `compareReversions` (the per-ledger reversion bitsets — `ZonePerLedger`/`SubPLReversions`, the projection the FSM's already-reverted gate reads — vs the reverted set derived from baseline tx-row markers plus the replayed RevertedTransaction logs; exact equality both ways — a lost bit re-admits a double revert, an unaudited bit blocks a legitimate one; driven purely by audit-derived state — no persisted marker (pending-cleanup included) can exempt a live ledger, stored rows for non-live ledgers are flagged since DeleteLedger deletes them at apply on both the live path and the replay, and undecodable rows are reported), and `compareNumscripts` (SubAttrNumscriptContent immutable version entries + SubAttrNumscriptVersion latest pointers vs SavedNumscript/DeleteLedger logs — a save writes immutable content and advances the latest pointer to the greatest stored semver; catches altered/missing/extra content and a latest pointer that is not the greatest saved semver; baseline-seeded under archiving so surplus/injected rows are flagged, no archive-orphan tolerance), and — the single peer-store exception per scope refinement (a) above — `compareReverseMapOrphans` (the readstore reverse map `0x03` vs the stored SubAttrIndex registry **and** the audit-replayed `MetadataSchema`: a row is an orphan when its `(ledger, target, metadata key)` is in *neither* — either because the replay observed a `RemovedMetadataFieldType` for it, or because it is simply absent from both. **Every oracle term is frozen at the verified log sequence, so a verdict is only reached when the peer's fold cursor is exactly aligned with it** (`indexedSequence == lastSequence`); the malformed-key class needs no oracle and always runs. The two unaligned positions are skips and are **not** symmetric. *Behind* is the ordinary state on a live cluster (the registry is written at Raft apply while the rmap folds later), so nothing can be concluded. *Ahead* cannot happen by race: the builder folds FROM the primary log stream and writes its cursor only for logs it has already read out of the primary store, so `progress(t) <= maxLogSeq(t)` at every instant — and **`Check()` pins the peer snapshot strictly BEFORE the primary one so the two pinned values inherit that ordering**. Do not reverse those two pins: taken the other way the gap admits logs applied and folded between them, and the pass then judges rows for ledgers and fields created after the primary pin against oracles that predate them, reporting a healthy cluster as corrupt. The ordering is what removes the need for cross-store atomicity — an ordering that can only leave the peer behind suffices, because behind is already a skip. Ahead remains reachable one way only: a primary-store rollback beneath the read-index cursor (`RestoreCheckpoint` on a follower restore) lowers `maxLogSeq` while the read index keeps its progress. Unlike `usagebuilder`, which detects this and calls `usagestore.Reset()`, **the index builder has no rollback reset**, so the read index legitimately holds rows for logs the restored primary never had; that divergence belongs to the missing reset, not to the purge path this pass audits, so it is logged loudly and skipped rather than reported (reporting would paint `Check()` red on a restore that never self-heals). The unknown-ledger verdict is driven by liveness alone — never by a separate append-only "was deleted in the replay" set consulted first — so a ledger recreated under the same name keeps its rows legitimate by construction rather than by relying on the retained tombstone that makes that lifecycle unreachable. The schema term is what makes the pass precise for its target — `RemovedMetadataFieldType` is the one log that both removes the schema field type and runs `purgeReverseMapForKey`, so "absent from the replayed schema + live rmap rows" means exactly "the point-delete scan missed rows". It also keeps `DropIndex` residue out: `DropIndex` removes the registry entry but leaves the schema field declared and purges no readstore rows at all, and since `Check()` has no warning channel a registry-only oracle would make every cluster that ever dropped a metadata index permanently red — that leak is `EN-1621`, not this pass. The schema is the **replayed** one, never stored `LedgerInfo`, or the pass would be self-referential; likewise the live-ledger set comes from `knownLedgers`. Two further classes share the enum: rows for a ledger the audit does not list as live, and malformed keys — reported, never silently skipped. Findings are aggregated per `(ledger, namespace, metadata key)` with a row count and one sample entity, so a field dropped on a large ledger cannot emit millions of events. The 4-byte encoding version is deliberately **not** validated: current and pending versions legitimately coexist during a per-replica rewrite, and stale versions are reclaimed at boot by `purgeOrphanVersions`. **Known coverage limit**: once `EN-1621` makes `DropIndex` purge rows, a regression in that new purge would strand rows while the schema field is still declared and this oracle would not catch it — the oracle must be revisited then); extend the list as new persisted projections land. **Known projection gaps**: `LedgerInfo.DefaultEnforcementMode` and the ledger-metadata attribute (`SubAttrLedgerMetadata`) have no compare pass yet (both are rebuilt on restore by RebuildDelta).

9. **Never bypass the FSM coverage gate** — Every cache-attribute read on the FSM hot path MUST go through `Scope.GetX(...)` so the per-order `coverage_bits` admit it. Reading the underlying `Registry.X.KeyStore().M` (or any other parent-cache iterator) directly skips the gate and produces non-deterministic FSM behavior: the gate is what binds the order to the admission-declared preload set, and a direct read silently sees keys the proposer never declared. There is NO documented exception — paths that need to iterate (e.g. cascade-on-delete) MUST either declare the relevant `preload.Needs` upfront, defer the work to a lifecycle path (`batch.deleteLedgerData` + `MarkLedgerForCleanup`), or be rejected at design review. New helpers that scan the parent KeyStore from inside an order/TU handler are the violation, even when wrapped in a method on `WriteSet`. The coverage gate exists precisely so admission's declared key set is the FSM's only legitimate read horizon — under no circumstances should the apply path widen it on the fly.

10. **An accepted `raftcmdpb.Order` is immutable until audit capture** — Once admission has converted a request into an `Order`, neither admission nor FSM processing may mutate its *business* payload before the order is serialized for the audit chain. The chain binds the accepted order's **business-intent bytes**: `AuditItem.SerializedOrder` is the business-intent projection (order with `OrderTechnical` excluded, via `processing.MarshalOrderBusinessIntent`) marshalled AFTER `ProcessOrders` runs, while for a keyed proposal the idempotency hash is frozen from the SAME projection of the SAME order BEFORE processing (`processor.HashProposal`) and re-derived from the audited bytes by the checker (`recomputeProposalHash` → `processing.HashOrders`). Any in-place mutation between those two points makes the audited bytes prove a different order than the one accepted — and, with an idempotency key, a false `CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH`. The rule covers *indirect* mutation through any structure the order still aliases: maps, slices, byte slices, nested messages. The canonical trap is merging Numscript `set_tx_meta` output into `CreateTransactionOrder.Metadata` — build effective transaction metadata in a map independent of `order.Metadata` instead. Only `OrderTechnical` fields (`coverage_bits`, `inputs_resolution_hash`, `preload_unavailable`) are outside this rule: both `hashOrder` AND the audit serialization (`marshalOrdersForAudit`) exclude the technical sub-message through the shared `processing.MarshalOrderBusinessIntent`, so admission may still stamp them without affecting either the idempotency hash or the audit business-intent hash (EN-1558).

### Code style

7. **Prefer parameters over separate methods** — When adding a boolean mode (dry run, force, preview), add it as a parameter to the existing method rather than creating a new method.
8. **Numscript syntax** — Literal account names require `@` prefix (e.g., `@funding:pool`). Multiple `send` blocks per script are supported. Variables don't use `@`.

## Reference Implementation

**The reference implementation is `github.com/formancehq/ledger`.** Follow its patterns for application structure, dependency injection (fx), lifecycle management, HTTP/gRPC servers, OpenTelemetry, and error handling.

## Documentation Maintenance

**CRITICAL**: Always maintain documentation when making changes.

- **Document new technical mechanisms** — when introducing a new technical mechanism, subsystem, or non-obvious invariant, add a dedicated page under `docs/technical/architecture/` and link it from the corresponding `README.md`
- **Update `docs/technical/contributing/api-comparison.md`** when adding, modifying, or removing API endpoints
- **Update `docs/ops/cli.md`** when modifying CLI commands, flags, or behavior
- **Update `openapi.yml`** if HTTP endpoints change
- **Update code comments** if interfaces or behavior change
- **Keep documentation in English**
- **Regenerate demo GIFs** after CLI changes: `just generate-demo`

## Pre-commit Checks

**CRITICAL**: Before completing any task, run pre-commit checks.

```bash
# Preferred: uses nix develop for reproducible toolchain
nix develop --command bash -c "just pre-commit"

# Alternative: direnv-based
direnv allow && eval "$(direnv export bash)" && GOROOT= just pre-commit
```

This runs `go generate ./...`, `go mod tidy`, and `golangci-lint run --fix`.

Always verify compilation with `GOROOT= go build ./...` before submitting. The `GOROOT=` prefix is required to avoid Go toolchain version mismatch errors when nix is not active.

## Mock Generation

**CRITICAL**: After any change to interfaces annotated with `//go:generate mockgen`, regenerate mocks immediately with `go generate ./...`.

Interfaces with mockgen: `Transport` (`internal/infra/node/transport.go`), `Controller` (`internal/application/ctrl/controller.go`), `Admission` (`internal/application/ctrl/controller_default.go`), `Spool` (`internal/storage/spool/spool.go`), `WAL` (`internal/storage/wal/wal.go`), `InMemoryStore` (`internal/domain/processing/store.go`), `Checker` (`internal/infra/health/healthcheck.go`), `Proposer` (`internal/infra/state/metadata_converter.go`).

## JSON Property Naming

**CRITICAL**: All JSON properties must use **camelCase** (OpenAPI spec and Go struct tags).

## Protocol Buffers

**CRITICAL**: After modifying any `.proto` file, **immediately** run `just generate-proto`. Realign field numbers sequentially when adding/removing fields.

See [docs/technical/contributing/protobuf.md](docs/technical/contributing/protobuf.md) for full details (file locations, vtprotobuf, Uint256 wire format, adding new command models).

## Conventions

For full conventions with examples, see [docs/technical/contributing/conventions.md](docs/technical/contributing/conventions.md).

Key rules:
1. **One file per command** and **one file per HTTP handler**
2. **No global variables** for flags - use structs
3. **Group variable declarations** in `var (...)` blocks
4. **No type aliases** - use original types directly
5. **Never ignore errors** - handle explicitly or `_ = ...` with comment
6. **Struct methods colocation** - all methods in same file as struct. If a file grows large, extract sub-types (composition) rather than splitting methods across files
7. **Build into `build/`** directory - never leave binaries in repo root

## File Structure

- **Server**: `cmd/server/` - main server binary entry point
- **CLI**: `cmd/ledgerctl/` - one file per sub-command. See [docs/ops/cli.md](docs/ops/cli.md).
- **Domain**: `internal/domain/` - value objects, **business errors emitted by the FSM**, domain services (`processing/`, `accounttype/`, `analysis/`, `replay/`), and cryptographic primitives (`crypto/signing/`, `crypto/keystore/`). Errors in this package are FSM-generated business outcomes (e.g. `ErrInsufficientFund`, `ErrEmptyTransaction`, `ErrLedgerNameRequired`). Admission / integration / config validators live in `internal/application/<layer>/errors.go` and use `domain.NewValidationSentinel` to build their own sentinels — do NOT pile non-FSM errors into `internal/domain`.
- **Bootstrap**: `internal/bootstrap/` - composition root (fx wiring, config, TLS, persisted config)
- **Application**: `internal/application/` - use cases (`admission/`, `ctrl/`, `events/`, `check/`, `indexbuilder/`, `mirror/`)
- **Infrastructure**: `internal/infra/` - consensus (`node/`, `state/`), caching (`cache/`, `attributes/`), transport, health, monitoring, `backup/`, `bloom/`, `coldstorage/`, `preload/`, `receipt/`
- **Utilities**: `internal/pkg/` - zero/low-dependency utilities (`kv/`, `signal/`, `futures/`, `commands/`, `bitset/`, `bytesize/`, `filterexpr/`, `semver/`, `tarutil/`, `vtmarshal/`, `worker/`)
- **Storage**: `internal/storage/` - Pebble DAL, WAL, spool, `readstore/`, `pebblecfg/`
- **Query**: `internal/query/` - CQRS read-side queries
- **Adapters**: `internal/adapter/` - transport layer (`grpc/` primary API, `http/` REST compat, `json/` serialization, `auth/` JWT/Ed25519 authentication, `v2/` v2 compatibility layer)
- **Proto definitions**: `misc/proto/` -> generated code in `internal/proto/`
- **Demos**: `misc/demo/` - VHS tape files for CLI demos
- **Numscript examples**: `misc/numscript/examples/`
- **Public packages**: `pkg/` - public API (`actions/`, `scenario/`, `testserver/`)
- **Tests**: `tests/` - test suites (`e2e/`, `scenarios/`, `antithesis/`, `perf/`, `schemathesis/`)
- **Operator**: `misc/operator/` - Kubernetes operator (separate Go module). CRD types (`api/v1alpha1/`), controllers (`internal/controller/`), Helm charts (`helm/`), kubectl plugin (`cmd/kubectl-ledger/`), web UI (`ui/`), e2e tests (`e2e/`)

## Build Tags (Optional Features)

The default build (`go build .`) produces a **light binary** (~60 MB) without heavy optional dependencies. To include optional features, use positive build tags:

| Tag | Feature | Heavy dependencies |
|-----|---------|-------------------|
| `kafka` | Kafka event sink | `IBM/sarama` |
| `nats` | NATS JetStream event sink | `nats-io/nats.go`, `nats-io/nats-server` |
| `clickhouse` | ClickHouse event sink | `ClickHouse/clickhouse-go` |
| `databricks` | Databricks event sink | `databricks/databricks-sql-go` |
| `s3` | S3 cold storage & backup | `aws-sdk-go-v2` |
| `azure` | Azure Blob Storage backup | `azure-sdk-for-go/sdk/storage/azblob`, `azure-sdk-for-go/sdk/azidentity` |
| `pyroscope` | Pyroscope continuous profiling | `grafana/pyroscope-go` |

Build with all features: `just build-full` or `go build -tags "kafka,nats,clickhouse,databricks,s3,azure,pyroscope" .`

Scenario tests use a separate build tag: `go test -tags scenario ./tests/scenarios/... -timeout 20m`

Tests with event-sink feature tags (`kafka`, `clickhouse`) start Testcontainers from `TestMain`, so Docker access is required even for compile-only checks such as `-run '^$'`.

## Testing Conventions

See [docs/technical/contributing/testing.md](docs/technical/contributing/testing.md) for full testing guidelines.

Key rules:
- **Never use `time.Sleep`** in tests - use `require.Eventually`
- **Always use `t.Parallel()`** in unit tests
- **Use gRPC client** (`servicepb.BucketServiceClient`) in integration tests
- **Use helper functions** from `tests/e2e/testutil/` (helpers and server setup)
- **E2E tests** use the `e2e` build tag and Ginkgo/Gomega framework: `go test -tags e2e ./tests/e2e/... -timeout=600s`
- **Never hand-roll mocks** — if a test needs to fake an interface, add a `//go:generate mockgen` directive on the interface (see [Mock Generation](#mock-generation) above for the standard flag set), run `go generate ./...`, and use the generated `MockXxx` in the test. Hand-rolled fakes drift from the interface, lose call recording for free, and duplicate effort.

### Running all tests (with all optional features)

```bash
# Unit tests with all features
just test-full
# or: go test -tags "kafka,nats,clickhouse,databricks,s3,pyroscope" ./... -timeout 20m

# E2E tests with all features
just test-e2e-full
# or: go test -tags "e2e,kafka,nats,clickhouse,databricks,s3,pyroscope" ./tests/e2e/... -timeout 20m

# E2E tests for a specific feature (e.g., ClickHouse sink)
go test -tags "e2e,clickhouse" ./tests/e2e/... -timeout 20m
```

## Configuration Safety Checks

The server persists critical config (`node-id`, `cluster-id`, `idempotency-ttl`, `storage-schema-version`) in Pebble under the Global zone on first boot and validates on subsequent boots. Mismatch on `node-id`/`cluster-id` is fatal. Use `--unsafe-skip-config-validation` to bypass (dangerous). Schema version mismatches are never bypassable, even with `--unsafe-skip-config-validation`. See [docs/ops/deployment.md](docs/ops/deployment.md) and [docs/ops/cli.md](docs/ops/cli.md) for details.

Key files: `internal/bootstrap/persisted_config.go`, `internal/bootstrap/config_validation.go`, `internal/bootstrap/module.go`.

## Request Signing

Ed25519 request signing for authenticity and integrity. See [docs/ops/signing.md](docs/ops/signing.md) for operations and [docs/ops/maintenance-mode.md](docs/ops/maintenance-mode.md) for maintenance mode.

## Architecture

See [docs/technical/architecture/](docs/technical/architecture/) for detailed architecture documentation. Key design principles:

- **Single Raft group** manages all ledgers
- **FSMs must be fast** - they run in the critical path of Raft consensus
- **Uber fx** for dependency injection - see [docs/technical/contributing/getting-started.md](docs/technical/contributing/getting-started.md)
- **Formance go-libs** for service lifecycle, OTLP, HTTP server

- I would like you to respect the concepts of DRY (Don't Repeat Yourself).

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **ledger** (41000 symbols, 142383 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> Index stale? Run `node .gitnexus/run.cjs analyze` from the project root — it auto-selects an available runner. No `.gitnexus/run.cjs` yet? `npx gitnexus analyze` (npm 11 crash → `npm i -g gitnexus`; #1939).

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows. For regression review, compare against the default branch: `detect_changes({scope: "compare", base_ref: "main"})`.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `query({search_query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `context({name: "symbolName"})`.
- For security review, `explain({target: "fileOrSymbol"})` lists taint findings (source→sink flows; needs `analyze --pdg`).

## Never Do

- NEVER edit a function, class, or method without first running `impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `rename` which understands the call graph.
- NEVER commit changes without running `detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/ledger/context` | Codebase overview, check index freshness |
| `gitnexus://repo/ledger/clusters` | All functional areas |
| `gitnexus://repo/ledger/processes` | All execution flows |
| `gitnexus://repo/ledger/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
