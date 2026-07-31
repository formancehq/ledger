# unfiltered-fast-path-equals-account-fold — Redundant PIT aggregate scopes agree

## Catalog candidate

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Safety |
| **Property** | For the same immutable PIT view, ledger incarnation, temporal axis, and timestamp, an unfiltered public aggregate backed by ledger-wide `scopeAsset` summaries equals a public exhaustive-address aggregate backed by per-account `scopeVolume` rows. Equality covers input and output for every asset/precision/color bucket and remains true with the same `useMaxPrecision` and `collapseColors` options on both calls. |
| **Invariant** | When two successful responses carry the same non-empty `view_token`, invoke `AlwaysOrUnreachable(canonical(fastResult) == canonical(rowResult), "pit: unfiltered asset summaries equal the exhaustive account fold")`. The canonicalizer rejects duplicate buckets and compares input/output by `(asset, color)`, independent of transport order. Add `Sometimes(allFourOptionModesPaired, "pit: redundant aggregate scopes were compared in every transform mode")` after matching pairs have been observed for `{plain, max-precision, collapse-colors, both}`. |
| **Antithesis Angle** | Publish backdated postings across several accounts, assets, precisions, and colors; then interleave compaction, tiering, multipart cold reads, cancellation, and replica restart. Asset-summary and per-account records occupy distinct key ranges and can route through different cold parts, so a lost catalog range, bad part boundary, or one-scope compaction defect can leave either query path internally plausible while the pair disagrees. |
| **Why It Matters** | PIT deliberately stores the same monetary effects twice: once per account and once as a ledger-wide asset summary. The public unfiltered fast path trusts the summary without enumerating accounts. A divergence can therefore return a silent wrong total even when filtered queries remain correct, or vice versa. The redundancy supplies a strong oracle that does not need a separate model of the audit log. |
| **Confidence** | High. Publication writes every effect into both scopes, and the two read paths are intended to represent the same totals. |

## Assertion rationale

`AlwaysOrUnreachable` is appropriate because equality is mandatory whenever both reads complete from one immutable view, while projection lag or archive faults may legitimately prevent a pair. Invoke it only after both responses succeed and their decoded, non-empty `view_token` values are equal; executions without a pair must not produce a vacuous safety success. The guard must not turn an expected classified PIT failure into a numerical mismatch. The companion `Sometimes` assertion is a coverage/liveness sonde: it proves the campaign obtained comparable responses for every transformation mode.

Canonical comparison uses arbitrary-precision integers and keys each public bucket by `(asset, color)`; the formatted asset already carries precision. It rejects duplicate keys, requires grouped results to be empty, and compares input and output independently. Do not compare display order or derive equality only from net balance. Both calls pass identical `useMaxPrecision` and `collapseColors` flags and leave `groupByPrefixes` empty, so the SUT applies the same deterministic transformation implementation after the two distinct storage reads.

## Code evidence

- `internal/storage/balancehistorystore/publish.go:39-84` duplicates every effect across both temporal axes and both `scopeVolume` and `scopeAsset`; only the volume identity retains the account.
- `internal/storage/balancehistorystore/view.go:616-672` implements `AggregateAll` by reading `scopeAsset` identities and summing them across manifest runs.
- `internal/storage/balancehistorystore/view.go:686-746` implements exhaustive `ReadVolumes(..., nil)` from the distinct `scopeVolume` identity catalog.
- `internal/query/aggregate_history.go:64-139,179-227` sends an unfiltered, ungrouped request through `AggregateAll`, while exact-account, prefix-selected, and matching requests use per-account rows and the row accumulator.
- `internal/application/ctrl/volume_view.go:252-317,397-463` classifies an address-only `NOT` as a historical predicate with no bounded candidates. `prepareTemporalAccountSelection` consequently supplies only `match`, which makes `aggregateHistoricalVolumes` call `ReadVolumes(..., nil)` before rejecting the reserved sentinel address.
- `internal/application/ctrl/volume_view_test.go:208-238` confirms `NOT(address prefix)` uses the complete historical universe and does not invoke the current read-store compiler.
- `pkg/actions/filters.go:25-48,75-90` already provides the hardcoded address and `NotFilter` constructors needed by the workload.
- `internal/query/aggregate.go:33-89,103-241,408-420` shows both public paths finish through the same `volumeAggregator` for ungrouped results, including max-precision and color-collapse behavior. The metamorphic comparison therefore isolates scope/read divergence; it is not an independent oracle for bugs common to the final aggregator.
- `internal/storage/balancehistorystore/store_test.go:66-110` samples both APIs for one simple publication, but it does not fold all account rows and compare the two scopes across complex layouts or faults.
- `internal/storage/balancehistorystore/view.go:64-140` hashes the layout-independent logical records, `internal/storage/balancehistorystore/verify.go:24-49` verifies the physical projection, and `internal/application/balancehistory/verifier.go:629-644` compares the served digest with an authoritative replay. Replay certification should detect scope divergence; this property adds a cheaper direct online oracle for the algebraic relationship between the two redundant scopes.
- `internal/application/ctrl/volume_view.go:161-176` binds the opaque token to ledger ID, selector, manifest version, audit/log watermarks, retention floors, audit hash, logical digest, and physical manifest digest.
- `internal/adapter/grpc/point_in_time.go:15,70-145` serializes the complete `PointInTimeView` into the `x-point-in-time-view-bin` response trailer. `tests/e2e/business/point_in_time_balances_test.go:465-495` is reusable repository code for capturing and decoding that trailer from the raw generated client.
- `tests/antithesis/workload/internal/ledger.go:15-108` provides the typed driver-owned ledger-prefix registry that prevents generic drivers from writing to a property ledger.

## Failure scenario and oracle

1. A `first_` setup command creates the fixed ledger `PrefixPITScope.WithSuffix("oracle")`; add `PrefixPITScope` to `ownedLedgerPrefixes` so generic drivers cannot select it. Seed several successful proposals using `world` and accounts such as `pitscope:users:a`, but never create the valid reserved address `pitscope:never-created`. Include backdated postings, a reversal, two colors, two precisions of one asset base, and another asset.
2. Build the exhaustive public filter as `actions.NotFilter(actions.AddressExactFilter("pitscope:never-created"))`. Because the ledger is driver-owned and the setup never uses that address, the predicate is true for every historical identity, including `world` and accounts absent from the current read store. The unbounded `NOT` candidate plan forces `ReadVolumes(..., nil)` rather than `AggregateAll`.
3. Connect to one explicit replica with `internal.DialPerNode`; use the same connection and consistency mode for both calls. Issue otherwise identical PIT requests, first with `filter=nil` and then with the exhaustive `NOT` filter, capturing `grpc.Trailer` for each.
4. Decode exactly one `x-point-in-time-view-bin` value into `servicepb.PointInTimeView`. If either call fails with a classified PIT error or the tokens differ, record no numerical sample and make another bounded attempt; do not assert equality across views. If the tokens match, canonicalize both successful responses and invoke the safety assertion.
5. The parallel driver chooses one of the four `{useMaxPrecision, collapseColors}` combinations per invocation to bound I/O. A final quiescent `eventually_` command exercises all four combinations and calls the coverage assertion only when every mode obtained a same-token pair. Reuse one shared comparison helper so the safety assertion has one static callsite and one globally unique message.
6. Repeat throughout publication, compaction, tiering, cold-cache eviction, and restarts. Faults may cause fail-closed responses; every paired success must agree.

## Instrumentation status

- **Partially implemented SDK instrumentation:**
  `parallel_driver_pit_scope_equivalence` samples one axis/transform case during
  faults; `eventually_pit_scope_equivalence` requires all eight axis/transform
  cases after quiescence. Both call the single assertion callsite in
  `internal.ComparePITScopeCase`. These assertions cover paired-success
  correctness, but do not prove that a sample crossed compaction, tiering, a
  cold fetch, multipart I/O, cancellation, or restart.
- **Implemented fixture:** `first_default_ledger` creates the fixed
  `pitscope-oracle` ledger, backdated colored/multi-precision/multi-asset
  postings and an at-effective-date reversal. `PrefixPITScope` isolates it from
  generic drivers; `pitscope:never-created` is reserved and never written.
- **Implemented public oracle:** `AggregatePointInTime` decodes and validates
  exactly one `x-point-in-time-view-bin` trailer. The comparator uses
  `NOT(address == pitscope:never-created)` to force the unbounded row path,
  discards different-token pairs, rejects duplicate buckets and compares
  arbitrary-precision input/output by `(asset, color)`.
- **Existing deterministic coverage:** publication and store tests cover construction and simple reads; semantic verification covers record integrity and authoritative replay. There is no explicit metamorphic comparison of the two stored scopes.
- **Non-duplicate boundary:** `integrity-dual-axis-reversal-exactness` is the stronger audit-oracle property when it checks both account and ledger-wide results. This candidate is retained only as the cheaper same-view metamorphic check that can localize divergence between the two materializations without replaying authority.
- **Chosen placement — workload only:** do not add a production-side all-account scan. The relationship is fully public, the controlled ledger bounds the expensive row path, and the existing full verifier already performs the stronger authoritative semantic comparison. SUT-side assertions remain more valuable for invisible maintenance transitions.
- **Shared helper:** implemented in `tests/antithesis/workload/internal/point_in_time.go`
  and `pit_scope.go`; unit tests cover trailer completeness, canonical ordering,
  duplicate rejection and the eight-case menu.
- **Drivers:** implemented with the existing sole `first_default_ledger` setup
  command so every `main` timeline receives the fixture. Adding a second
  `first_` command would be unsound because the Composer selects exactly one.
- **Assertion details:** include replica address/node ID, authenticated ledger
  ID, axis, cutoff, both option booleans, both audit/log watermarks, manifest
  versions/tokens, canonicalization errors, and both canonical result slices on
  divergence.
- **Harness prerequisite:** implemented in both Compose and Kubernetes with
  balance history and the cold tier enabled, small retained runs/segments, and
  accelerated maintenance, tiering, verification, and remote-GC cadences.
  Kubernetes MinIO now has a PVC. Dedicated reachability properties still need
  to prove that the target physical transitions actually occurred in a run.
- **Scheduling decision:** evaluate the safety check periodically through the parallel command and once after quiescence. Do not gate it only on observed maintenance boundaries: no public response identifies a compaction/tier event, and separate maintenance reachability properties are responsible for proving those transitions occurred.

## Assumptions

- The fixed property ledger is isolated through the registered driver-owned prefix, and no setup or property driver ever creates `pitscope:never-created`. This makes `NOT(address == reserved)` exhaustive without an identity-enumeration API.
- A pair is comparable only when both responses identify the same immutable view token, axis, and requested timestamp.
- Expected fail-closed errors are classified by the PIT error properties; this property constrains successful numerical results only.
- `groupByPrefixes` stays empty. Grouping intentionally forces the row path and is covered by the aggregate protocol property, not this fast-path equivalence check.

## Open questions

- None.

### Investigation Log

#### Is a sampled SUT-side diagnostic acceptable, or should this remain a bounded workload-only check?

- **Examined:** `antithesis/scratchbook/existing-assertions.md`; `internal/application/balancehistory/verifier.go:30-67,333-455,535-646`; `internal/storage/balancehistorystore/benchmark_test.go:12-80`; `internal/storage/balancehistorystore/view.go:616-746`; and the SUT/workload assertion split documented by the existing research.
- **Found:** `ReadVolumes(..., nil)` enumerates every historical identity across the pinned runs, whereas the summary path exists specifically to avoid account enumeration. Production already schedules bounded physical verification and periodically performs the strictly stronger full authoritative replay plus served-semantic-digest comparison. The relationship is observable through the public API and can be bounded to a small driver-owned ledger.
- **Not found:** a production callsite that could run the all-ledger/all-timestamp fold cheaply or add coverage beyond the full verifier without creating another scan policy and sampling mechanism.
- **Conclusion:** resolved in favor of workload-only instrumentation. A SUT diagnostic is technically possible but unjustified duplication and cost for this externally visible property.

#### Can the public workload construct a provably exhaustive historical address selection without an identity enumerator?

- **Examined:** `internal/application/ctrl/volume_view.go:217-249,252-317,397-513`; `internal/application/ctrl/volume_view_test.go:208-238`; `internal/query/aggregate_history.go:64-139,179-275`; `pkg/actions/filters.go:25-48,75-90`; `internal/adapter/grpc/point_in_time.go:15,70-145`; `tests/e2e/business/point_in_time_balances_test.go:465-495`; and `tests/antithesis/workload/internal/ledger.go:15-108`.
- **Found:** an address-only `NOT` has no bounded candidate set, so PIT evaluates it over `ReadVolumes(..., nil)`, the complete historical identity universe. On a registered driver-owned ledger, reserving one valid address that no driver creates makes `NOT(address == reserved)` true for every real historical account, including `world`. The raw generated client can capture and decode the complete view trailer; equal non-empty tokens bind the two results to the same ledger, selector, watermarks, floors, hashes, and manifest digest.
- **Not found:** an API parameter that pins the second request to the first response token. Consequently the helper must discard different-token pairs and retry within a fixed bound; it must never compare two merely adjacent responses.
- **Conclusion:** resolved. The public `NOT` filter plus an isolated reserved address is a sound exhaustive row-path oracle; no test-only identity enumerator or direct Store access is required.

#### Should the assertion run only around maintenance transitions, or periodically?

- **Examined:** `tests/antithesis/README.md:19-42,147-180`; `tests/antithesis/workload/internal/driver.go:11-76`; `tests/antithesis/workload/internal/pernode.go:16-182`; `tests/antithesis/k8s/cluster.yaml:40-78`; `tests/antithesis/config/docker-compose.yaml:11-47`; and `internal/bootstrap/balance_history_config.go:15-104`.
- **Found:** Antithesis schedules many `parallel_driver_` instances throughout fault injection and reserves `eventually_` commands for post-writer quiescence. The public PIT provenance has no compaction/tier marker, so a workload cannot soundly label an individual sample as immediately before or after maintenance. Periodic parallel attempts naturally cross background maintenance, while a final quiescent command supplies reliable same-token reachability and complete option coverage. The current Antithesis manifests configure MinIO but leave the opt-in history subsystem disabled.
- **Not found:** a public maintenance event or query field that would make a transition-gated workload assertion more precise than periodic sampling.
- **Conclusion:** resolved in favor of periodic parallel safety checks plus one quiescent all-mode reachability check. Enabling and accelerating PIT maintenance in the harness is an explicit prerequisite; maintenance-specific properties provide the transition reachability signals.
