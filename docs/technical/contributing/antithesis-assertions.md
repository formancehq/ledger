# Ledger internal Antithesis assertions

Internal assertions complement the Jepsen transfer model, which remains the
independent end-to-end oracle. They report specific contract failures that a
failed HTTP response or later recovery could hide. They do not classify every
storage error, internal error, or failed request as corruption.

## Where the SDK is live, and guarding its cost

The SDK is pinned to the Formance fork
(`github.com/formancehq/antithesis-sdk-go`, branch
`feat/assert-enabled-default-no-op`) through a `replace` in `go.mod`, because
the fork keeps the upstream module path. It is no-op by default: `assert`,
`lifecycle`, `random` and `instrumentation` compile to no-ops unless
`enable_antithesis_sdk` is set. Nothing has to be tagged to get a quiet, fast
binary — released builds, local `go build` and the default test suite all get
one — so a forgotten tag costs observability, never throughput.

A no-op call is still an ordinary Go call, so its **arguments are evaluated**:
the details map is built and any `err.Error()` or `id.Hex()` inside it run
before the empty callee is reached. The fork adds `assert.Enabled`, a `const`
that is false in an unarmed build, so wrapping assertion-only work in it makes
the compiler discard the work entirely — verified: the guarded helper is not
merely skipped but absent from the linked binary.

```go
if assert.Enabled {
    details := map[string]any{"raftIndex": idx, "lag": lag}
    assert.Sometimes(cond, "some milestone", details)
}
```

Write the guard in that exact positive form: no `else`, not negated, not a
comparison. The fork's instrumentor recognises precisely this shape and skips
the "not taken" coverage edge it would otherwise synthesise — an edge that can
never be reached, since the condition is constant. Assertions inside a guard are
still discovered and still registered in the catalog, so no property is lost.

Guard by call frequency: wrap the SDK call on a per-request, per-proposal or
per-order path. Leave cold invariant branches unguarded — a guard buys nothing
there and reads worse.

Guard the SDK call. What the guard eliminates is the ~540 ns and 6 allocations
of the call itself, including the details map an unarmed build would otherwise
still build.

For the work that feeds it, the line is drawn by **what can read the result**,
not by whether the work exists only for the property.

A value that outlives the guard stays unconditional: a struct field, a return
value, anything another function can reach. Guarding those buys almost nothing
and creates a real hazard — a variable that is correct in an armed build and
silently zero in production, waiting for whoever first reads it for a metric.
The `createdLogs` walk behind the commit-outcome counters benchmarks at 4.6 ns
and zero allocations against the ~540 ns it feeds, under 1% of the win. So
`OrdersResult.CreatedTransactions`, `RevertedTransaction`, `stagedTransactions`,
`resolved` and `oldTermResolved` are all deliberately unconditional.

A computation that cannot escape the guard belongs inside it: nothing outside
can observe it, so there is no zero to mistake for a real value.
`Machine.CommitPreparedBatch` walks the batch's results and builds a details map
per result purely to feed two `Sometimes` calls; the whole walk sits inside
`if assert.Enabled`, and an unarmed build does not allocate a map per commit.
The applier's `runCommitter` is the contrast: its walk over `work.futures`
resolves every future, so the walk and the `oldTermResolved` count it carries
stay unconditional, and only the details map and the SDK call are guarded.
The test is whether deleting the guard's body would change anything but the
assertions.

Five builds set `enable_antithesis_sdk`, and each would be silently useless
without it:

| Build | Why it must be armed |
|---|---|
| `Dockerfile.antithesis` | The instrumented SUT. Unarmed, the generated `assert.AssertRaw` registrations do nothing, the catalog is empty, and the campaign reports no violations because no property was ever registered — a silent pass. |
| `tests/antithesis/workload/Dockerfile` | The drivers that produce the campaign's `Sometimes` coverage. |
| `tests/antithesis/run_model_test.sh` | The **driver only**. It reads the SDK's local JSON output and requires specific assertions, so an unarmed driver yields an empty stream. The server is left unarmed deliberately: it is never given `ANTITHESIS_SDK_LOCAL_OUTPUT`, so arming it would buy no signal and only cost throughput on a gate whose coverage probes depend on completed work. |
| `just test-antithesis-assertions` | The emission contract tests below. |
| `Tests-Antithesis-Workload` (CI) | The workload module's own tests. Several of them re-exec a driver with `ANTITHESIS_SDK_LOCAL_OUTPUT` and decide the case from the assertions it emitted, which is exactly what the no-op SDK does not write. They skip in an unarmed build rather than fail on the missing file, so the tag is what makes them run at all. |

Both images build the instrumentor from the fork at the same SHA as the
`replace`, because `go install pkg@version` ignores `replace` and so cannot
reach a fork that keeps the upstream module path. An instrumentor that does not
know the `assert.Enabled` shape leaves an unreachable coverage edge at every
guarded site, so the two are not merely meant to stay in step:
`check-repo-invariants` fails `SDK_PIN_DRIFT` when the fork commit, the fork
module path or the SDK version disagree across the two `go.mod` replaces, the
two Dockerfile checkouts and the two `-instrumentor_version` flags.

Both images also pass `-instrumentor_version`, which sets the SDK version the
*generated notifier module* requires. With the `replace` in place this does not
decide which SDK is built — a versionless `replace` redirects every selected
version, so the notifier's requirement cannot outrank it, and instrumenting both
ways confirms the fork is used either way. The flag keeps the generated require
line matching the one we declare, and it restores the real guarantee if the
`replace` is ever dropped: the flag then defaults to the instrumentor's own
`SDK_Version` ("0.8.0"), and stable `v0.8.0` outranks a prerelease in module
resolution, which would silently select the upstream live-by-default SDK. That
is about module selection only — dropping the `replace` today would not build
at all, since upstream has no `assert.Enabled`.

## Safety properties

The existing errors, panics, audit bytes and idempotency decisions remain in
place after reporting. SDK assertions do not stop execution. Coverage misses
remain safe admission-contract rejections and are never frozen as idempotency
failures. No assertion adds a storage read to the FSM apply path.

| Constant property | Guard |
|---|---|
| `admission declared every FSM attribute access` | The coverage gate refuses an undeclared access. |
| `revert target has allocated transaction state` | An allocated transaction is missing its projection, after the ordinary unknown-ID guard. |
| `revert target has nonempty original postings` | An allocated revert target has empty postings. |
| `effective-date revert target has a timestamp` | An effective-date revert target has no timestamp. |
| `linearizable query snapshot covers its read barrier` | A main-store snapshot is below the query's established read barrier. |
| `linearizable audit snapshot covers its read barrier` | The separate audit-read snapshot guard detects the same violation. |
| `committed volume is present in pebble` | A successful sentinel read finds a required post-commit volume absent. An I/O error does not establish absence. |
| `posting has a volume update` | An expected posting key is missing from logical updates. |
| `volume delta matches posting quantities` | Gross input/output changes differ from the postings. |
| `nonzero volume delta is explained by postings` | An additional nonzero update has no posting, including an unrelated balanced pair. |
| `all volume updates conserve double entry` | Existing conservation check over all logical updates fails. |
| `persisted volume updates conserve double entry` | Existing conservation check after persistence-class partitioning fails. |

Existing property names, including volume monotonicity, cache/Pebble divergence,
aggregated conservation, preload generation crossings, stale proposals and
blocked `WaitForApplied`, are preserved. Accounting checks use the full ledger,
account, asset and color key, include `world`, and permit forced overdrafts.
See the [sentinel lifecycle](../architecture/subsystems/fsm/deterministic-fsm.md)
for the pre-purge and post-commit boundaries.

## Coverage and campaign applicability

These `Sometimes` properties describe actual milestones. An unmet target is an
exploration gap; it does not prove corruption. Reporting must retain this
applicability table alongside the catalog and list unmet applicable targets.
Cataloged properties outside a campaign's workload are non-applicable; never
turn a disabled feature or tolerated failure into a successful observation.

| Property | Milestone | Applicable campaign |
|---|---|---|
| `multi-transaction proposal committed` | Successful commit of a non-replayed proposal with at least two created transactions. | Transfer atomic bulks |
| `atomic proposal rejected after staging an earlier transaction` | A non-skippable later handler failure after a create or revert staged successfully. This does not assert rollback correctness. | Transfer atomic bulks; mixed revert rollback also has a Ledger integration regression |
| `transaction revert committed` | Successful commit containing a new revert outcome. | Transfer |
| `repeat revert rejected` | The existing already-reverted guard rejects a known transaction. | Transfer |
| `successful idempotency outcome replayed` | A live matching key/hash returns the original successful outcome. Expired keys and frozen failures do not count. | Idempotence with references and key-only idempotence |
| `idempotency body conflict rejected` | A live key has a different body hash and is rejected. | Both idempotence variants |
| `indexed snapshot aligned after waiting for projection` | An indexed snapshot becomes aligned after actual lag; a canceled wait does not count. This is not a query-success claim. | Indexed reads under sufficient contention |
| `nonempty sentinel verification completed` | The enabled, nonempty sentinel callback finishes both post-commit checks. A disabled callback or empty batch does not count. | Every sentinel-enabled campaign with writes |

Commit facts are captured as scalar values in the proposal's `ApplyResult` and
carried by its `PreparedBatch`. Post-commit reporting never consults the live
WriteSet or FSM state, which may already belong to the next preparation. This
bookkeeping is neither persisted nor added to protobuf messages.

## A coverage probe single-node runs often miss

`just test-model 180` (single node) intermittently reports
`coverage index tx_builtin:TX_BUILTIN_INDEX_DESTINATION_ADDRESS served a
model-verified page` as never satisfied. The runner prints that miss as a
diagnostic and still exits PASS, so what varies between repeated runs of one
unchanged commit is whether the `COVERAGE PROBES NOT SATISFIED IN THIS RUN`
banner appears at all, on either side of an unrelated change. A single run —
banner or no banner — identifies nothing on its own. The alternating PASS and
FAIL first recorded here is the same instability, seen while the miss was
still a hard gate.

Satisfying a probe is narrow. One query must do three things at once
(`coverage.go`, `coverageHits`): need that index — the address leaf must roll
the `DESTINATION` role, one of three; come back non-empty; and pass oracle
verification. The generator deliberately emits unmatchable filters, so empty
pages are common.

Several things can consume an opportunity without producing a finding, which is
why `model findings: none` accompanies the miss and why it is not by itself
evidence of any one cause:

- the query never rolls a matching destination-role filter at all;
- a needed index is not yet active, making a not-ready rejection legal
  (`indexes.go`, `validateIndexedTransactionQuery`);
- the call fails transiently and returns before coverage is recorded
  (`queries.go`, the `IsTransient` early return).

So index-readiness delays and throughput changes can contribute alongside plain
exploration starvation. Treat a miss as an open question, not a diagnosis:
reproduce several times on both sides before attributing it to a change, and on
the cluster topology CI gates (`just test-model-cluster`) rather than this one.

## Local verification and campaign handoff

Run the pinned environment and repository gates:

```bash
bash scripts/agent-check
bash scripts/agent-validation-env --ephemeral nix develop --command \
  go test -race ./internal/infra/state ./internal/domain/processing \
  ./internal/query ./internal/application/ctrl
just test-antithesis-assertions
AI_REVIEW_BASE_SHA=<exact-base-sha> bash scripts/agent-check-pr
```

The default suite compiles the assertion sites but cannot observe one emit, so
`just test-antithesis-assertions` is the target that proves emission: it builds
the packages that hold guarded code with `enable_antithesis_sdk` and runs the
contract tests. It names those trees rather than the whole module, because an
armed run of everything costs far more than the guarded code is worth — so a
guard added outside them would never execute armed and never reach a coverage
profile, while every gate stayed green. `check-repo-invariants` rejects that:
an `if assert.Enabled` outside the recipe's trees fails
`ARMED_COVERAGE_UNREACHABLE`, and widening the recipe widens what it accepts.

`TestAntithesisStateEmission` and each affected package's
`TestAntithesisContractEmission` run deliberately corrupt fixtures in isolated
test subprocesses through `internal/pkg/antithesistest.Emitted`. It sets
`ANTITHESIS_SDK_LOCAL_OUTPUT` before SDK initialization and checks the unique
property, `hit`, and `condition` in its JSON records.

It also requires the child's `--- PASS` marker for the test it named. `go test`
exits 0 when `-test.run` matches nothing and a skip is also a pass, so exit
status alone cannot tell "the property was not emitted" from "the test never
ran" — and the second silently satisfies every row that expects no emission,
which is precisely the drift these tables exist to catch. A renamed or skipped
target now fails instead of passing quietly. These fixtures are not drivers in the workload binary. Separate
integration tests read committed business projections after transfer/revert
rollback and successful idempotency replay. The unexpected-balanced-pair
regression must fail if the reverse delta check is removed.

Build through the existing `Dockerfile.antithesis` (forked SDK/instrumentor at
the pinned SHA, `enable_antithesis_sdk`, race detector, CGO and symbols), and inspect the generated catalog, including
properties that have not fired. SDK JSON evidence alone does not prove that
the deployed image catalogs every property.

The companion `jepsen.ledger` integration enables sentinels on all three nodes,
adds transfer and idempotence, and gives key-only idempotence
(`--no-references`) its own configuration-image identity. Keep its clean-tree
and exact `LEDGER_REF` checks. Update the default pin only after publishing the
tested Ledger revision. A local clean committed candidate can be tested using
explicit `LEDGER_REPO` and `LEDGER_REF` overrides without moving the default pin.

For each local variant, retain successful-write counts, final checker result,
exact Ledger/harness revisions, configuration/image identities, catalog and
applicable coverage gaps. Compare sentinel-enabled throughput and completed
operations to the same baseline configuration. A command that merely ran the
checker is insufficient: the final audit must be conclusive. Record remote
Antithesis evidence separately; unit tests and local SDK output are not an
Antithesis campaign pass. Paid campaigns require separate authorization.

Assertion choices follow the [Antithesis assertion semantics](https://antithesis.com/docs/product/writing_tests/assertions/)
and [Sometimes guidance](https://antithesis.com/docs/best_practices/sometimes_assertions/).
