# Ledger internal Antithesis assertions

Internal assertions complement the Jepsen transfer model, which remains the
independent end-to-end oracle. They report specific contract failures that a
failed HTTP response or later recovery could hide. They do not classify every
storage error, internal error, or failed request as corruption.

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

## Local verification and campaign handoff

Run the pinned environment and repository gates:

```bash
bash scripts/agent-check
bash scripts/agent-validation-env --ephemeral nix develop --command \
  go test -race ./internal/infra/state ./internal/domain/processing \
  ./internal/query ./internal/application/ctrl
AI_REVIEW_BASE_SHA=<exact-base-sha> bash scripts/agent-check-pr
```

`TestAntithesisStateEmission` and each affected package's
`TestAntithesisContractEmission` run deliberately corrupt fixtures in isolated
test subprocesses. They set `ANTITHESIS_SDK_LOCAL_OUTPUT` before SDK
initialization and check the unique property, `hit`, and `condition` in its
JSON records. These fixtures are not drivers in the workload binary. Separate
integration tests read committed business projections after transfer/revert
rollback and successful idempotency replay. The unexpected-balanced-pair
regression must fail if the reverse delta check is removed.

Build through the existing `Dockerfile.antithesis` (SDK/instrumentor v0.7.0,
race detector, CGO and symbols), and inspect the generated catalog, including
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
