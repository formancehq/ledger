# Read consistency and projection certification: boundaries and evidence

The [native manifest](read-consistency-projections.json) defines the temporal
correctness audit for Ledger reads. Every response must correspond to a state
permitted by its requested consistency mode, fixed primary-store horizon, and
the readiness/version contract of each projection it consults. This domain owns
certification and publication of that state; it does not re-audit filter meaning
once the state is fixed.

## Temporal coordinates

Record every applicable coordinate before comparing reads or calling a result
stale. A response body without these coordinates is insufficient evidence.

| Coordinate | Meaning and production anchor |
|---|---|
| Mode and route | Default reads pass through `internal/bootstrap/controller_routed.go` and `internal/infra/node/read_index.go`; stale reads omit the quorum barrier. Record entry point, node role, forwarding decision, and whether `ReadIndexAndWait` ran. |
| Raft barrier `R` | The commit index returned by `ReadIndex` and awaited by the FSM. It is an admission floor, not a projection cursor. |
| Main horizon `H` | `internal/query/aligned_snapshot.go` reads `LastAppliedIndex` from the `dal.ReadHandle` used for main-store leaves and enrichment. It must satisfy `H >= R` when `R` exists and remain fixed while projections catch up. |
| Native sequence | The main handle's log/audit sequence used for target-specific trimming and history leases. It differs from the Raft index because an entry may emit no native item. |
| Projection position | The readstore Raft certificate, audit-index progress, or usage progress read from the immutable peer-store snapshot supplying values. Record source cursor separately from Raft certification. |
| Served index state | Index identity, target/field, `CurrentVersion`, pending version, and readiness from the pinned read-store view. A registry version does not select the local query keyspace. |
| Checkpoint identity | Checkpoint id, declared boundary, main/read directories, frozen certificates, ready marker, and acquisition lifetime. Live and checkpoint stores are not interchangeable evidence. |

For a projection-backed default live query the expected chain is:

```text
ReadIndexAndWait -> R
open one reserved main handle -> H and native sequence
require H >= R
wait only for consulted projection certificates >= H
open projection snapshot and re-read certificate/version from it
trim projection-ahead membership to the main pin where required
execute and enrich from those pinned views
```

Stale mode removes only the first line. Point reads and main-store-only queries
owe no read-index wait. `query.AlignmentOwed` must match compilation's storage
choices. That correspondence is temporal evidence; predicate membership at a
fixed state belongs to query semantic equivalence.

## Projection publication contracts

The read index, audit index, and usage store are peer projections rebuilt from
committed primary history. Their cursor, durability, and public freshness
contracts differ, so a generic "eventually consistent" rule is not an oracle.

### Read index and versions

The index builder takes a bounded primary snapshot, folds the visible native
items, and publishes terminal writes and the Raft certificate in one Pebble
batch. Native progress remains separate for replay, history, and reclamation.
A certificate at `H` claims completeness through that applied horizon,
including entries which emitted no log item.

Index creation and retyping add a second state machine. Queries serve
`CurrentVersion` until the pending version is complete and its switch commits.
Certification alone does not promote a rewrite. Drops, field removal, and
ledger deletion must remove versioned keyspaces without invalidating readers
holding leases; a reader whose pin predates a folded ledger deletion is
rejected (`ErrLedgerNotFound`), never served the wiped keyspace as empty.
Findings must locate the defect in row folding, certification, activation,
or reclamation.

Startup dispatch must describe the durable read-store cursor, not the newer
main registry. Recover the cursor, ledger history, and active index versions
from one read-store snapshot, and apply later drops or ledger deletions only
when their logs are folded. An intermediate query-checkpoint certificate can
wake a live read while checkpoint materialization waits for audit progress;
it therefore claims complete pre-drop membership even before startup catch-up
finishes. The regression witness closes and reopens both stores, starts a new
live read pinned before the drop, and checks a non-empty result at that
intermediate certificate. Record the actual startup read-admission path and
audit notification, rather than assuming requests survive a process restart.
Generic recovery coverage must also preserve backfill/rewrite ownership,
tombstone high-water marks, and ledger-delete replay obligations after rollback.

An aligned read-index snapshot may legally be ahead of the main handle only
when target-specific gates project it back to the main pin:

- transaction and log membership use `query.MainHorizonKeep`;
- account metadata resolves event history at the pin and has-asset rows use
  first-touch stamps;
- folded account membership applies an account-wide ephemeral purge to current
  has-asset membership in the same batch as aligned progress, while metadata
  history and account-to-transaction mappings remain queryable at older pins;
- schema and `IndexVersionState` come from their owning pinned views;
- ledger liveness is re-read as a live point lookup issued once the projection
  snapshot is open (`requireLedgerLive`), so a folded deletion rejects the read.

Account-wide ephemeral purge is an explicit exception to projection-ahead
reads. If the main handle is pinned before a purge while the aligned index
snapshot is acquired after it, the physically deleted current has-asset
membership cannot be reconstructed at the older main pin. Callers
that require that pre-purge membership must use a checkpoint whose main and
index snapshots are aligned at the same applied horizon.

### Audit and usage projections

Audit-index and usagebuilder paths enter scope when their values are served.
Establish their documented freshness promise before judging lag. Usage is
asynchronous, provides snapshots for coherent multi-counter reads, and rebuilds
online from audit history; it is not a strong read-index projection and does not
participate in query checkpoints. `DefaultController.WithStores` therefore
marks historical controllers so checkpoint responses omit live usage instead
of mixing timelines. Missing checker coverage is not itself a defect for an
explicitly rebuildable peer projection.

Progress must not lead visible folded state. Failure evidence distinguishes
batch visibility from durability/flush guarantees. Compare every projected
key/value with an independent fold of the exact source prefix; totals alone are
too weak.

## Query-checkpoint publication

A query checkpoint is a frozen main/read-store pair. The `.ready` marker is
published last; markerless final or temporary directories are not discoverable
state and must be rebuilt or removed. Opening re-validates that the frozen read
projection certifies the main horizon. A frozen mismatch is an error because it
cannot catch up.

Deletion may block new acquisitions but must preserve both components until an
existing reader releases them. Evidence must name the filesystem phase, marker,
registry state, lease, and both horizons. Durable cross-cluster reconstruction
of a checkpoint declaration belongs to `persistence-restore-replay`; coherent
publication of the materialized pair belongs here.

## Direct, prepared, and neighboring domains

Direct/hardcoded and prepared reads are in scope only for their temporal
envelope. For equivalent store dependencies, trace the barrier context,
reserved handle, `H`, `AlignmentOwed`, aligned snapshot, version resolver,
horizon trim, and release order. A different set caused by binding, compilation,
sort defaults, aggregation, or iterators at an identical horizon/version is not
a read-consistency finding.

| Overlap | Owner and rejection boundary |
|---|---|
| Predicate acceptance, binding, boolean/range semantics, membership/order, and pagination at fixed state | `query-semantic-equivalence`. Reject here after identical horizon, projection snapshot/version, and readiness are proved and the correction changes query semantics. |
| Barrier propagation, stale/default labeling, fixed pin, waits/certificates, ahead trimming, readiness/version and checkpoint publication | `read-consistency-projections`. Semantic audits hand temporal evidence here without duplicating the finding. |
| Consistency-header parsing and wire/error representation | `api-boundary-contracts`, unless the decoded mode is correct and internal routing drops it. |
| Quorum, elections, membership, forwarding, and snapshot-install ordering | `raft-membership-leadership`. This domain consumes a valid `ReadIndex` and owns enforcement after that boundary. |
| Durable loss or incorrect reconstruction across restart/restore | `persistence-restore-replay`. Live/frozen mis-certification of intact state stays here. |
| Worker cancellation and shutdown joining | `concurrency-lifecycle-shutdown`, unless a reachable response falsely claims certified completeness. |
| Checker failure over primary projections | `integrity-verifier-soundness`; peer readiness and served-result consistency stay here. |
| Conservation and amount arithmetic | `accounting-invariants`; temporal omission/duplication of a correct row stays here. |

Cross-page writes are not generally defects because cursors do not retain a
snapshot across requests. Unsupported snapshot guarantees, performance/lag
alone, new query semantics, checker expansion, generic recovery, and test-driver
implementation are excluded. Ambiguous promises become questions, not findings.

## Proof and rejection criteria

Every finding supplies:

1. a reachable production entry point and consistency mode;
2. the exact event ordering or fault point, including node role;
3. `R`, `H`, native sequence, projection position, and version/checkpoint id;
4. expected and observed identities, values, or errors from a non-empty fixture
   with positive and negative witnesses;
5. the violated invariant and single correction owner;
6. repository evidence for the faulty gate or publication order;
7. a regression plan which fails when that mechanism is disabled;
8. the existing test gap and current-SHA reachability.

Reject or downgrade when requests do not share the required horizon/version,
the mode permits the lag, the projection is not consulted, a documented folded
account or cross-page exception applies, the transition is unreachable, or an
existing guard prevents the interleaving. Agreement between shared helpers,
green tests, advancing counters, or an empty fixture is not proof.

Use controller alignment/horizon, aligned-snapshot, indexbuilder
progress/retype/checkpoint, readstore lease/version/progress, usage integration,
e2e cross-store/checkpoint, and Antithesis projection tests as evidence leads.
Inspect their assertions and reachability, and record exact commands and fault
points. Label proposed checks as unexecuted.

## Deduplication and later execution

Refresh open PRs, qualified reports, and backlog state before a campaign. A
historical fix/test is a lead, not proof of current defect or novelty. Use stable
ids `read-consistency-projections/<root-cause-name>` and retain rejected/fixed
hypotheses for deduplication.

Validate the JSON with the launcher predicate and verify every glob/document.
The temporary contract-test pattern in `scripts/aiaudit/manifest_test.go` may be
copied for this id: valid input reaches the dirty-HEAD guard and malformed input
fails before provider invocation. Remove the temporary test afterward. Run
`bash scripts/agent-check` and
`AI_REVIEW_BASE_SHA=<exact-base-sha> bash scripts/agent-check-pr`.

Only a separately authorized task after merge may run
`bash scripts/ai-audit read-consistency-projections`, followed by an independent
challenge at the same clean SHA. Preparing this manifest does not launch the
audit, create findings, or publish Jira issues.
