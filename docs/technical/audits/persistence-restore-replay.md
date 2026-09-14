# Persistence, incremental restore, and replay evidence contract

The [native manifest](persistence-restore-replay.json) audits whether durable
Ledger state survives persistence, backup, checkpoint plus incremental restore,
and committed-history replay. Its central oracle is the repository's
[incremental restore invariant](../architecture/subsystems/backup/incremental-restore-contract.md):

```text
live(prefix + delta) == restore(checkpoint(prefix), delta)
```

The comparison is logical and the delta must be non-empty and contain the
operation being tested. This domain adds scope and evidence requirements to the
native audit workflow; it does not run an audit, add a restore guarantee, or
authorize code changes, Jira publication, or live-store mutation.

## Reachability and proof

For every candidate, establish all of the following at the audited SHA:

1. **Production mutation:** trace an admitted order or supported persistence,
   backup, restore, or replay entry point to the exact primary-store keys and
   mutation order. Identify acknowledgement and durable boundaries rather than
   assuming the intended final state is atomic.
2. **Checkpoint and delta:** construct meaningful prefix state, identify the
   full-checkpoint log/audit boundaries, and commit the target effect afterward.
   Prove that a non-empty manifest export contains the needed log, audit entry,
   audit item, and applied-proposal evidence over the correct ranges.
3. **Lifecycle classification:** classify every affected value as preserved,
   rebuilt, or deliberately discarded. For rebuilt state, name the live writer,
   exported fact, deterministic fold, checkpoint seed, later deletion/reset
   behavior, and independent verifier. “Already in the checkpoint” is not proof
   for a value the delta can change.
4. **Restore execution:** trace download, export validation/application,
   `RebuildDelta`, restore preparation, validation/finalization and bootstrap.
   Enumerate the durable staging prefix at any interruption and the fresh-process
   path that retries, rejects, or activates it.
5. **Observable parity:** compare complete logical keys and values against the
   uninterrupted source, run `CheckStore` without findings or errors, and test a
   subsequent admission/apply/read that consumes the rebuilt state when
   practical. Counts or row presence alone do not establish parity.
6. **Falsification:** cite existing guards and tests, then give a trigger,
   failure point, expected residual state and assertion that uniquely separates
   the alleged restore defect from corruption, setup failure, read-index lag, or
   another domain's failure.

A missing replay case or test is not by itself a defect. P0/P1/P2 findings need
a reachable committed effect and a material state or behavior mismatch. When
retention, discard, or post-restore semantics are not established by current
code and authoritative documentation, emit an audit question instead of
inventing a stronger guarantee.

## State classification and source map

The audit must refresh this inventory from current writers and variants; the
table is a discovery map, not a frozen exhaustive schema.

| Boundary | Sources and required evidence |
| --- | --- |
| Live primary mutations | `internal/domain/processing/`, `internal/infra/state/`, `internal/infra/attributes/`: enumerate every audited apply mutation, including deletion/range-deletion and proposal-local effects. Record persisted class and exact keys. |
| Durable storage | `internal/storage/dal/`, `wal/`, `spool/`, `pebblecfg/`: establish write-session commit, checkpoint, cursor and recovery semantics. A cache or snapshot read is not proof that the primary write is durable. |
| Backup publication | `internal/application/backup/`, `internal/infra/backup/manager.go`, `manifest.go`, `incremental.go`, `segment.go`: trace sequence capture, segment contents, object upload, manifest swap and orphan pruning. The current published manifest must always remain fully restorable. |
| Raw delta application | `internal/infra/backup/restore.go`: verify manifest/type/range/key-shape/stream validation, batch commits, partial-prefix behavior and retry. Permanent history residency must equal the source logically. |
| Derived-state rebuild | `internal/infra/backup/rebuild.go`, `internal/domain/replay/`: compare every replay branch and fold with its live writer. Essential facts may live in `LedgerLog`, chain-bound serialized `AuditItem`, or `AppliedProposal`; absence must not silently become a default. |
| Restore preparation and bootstrap | `internal/infra/attributes/prepare.go`, `internal/bootstrap/`, restore gRPC and `ledgerctl` commands: distinguish retained genesis boundary and business provenance from cleared source identity, peers, transient jobs, cache/bloom state, and restored query-checkpoint provenance. |
| Projection and cascade inventory | Balances/volumes, transactions/references, LedgerInfo/account types/default enforcement, boundaries/mirror progress, ledger metadata, indexes/registry, numscripts, reversions, idempotency outcomes, signing, policy, maintenance mode, sink config and query checkpoints. Test create/update/delete/reset variants actually present at the SHA. |
| Integrity oracle | `internal/application/check/` and `CheckStore`: identify the independent audit-bound witness and actual checked fields. A clean checker result corroborates direct parity; it cannot launder an omitted projection or shared replay mistake. |
| Cross-lifecycle evidence | `internal/infra/backup/*_test.go`, `tests/e2e/cluster/restore*_test.go`, restore-enabled model tests: record the exact post-checkpoint mutation, non-empty delta assertion, restore composition, logical comparison, checker result and next consuming operation actually reached. |

Use the three classifications precisely:

- **Preserved:** the checkpoint value remains correct because no supported
  post-checkpoint effect can change it. Cite that closed mutation surface.
- **Rebuilt:** the delta can change it. Identify exported authority, seed and
  fold for creation, update, deletion, purge, revoke, promotion and retry as
  applicable.
- **Discarded:** source-cluster-local, cache-like or otherwise invalid state is
  removed during restore preparation. Identify the destination-cluster reseed
  path and prove it does not overwrite newer rebuilt primary values.

The current contract deliberately preserves permanent log, audit, audit-item
and applied-proposal history. It clears source identity, peers, cluster-transient
jobs, cache and persisted bloom state, and marks query-checkpoint rows with
restore provenance. Those facts are starting counterevidence, not permission to
skip inspecting their current implementations and callers.

## Delete and reset parity

Incremental parity includes absence. For a checkpoint-era ledger deleted in the
delta, compare the full ledger-scoped keyset after uninterrupted apply and
restore. Cover attributes, metadata, transaction/reference state, indexes and
registry entries, numscripts, reversions, boundaries/mirror state and every
other current child owned by `DeleteLedger`. Include a live neighboring ledger
so an over-wide range deletion cannot pass.

Apply the same rule to row-absence semantics such as signing-key revocation,
index drop, metadata/numscript deletion, query-checkpoint deletion, ephemeral or
transient volume purge, and any reset or replacement. A creation-only replay
matrix is insufficient. When live apply derives a cascade from state not present
at restore time, prove the exported record carries the resolved set or another
authoritative deterministic reconstruction exists.

## Ownership and deduplication

Choose the owner by the missing protection and required correction. Shared
files, crashes, snapshots or a corrupt restored store do not justify duplicates.

| Owner | Boundary |
| --- | --- |
| `persistence-restore-replay` | Primary durability, backup publication, exported-history completeness, checkpoint/delta reconstruction, restore preparation/finalization and logical parity. It owns corruption introduced by those paths. |
| `process-boundary-recovery` | Real OS death/re-exec, reopen and durable residual state independent of backup/restore. A process restart is only an injection mechanism here when it exposes a concrete restore staging/parity defect. |
| `raft-membership-leadership` | Quorum, membership, leadership, transport and Raft snapshot transfer/install ordering. A backup checkpoint or restored genesis boundary belongs here only when the missing protection is restore reconstruction. |
| `integrity-verifier-soundness` | Missing/unsound checker passes and lost checker-specific signals. Here `CheckStore` is an oracle; a restore loss remains persistence-owned even if checker detects it. |
| `read-consistency-projections` and `query-semantic-equivalence` | Peer index/checkpoint publication, temporal convergence and fixed-state query-plan equivalence. This domain owns restoration of the primary facts and complete history from which local projections rebuild. |
| `idempotency-retries-partial-failures` | General logical/transport retry identity and outcome retention. This domain owns duplicate or missing effects caused specifically by export application, replay or restore retry. |
| `fsm-determinism-cache-coverage` | Pure apply determinism, declared coverage, gated cache access and accepted-order immutability. Here the live fold supplies one side of parity; a general replica divergence is not a restore finding. |
| `configuration-startup-contracts`, `filesystem-confinement-contracts`, `operator-reconciliation-durability` | Persisted configuration validation, path confinement and Kubernetes orchestration respectively. Inspect handoffs for reachability, but keep the finding with the missing root protection. |

The challenge pass must actively search these neighboring domains, existing
qualified results, tickets and PRs for the same mechanism. If external novelty
evidence is unavailable, leave novelty unresolved rather than inventing an ID.

## Falsifiable diagnostics and rejection criteria

Start with a healthy production-path fixture. Take a full checkpoint, commit the
target operation afterward, and prove its export range is non-empty. Restore via
`ApplyExportsAndRebuild` or the real service, compare exact logical values and
keysets with uninterrupted execution, run `CheckStore`, then exercise the next
consumer. Use an independent oracle: shared live/rebuild helpers may be valid
implementation, but agreement through the same mistake is not proof.

For interruption hypotheses, enumerate state after every durable prefix:
checkpoint files, uploaded objects, published manifest, applied raw segment
batches, rebuilt projection batch, restore marker and activated directory.
Inject failure immediately before and after the disputed boundary, retry using
the supported lifecycle, and assert final state, attempt count and temporary
cleanup. A permanently unavailable dependency, setup crash, unrelated mismatch,
or partial state that is never activated and remains safely retryable rejects
the claimed correctness impact.

Reject or downgrade a candidate when current evidence shows that the state is
explicitly discarded and safely reseeded, the delta cannot change a preserved
value, the alleged export state is unreachable, validation fails before
activation, retry deterministically converges, the comparison confuses
source-cluster provenance with destination progress, or the root cause and fix
belong wholly to a neighboring domain. Treat undocumented desired semantics as
a question. Existing test names or green suites neither prove nor disprove the
invariant without the precise branch and assertions above.

Providers may run existing focused tests and permitted temporary diagnostics in
the native read-only sandbox. They must not edit tracked files, implement a new
reproducer, mutate live stores, start nested audit/challenge workflows, or
publish Jira issues. Use repository-pinned tools, isolated storage, deterministic
synchronization and no `time.Sleep`.

For this manifest-only PR, validate the JSON with the predicate in
`scripts/ai-audit`, verify every scope glob and related-document path, run
`bash scripts/agent-check`, and run `agent-check-pr` with the exact base SHA.
These checks validate the contract artifact; they do not execute or establish
the product audit.
