# recovery-source-missing-heals-same-process — Restored source heals without restarting Ledger

## Catalog candidate

| | |
|---|---|
| **Priority** | P0 — the repository's repair contract says a restored source can be certified and reopened, but a confirmed implementation gap can leave PIT unavailable indefinitely when a non-builder path persisted the marker. |
| **Type** | Liveness (progress). |
| **Property** | When a verifier or cold read persists `SOURCE_MISSING` and the required source object/range is restored, the same Ledger process eventually performs full repair, clears the marker, and serves an exact PIT result without needing a process restart. |
| **Invariant** | `Sometimes(sourceRestored && sameProcess && repairCertified && ready && resultEqualsOracle)`, message `pit: restored source heals SOURCE_MISSING without process restart`. `Sometimes` matches eventual recovery after a meaningful repair event and also gives Antithesis a checkpoint at the rare successful transition. |
| **Antithesis Angle** | Tier a run or archive a primary chapter, remove one required object, force a no-retry read or full verifier pass to persist `SOURCE_MISSING`, restore the exact object, quiesce writes/faults, and keep the process alive. Run a restart control branch to demonstrate that boot reload currently triggers repair. |
| **Why It Matters** | Object stores can have repaired routing, restored objects, or transient operator intervention without a Ledger restart. Requiring an undocumented restart turns a recoverable dependency fault into permanent PIT unavailability. |
| **Confidence** | High — the intended repair flow and stale process-local state path are both grounded in current repository code and architecture documentation; a full-system runtime reproduction is still needed. |

## Intended contract versus confirmed implementation gap

### Intended contract

The repository describes restored-source repair as an ordinary recovery path,
not as a restart-only operation:

- the error contract marks `HISTORY_SOURCE_MISSING` as unavailable "until
  repaired";
- the architecture requires the builder to reach a pinned source head, force a
  WAL barrier, obtain independent semantic certification, and only then call
  `ClearFailure`;
- `Builder.handleBuildError` says that once a missing source answers again the
  builder rebuilds from audit sequence 1, and its steady worker retries rather
  than terminating;
- the verification plan explicitly includes missing and restored cold
  segments; and
- there is no CLI, API, or operational documentation that makes process restart
  the repair procedure. The builder is the only production caller of
  `ClearFailure`.

These are repository claims that this property should test. They establish the
intended recovery outcome; they do not prove the implementation achieves it.

### Confirmed implementation gap

`HistoryVerifier.markSourceMissing` and the cold-view fetch path call `Store.MarkSourceMissing`, which persists the store marker and invalidates views, but neither updates the builder's process-local `sourceMissing` atomic. If the builder was already ready and its manifest remains at the unchanged source head, the next tick can take the `readyAfterBuild && !repairing` early return. On this steady-state path the marker can remain uncertified and uncleared until a separate builder-owned repair transition or process restart. Restart runs `restoreRebuildState`, observes the persistent marker through `OpenView`, sets the builder atomic, and schedules genesis repair.

The gap is specific to non-builder marker origins. Builder-originated
`ErrSourceMissing` does call `handleBuildError`, which sets `sourceMissing` and
`rebuildFromGenesis`; `TestBuilderStartRetriesAfterSourceRepair` proves that
same-process path. Periodic verifier success does not close the external-marker
gap: `Verify` can certify repaired content, but clearing remains deliberately
owned by the builder. This is therefore a confirmed static control-flow defect
against the repository's intended repair flow, not merely an undocumented
feature request. It is not yet a full-system reproduced incident.

## Code evidence

- `internal/application/balancehistory/verifier.go:868-886` routes source replay/physical source failures to `store.MarkSourceMissing` only.
- `internal/storage/balancehistorystore/view.go:574-591` maps a missing cold object and persists `SOURCE_MISSING` directly from the query-owned view, again without notifying the builder.
- `internal/storage/balancehistorystore/store.go:303-315` persists the marker and invalidates current views, but has no callback to the builder.
- `internal/application/balancehistory/builder.go:556-578` computes `repairing` solely from the builder's `rebuilding` and `sourceMissing` atomics; an already-ready, caught-up builder returns early when both are false.
- `internal/application/balancehistory/builder.go:679-702` reloads the persistent marker on process restart and then does set `sourceMissing` and `rebuildFromGenesis`.
- `internal/application/balancehistory/builder.go:610-655,782-838` implements same-process retry when the builder itself observes source loss, including the source-repair reset after the source answers again.
- `internal/application/balancehistory/verifier.go:341-363` keeps certification separate from marker clearing and assigns the latter to the builder.
- `internal/storage/balancehistorystore/view.go:256-268,360-369` ensures reads stay fail closed while the marker remains, so the bug is unavailability rather than partial success.
- `internal/application/balancehistory/builder_test.go:582-615` covers builder-originated same-process source repair; `internal/application/balancehistory/verifier_test.go:316-345,500-549` covers marker persistence and explicit certification/clearing, but not notification of an already-ready builder after verifier- or query-originated failure.
- `docs/technical/architecture/subsystems/read-path/point-in-time-balances.md:503-519,727-728,1090-1109` defines process-local readiness, certified source repair, and restored-cold-segment verification without specifying restart as a recovery step.

## Candidate SUT instrumentation

Existing status: **missing**. There is no existing PIT assertion or metric that records which component originated a source-missing marker or whether the builder observed it.

- `Reachable`, `pit: non-builder path persisted SOURCE_MISSING while builder was ready`, at verifier and cold-fetch marker origins, with the origin in assertion details if dependency wiring permits it.
- `Sometimes(builderObservedPersistentMarker)`, `pit: ready builder observes externally persisted source failure`, at the builder tick boundary. This is the search-guidance checkpoint most likely to reveal the current gap.
- `Reachable`, `pit: same-process source repair completed`, after `ClearFailure` succeeds without the builder process identity changing.

## Open questions

None.

### Investigation Log

#### Is same-process healing intended, or is restart an accepted recovery requirement?

- **Examined:** the PIT architecture's readiness, persistent-repair, error-contract,
  and verification-plan sections; `Builder.Start`, `tick`, `handleBuildError`,
  `restoreRebuildState`, `processOnce`, and `completeCaughtUpHistory`;
  `Store.MarkSourceMissing`, `ClearFailure`, and `ResetForSourceRepair`; all
  repository call sites of those methods; builder, verifier, and store tests;
  CLI and operations documentation; and the repository history for these
  symbols and documentation at `fb3f9f833`.
- **Found:** the repository claims that restored source is repaired through the
  running builder's retry, full-prefix rebuild, durability barrier, semantic
  certification, and `ClearFailure`. The builder is the only production caller
  of `ClearFailure`, and builder-originated source loss already follows this
  same-process path. The verification plan includes restored cold segments.
- **Not found:** any API, CLI, operator procedure, comment, test, or architecture
  decision declaring restart necessary or acceptable as the terminal recovery
  procedure. The PIT subsystem has no earlier repository revision that defines
  a different contract; it was introduced by the current commit.
- **Conclusion:** resolved in favor of same-process healing as the intended
  contract. Restart is a useful control proving durable-marker recovery, not an
  accepted prerequisite. The property remains valid and tests a claimed
  guarantee rather than treating the current behavior as correct.

#### Which repair notification mechanism is preferred?

- **Examined:** builder wake/ticker wiring, the generic `signal.Notifications`
  fan-out, the store's internal `changed` channel and generation invalidation,
  bootstrap dependency wiring, all `MarkSourceMissing` and `ClearFailure` call
  sites, and searches for a balance-history recovery coordinator or other
  store-state subscription API.
- **Found:** no repository decision selects a repair-notification mechanism.
  The store's `changed` channel is private and currently wakes watermark
  waiters; builder notifications carry FSM log commits only; no recovery
  coordinator exists. The builder's 200 ms ticker is explicitly its correctness
  backstop, and the durable store marker is authoritative across restart.
- **Not found:** an existing callback from store to builder, an exported failure
  subscription, or a documented preference among polling, signaling, and
  coordination. Direct notification from query-owned storage code would also
  cross the current storage-to-application dependency direction.
- **Conclusion:** removed as an open property question because the observable
  invariant is implementation-independent. A fix can poll the durable marker
  on the existing builder tick or add a store-owned signal without changing the
  property; choosing between those seams belongs to implementation design
  review, not property acceptance.
