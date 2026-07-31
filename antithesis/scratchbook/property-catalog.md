---
sut_path: /Users/flemzord/Developer/Formance/ledger
commit: fb3f9f8334c7fbbcc27c2c211c77da36762e6aaf
updated: 2026-07-31
external_references: []
---

# Ledger v3 PIT property catalog

## Summary

This catalog treats the accepted PIT design as a set of claims to falsify. P0
properties protect monetary correctness, authoritative boundaries and destructive
operations. P1 properties cover progress or less common lifecycle paths.

Pure parsing, authorization and single-format compatibility checks were not
promoted when deterministic tests can fully exercise them. The confirmed HTTP
empty-`pit` fallback gap remains recorded in
`sut-analysis.md`, but is a normal regression-test/fix target
rather than an Antithesis state-space property.

## Public monetary and protocol correctness

These properties validate the result and provenance visible to clients while
faults move the primary/history boundary or alter temporal state.

### concurrency-api-primary-history-boundary — One primary/history boundary

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | Every successful PIT aggregate uses one primary-snapshot ledger incarnation and covers at least that snapshot's log head and the caller's explicit minimum. |
| **Invariant** | `Always(success => trailer.logWatermark >= max(minLogSequence, observedPrimaryHead) && trailer.ledgerId == observedLedgerID && result == oracle(trailer.logWatermark))`; one mixed or behind success is invalid. |
| **Antithesis Angle** | Interleave writes, delete/recreate, routing and builder lag between primary snapshot, watermark wait, history open and response serialization. |
| **Why It Matters** | A TOCTOU error can return plausible money for the wrong incarnation or prefix. |

**Open Questions:**

- None.

### integrity-dual-axis-reversal-exactness — Exact effective and insertion history

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | Backdated/future transactions and both reversal modes affect effective and insertion cutoffs exactly once according to their documented timestamps. |
| **Invariant** | `Always(success => aggregate == independentEffectOracle(axis, requestedAt, trailer.logWatermark))`; `Always` matches a monetary invariant on every success. |
| **Antithesis Angle** | Reorder timestamps and commits, retry ambiguous writes, reverse during leader changes, and read both axes at boundary timestamps. |
| **Why It Matters** | Applying a correction on the wrong axis silently rewrites financial history. |

**Open Questions:**

- None.

### protocol-current-metadata-historical-money — Current metadata selects historical money

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Safety |
| **Property** | Metadata filters use the current read-store selection while monetary values come from the requested historical view. |
| **Invariant** | `Always(success => result == historicalOracle(selectedByCurrentMetadata))`; this must hold for every completed filtered query. |
| **Antithesis Angle** | Race metadata/index updates, backdated postings, builder lag and follower-local reads. |
| **Why It Matters** | Mixing a historical selection snapshot with current-only product semantics returns the wrong customer cohort. |

**Open Questions:**

- None. Per-node checks wait for `GetIndexStatus.currentVersion > 0` and
  `lastIndexedSequence` to cover the metadata mutation; schema retypes also wait
  for the expected version switch.

### integrity-ledger-incarnation-isolation — Recreated ledgers never inherit history

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | Delete/recreate of one ledger name never exposes the predecessor incarnation's effects. |
| **Invariant** | `Always(success => trailer.ledgerId == currentLedgerID && result == oracle(currentLedgerID))`; one cross-incarnation row is a violation. |
| **Antithesis Angle** | Delete/recreate while replicas lag, views stay pinned, compaction/tiering run and requests route through different nodes. |
| **Why It Matters** | Cross-incarnation leakage exposes deleted financial data under a reused name. |

**Open Questions:**

- None.

### protocol-leader-forwarding-provenance — Forwarding preserves leader view

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Safety |
| **Property** | A leader-consistency PIT call sent to a follower returns the same result and provenance as the leader view that executed it. |
| **Invariant** | `Always(stableLeaderAndSameTokenPair => forwardedPayload == directPayload && forwardedView == directView)`, paired with `Sometimes(stableLeaderAndSameTokenPair)`; term/leader changes or different manifest tokens discard the pair. |
| **Antithesis Angle** | Forward through every follower while transfers, partitions and response-trailer transport race. |
| **Why It Matters** | A dropped or substituted trailer makes a forwarded result unverifiable. |

**Open Questions:**

- None. Before/after per-node `GetClusterState` probes establish a stable
  leader/term window; matching decoded view tokens establish a common manifest.

### unfiltered-fast-path-equals-account-fold — Redundant scopes agree

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Safety |
| **Implementation** | Partially implemented in `first_default_ledger`, `parallel_driver_pit_scope_equivalence`, `eventually_pit_scope_equivalence`, and the shared trailer/canonicalization helpers. Paired-success correctness is covered; physical compaction/tiering/cold/restart reachability still needs dedicated signals. |
| **Property** | At one immutable view, the unfiltered ledger-wide asset summary equals an exhaustive fold of every per-account historical row after identical precision/color options. |
| **Invariant** | `AlwaysOrUnreachable(sameViewPair => canonical(summary) == canonical(accountFold))`, paired with `Sometimes(sameViewPairCompared)` for meaningful coverage. |
| **Antithesis Angle** | Exercise distinct key ranges and cold parts across publication, compaction, tiering, cancellation and restart. |
| **Why It Matters** | Either redundant scope can remain internally plausible while returning a different total. |

**Open Questions:**

- None. Use a small driver-owned ledger, select the exhaustive historical row
  path with `NOT(address == reserved-never-created)`, require equal decoded view
  tokens, sample periodically, and cover all aggregation options once after
  quiescence.

## Projection, compaction and cold-storage safety

These properties protect the asynchronous replica-local projection and its
physical transformations.

### integrity-authoritative-prefix-only — Publish only complete audit prefixes

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | The builder advances only over consecutive complete proposals with every referenced audit item and resolved log present. |
| **Invariant** | `AlwaysOrUnreachable(publication => sourceRangeConsecutiveAndComplete && sourceCompleteFromGenesis)`; optional publication may be absent, never partial. |
| **Antithesis Angle** | Partition cold source, purge chapters, kill during batches and mix failed/no-log proposals. |
| **Why It Matters** | One skipped source record permanently changes every later historical balance. |

**Open Questions:**

- None.

### integrity-atomic-publication-restart — Publication is old-or-new after crash

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | A crash exposes either the previous complete manifest or the complete new publication, never torn run/manifest visibility. |
| **Invariant** | `Always(reopenedState == previousCompleteState || reopenedState == nextCompleteState)`; a third state is forbidden. |
| **Antithesis Angle** | Terminate around run writes, latest-pointer batch and history WAL sync. |
| **Why It Matters** | Torn visibility can return wrong money while all individual records decode successfully. |

**Open Questions:**

- None.

### integrity-layout-independent-semantics — Physical rewrites preserve logical state

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | For one authoritative prefix, monetary results and canonical semantic digest are independent of batching, run IDs, compaction layout and hot/cold placement. |
| **Invariant** | `AlwaysOrUnreachable(commonSourcePrefix => canonicalSemanticDigest == authoritativeReplayDigest)` plus public oracle equality; optional full verification may be absent, but any completed comparison must agree. |
| **Antithesis Angle** | Let replicas develop different physical layouts through throttling, restarts, compaction and tiering, then verify a common source prefix. |
| **Why It Matters** | Layout-dependent state can make replicas return different money even though they consumed the same audit. |

**Open Questions:**

- None.

### concurrency-verifier-maintenance-coherence — Verification remains coherent and progresses

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Safety with liveness/reachability companion |
| **Property** | Every completed sampled or full verifier pass uses one pinned manifest/source boundary and cannot falsely certify or quarantine a mixed maintenance view; after faults heal, a cold sample and full replay both complete under observed maintenance activity. |
| **Invariant** | `AlwaysOrUnreachable(completedPass => onePinnedManifest && successIsInternallyCoherent && quarantineHasConcreteMismatch)` plus `Sometimes(sampledColdPassCompleted && fullReplayCompleted && concurrentMaintenanceObserved)`. |
| **Antithesis Angle** | Run the two-second sampled verifier and every-fourth-pass full replay across publication, compaction, tiering, cold faults, GC and restart; require same-process progress after heal. |
| **Why It Matters** | A mixed pass can falsely quarantine valid history; a starved verifier silently removes the projection's integrity backstop. |

**Open Questions:**

- None. Add SUT phase/cursor/replay assertions; public PIT remains the separate
  serving oracle.

### concurrency-pinned-view-maintenance-stability — A view never mixes manifests

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | A valid pinned view remains exact through publication, compaction, tiering and GC; reset/failure may only invalidate it. |
| **Invariant** | `Always(success ? result == oracle(pinnedWatermark) : exactFailClosedReason(error))`. |
| **Antithesis Angle** | Stretch cold/filtered reads while every maintenance transition and MinIO fault runs concurrently. |
| **Why It Matters** | Mixed manifests omit or double-count money without obvious corruption. |

**Open Questions:**

- None.

### concurrency-compaction-cas-preserves-suffix — Compaction cannot drop concurrent publications

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | A streamed compaction replaces only its exact inputs and preserves every run published while it was outside the mutation lock. |
| **Invariant** | `Always(compactionComplete => postWatermark >= preWatermark && postResult == oracle(postWatermark))`. |
| **Antithesis Angle** | Schedule builder publication/reset around compaction view, reservation, chunk commits and final CAS; kill near NoSync writes. |
| **Why It Matters** | A stale compaction CAS silently loses acknowledged monetary effects. |

**Open Questions:**

- None.

### concurrency-tier-lease-precedes-local-delete — Tiering never removes the last usable copy

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | Local run bytes are removed only after verified durable archive references exist and every run lease has drained. |
| **Invariant** | `AlwaysOrUnreachable(removeLocal => archivedAndVerified && archiveParts > 0 && runLeases == 0)`. |
| **Antithesis Angle** | Pause upload/verification/local-delete while views open, compaction races, MinIO fails and the node terminates. |
| **Why It Matters** | Early deletion turns a transient S3 problem into permanent historical-data loss. |

**Open Questions:**

- None.

### integrity-cold-content-verified — Cold bytes are verified before use

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | Missing, truncated or corrupted cold parts never contribute records to a successful result. |
| **Invariant** | `Always(coldOutcome.success ? checksumVerified && resultMatchesOracle : exactMissingOrCorruptReason)`. |
| **Antithesis Angle** | Fault multipart fetch/cache publication and remove or alter exact objects while views and verifier replay run. |
| **Why It Matters** | Treating absent cold rows as zero silently changes historical balances. |

**Open Questions:**

- None.

### concurrency-builder-source-snapshot-archive-purge — Hot/cold handoff is one snapshot

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | A source read returns one complete consecutive batch even if archive confirmation purges its former hot range concurrently. |
| **Invariant** | `AlwaysOrUnreachable(hotColdRead => consecutiveAndComplete(batch))`; the archived path is optional but never incoherent. |
| **Antithesis Angle** | Pause after registry snapshot, archive/confirm through the real worker, purge hot keys, evict cold readers and resume. |
| **Why It Matters** | One handoff gap corrupts the projection or creates false permanent source loss. |

**Open Questions:**

- None.

### concurrency-remote-gc-live-roots-protected — Remote GC never deletes a root

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | Remote delete is never called for a latest-manifest digest and never runs while active manifest leases make reset/version ABA ambiguous. |
| **Invariant** | `AlwaysOrUnreachable(delete => !rooted[digest] && activeManifestLeases == 0)`. |
| **Antithesis Angle** | Age candidates, then interleave views, reset, compaction, upload, clock, delete and restart. |
| **Why It Matters** | One false-positive delete can remove the only cold copy of exact financial history. |

**Open Questions:**

- None.

### concurrency-remote-gc-inventory-upload-linearization — Inventory proof is epoch-bound

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | A completed inventory is valid only for its archive mutation epoch; upload/reset invalidates in-flight pages before remote I/O can create objects. |
| **Invariant** | `AlwaysOrUnreachable(inventoryComplete => inventoryEpoch == currentMutationEpoch)`. |
| **Antithesis Angle** | Pause list/page sync, upload-before-CAS and reset while terminating the process at each boundary. |
| **Why It Matters** | A stale empty proof can authorize destination rotation or strand uncollectable objects. |

**Open Questions:**

- None.

### security-pit-remote-gc-owner-containment — GC stays inside its node namespace

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | One replica's collector never lists or deletes primary chapter objects or another distinct owner's PIT namespace. |
| **Invariant** | `AlwaysOrUnreachable(delete => keyWithinBoundOwnerNamespace && destinationIdentityMatches)`. |
| **Antithesis Angle** | Scale replicas, populate adjacent prefixes and race cursor restart/destination validation. |
| **Why It Matters** | Cross-owner deletion destroys history outside the collector's proof horizon. |

**Open Questions:**

- None. Repository ownership intentionally follows the stable Raft node ID;
  same-ordinal replacement is same-owner continuation, with content accepted
  only through independent rebuild/digest verification.

## Recovery and distributed progress

These properties test replay, repair, restore, joins and cluster-wide eventual
availability after faults stop.

### boot-readiness-reconciles-persisted-history — Ordinary boot proves retained history again

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Safety with liveness companion |
| **Property** | After an ordinary same-PVC restart, retained complete history remains fail closed until the new process freshly validates source sequence/log/hash, catches up exactly and completes its boot WAL barrier. |
| **Invariant** | `AlwaysOrUnreachable(readinessOpen => freshProcessSourceProof && exactHead && bootWALBarrierSucceeded)` plus `Sometimes(gracefulRestart && exactPITReopened)`. |
| **Antithesis Angle** | Gracefully restart a directly addressed replica, race endpoint availability with asynchronous boot, and retain unrelated cluster activity. |
| **Why It Matters** | A persisted peer manifest is not authority and the previous process's readiness proof cannot survive restart. |

**Open Questions:**

- None.

### recovery-unsynced-suffix-replays — Lost async suffix is rebuilt exactly

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Liveness |
| **Property** | After hard restart loses a NoSync PIT suffix, the replica remains fail closed until it replays the authoritative suffix and serves the exact result. |
| **Invariant** | Correlated SUT `Reachable(publicationCommitted && !walBarrierCovered)` anchors the crash window; then `Sometimes(restartedAfterObservedBehindPrefix && exactPITAtRequiredWatermark)` plus the global success-exactness `Always`. |
| **Antithesis Angle** | Kill only after the publication-before-barrier reachability signal and restart with the primary PVC preserved. |
| **Why It Matters** | A replayable peer-store loss must not become permanent unavailability or duplicated money. |

**Open Questions:**

- Can the target Antithesis storage model actually discard un-fsynced PVC bytes
  on grace-zero pod deletion, or does this require a node/disk fault? `(needs human input)`

### recovery-repair-gate-survives-crashes — Repair cannot reopen early

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | Repeated crashes during quarantine/reset/rebuild/certification never reopen PIT until the rebuilt source prefix is durably certified. |
| **Invariant** | `Always(repairIncomplete => !successfulPIT)` and `Sometimes(certifiedRepair && exactPITAvailable)`. |
| **Antithesis Angle** | Terminate at every persistent failure-state and certification subphase. |
| **Why It Matters** | Early reopen can serve a partially reconstructed or unauthoritative history. |

**Open Questions:**

- None. Use SDK-only phase signals. The forced WAL barrier covers the rebuilt
  prefix and synchronous marker deletion is the final durable barrier before
  readiness reopens.

### recovery-source-missing-heals-same-process — Restored source heals without restart

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Liveness |
| **Property** | After a verifier/read persists `SOURCE_MISSING`, restoring the exact source lets the running node rebuild, certify, clear the marker and serve PIT. |
| **Invariant** | `Sometimes(sourceRestored && nodeNotRestarted && exactPITSucceeded)`. |
| **Antithesis Angle** | Remove/restore one required object and explore the handoff between durable failure state and builder-local atomics. |
| **Why It Matters** | Current code tracing predicts permanent PIT unavailability until operator restart. |

**Open Questions:**

- None. Repository repair semantics intend same-process healing; current code
  fails only because verifier/query-originated markers do not update the
  builder's repair state.

### transient-minio-failure-recovers-without-sticky-source-loss — Transient I/O is not proven loss

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Liveness with safety transition guard |
| **Property** | A MinIO transport/checksum-read failure with unchanged object identity never creates a new persistent source-loss/corruption marker and the same process serves exact PIT after heal. |
| **Invariant** | `AlwaysOrUnreachable(transientIO => failureStateAfter == failureStateBefore && publicReason == EXTERNAL_SERVICE_ERROR)` plus `Sometimes(sameProcessValidatedSameObjectAfterHeal && exactPIT)`. |
| **Antithesis Angle** | Fault only MinIO I/O around chapter/PIT checksum validation without terminating MinIO or mutating objects, then heal and revalidate the same identity. |
| **Why It Matters** | Current code can convert temporary dependency failure into durable `SOURCE_MISSING` and compound the repair-handoff defect. |

**Open Questions:**

- None. This is a known-failing code path until operational errors retain their
  original classification.

### lifecycle-follower-snapshot-install-fails-closed — Follower replacement cannot serve old history

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Safety |
| **Property** | During in-process follower snapshot/primary-store replacement, local PIT either matches the activated authority or fails closed; pre-replacement history is never served. |
| **Invariant** | `Always(snapshotInstallActive => !success || result == activatedAuthorityOracle)`. |
| **Antithesis Angle** | Direct stale-local reads to the joining/restoring follower around applier gating, store swap and the next builder tick. |
| **Why It Matters** | A replica can otherwise expose financial state intentionally removed by restore. |

**Open Questions:**

- None. Stale consistency intentionally targets the local synchronizing replica;
  use an ordinal/direct no-retry connection plus local `GetClusterState` and
  `GetIndexStatus` bounds. Normal snapshot install is forward-only.

### restore-ahead-history-fails-closed — Administrative rollback cannot serve retained future history

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | After activating an older primary authority while retaining an ahead PIT store, every request fails closed until reset/rebuild/sync/certification, and no success exceeds the restored head. |
| **Invariant** | `Always(postRestoreSuccess => watermark <= restoredHead && result == restoredOracle)` with a SUT gate assertion at the rollback/divergence branch. |
| **Antithesis Angle** | Preserve a separately mounted peer store, restore an older frozen primary, and probe the exact replica before the builder's next reconciliation tick while crashing repair subphases. |
| **Why It Matters** | An ahead success resurrects monetary effects deliberately removed from the authoritative audit. |

**Open Questions:**

- None. This requires a retained-history administrative-restore profile distinct
  from the current fresh-PVC restore campaign.

### out-of-order-chapters-fail-closed — Mixed archive order cannot yield partial money

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | If N+1 is archived while N remains hot/non-archived, PIT is either exact or fail closed, never a partial/zero result. |
| **Invariant** | `Always(mixedTopologyOutcome.success ? matchesOracle : exactFailClosedReason)`. |
| **Antithesis Angle** | Use real asynchronous archiving to reach the FSM-accepted but PIT-source-rejected topology. |
| **Why It Matters** | The confirmed code mismatch can make correct source data look incomplete. |

**Open Questions:**

- None.

### out-of-order-chapters-recover — Completing the archive prefix restores PIT

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Liveness |
| **Property** | After chapter N is also archived and faults stop, every replica eventually rebuilds/certifies and serves the exact result. |
| **Invariant** | `Sometimes(archivePrefixComplete && exactPITAvailableOnEveryReplica)`. |
| **Antithesis Angle** | Transition from the mixed topology into a complete archived prefix across restarts and MinIO faults. |
| **Why It Matters** | Fail-closed safety is insufficient if a valid topology never recovers availability. |

**Open Questions:**

- None.

### linearizable-pit-partition-fails-closed — Default reads require a quorum barrier

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety and liveness |
| **Property** | Default PIT reads never bypass `ReadIndex` under partition: quorum-confirmed requests are exact, isolated nodes return narrow transient failures, and stable voters recover exact progress after heal. |
| **Invariant** | `Always(partitionOutcome.success ? exactAtTrailer : narrowRaftTransient(error))` plus `Sometimes(healedStableMembership && allVotersExact)`. |
| **Antithesis Angle** | Probe one no-retry node while isolating Raft links, preserve workload gRPC where possible, then heal and run the exact-index gate. |
| **Why It Matters** | Falling back to local history during quorum loss would silently weaken default consistency. |

**Open Questions:**

- Can the tenant selectively cut named Raft links while preserving workload gRPC
  and MinIO? `(needs human input)` The repository-controlled live-leader/no-quorum
  variant remains runnable without that capability.

### coordination-quiescent-pit-convergence — Replicas converge logically

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Liveness |
| **Property** | After writers/faults stop and replicas share a durable Raft prefix, every participating replica eventually serves the same exact monetary result at a sufficient watermark. |
| **Invariant** | `Sometimes(quietCommonPrefix && allParticipatingReplicasExactAndEqual)`. |
| **Antithesis Angle** | Accumulate independent batching/compaction/tiering layouts under partitions, restarts and leadership changes before quiescence. |
| **Why It Matters** | A locally rebuildable projection must not leave one healthy voter permanently divergent. |

**Open Questions:**

- None. Use stable leader-reported Raft membership, including learners; restart
  measurement if membership/suffrage changes, and require the exact-index gate
  within 60 seconds inside the ten-minute eventual command.

### coordination-scale-up-no-premature-local-pit — New replicas backfill before serving

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Safety |
| **Property** | A new/empty replica never serves local PIT until its primary source and history projection are reconciled to the required prefix. |
| **Invariant** | `Always(newReplicaNotReady => localPITFailsClosed)` and `Sometimes(promotedAndExactLocalPIT)`. |
| **Antithesis Angle** | Direct requests to the new ordinal through join, snapshot install, promotion, archive fetch and builder backfill. |
| **Why It Matters** | Kubernetes readiness or Raft membership alone cannot certify the peer projection. |

**Open Questions:**

- None. Kubernetes readiness covers only the local Raft loop, so it is not the
  PIT gate. Predeclared headless-service ordinal FQDNs let the workload address
  the new pod before membership discovery.

### lifecycle-backup-restore-rebuilds-history — Restore rebuilds the peer projection

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Liveness |
| **Property** | After restoring the authoritative primary into fresh storage, PIT remains fail closed while rebuilding and eventually equals the restored audit/log oracle. |
| **Invariant** | `Always(rebuildIncomplete => !partialSuccess)` and `Sometimes(restoreComplete && exactPITAvailable)`. |
| **Antithesis Angle** | Use an isolated template to backup, delete PVCs, restore, recreate followers and fetch authoritative cold chapters. |
| **Why It Matters** | Primary backup does not contain the replica-local history store; recovery must not assume it survived. |

**Open Questions:**

- None. Extend the isolated `model` template and scope this property to its
  fresh-PVC path. Primary chapter archives are an independent DR prerequisite,
  so MinIO must persist and the fixture must archive through the real worker
  before backup.

### idempotency-keyed-apply-changes-pit-once — Ambiguous keyed retries change history once

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Safety |
| **Property** | Retrying an ambiguous keyed Apply produces exactly one monetary effect in both PIT axes. |
| **Invariant** | `Always(reconciledKeyedOutcome => effectMultiplicity == 1 && pit == oracle)` plus `Sometimes(postCommitPreResponseReached && ambiguousRetryReconciled)`. |
| **Antithesis Angle** | Pause after the keyed proposal future resolves but before response serialization, terminate or time out the RPC, transfer leadership and retry the same key while builders process independently. |
| **Why It Matters** | Duplicate or absent historical effects can diverge from otherwise correct live/idempotency responses. |

**Open Questions:**

- None.

### replay-remote-delete-ack-is-idempotent — Delete-before-ack eventually clears

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Liveness |
| **Property** | After crash between remote delete and durable acknowledgement, retrying exact delete is harmless and the candidate queue eventually clears. |
| **Invariant** | Correlated SUT `Reachable(remoteDeleteSucceeded && !durableAckCommitted)` anchors the crash window, followed by `Sometimes(deleteBeforeAckReached && objectAbsent && candidateCleared)`. |
| **Antithesis Angle** | Terminate only after the delete-before-ack reachability signal, then restart with durable candidate state. |
| **Why It Matters** | A wedged acknowledgement leaks objects/metadata; non-idempotent recovery can delete the wrong key. |

**Open Questions:**

- None for the non-versioned MinIO campaign.

### remote-gc-queue-converges — Durable remote-GC work eventually drains

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Liveness |
| **Property** | Once uploads/mutations stop, views drain, S3 heals, a fresh inventory completes and grace elapses, a non-empty durable candidate queue drains across restart. |
| **Invariant** | `Sometimes(recoveryPremise && queueEmptyWithinSuccessfulCallBound)`, where `B = (I0 + 1) + ceil(Q0/DeleteLimit)`, plus safety checks on successful calls. |
| **Antithesis Angle** | Combine partial cursors, view-held roots, S3 failure and delete-before-ack residue, then count only healthy completed `Collect` calls. |
| **Why It Matters** | A safe collector can still leak remote objects and local candidate metadata forever. |

**Open Questions:**

- None. Execution requires coherent SUT diagnostic state and persistent MinIO.

## Resource and operational liveness

### cancelled-cold-reads-release-resources — Cancellation drains all cold-read ownership

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Liveness with singleflight safety |
| **Property** | Cancelled cold PIT reads terminate and release request/view goroutines, manifest/run/cache leases, readers, inflight fetch participation and temporary files in the same process. |
| **Invariant** | `Sometimes(cancelledBurst && resourcesReturnToBaseline)` plus `Always(inflightLoadsPerDigest <= 1 && joinedWaiterCancellationDoesNotCancelOtherLiveWaiters)`. |
| **Antithesis Angle** | Fan out same/different digest reads under MinIO stalls, cancel leaders/joiners in varied order, then heal without restarting. |
| **Why It Matters** | Leaked ownership blocks GC and can exhaust cache bytes, file descriptors and goroutines. |

**Open Questions:**

- None. Current first-miss context ownership is expected to expose a joined-waiter
  cancellation defect.

### live-path-survives-pit-dependency-fault — PIT dependency faults do not stop live Ledger

| | |
|---|---|
| **Priority** | P0 |
| **Type** | Liveness |
| **Property** | With Raft quorum and workload-to-Ledger connectivity intact, Ledger writes and live volume reads still succeed while Ledger-to-MinIO PIT links are faulted. |
| **Invariant** | `Sometimes(minioIsolation && liveWriteAndReadSucceeded)`; it is a progress condition under a deliberately isolated optional dependency. |
| **Antithesis Angle** | Apply asymmetric MinIO-only network faults while cold PIT work and ordinary live traffic run. |
| **Why It Matters** | PIT is opt-in derived state and must not introduce a hidden FSM/data-plane dependency. |

**Open Questions:**

- None, provided the launch environment can isolate Ledger-to-MinIO links.

### resources-logical-run-debt-reconverges — Compaction debt drains after recovery

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Liveness |
| **Property** | After an observed maintenance stall, faults and writes stop, and every logical level eventually falls below the compaction threshold. |
| **Invariant** | `Sometimes(recoveryPhase && everyLevelRunCount < threshold)`, guarded by prior observed debt. |
| **Antithesis Angle** | Stall shared maintenance/S3, build run debt, restart around prepared output and then heal. |
| **Why It Matters** | Persistent debt increases query fan-in, files, memory and local disk even when values remain correct. |

**Open Questions:**

- None. Add a coherent test/debug diagnostic. For initial run count `R0`,
  threshold `N`, and at most `M` compactions per completed pass, require at most
  `ceil(floor((R0-1)/(N-1))/M)` fault-free completed passes; do not translate
  this into a wall-clock bound.

### resources-s3-stall-does-not-block-shutdown — S3 stalls do not wedge graceful restart

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Liveness |
| **Property** | If S3 work is blocked, runtime cancellation followed by network recovery eventually drains maintenance and lets the replica stop and rejoin. |
| **Invariant** | Correlated old-process SUT `Reachable(maintenanceS3OperationActiveAtCancel)` followed by `Sometimes(s3InFlightBeforeShutdown && oldProcessDrainCompleted && gracefulRestartCompleted && replicaRejoined)`. |
| **Antithesis Angle** | Blackhole each S3 phase, request rolling restart, branch around cancellation and then heal the link. |
| **Why It Matters** | Shutdown synchronously waits for maintenance; an uncancellable S3 call can wedge rollout indefinitely. |

**Open Questions:**

- None. Add a positive-grace rolling-restart driver distinct from the existing
  grace-zero path and assert the old process drains. Repository evidence proves
  context propagation only; the real MinIO-stall campaign must prove prompt
  cancellation through the linked SDK/transport at runtime.

## Assumptions

- The campaign uses the accelerated, statically valid settings in
  `deployment-topology.md` on all replicas.
- Successful monetary responses are compared only through their returned source
  watermark; client attempts are not assumed committed until reconciled.
- Physical manifest IDs/tokens are replica-local and are not compared across
  replicas unless a stable leader pair executes both calls.
- Liveness is evaluated after a fault-free, write-free recovery phase.

## Open Questions

- Are node termination and clock faults enabled for the tenant?
- Will the first MinIO campaign use a PVC or avoid object-store termination?
- Is test-only/SUT-side instrumentation acceptable for internal manifest,
  verifier, cache and remote-GC conditions that the public API cannot expose?
- Which restore topology is the supported product target: fresh peer store,
  retained peer store, or both?
