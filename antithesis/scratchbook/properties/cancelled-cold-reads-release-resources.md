# cancelled-cold-reads-release-resources — Cancelled cold reads release all request-owned resources

## Catalog entry

| | |
|---|---|
| **Priority** | P1 |
| **Type** | Liveness |
| **Property** | After cold PIT RPCs are cancelled, deadline-expired, or disconnected, every same-process request eventually terminates and releases its historical view, manifest/run leases, indexed and download readers, archive-cache leases, inflight singleflight participation, and temporary fetch files; cancellation of one fetch participant must not cancel a peer whose own context is still live. Verified cache entries may remain, but after the cancellation wave no longer pins them the cache must be within its configured byte budget. |
| **Invariant** | For one direct-replica cancellation wave, take a node-local baseline `B` before starting any cohort RPC, require a cold miss and joined fetch, cancel the chosen callers, heal injected faults, stop launching PIT calls to that replica, and retain the same process epoch. Workload `assert.Sometimes(waveStarted && allClientsTerminal && sameProcess && activePITRequests == B.activePITRequests && activeViews == B.activeViews && manifestLeaseRefs == B.manifestLeaseRefs && runLeaseRefs == B.runLeaseRefs && archiveCacheLeases == B.archiveCacheLeases && openIndexedReaders == B.openIndexedReaders && openDownloadReaders == B.openDownloadReaders && inflightFetches == B.inflightFetches && inflightWaiters == B.inflightWaiters && temporaryFetchFiles == B.temporaryFetchFiles && cacheBytes <= cacheMaxBytes, "pit: cancelled cold reads release request view and cache resources")`. `Sometimes` is the liveness assertion because the required drain state must eventually be reached after a demonstrated wave; the `waveStarted` and `sameProcess` guards prevent startup and process death from satisfying it. Add SUT `assert.AlwaysOrUnreachable(!(joinedInflight && callerContextStillLive && returnedPeerCancellation), "pit: cold singleflight cancellation is scoped to its caller", details)` at a joined waiter's return, where `returnedPeerCancellation` means `errors.Is(err, context.Canceled)` or `errors.Is(err, context.DeadlineExceeded)` while `ctx.Err() == nil`. Add per-view `AlwaysOrUnreachable` after close to require all resources acquired by a cancelled view to have one matching release. |
| **Antithesis Angle** | Build and tier enough multipart history to evict a selected digest from the 8 MiB cache, then send concurrent direct-to-one-replica PIT reads that intersect the same cold part. Pause after the first miss registers its inflight call and after a waiter joins; cancel the fetch leader while leaving a waiter live to exercise cancellation isolation. In a second wave, pause after indexed readers and cache leases are installed, cancel all callers, and combine MinIO partition/hang/throttle plus thread pauses around fetch completion and view close. Heal faults and poll the coherent resource snapshot. A replica restart abandons the epoch and starts a new wave; it cannot count as successful drain. |
| **Why It Matters** | A leaked view keeps every referenced run and manifest generation alive; any active manifest lease conservatively blocks all remote deletion, while run leases prevent local tier removal and GC. Cache leases may hold the cache over budget, open readers consume file descriptors, and stuck handlers/fetches consume goroutines. Repeated client cancellation can therefore become node-wide resource exhaustion and indefinitely stop reclamation without ever returning an incorrect balance. |
| **Confidence** | High — ownership and maintenance blocking are explicit in the code. The first-caller-context singleflight cancellation leak is also structurally visible; campaign confidence remains dependent on adding the pause point and coherent counters below. |

**Open Questions:**

- None.

## Assertion rationale

This is primarily a liveness property: cancellation is an event and complete
resource drain is a later state. A startup-time `Always` check or an unguarded
`Sometimes(resources == 0)` would pass without exercising a cold read. The
workload therefore establishes a wave, proves both cold acquisition branches,
then waits for a same-process postcondition after faults are healed.

The singleflight clause is safety rather than eventual progress. A joined
caller's own context is the authority for cancellation. Returning another
caller's `Canceled` or `DeadlineExceeded` error is immediately wrong even if a
later retry could succeed, so `AlwaysOrUnreachable` belongs at every joined
waiter return. It is optional-path-safe because a cache hit or unshared miss
does not reach that assertion.

The per-view close assertion prevents aggregate totals from hiding a swap (one
view leaks while an unrelated view closes). Give each view a bounded internal
resource ledger populated when it acquires its manifest/run lease and each
cold reader/cache lease. On `View.Close`, assert that every acquired class has
exactly one release and no live resource. Keep the assertion name stable and
put counts, manifest version, run ID, digest prefix, and cancellation kind in
`details`, not in the name.

## Resolved observability and SUT instrumentation

The current public surface cannot implement the oracle. Request telemetry has
total requests, errors, and duration but no active-handler gauge. Public
`CacheStats` has bytes, entries, and cache leases, while the exported metric
reports only bytes. Manifest/run leases are internal maps; inflight calls,
waiters, open readers, and temporary downloads have no counters. A
process-wide `runtime.NumGoroutine` or file-descriptor count is too noisy to
attribute to PIT and must not be the oracle.

Add a test/debug node-local snapshot, read coherently or represented by paired
monotonic acquire/release counters, with these bounded fields:

- process epoch and active PIT request/view counts;
- total manifest-lease and run-lease references;
- archive-cache lease count, cache bytes, cache maximum, and retained entries;
- inflight fetch-call count and joined-waiter count;
- open indexed-reader and cold-download-body counts; and
- incomplete `.fetch-*.tmp` count.

Retained entries and bytes do **not** return to the baseline: verified objects
are an intentional cache. Only live pins/inflight resources return to baseline,
and unpinned bytes must be at or below `cacheMaxBytes`. The snapshot must expose
totals, not digest- or ledger-labelled metrics, to retain bounded cardinality.
The workload should serialize this focused wave against its other PIT reads on
the target replica so unrelated activity cannot exchange resources with the
baseline.

Add two stable reachability signals:

- `assert.Reachable("pit: cancelled cold read held an indexed reader and cache lease", details)` when cancellation is observed after a cold part installed both resources; and
- `assert.Reachable("pit: cancelled cold fetch had a live singleflight peer", details)` when the fetch owner's cancellation races at least one joined caller whose context is live.

The first proves the close-after-acquisition branch rather than only an early
cancel before `Fetch`; the second proves the leader/waiter cancellation branch.
After drain, a maintenance-side signal that root capture proceeds after an
active-view block is useful corroboration, but the resource snapshot is the
authoritative oracle.

## Evidence

- `internal/adapter/grpc/server_bucket.go:1386-1445` executes the PIT controller synchronously with the RPC context. There is no separate PIT worker goroutine, so an active-handler counter is the precise request-goroutine proxy.
- `internal/application/ctrl/controller_default.go:937-1008` starts PIT observation, opens the historical view, and defers `HistoricalVolumeView.Close` around the aggregation. Cancellation must unwind this frame before the view is released.
- `internal/storage/balancehistorystore/view.go:159-253` acquires one manifest lease plus one run lease for every manifest run before returning a view; all error exits after acquisition call `View.Close`.
- `internal/storage/balancehistorystore/view.go:337-357` closes every installed indexed reader and archive-cache lease, closes the Pebble snapshot, and releases manifest/run leases under `sync.Once`.
- `internal/storage/balancehistorystore/view.go:574-602` lazily fetches a cold part, closes the lease on indexed-open failure, and otherwise retains both the lease and reader until view close.
- `internal/storage/balancehistoryarchive/indexed.go:170-179` confirms that an indexed reader owns a file descriptor and that closing the parent cache lease does not close that reader; both resources must be observed independently.
- `internal/storage/balancehistoryarchive/cache.go:149-213` implements singleflight in the caller goroutine. The first miss installs `inflight[digest]`; waiters select between their own context and `call.done`; every terminal leader path deletes the entry and closes `done`.
- `internal/storage/balancehistoryarchive/store.go:177-188` passes the first miss's request context into the shared `download`. Combined with `cache.go:168-176,182-212`, a cancelled first caller publishes its cancellation as `call.err` to joined callers even when their contexts remain live. The proposed safety assertion is expected to expose this current defect.
- `internal/storage/balancehistoryarchive/store.go:219-245` opens the cold response body and a `.fetch-*.tmp` file, checks the context during copying, closes both handles, and removes incomplete downloads on failure.
- `internal/storage/balancehistoryarchive/cache.go:384-483` makes cache leases idempotent eviction pins and retries byte-budget eviction on release; active leases are allowed to put the cache temporarily over budget.
- `internal/storage/balancehistoryarchive/metrics.go:18-69` exports cache bytes but not active leases, inflight calls, waiters, readers, or request handlers, confirming the instrumentation gap.
- `internal/storage/balancehistorystore/remote_gc.go:740-776` rejects all remote deletion while any manifest lease exists because version-only lease identity cannot disambiguate reset ABA.
- `internal/storage/balancehistorystore/tier.go:717-729` refuses local removal while a run lease exists, and `internal/storage/balancehistorystore/gc.go:30-56` protects leased manifests and runs from local GC.
- `internal/storage/balancehistorystore/tier_test.go:259-338` proves cancellation propagation during a blocked fetch and inside a cached catalog loop, but it does not assert manifest/run/cache lease, reader, inflight, temporary-file, or request-handler drain.
- `internal/storage/balancehistoryarchive/store_test.go:228-260` proves sixteen concurrent misses use one cold fetch, but all contexts are immortal and no leader/waiter cancellation race is tested.
- `antithesis/scratchbook/deployment-topology.md:70-113,205-228` specifies the cold-enabled 8 MiB cache campaign, short RPC deadlines, direct-replica reads, and thread/network interleavings needed to reach the property. `antithesis/scratchbook/existing-assertions.md` records no PIT-specific SDK assertions.

## Instrumentation status

Existing status: **insufficient**. Cancellation propagation and successful
singleflight have deterministic coverage, `CacheStats.Leases` exists for
in-process tests, and operations expose bounded request/error/cache-byte
telemetry. None of these prove full same-process drain or cancellation
isolation. Implement the coherent snapshot, per-view resource ledger, joined
waiter bookkeeping, pause points, and stable assertions above before enabling
the catalog property.

## Investigated questions

### Must cached entries and bytes return to their pre-wave baseline?

- **Examined:** cache admission, LRU accounting, lease release, eviction, startup scan, and `Store.Close` semantics.
- **Found:** immutable verified files intentionally remain cached without open process-owned handles. Active leases may temporarily exceed the byte maximum; each final lease release retries eviction.
- **Not found:** any contract that empties the cache after a request or requires entry count to return to baseline.
- **Conclusion:** resolved. Require lease/inflight/reader/temp-file drain and `cacheBytes <= cacheMaxBytes`; do not require retained entries or bytes to equal the baseline.

### Can process-wide goroutine or file-descriptor counts prove request cleanup?

- **Examined:** the synchronous gRPC/controller call chain, PIT metrics, cache stats, archive reader ownership, and the multi-worker topology.
- **Found:** the PIT RPC runs on its server handler goroutine, but the process also has Raft, builder, verifier, maintenance, transport, Pebble, telemetry, and SDK goroutines/file descriptors that vary independently.
- **Not found:** a current PIT active-handler, open-reader, inflight-fetch, or waiter gauge.
- **Conclusion:** resolved. Instrument logical PIT ownership counters; do not use process-wide runtime counts as the property oracle.

### Is singleflight cancellation scoped to the caller that cancels?

- **Examined:** the complete `cache.acquire` loop and `Store.Fetch` closure.
- **Found:** the first miss performs the shared load synchronously with its own context. On failure it stores that error in `call.err`; joined waiters return it without checking whether the error came from their own context or retrying a cancellation failed by the leader.
- **Not found:** waiter reference counting, a fetch context independent of the leader, retry-on-peer-cancellation logic, or a deterministic cancellation-race test.
- **Conclusion:** resolved as a likely current violation. A live joined caller must not receive a peer's cancellation; the SUT safety assertion and leader-cancel/waiter-live workload make the defect checkable.

### May process restart satisfy the drain property?

- **Examined:** the requested resource classes and the deployment's crash-fault campaign.
- **Found:** process death destroys goroutines, readers, and in-memory lease/inflight maps, so accepting a new process's zero counters would hide an unwind leak in the killed process.
- **Not found:** a durable need to carry these ephemeral resources across restart.
- **Conclusion:** resolved. Require the same process epoch for a wave; abandon and retry the wave after restart.

### Investigation log

- Traced the public `AggregateVolumes` RPC context through the controller, local volume-view provider, historical store view, cold-part reader, archive store, and cache singleflight.
- Traced cleanup in the reverse direction through indexed-reader close, cache-lease release/eviction, snapshot close, and manifest/run lease release.
- Traced the maintenance consequences through local GC, tier local-removal gating, and remote-GC root capture.
- Compared deterministic cancellation and concurrent-cache-miss tests with available metrics, cache stats, Antithesis topology, workload oracle guidance, and the existing assertion inventory.
