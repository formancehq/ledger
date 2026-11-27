# Account incarnation benchmark prototype

This branch measures the cost of a cache-backed `AccountIncarnation` attribute before account lifecycle rules change.

## Write path

When `AccountIncarnationWritesV1` is active in the committed policy, admission declares an `AccountIncarnation` key for every account whose volume a posting or whose metadata an account operation can touch, including `world`. The key is `(ledger, address)`. Coverage extraction reads a read-only in-memory policy snapshot published only after the corresponding Pebble batch commits; recovery republishes it from the stored policy. Admission requires an explicit committed-policy source at construction. It adds incarnation keys at the volume and account-metadata need sources, including Numscript-discovered needs. A proposal prepared just before activation may still carry the old coverage and is rejected by the final proposal gate after activation. After the orders and transient-volume validation, the FSM collects accounts with volume or metadata writes, including metadata deletions, and checks their declared keys through the proposal-wide coverage gate. For a persistent account without an incarnation, it writes `{persistence, inserted_at, updated_at, volume_count}` in the same batch. Later accepted writes update `updated_at` once per account and proposal while preserving `inserted_at`. Both timestamps use the FSM's effective proposal HLC date; they describe writes in this ledger rather than a caller-supplied transaction date. Query reads do not update them.

Account reads hydrate the existing `Account.insertion_date` and `Account.updated_at` fields from the incarnation in the same Pebble snapshot as volumes and metadata. Get, list, and prepared account-query enrichment share this path. Accounts without an incarnation return neither date. Each returned account incurs one additional point read. The `world_to_bank` and `any_unbounded_to_any` write workloads do not exercise account hydration.

The attribute uses the standard two-generation cache. Rotation can evict the in-memory entry; a subsequent admission loads it from the primary attribute zone. There is no always-resident map of accounts in the FSM.

This prototype has no dedicated Bloom filter for incarnation keys. A cache miss checks Pebble, including when the account has no incarnation yet.

The probe runs only when the execution plan contains an incarnation key. This lets older hand-built test plans retain their existing shape. Production admission adds the key through `addVolumeNeed` and account-metadata coverage. `AttributeCoverage` supplies preload and key coverage only; it does not assert an incarnation version. Concurrent proposals that first touch the same account apply in Raft order. The first opens the incarnation, and later accepted writes update its timestamp.

## Deliberate limits

The FSM tracks the number of persisted `(asset, color)` volume cells per incarnation. A normal account retains its incarnation at zero cells. An ephemeral account closes its incarnation when its last cell is purged; a later write opens a new one with a new `inserted_at`. Other assets can keep the same ephemeral incarnation alive, and account metadata is retained after closure. Transient accounts still have no incarnation. This remains a benchmark prototype: it does not enforce a pinned type or add an explicit account lifecycle operation.

## Capability-gated migration

`inserted_at` and `updated_at` describe the first and latest accepted account-state writes in v3 FSM time. The current volume and metadata attributes cannot recover those dates after a balance drains or metadata is deleted. A backfill clears the existing incarnation projection when starting at cursor zero, then replays the retained global log and its accepted proposal boundaries, including resolved transaction postings, account metadata writes and deletions, account-type changes, and ledger deletion/recreation. The audited new, purged, and ephemeral volume lists reconstruct each account's persisted cell count, including closure and reopening; duplicate volume annotations in one accepted proposal count once. Cursor commits include the rebuilt attributes, so a restart continues without clearing completed batches. A missing history segment makes exact dates unavailable; the migration must stop or report the affected ledger instead of stamping the migration time. Mirrored v2 ledgers receive v3 apply dates, not the source's v2 account-row dates.

The capability foundation in PR #2327 provides replicated `ClusterPolicy` activations, committed member evidence, and proposal declarations. Account incarnation backfill requires a cluster-wide stopped cutover. All members must run the compatible binary before the first activation; mixed-version activation is unsupported.

Use two monotonic activation milestones:

| Milestone | Effect | Activation evidence |
| --- | --- | --- |
| `AccountIncarnationWritesV1` | Admission declares and preloads touched account incarnations; the FSM creates or updates them for marked proposals. Historical dates stay hidden from account reads until the second activation. | Every current voter and learner has completed the local backfill. Old binaries are gone before activation. |
| `AccountIncarnationHistoryCompleteV1` | Account reads expose `inserted_at` and `updated_at`; future account-lifetime rules may rely on historical coverage. | Every current member has a completed local backfill marker for the same migration definition. Checkpoint backup and restore copy the marker and incarnation attributes together. |

`SupportedIDs()` describes binary support. The member advertises the second ID only when its local store has a completed, versioned migration marker. Account reads check both the committed activation and that local marker. The first capability is derived from incarnation coverage in the final execution plan; FSM apply follows this persisted proposal declaration. The leader's final proposal gate rejects an unmarked account write after activation, including one prepared before activation. While an activating policy proposal is pending, it also rejects later unmarked account writes, closing the enqueue-to-apply interval.

The operator requires a stopped rc.4 to rc.5 upgrade. It commits and verifies a drain cutoff on every member, stops all old Pods, then recreates the StatefulSet. OrderedReady starts each replacement Pod in turn. After opening and validating its Pebble store, each Pod folds the frozen audit history and seals a local completion marker **before** constructing Raft, admission or listeners. The source RC has no active incarnation capability; startup backfill is allowed only with an applied, unreleased drain barrier. Pebble commits account rows and a versioned audit cursor together every 256 entries. SIGTERM cancels the scan; a replacement Pod resumes from its durable cursor, replaying prior audit entries to reconstruct transient fold state without rewriting their projections. Missing history or a changed applied index fails startup and keeps the Pod unready.

After Raft starts on all converted members, membership evidence includes the completion marker. The leader automatically activates both capabilities once every current voter and learner advertises the marker. Business admission on a converted store waits for `AccountIncarnationWritesV1` to commit, closing the gap between migration and activation. The operator waits for both committed activations before releasing the drain. Account reads fail closed if the active history capability is present without a complete local marker. The startup probe allows two hours for migration; a configured probe override must allow at least the expected migration duration. Readiness still reports the local Raft loop, not quorum, so OrderedReady can start the next Pod.

An empty store, whether bootstrap or learner, with no applied or audited history seals a completion marker before Raft starts. Join preflight advertises that evidence, so an empty learner can join after history activation. It must still catch up through the normal Raft log or checkpoint path before promotion; the marker does not prove that it is current. A learner carrying non-empty legacy history without a completion marker must be converted under an applied drain or restored from a converted checkpoint before joining a cluster with active incarnation capabilities. A leader checkpoint can replace a converted follower's store; the synchronizer migrates the incoming checkpoint in staging before activating it.

The standalone `migrate-incarnations` command remains a recovery tool for a stopped store with active writes and an applied drain barrier. The store checker reconstructs the projection from audited history and compares the durable attribute range in both directions; it is a separate verification step, not part of startup.

Backfill only the durable attribute zone; do not load every historical incarnation into the two-generation FSM cache. The migration clears stale incarnation cache slots on completion. Admission loads cold entries from Pebble. The prototype has no incarnation Bloom filter. A future Bloom filter would need a complete, sealed rebuild before negative lookups are safe.

`TestAccountIncarnationRolloutAcrossOfflineMigration` exercises the data path across these milestones with real admission and FSM proposals. Its in-process proposer replaces Raft transport and the membership activation gate, which have separate tests. A three-node stopped upgrade and activation remains an end-to-end deployment test to run before enabling this rollout in production.

The local backfill reads audit-linked logs in sequence. An out-of-order log reference falls back to a point read without changing the order in which the audit items are folded. A temporary growable Bloom filter is seeded from existing incarnation keys at the frozen snapshot, including rows from a previous partial migration. It skips Pebble reads only for keys that are definitely absent; positive results still read the stored row so historical timestamps merge with post-activation writes. New keys enter the filter after their batch and migration cursor commit together. The filter is not persisted and is rebuilt on restart; false positives affect speed, not output.

The current `main` binary rejects unknown cache-slot types during restore and does not update incarnation state. The preparatory rollout must prove old/new checkpoint and proposal compatibility; after the first incarnation cache row or semantic proposal is committed, downgrade requires a separately tested recovery path. Removing `opened_at_raft_index` avoids inventing an unprovable historical opening index; future lifetime identity needs its own explicit design.

## Local benchmark

Run the paired admission benchmarks on the same checkout and host:

```bash
go test ./internal/infra/plan -run '^$' -bench 'BenchmarkAdmissionCoverage' -benchtime=1s -count=3 -benchmem
go test ./internal/infra/state -run '^$' -bench '^BenchmarkEnsureAccountIncarnationsHot$' -benchtime=100x -count=3 -benchmem
```

`BenchmarkAdmissionCoverage` is the volume-only baseline. `WithIncarnations` adds a hot incarnation key per account. `WithColdIncarnations` persists those keys in Pebble but leaves them outside the cache, modeling the first admission after rotation. The benchmark builds coverage, resolves preloads, and assigns coverage bits. It excludes Raft apply, Pebble commit, and network latency; its ratios must not be interpreted as end-to-end transaction latency.

On an Apple M5 Pro (Darwin arm64), the median of three 100-iteration runs was:

| Shape | Volume-only | Hot incarnations | Cold incarnations |
| --- | ---: | ---: | ---: |
| 10 orders, 1 posting each | 17.9 µs | 25.8 µs | 53.5 µs |
| 50 orders, 2 postings each | 90.4 µs | 139.7 µs | 355.4 µs |
| 100 orders, 5 postings each | 351.8 µs | 663.6 µs | Not measured |

The hot 50-order case rises from 1,226 to 1,755 allocations per operation. These synthetic orders use a new source and destination address for every posting, so they represent a high-cardinality case. A follow-up benchmark should measure repeated addresses and the full FSM commit path before deciding whether every normal account needs this attribute.

The isolated FSM step with hot incarnation entries measured 0.5 µs for 2 accounts, 5.8 µs for 20 accounts, and 31.3 µs for 100 accounts (medians of three runs). It includes volume-key collection, account deduplication, coverage checks, and cache reads. It excludes posting arithmetic and persistence.

## ACME cluster benchmark (2026-11-11)

These results belong to the earlier posting-only prototype image. They do not measure `world` coverage or timestamp updates. Repeat both workloads with the current branch before using the numbers to assess this revision.

The ACME `eks-acme-dev-euw1-01` benchmark pool ran one five-minute k6 test per image and workload. Each run created a fresh three-replica Ledger cluster in `ledger-v3`. Every replica had 8 CPUs, 16 GiB of memory, and separate io2 data and WAL volumes. The k6 profile ramped to 400 VUs in 30 seconds, to 800 VUs over four minutes, then to zero in 30 seconds. Requests used atomic bulk writes of 50 transactions. The same script and configuration were used for both images within each workload. The benchmark operator serialized the four runs in the `acme-ledger-m8a` execution group.

The baseline was `main` at `010eb6354a14b7b951082c9f1430209ebbc6869c`; the incarnation prototype was `378de9b21500518217b8c9b6351fff699dd15ea5`, based on that commit. The tested OCI digests were `sha256:05284fc0f6b224119200c7b9b2b63c392a94ba303c03f3843cea0d5db9e3a21d` and `sha256:7abe97db0616bb7ca8f8cca9d570cc3e3c191b77adc87f92bbb39a234b509413`, respectively. Private, unique SemVer aliases pointed to those exact digests because the installed Ledger Operator (`v3.0.0-rc.3`) rejects development tags and digest suffixes in `spec.image.tag`. The image revisions and alias digests were verified before submission.

| Workload | `main` transactions/s | Prototype transactions/s | Throughput change | `main` bulk p95 | Prototype bulk p95 | Failed requests |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `world_to_bank` | 160,142 | 155,926 | -2.6% | 266 ms | 270 ms | 2/960,875 vs 5/935,582 |
| `any_unbounded_to_any` | 64,210 | 40,385 | -37.1% | 677 ms | 1,180 ms | 0/385,263 vs 0/242,317 |

The throughput values are k6's `transactions_created` rate over the full run; latency is the `bulk_latency` p95. Both `world_to_bank` runs passed the 500 ms p95 threshold. Both `any_unbounded_to_any` runs crossed it. The unbounded workload assigns a new source and destination address to every transaction, so it measures repeated incarnation creation and cold account lookup rather than a typical reuse pattern. The prototype reduced throughput substantially in that case. Average Ledger process CPU across the three replicas was approximately 17.5 vs 17.1 cores for `world_to_bank`, and 14.4 vs 12.0 cores for `any_unbounded_to_any` (`main` vs prototype), measured from `process_cpu_time_seconds_total` over each five-minute load window. Lower CPU in the second prototype run accompanies lower throughput; these measurements alone do not identify the bottleneck.

Grafana snapshots: [`main` world-to-bank](https://grafana.internal.frmnc.net/dashboard/snapshot/3xrk6RmjmOJ1I7pPv1Uj73KjjNmcuvH5), [prototype world-to-bank](https://grafana.internal.frmnc.net/dashboard/snapshot/BGFddtC7aaO7PlEvcGticTfxKEYjZTnf), [`main` unbounded-to-any](https://grafana.internal.frmnc.net/dashboard/snapshot/oMsoEVbRJ6Zzf44uReP8DJU8MZxKBrbG), [prototype unbounded-to-any](https://grafana.internal.frmnc.net/dashboard/snapshot/CZ3BDwZ1PklbGrMtlDpXxPG09vsjuuCD).

The corresponding Benchmark CR names are `ledger-010eb6354a14-43008000001-1`, `ledger-378de9b21500-43008000003-1`, `ledger-010eb6354a14-43008000002-1`, and `ledger-378de9b21500-43008000004-1`, in the same order as the snapshots above. The operator marked all four `Completed` and retained their reports.

The GitHub benchmark submission job could not reach ACME because its Tailscale OAuth secret was unavailable. The runs were submitted with the same Pulumi program from an authenticated local session. This is one run per cell, with `main` workloads executed before prototype workloads; repeat and interleave runs before treating small differences as conclusive. The high-cardinality regression is large enough to justify optimizing incarnation lookup and write behavior before advancing the account lifecycle design.

## ACME timestamp write benchmark (2026-11-11)

The timestamp revision was tested against `main@5534f9e6cef7a0818914726593e006aad4e52969` on the same ACME pool and three-replica hardware as above. The branch image was built from `b1bbce79b5631033e317d0ea62bdd2e21730298c`. The tested OCI digests were `sha256:f894547adb843f6f2f849092f7d1373c92c141c89bc5a6d0d3a106d0ec9e4bae` (`main`) and `sha256:c975d85a34be565122f8817e8c2a2e6a605b775b410f83e4e699ba71db01a101` (branch). Both alias tags were checked against these digests before submission. Both write-workload pairs reached `Completed`.

The write workloads retained the five-minute 400-to-800 VU profile and atomic bulks of 50 transactions. Each main/branch pair used the same script and configuration.

| Workload | `main` throughput | Branch throughput | Change | `main` p95 | Branch p95 | Failed requests |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `world_to_bank` | 162,647 tx/s | 160,155 tx/s | -1.5% | 375 ms | 378 ms | 2/975,884 vs 1/960,932 |
| `any_unbounded_to_any` | 50,645 tx/s | 40,518 tx/s | -20.0% | 961 ms | 1,677 ms | 0/303,868 vs 0/243,107 |

Throughput divides each successful k6 counter by the 300-second load profile. The p95 values are estimates from the exported OpenTelemetry histogram buckets over each run, not the exact in-process k6 Trend quantiles. Failed requests use the `errors_total` counter. Each row has one run per image; the `world_to_bank` difference is small enough to require repetitions before drawing a firm conclusion. The unbounded workload still shows a substantial throughput and latency regression.

Grafana snapshots: [`main` world-to-bank](https://grafana.internal.frmnc.net/dashboard/snapshot/eIjHtv12pIbCAkLG4XSig21vuj5ABt7y), [branch world-to-bank](https://grafana.internal.frmnc.net/dashboard/snapshot/59VHwZar46VfvV2YhJogS2LflRyPosJF), [`main` unbounded-to-any](https://grafana.internal.frmnc.net/dashboard/snapshot/NkA2MlJxDVKakSI8EgUKiPoDjrUHYAjS), [branch unbounded-to-any](https://grafana.internal.frmnc.net/dashboard/snapshot/AQFRNevHo2eeoQ9Eiv1pBoOeJGUUdsNC).

The Benchmark resource names are `ledger-5534f9e6cef7-w2b-1111`, `ledger-b1bbce79b563-w2b-1111`, `ledger-5534f9e6cef7-any-1111`, and `ledger-b1bbce79b563-any-1111`, in the same order as the snapshots. The operator serialized the runs, but admitted them in a different order from submission. The comparison does not isolate the branch diff as strictly as the earlier paired run.

## ACME historical migration benchmark (2026-11-23)

An isolated three-replica cluster ran the legacy `main@5534f9e6cef7` image with the same 8 CPU, 16 GiB memory, 500 GiB io2 data volume, and 15 GiB io2 WAL volume per replica. A single `any_unbounded_to_any` k6 run completed 1,000,000 atomic bulks of 50 transactions, with two unique account addresses per transaction: 50,000,000 accepted transactions and 100,000,000 distinct accounts per replica. All 1,000,000 requests succeeded, with no dropped or failed iterations. The load itself took approximately 15 minutes 40 seconds; it is excluded from migration timings.

After stopping the isolated cluster, each replica's retained data volume was mounted by a separate migration job. The three jobs ran in parallel against their own local Pebble stores; they did not join Raft. The helper used the branch's actual `incarnationmigration.RunWithPause` path and a local `AccountIncarnationWritesV1` policy injection so that a legacy `main` store could be migrated without an operator-driven capability activation. The injected policy makes these volumes unsuitable for rejoining the original cluster. The jobs used the branch's migration code from PR #2529 with a migration-local growable Bloom filter for definite-new account keys and a sequential reader for audit-linked logs. The filter avoids a Pebble point lookup for every first touch; the sequential reader avoids a point lookup for every log. Neither changes the normal write path.

Each migration job had a 6 CPU, 12 GiB memory limit. All three began at an empty migration cursor and finished with `complete=true`, `audit_cursor=1,000,001`, and `applied_index=1,000,009`. The duration below is measured inside `RunWithPause`, excluding source checkout and helper compilation.

| Replica | Migration duration | Job start to completion |
| --- | ---: | ---: |
| 0 | 27 min 51 s | 29 min 11 s |
| 1 | 14 min 40 s | 15 min 53 s |
| 2 | 14 min 34 s | 15 min 43 s |

The second completed local migration finished 15 min 53 s after the jobs started; all three finished after 29 min 11 s. This is a measurement of local conversion under three simultaneous jobs, not a measured service outage. A real offline upgrade also needs image startup, Raft quorum formation, catch-up, and any operator sequencing. If migration is strictly one node at a time while all nodes remain stopped, the time to migrate two nodes is roughly the sum of two node durations, before Raft starts. Replica 0 showed substantially more Pebble compaction and took nearly twice as long as the other two, so use the slowest replica when planning completion of all three. The data generator and migration helper were temporary benchmark resources; these volumes were never rejoined to the cluster.

### Profiling and reduced migration time

A second isolated ACME cluster used the same hardware and 100-million-account shape. The first load accepted 999,102 of 1,000,000 bulks; a separate load accepted 898 bulks with fresh account addresses, restoring the total to exactly 1,000,000 accepted bulks and 100,000,000 distinct accounts. The three local migration jobs ran in parallel with CPU, allocation, heap and Pebble metrics profiles. They measured three variants on different replicas, so the times alone do not isolate a single change:

| Replica | Variant | Migration duration |
| --- | --- | ---: |
| 0 | Branch migration before this profiling pass | 28 min 55 s |
| 1 | Direct history reader, without other changes | 17 min 50 s |
| 2 | Direct reader, one forward audit-item iterator, account types compiled on changes | 13 min 09 s |

The baseline profile attributed 1,247 CPU seconds and about 9.1 TB of cumulative allocation to incarnation point lookups, mostly Pebble SST filter-block reads. Even a rare false positive in the migration-local 16-bit-per-key Bloom filter can trigger an expensive point lookup against the growing projection. The direct-reader variant still spent 656 CPU seconds in those lookups and allocated about 4.9 TB through Pebble filter-block reads; the combined variant spent 423 CPU seconds and allocated about 3.2 TB there. These are cumulative CPU/allocation figures across goroutines, not elapsed time or retained memory. The baseline kept one Pebble snapshot open but its reported pinned size was zero. The direct reader alone therefore has no established independent benefit from this experiment.

The migration-local filter was increased from 16 bits and 8 probes to 32 bits and 16 probes per key. A third clean three-replica ACME cluster completed all 1,000,000 bulks in one run, without failed requests. Its local migration jobs used that more precise filter together with the sequential audit-item reader and account-type cache. All three started with an empty migration cursor and finished with `complete=true`, `audit_cursor=1,000,001`, and `applied_index=1,000,009`:

| Replica | Migration duration |
| --- | ---: |
| 0 | 5 min 25 s |
| 1 | 5 min 27 s |
| 2 | 5 min 16 s |

All three jobs completed within about 6 min 35 s of launch, including checkout and compilation of the benchmark helper. On replica 0, cumulative allocation fell from about 9.5 TB in the baseline profile to about 395 GB; the incarnation point-read path disappeared from the leading CPU stacks. The remaining CPU is mainly Pebble compaction, sequential log decoding, and committing incarnation batches. A 32-bit-per-key growable filter uses roughly 0.5 GiB at this cardinality; the observed migration pods stayed below their 12 GiB memory limit. This third run measures local conversion, not Raft restart or actual service downtime. The full store-checker verification is separate from the `migrate-incarnations` command and was not included in any migration duration.

### Cold restart and batch-size probe

A fourth isolated ACME cluster used the same legacy image and hardware. Its first k6 load stopped at the generator's implicit ten-minute limit after 550,081 accepted bulks; two follow-up loads with disjoint account addresses brought the total to exactly 1,000,000 accepted bulks (50 million transactions, 100 million distinct account addresses). The cluster was then restarted by deleting all three pods at 19:25:00 UTC. An independent pod polled `/readyz` and a real account read once per second. The first failed account read occurred at 19:25:02; the first successful one occurred at 19:26:16, giving a **74-second observed service interruption** and 76 seconds from the deletion command. The first pod became Ready at 19:25:41, the second at 19:26:18, and the third at 19:26:56. `/readyz` returned 200 on the first pod while account reads still returned 503 before Raft quorum, so pod readiness alone understates service downtime.

On the first replica, Pebble opened in 21.7 seconds after the process began opening the existing database, and the FSM state was recovered from the store about 0.7 seconds later. The remaining time to service availability includes the StatefulSet's `OrderedReady` startup of the second pod and Raft election. Starting the first two replicas in parallel is a plausible operational optimization, but this benchmark did not test it. This restart used the legacy image and unmigrated data; its 74 seconds must not be presented as a measured post-migration restart or added to the migration duration as a validated end-to-end outage. The PR image was unavailable for this experiment. The later [migrated-volume experiment](#restart-on-migrated-100-million-account-volumes-2026-11-24) measured a 34-second warm restart, but the exact service interruption during the first restart after migration remains unmeasured.

After the restart, the cluster was stopped and its three volumes migrated with the PR head and different audit batch sizes. Every migration completed at `audit_cursor=1,000,002` and `applied_index=1,000,012`; the extra audit entry reflects the load attempts. The runs are on different replicas, so small differences are confounded by replica and compaction variation:

| Replica | Audit entries per commit | Local migration duration |
| --- | ---: | ---: |
| 0 | 256 (current) | 5 min 23 s |
| 1 | 1,024 | 5 min 45 s |
| 2 | 4,096 | 6 min 19 s |

Increasing the batch size did not produce a gain in this run and increased observed working memory. Keep the current 256-entry batch. The remaining profile is dominated by Pebble compaction, log decoding, and incarnation serialization. Changing Pebble's compaction or compression policy would require a separate correctness and durability test; these profiles alone do not justify such a change.

### Restart on migrated 100-million-account volumes (2026-11-24)

A fresh isolated three-replica ACME cluster accepted exactly 1,000,000 legacy bulks with no failed requests: 50 million transactions and 100 million distinct accounts per replica. All replicas were then upgraded to the PR image built from `a3e8757e8620f5aef817d3be1f17eab83bb6554b` (`sha256:b023c8052910035d63836071c20715ac2ffa91e298147d1db379edbd49ecabb0`), while incarnation writes remained inactive. The image was published under a temporary tag because the available GitHub token could not push to the repository's GHCR package. The benchmark injected the write activation into each stopped local store, as in the earlier migration profiles; this is not a replicated operator cutover. The `Ledger` Kubernetes resource was retained throughout the stop and restart so its controller did not delete the business ledger.

The cluster was stopped at 09:00:18 UTC. The three offline migration runs began at 09:01:09–09:01:16, including job scheduling, checkout and helper compilation. The actual `RunWithPause` durations were 333.1, 327.4 and 326.2 seconds. All finished with `complete=true`, `audit_cursor=1,000,001` and `applied_index=1,000,014`. Profile writing finished by 09:07:08; job cleanup followed, and the cluster resource was recreated at 09:07:42 with the original cluster ID and volumes. Its pods became Ready at 09:08:17, 09:08:50 and 09:09:26. The external probe's node was terminated during the long outage, so its first successful read after this initial restart was not captured. A replacement probe verified account reads by 09:09:44. Thus this run establishes an **upper bound of 9 min 26 s** from the stop command to an observed successful read, not an exact end-to-end outage duration.

With that probe pinned to the Ledger node pool, a second simultaneous restart of all three pods measured the missing post-migration startup component: account reads failed from 09:10:20 through 09:10:53 and succeeded at 09:10:54, an observed **34-second interruption**. The first and second pods became Ready at 09:10:40 and 09:11:02, while the third became Ready at 09:11:19. The first node opened its warm Pebble store in 19 ms and recovered its FSM state about 0.3 seconds later; the leader reported readiness at 09:10:54. This is a warm restart of the migrated store and is not interchangeable with the first restart after migration, which had additional pod and storage startup cost.

The operational path has avoidable overhead beyond the approximately 5.5-minute conversion: the benchmark jobs spent roughly 50–58 seconds before migration on checkout and compilation, and up to 26 seconds afterwards writing profiles. A production migration image with the binary already built would remove those two benchmark-only delays. Running the first two replicas in parallel rather than using `OrderedReady` remains an unmeasured way to shorten quorum restoration. The local policy injection in this experiment is not the production rollout procedure; the current startup path migrates under the stopped-upgrade drain and activates both capabilities through Raft after every member reports completion.
