# Account incarnation benchmark prototype

This branch measures the cost of a cache-backed `AccountIncarnation` attribute before account lifecycle rules change.

## Write path

Admission declares an `AccountIncarnation` key for every account whose volume a posting or whose metadata an account operation can touch, including `world`. The key is `(ledger, address)`. After the orders and transient-volume validation, the FSM collects accounts with volume or metadata writes, including metadata deletions, and checks their declared keys through the proposal-wide coverage gate. For a normal account without an incarnation, it writes `{persistence, inserted_at, updated_at}` in the same batch. Later accepted writes update `updated_at` once per account and proposal while preserving `inserted_at`. Both timestamps use the FSM's effective proposal HLC date; they describe writes in this ledger rather than a caller-supplied transaction date. Query reads do not update them.

Account reads hydrate the existing `Account.insertion_date` and `Account.updated_at` fields from the incarnation in the same Pebble snapshot as volumes and metadata. Get, list, and prepared account-query enrichment share this path. Accounts without an incarnation return neither date. Each returned account incurs one additional point read. Measure this with an account-read workload; the `world_to_bank` and `any_unbounded_to_any` write workloads do not exercise account hydration.

The attribute uses the standard two-generation cache. Rotation can evict the in-memory entry; a subsequent admission loads it from the primary attribute zone. There is no always-resident map of accounts in the FSM.

This prototype has no dedicated Bloom filter for incarnation keys. A cache miss checks Pebble, including when the account has no incarnation yet.

The probe runs only when the execution plan contains an incarnation key. This lets older hand-built test plans retain their existing shape. Production admission adds the key through `addVolumeNeed` and account-metadata coverage. `AttributeCoverage` supplies preload and key coverage only; it does not assert an incarnation version. Concurrent proposals that first touch the same account apply in Raft order. The first opens the incarnation, and later accepted writes update its timestamp.

## Deliberate limits

This remains a benchmark prototype of the account model. It creates incarnations for normal accounts touched by postings or metadata writes, including `world`. It does not close or reopen an incarnation, enforce a pinned type, or change ephemeral and transient volume behavior. The capability-gated migration below supplies historical account dates; it does not add account lifecycle operations.

## Capability-gated migration

`inserted_at` and `updated_at` describe the first and latest accepted account-state writes in v3 FSM time. The current volume and metadata attributes cannot recover those dates after a balance drains or metadata is deleted. A backfill must replay the retained global log and its accepted proposal boundaries, including resolved transaction postings, account metadata writes and deletions, account-type changes, and ledger deletion/recreation. It must reproduce the final per-proposal write set used by `ensureAccountIncarnations`, rather than treating every historical log as an account write. A missing history segment makes exact dates unavailable; the migration must stop or report the affected ledger instead of stamping the migration time. Mirrored v2 ledgers receive v3 apply dates, not the source's v2 account-row dates.

The capability foundation in PR #2327 provides replicated `ClusterPolicy` activations, committed member evidence, and proposal declarations. This branch adds the specific rolling path for account incarnations. It still requires all members to run the compatible binary before the first activation; mixed-version activation is unsupported.

Use two monotonic activation milestones:

| Milestone | Effect | Activation evidence |
| --- | --- | --- |
| `AccountIncarnationWritesV1` | Admission declares and preloads touched account incarnations; the FSM creates or updates them for marked proposals. Historical dates stay hidden from account reads. | Every current voter and learner advertises software support. Old binaries are gone before activation. |
| `AccountIncarnationHistoryCompleteV1` | Account reads expose `inserted_at` and `updated_at`; future account-lifetime rules may rely on historical coverage. | Every current member has a validated local backfill marker for the same migration definition, and is caught up enough to receive the activation. The marker must survive checkpoint creation and restore. |

`SupportedIDs()` describes binary support. The member advertises the second ID only when its local store has a completed, versioned migration marker. Account reads check both the committed activation and that local marker. The first capability is derived from incarnation coverage in the final execution plan; FSM apply follows this persisted proposal declaration. The leader's final proposal gate rejects an unmarked account write after activation, including one prepared before activation. While an activating policy proposal is pending, it also rejects later unmarked account writes, closing the enqueue-to-apply interval.

Roll out in these stages:

1. Ship both readers and the migration implementation dormant. During the mixed-binary rollout, do not write incarnation attributes or cache slots, and do not expose account dates. The new binary must continue to propose and apply the old format.
2. After all members run the compatible binary and have committed support evidence, activate `AccountIncarnationWritesV1` in `ClusterPolicy`. New account-writing proposals carry its semantic marker. The FSM updates incarnations for these proposals, including accounts whose historical incarnation has not yet been reconstructed.
3. Restart each node one at a time. Before it starts Raft or serves requests, the node folds accepted audit-linked logs through its durable applied index. It merges historical first/last timestamps with post-activation incarnation rows, preserving newer `updated_at` values and resetting on ledger deletion. Pebble commits every 256 audit entries with an atomic cursor and definition version; a restart resumes from that cursor. Missing audit history fails startup. Keep enough voters available for quorum throughout.
4. A leader checkpoint can replace a converted follower's Pebble store. The synchronizer migrates the incoming checkpoint in staging before activating it. After installation, membership re-announces local readiness from the installed marker. A freshly joined learner also refreshes its committed membership evidence after the converted checkpoint arrives. Account reads fail closed if the active history capability is present without a complete local marker.
5. Once every current member has reported local completion, run the store checker and activate `AccountIncarnationHistoryCompleteV1` in `ClusterPolicy`. The activation gate requires every member to advertise it. The checker rebuilds the incarnation projection from audited history in temporary storage and compares the durable attribute range in both directions. Keep both activations monotonic.

Backfill only the durable attribute zone; do not load every historical incarnation into the two-generation FSM cache. The migration clears stale incarnation cache slots on completion. Admission loads cold entries from Pebble. The prototype has no incarnation Bloom filter. A future Bloom filter would need a complete, sealed rebuild before negative lookups are safe.

An empty new learner cannot currently join after the history capability is active: join admission requires capability evidence before the learner receives a checkpoint. Seed it from a converted backup before joining, or add a pending-learner protocol in a follow-up. This restriction does not affect rolling restarts of existing members.

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

## ACME timestamp and account-read benchmark (2026-11-11)

The current timestamp and account-read revision was tested against `main@5534f9e6cef7a0818914726593e006aad4e52969` on the same ACME pool and three-replica hardware as above. The branch image was built from `b1bbce79b5631033e317d0ea62bdd2e21730298c`. The tested OCI digests were `sha256:f894547adb843f6f2f849092f7d1373c92c141c89bc5a6d0d3a106d0ec9e4bae` (`main`) and `sha256:c975d85a34be565122f8817e8c2a2e6a605b775b410f83e4e699ba71db01a101` (branch). Both alias tags were checked against these digests before submission. All six Benchmark resources reached `Completed`.

The write workloads retained the five-minute 400-to-800 VU profile and atomic bulks of 50 transactions. The new `account_reads` script seeded 200 accounts, waited for the first page to appear in the index, then issued one `GetAccount` and one 50-account `ListAccounts` request per iteration. Its five-minute profile ramped from 50 to 100 VUs. Each main/branch pair used the same script and configuration.

| Workload | `main` throughput | Branch throughput | Change | `main` p95 | Branch p95 | Failed requests |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `world_to_bank` | 162,647 tx/s | 160,155 tx/s | -1.5% | 375 ms | 378 ms | 2/975,884 vs 1/960,932 |
| `any_unbounded_to_any` | 50,645 tx/s | 40,518 tx/s | -20.0% | 961 ms | 1,677 ms | 0/303,868 vs 0/243,107 |
| `GetAccount` | 10,334 reads/s | 9,907 reads/s | -4.1% | 8.87 ms | 9.03 ms | 0/3,100,289 vs 0/2,972,195 |
| `ListAccounts` (50 accounts) | 10,334 reads/s | 9,907 reads/s | -4.1% | 8.17 ms | 8.43 ms | 0/3,100,289 vs 0/2,972,195 |

Throughput divides each successful k6 counter by the 300-second load profile. The read workload issues both endpoints in each iteration, so its two rows share the iteration rate. The p95 values are estimates from the exported OpenTelemetry histogram buckets over each run, not the exact in-process k6 Trend quantiles. Failed requests use the `errors_total` counter. Each row has one run per image; the `world_to_bank` difference is small enough to require repetitions before drawing a firm conclusion. The unbounded workload still shows a substantial throughput and latency regression. The read workload shows a smaller cost for hydrating incarnation timestamps, including a 50-account page.

Grafana snapshots: [`main` world-to-bank](https://grafana.internal.frmnc.net/dashboard/snapshot/eIjHtv12pIbCAkLG4XSig21vuj5ABt7y), [branch world-to-bank](https://grafana.internal.frmnc.net/dashboard/snapshot/59VHwZar46VfvV2YhJogS2LflRyPosJF), [`main` unbounded-to-any](https://grafana.internal.frmnc.net/dashboard/snapshot/NkA2MlJxDVKakSI8EgUKiPoDjrUHYAjS), [branch unbounded-to-any](https://grafana.internal.frmnc.net/dashboard/snapshot/AQFRNevHo2eeoQ9Eiv1pBoOeJGUUdsNC), [`main` account reads](https://grafana.internal.frmnc.net/dashboard/snapshot/ae9lGQV7d7E7u0Dy4cMtjsgNczNpRh9E), [branch account reads](https://grafana.internal.frmnc.net/dashboard/snapshot/RtaISZiMQAirqTjZaQ0OD59512KpNRYP).

The Benchmark resource names are `ledger-5534f9e6cef7-w2b-1111`, `ledger-b1bbce79b563-w2b-1111`, `ledger-5534f9e6cef7-any-1111`, `ledger-b1bbce79b563-any-1111`, `ledger-5534f9e6cef7-read-1111`, and `ledger-b1bbce79b563-read-1111`, in the same order as the snapshots. The operator serialized the runs, but admitted them in a different order from submission. Current `main` has two commits beyond this branch's merge base, for OIDC issuer handling and exact ID filters. Neither changes these benchmark scripts, but this comparison does not isolate the branch diff as strictly as the earlier paired run.
