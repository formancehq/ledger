# Account incarnation benchmark prototype

This branch measures the cost of a cache-backed `AccountIncarnation` attribute before account lifecycle rules change.

## Write path

Admission declares an `AccountIncarnation` key for every account whose volume a posting or whose metadata an account operation can touch, including `world`. The key is `(ledger, address)`. After the orders and transient-volume validation, the FSM collects accounts with volume or metadata writes, including metadata deletions, and checks their declared keys through the proposal-wide coverage gate. For a normal account without an incarnation, it writes `{opened_at_raft_index, persistence, inserted_at, updated_at}` in the same batch. Later accepted writes update `updated_at` once per account and proposal while preserving the opening index and `inserted_at`. Both timestamps use the FSM's effective proposal HLC date; they describe writes in this ledger rather than a caller-supplied transaction date. Query reads do not update them.

The `AttributeCoverage` entry for an incarnation also carries admission's observation of `opened_at_raft_index` and an explicit assertion bit. Zero means that admission observed no incarnation. After preloading the declared keys and after the idempotency replay gate, the FSM compares those observations with its current cache before executing any order. A mismatch rejects the whole proposal with retryable `STALE_INPUTS_RESOLUTION`; it writes no audit failure and does not freeze an idempotency result. Two concurrent proposals that both observe an absent account cannot silently share the newly opened lifetime: the first can open it, while the second must be admitted again against that lifetime. The check uses the cache and the existing Raft order, without a global account lock or a Pebble read in the FSM.

The attribute uses the standard two-generation cache. Rotation can evict the in-memory entry; a subsequent admission loads it from the primary attribute zone. There is no always-resident map of accounts in the FSM.

This prototype has no dedicated Bloom filter for incarnation keys. A cache miss checks Pebble, including when the account has no incarnation yet.

The probe runs only when the execution plan contains an incarnation key. This lets older hand-built test plans retain their existing shape. Production admission adds the key through `addVolumeNeed` and account-metadata coverage. An assertion bit omitted by an older proposal skips the version check. `updated_at` is not an optimistic-concurrency version: the assertion remains bound to the stable opening index.

## Deliberate limits

This is a benchmark prototype, not a release-ready account model. It creates incarnations for normal accounts touched by postings or metadata writes, including `world`. It does not close or reopen an incarnation, enforce a pinned type, or change ephemeral and transient volume behavior. The checker deliberately exempts this experimental projection: the audit chain binds accepted orders and log sequences, but not the Raft index stored as `opened_at_raft_index`, so it cannot independently verify that field. Choose an audit-derived lifetime identifier and implement two-way checker verification before release. An upgrade from existing persisted state is also undefined. Do not ship this branch as a behavior change.

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

These results belong to the earlier posting-only prototype image. They do not measure the later assertion, `world` coverage, or timestamp updates. Repeat both workloads with the current branch before using the numbers to assess this revision.

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
