# ACME-dev Pebble beta.5 baseline pilot (2026-09-30)

The published `v3.0.0-beta.5` passed two write pilots and the final read pilot. No storage/server error was observed, and the three dedicated Ledger pods had no restarts. This is **beta feedback**, not a Pebble/RocksDB comparison and not a maximum-capacity result.

The raw artifacts remain in the local `build/acme-pebble-beta5-20260930/` directory (not versioned): k6 scripts and summaries, manifests, provenance, pod logs, Prometheus samples and CloudWatch results. Follow [the comparison protocol](../../../../../tests/perf/ACME_STORAGE_COMPARISON.md) for the subsequent paired campaign.

## Versions and deployment

- Source commit SHA: `36d580949105aa91e520a9b43949ec9bbf0c08cc` (annotated tag object SHA: `08286c96549f9cd6aa4cff04413c75506c8f597b`).
- Ledger image index digest: `sha256:a8445f6f91dbdd50721f69a869eabf35a62fc2b51b27a6a4a7f214e886928432`; runtime pods retain this exact digest after the ECR mirror rewrite.
- k6 1.4.2 image index digest: `sha256:3656673de3f30424e8ebcfa46acd9558d83b6a43612d0f668ffeac953950c6c7`.
- Context `eks-acme-dev-euw1-01`, namespace `ledger-v3`, new Cluster `perf-pebble-b5-0930`, existing shared beta.5 operator. Shared CRDs/operator and existing `arnaud`, `frederic`, `sylvain` Cluster objects were not modified.
- Three replicas: CPU request 500m / limit 1 core; memory request/limit 2Gi; GOMEMLIMIT 1,932,735,283 bytes. Main Pebble cache 256Mi, one concurrent main compaction. Read-index defaults, including 64Mi cache, retained. Main memtable default 256Mi and read-index memtable default 64Mi. WAL enabled with the beta's default immediate sync. Effective env/command overrides and the exact beta binary help are saved alongside this report.
- Each replica has a separate 20Gi data and 15Gi WAL io2 volume, verified through AWS EC2 as 5,000 IOPS per volume. Six volumes total, all disposable and isolated from existing stores.
- Three different dedicated amd64 nodes (two `r8a.xlarge`, one `m8a.2xlarge`), in different AZs, sharing hosts with existing Ledger workloads. The k6 runner is arm64 on a separate general node. Placement is recorded; this pilot is not a controlled, homogeneous-node A/B trial.

## Measured results

Every measurement window is 120 seconds. Writes use atomic=true, 50 Numscript `world -> bank` transactions per HTTP bulk, open-loop 2 then 20 bulks/s, 30-second warmup then a 5-second gap. Reads use 20 total req/s, round-robin across three endpoint kinds, 15-second warmup then a 5-second gap, with writes stopped. Latencies below are per HTTP request/bulk, not divided by the transaction count.

| Scenario | Offered rate | Validated measured count | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|
| Writes, 50 Numscript tx/bulk | 100 tx/s | 12,000 tx | 10.62 | 15.59 | 16.46 |
| Writes, 50 Numscript tx/bulk | 1000 tx/s | 120,050 tx | 10.91 | 15.65 | 16.78 |
| Account point read | ~6.67 req/s | 801 reads | 16.47 | 22.38 | 22.56 |
| Transaction point read | ~6.67 req/s | 800 reads | 15.34 | 22.28 | 22.48 |
| Transaction list, page size 20 | ~6.67 req/s | 800 reads | 6.57 | 11.48 | 11.65 |

The second write window validated 120,050 transactions (one boundary bulk above the nominal 120,000), approximately 1,000.4 tx/s. All measurement and warmup windows have zero HTTP/application errors and zero dropped iterations. k6 Counter submetric `rate` divides by the **entire** test duration, so throughput above uses measured count / 120 seconds rather than that misleading all-test rate.

The first write ledger's final bank balance is 1,505,000 (15,050 validated transactions including warmup). The second is 15,010,000 (150,100 validated transactions including warmup). Both equal 100 times the full successful transaction counter. The read dataset is a deterministic 1,000 transactions / 100 destinations; each receives ten deposits of 100, giving balance 1,000. Setup verifies both endpoint transactions and sampled balances before measurement. Every point read validates identity and content; every list validates 20 entries and their posting amounts. The final read window validated 2,401 requests, including one boundary iteration above nominal. This is a small, warm-cache dataset.

## Resource telemetry

Prometheus cAdvisor samples are retained at 15-second resolution over measurement windows. CPU is a 1-minute rate; RSS and throttling are sampled and are not process-lifetime maxima. No CPU throttling was reported in these samples.

| Phase | Replica ordinal | Mean mCPU | Peak sampled mCPU | Peak sampled RSS MiB | Peak throttle sec/sec |
|---|---:|---:|---:|---:|---:|
| write | 2 | 6.6 | 7.6 | 80.6 | 0.0000 |
| write | 1 | 6.4 | 7.3 | 82.4 | 0.0000 |
| write | 0 | 8.1 | 9.3 | 74.4 | 0.0000 |
| write2 | 2 | 30.3 | 33.4 | 438.3 | 0.0000 |
| write2 | 1 | 33.5 | 39.1 | 446.9 | 0.0000 |
| write2 | 0 | 50.3 | 62.7 | 445.7 | 0.0000 |
| read3 | 2 | 10.4 | 13.3 | 479.4 | 0.0000 |
| read3 | 1 | 7.8 | 9.0 | 475.6 | 0.0000 |
| read3 | 0 | 16.0 | 20.0 | 468.4 | 0.0000 |

AWS/EBS raw CloudWatch results include read/write operations, bytes, total time and queue length for all six volumes at 60-second resolution. Prometheus CPU/RSS/throttle JSON, effective tuning, pod exit states and server logs are saved. The scoped server logs contain no error/warning/panic or leadership-change message during the workload intervals. This is observational evidence, not a fault-injection or exhaustive correctness test.

## Harness findings

- The first read setup rejected the transaction response because it expected `data.id`; beta.5 returns `data.transaction.id`. The payload was correct. The script was corrected and rerun.
- The second read's strict `count==2400` threshold rejected 2,401 fully validated responses. k6 may schedule an iteration at the window boundary. The final scripts permit the nominal count through nominal+one iteration, while retaining zero errors, zero drops and response-content validation. The final read and second write Jobs exited 0 with all thresholds passing.
- The Benchmark CR reports `Failed` after successful k6 execution because its Grafana snapshot step has no configured URL (`Get "/api/search?type=dash-db": unsupported protocol scheme ""`). This is a reporting/configuration failure, not a Ledger or storage failure. No automatic `k6-report` ConfigMap was generated. Raw k6 summaries and Prometheus/CloudWatch telemetry are the evidence for this pilot. Grafana snapshot configuration must be supplied before the full comparative campaign.

## Interpretation and next comparison

These two-minute pilots show that this beta can sustain the tested rates under this resource profile. They do not locate the saturation knee, test a dataset larger than the block caches, or establish parity with RocksDB. The beta SHA differs from the pre-RocksDB reference `c6a99fc92f43f5127c898fd9a727adee3424310d`; a beta-versus-PR comparison also includes other code/default changes. For engine attribution, use the pinned pre-RocksDB reference and paired repeats on a matched effective profile, with larger datasets and homogeneous placements. Preserve the main/read-index cache, memtable, sync, compaction and Raft settings explicitly rather than assuming defaults stayed equal.

## Cleanup

Cleanup verified at 2026-09-30 08:00:42 UTC: no dedicated Cluster, Pod, PVC, Service, Benchmark, TestRun, Job or ConfigMap remains; all six EBS volumes are gone. Original clusters arnaud, frederic and sylvain remain Running with 3/3 replicas each. See `cleanup-state.json` for post-cleanup Kubernetes and EBS state. Only explicit benchmark names and the dedicated Cluster are removed. All three original clusters are checked again at handoff.
