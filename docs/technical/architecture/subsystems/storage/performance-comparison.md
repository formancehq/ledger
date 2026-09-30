# Pebble to RocksDB performance comparison

The `Storage Performance Comparison` workflow runs when its benchmark harness changes in a pull request. It builds test binaries from the Pebble baseline (`c6a99fc92f43f5127c898fd9a727adee3424310d`) and the PR head on one Linux runner, warms both, then alternates five measured runs per engine. The workflow uploads raw Go benchmark output, runner details, and a median summary.

The microbenchmark comparison covers warm DAL point lookups at 64, 512, and 4,096 bytes, small and large write batches, and one populated account reverse-map index lookup. The benchmarks use fresh stores for each case and the same source fixtures where possible. A positive RocksDB delta means slower `ns/op` for that case. These microbenchmarks do not establish HTTP latency or production capacity.

The workflow also runs a local single-node HTTP comparison on separate isolated runners at 30 writes/15 reads per second and 50 writes/25 reads per second. It builds both server binaries and alternates engine order over two runs each. Writes carry varied 4 KiB metadata. Each run has 20 seconds of warmup and 90 seconds of measurement against fresh data. The report includes p50/p95/p99 latency, successful request counts, failed and dropped iterations, and sampled process RSS. A run with errors or dropped iterations is invalid. The HTTP comparison is a limited signal: it does not measure three-node Raft capacity, the maximum sustainable rate, checkpoint pause time, or a production data distribution.

## Measured results

The [comparison run 36646581438](https://github.com/formancehq/ledger/actions/runs/36646581438) used Pebble baseline `c6a99fc92f43f5127c898fd9a727adee3424310d` and RocksDB commit `7c2143cb31006e8041ecb84c5b3591ba73b9e039`. The numbers below are the arithmetic mean of each engine's two measured runs; deltas are RocksDB relative to Pebble. Raw JSON, k6 output, and server logs are attached to the workflow run.

| Offered load | Metric | Pebble | RocksDB | Delta |
| --- | --- | ---: | ---: | ---: |
| 30 writes/s + 15 reads/s | Write p99 | 12.57 ms | 12.69 ms | +1.0% |
| 30 writes/s + 15 reads/s | Read p99 | 11.82 ms | 11.91 ms | +0.8% |
| 30 writes/s + 15 reads/s | Mean sampled RSS | 154.6 MiB | 141.7 MiB | -8.3% |
| 50 writes/s + 25 reads/s | Write p99 | 13.70 ms | 13.10 ms | -4.4% |
| 50 writes/s + 25 reads/s | Read p99 | 11.94 ms | 11.99 ms | +0.4% |
| 50 writes/s + 25 reads/s | Mean sampled RSS | 182.7 MiB | 154.3 MiB | -15.6% |

All eight measured runs completed without request errors or dropped iterations. Each 30/15 run completed roughly 2,700 writes and 1,350 reads; each 50/25 run completed roughly 4,500 writes and 2,250 reads. A previous [30/15 campaign](https://github.com/formancehq/ledger/actions/runs/36644657060) also had zero errors or drops and near-equal p99 latencies. The [100/50 campaign](https://github.com/formancehq/ledger/actions/runs/36645788901) saturated Pebble during warmup, so it provides no engine comparison. Two runs per engine are too few to claim statistical equivalence, and these offered rates do not establish either engine's maximum sustainable throughput.

The [comparison run 36652942406](https://github.com/formancehq/ledger/actions/runs/36652942406) repeated the same protocol on RocksDB commit `f554b70f87b1d19b49261767aa7e8094b2c58516`. It found a materially slower 30/15 latency result despite zero errors or dropped iterations. The table again uses the arithmetic mean of two measured runs per engine.

| Offered load | Metric | Pebble | RocksDB | Delta |
| --- | --- | ---: | ---: | ---: |
| 30 writes/s + 15 reads/s | Write p99 | 14.59 ms | 24.42 ms | +67.4% |
| 30 writes/s + 15 reads/s | Read p99 | 12.32 ms | 17.45 ms | +41.7% |
| 30 writes/s + 15 reads/s | Mean sampled RSS | 152.7 MiB | 140.3 MiB | -8.1% |
| 50 writes/s + 25 reads/s | Write p99 | 12.79 ms | 13.46 ms | +5.3% |
| 50 writes/s + 25 reads/s | Read p99 | 11.73 ms | 12.02 ms | +2.5% |
| 50 writes/s + 25 reads/s | Mean sampled RSS | 182.5 MiB | 154.4 MiB | -15.4% |

At 30/15, RocksDB's second measured write p99 was 32.30 ms versus 16.55 ms in its first run; its second warmup was already slower. The Pebble repeats were 15.54 and 13.64 ms. The available server logs show no write-stall or request-error explanation. This run cannot establish the cause or a stable engine penalty, but it rules out claiming latency equivalence from the earlier close results. Qualification needs more repetitions and CPU, I/O, and compaction tracing on the same runner before a performance acceptance decision.

The same run's storage microbenchmarks (five alternating samples per engine) found RocksDB 108% slower for 5-entry batches, 103% slower for 100-entry batches, and 66% slower for 1,000-entry batches. A 4 KiB point lookup was 24% slower, while 64- and 512-byte lookups were 12% and 17% faster, respectively; the populated account reverse-map lookup was 90% faster. These fixture-level results identify code paths to profile, but their ratios must not be applied to the HTTP results.

The [comparison run 36661203464](https://github.com/formancehq/ledger/actions/runs/36661203464) used RocksDB commit `59788cf7a5b69b3ac240fd414efcd2cc296642a6` and the same Pebble baseline. All eight measured HTTP runs again had zero request errors and dropped iterations. The two-run arithmetic means were:

| Offered load | Metric | Pebble | RocksDB | Delta |
| --- | --- | ---: | ---: | ---: |
| 30 writes/s + 15 reads/s | Write p99 | 13.64 ms | 14.83 ms | +8.8% |
| 30 writes/s + 15 reads/s | Read p99 | 12.06 ms | 12.45 ms | +3.3% |
| 30 writes/s + 15 reads/s | Mean sampled RSS | 152.5 MiB | 140.6 MiB | -7.8% |
| 50 writes/s + 25 reads/s | Write p99 | 12.24 ms | 14.06 ms | +14.9% |
| 50 writes/s + 25 reads/s | Read p99 | 11.62 ms | 12.16 ms | +4.6% |
| 50 writes/s + 25 reads/s | Mean sampled RSS | 182.7 MiB | 153.1 MiB | -16.2% |

On the same run's five-sample microbenchmarks, RocksDB batch writes were 121% slower for 5 entries, 81% slower for 100 entries, and 41% slower for 1,000 entries; a standalone batch commit was 51% slower. The HTTP write p99 varies materially between campaigns, while the batch microbenchmarks consistently favor Pebble. Neither observation justifies claiming performance equivalence or extrapolating a production throughput limit. Profile the batch path and repeat under a representative three-node load before setting an acceptance threshold.

To repeat on a Linux machine with the repository's Nix shell and both commits available:

```bash
PEBBLE_REF=c6a99fc92f43f5127c898fd9a727adee3424310d \
ROCKSDB_REF=<rocksdb-commit-sha> \
nix develop --command bash scripts/compare-storage-performance.sh
```

For an end-to-end qualification, run the existing `tests/perf` k6 workload against separately deployed three-node clusters built from the two pinned commits. Keep node type, CPU and memory limits, cache configuration, data size, replica count, request mix, and offered rate equal. Measure sustained throughput, p50/p95/p99 latency, error and dropped-iteration counts, aggregate CPU and RSS, disk growth, and checkpoint/restore pauses. Alternate run order and reject a run with request errors, dropped iterations, restarts, or resource throttling before interpreting an engine delta.
