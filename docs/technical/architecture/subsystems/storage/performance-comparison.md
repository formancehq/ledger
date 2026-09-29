# Pebble to RocksDB performance comparison

The `Storage Performance Comparison` workflow runs when its benchmark harness changes in a pull request. It builds test binaries from the Pebble baseline (`c6a99fc92f43f5127c898fd9a727adee3424310d`) and the PR head on one Linux runner, warms both, then alternates five measured runs per engine. The workflow uploads raw Go benchmark output, runner details, and a median summary.

The microbenchmark comparison covers warm DAL point lookups at 64, 512, and 4,096 bytes, small and large write batches, and one populated account reverse-map index lookup. The benchmarks use fresh stores for each case and the same source fixtures where possible. A positive RocksDB delta means slower `ns/op` for that case. These microbenchmarks do not establish HTTP latency or production capacity.

The workflow also runs a local single-node HTTP comparison on separate isolated runners at 30 writes/15 reads per second and 50 writes/25 reads per second. It builds both server binaries and alternates engine order over two runs each. Writes carry varied 4 KiB metadata. Each run has 20 seconds of warmup and 90 seconds of measurement against fresh data. The report includes p50/p95/p99 latency, successful request counts, failed and dropped iterations, and sampled process RSS. A run with errors or dropped iterations is invalid. The HTTP comparison is a limited signal: it does not measure three-node Raft capacity, the maximum sustainable rate, checkpoint pause time, or a production data distribution.

To repeat on a Linux machine with the repository's Nix shell and both commits available:

```bash
PEBBLE_REF=c6a99fc92f43f5127c898fd9a727adee3424310d \
ROCKSDB_REF=<rocksdb-commit-sha> \
nix develop --command bash scripts/compare-storage-performance.sh
```

For an end-to-end qualification, run the existing `tests/perf` k6 workload against separately deployed three-node clusters built from the two pinned commits. Keep node type, CPU and memory limits, cache configuration, data size, replica count, request mix, and offered rate equal. Measure sustained throughput, p50/p95/p99 latency, error and dropped-iteration counts, aggregate CPU and RSS, disk growth, and checkpoint/restore pauses. Alternate run order and reject a run with request errors, dropped iterations, restarts, or resource throttling before interpreting an engine delta.
