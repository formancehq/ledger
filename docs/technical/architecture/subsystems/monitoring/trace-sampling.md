# Trace sampling

## Requirement and scope

[EN-1588](https://formance-team.atlassian.net/browse/EN-1588) reports substantial
CPU, memory, and request-latency overhead when exporting telemetry at 8,000 TPS.
Ledger must preserve its existing cross-batch error discovery while avoiding
work proportional to every retained trace on every export batch. The local CPU
fix does not establish the ticket's end-to-end P99 target or eliminate its
memory cost. Collector rollout and a controlled load test are separate work.

## Local exporter contract

`tracesampling.Module` decorates the SDK exporter only when enabled. The default
is disabled. Enabled sampling records error trace IDs from status `ERROR`,
`error=true`, or span attributes named `exception.type` / `exception.message`.
It exports their pending spans and current siblings, and hash-samples other
traces using the configured success ratio. It does not inspect exception events.

Non-sampled spans are retained until 30 seconds after the first observed span
of their pending epoch. Error decisions expire 30 seconds after their first
observed error. Additional spans do not refresh either deadline. Expiration runs
before processing each non-empty export batch; idle exporters retain state until
another batch or shutdown. An error after pending expiry cannot recover earlier
spans. This is a bounded retention policy, not an unconditional guarantee of
complete long-running or distributed error traces. Each process sees only its
own exported spans.

The exporter holds its mutex while expiring entries, discovering errors, and
classifying the current batch. It releases the mutex before calling the delegate.
Pending spans selected for export are removed before that call; a delegate error
does not reinsert them. Shutdown clears local state and delegates shutdown after
attempting to export any pending spans already known to belong to errors. These
existing delivery semantics are unchanged by the CPU optimization.

## Expiration cost

Previously, every batch scanned both maps for expiry and scanned the entire
pending map again to discover which traces had become errors. With `P` pending
traces and `E` cached error IDs, this cost `O(P + E)` per batch even when nothing
expired and no error occurred. Increasing trace rate increased both the number
of batches and the size of those scans.

Now, error discovery looks up only the IDs in the current batch. Map insertions
are recorded in a FIFO of batches with their first-seen time and separate pending
and error ID slices. Time is captured under the mutex and lifetimes are fixed,
so expiry consumes only batches whose age is strictly greater than the window.
It never scans unexpired entries. Consumed slots are cleared to release their ID
slices; the queue is released when empty and on shutdown. Queue append cost is
amortized, and deletion work is proportional to expired insertions.

A pending trace promoted to an error is removed from the pending map immediately.
Its old pending ID may remain queued, but cannot delete a new pending epoch:
error membership prevents reinsertion until the later error deadline, by which
time the old pending deadline has already been consumed. Repeated errors do not
extend that deadline or enqueue duplicate IDs.

The queue stores IDs, not span pointers: about 16 bytes per recorded insertion,
plus slice capacity and batch overhead. A promoted trace has both pending and
error expiry records until the older one expires. Span retention and map
high-water capacity remain. With trace rate `R`, mean spans per trace `S`,
success ratio `q`, and window `W`, a short-lived, all-success steady workload can
retain approximately `(1-q) * R * S * W` spans. For 8,000 traces/s, `q=0.1`,
`W=30s`, this is 216,000 traces times `S`; TPS is not necessarily trace rate.
Disabling the wrapper removes this retention, but not SDK span recording,
snapshot allocation, serialization, or transport queues.

## Reproducing the local CPU experiment

`BenchmarkExportSpansPending` seeds the exporter through its public export path
with one span per non-sampled trace, then exports a fixed 512-span sampled batch
to a no-op delegate. The retained population stays constant. A one-hour window
prevents expiration during calibration. Setup allocations, SDK recording,
network export, and application latency are deliberately excluded.

```bash
bash scripts/agent-validation-env --ephemeral nix develop --command \
  go test ./internal/infra/monitoring/tracesampling -run '^$' \
  -bench BenchmarkExportSpansPending -benchmem -benchtime=200ms -count=3
```

For comparison, apply the benchmark file to base commit
`4d4798f8db4fdc11705313a210d042540322dfb5`, or use a Go `-overlay` replacing
`exporter.go` and `module_test.go` with their base contents and replacing the
new `exporter_expiry_test.go` with an empty `package tracesampling` file. Keep temporary copies and logs under `build/`. Do not compare different
workloads or use these batch timings as predictions of request P99.

A local run on Apple M5 Pro, darwin/arm64, Go 1.27.1 (three 200ms samples,
median time per 512-span batch) produced:

| Retained traces | Base | FIFO expiry + direct error lookup |
|---|---:|---:|
| 0 | 12.08 µs | 13.47 µs |
| 10,000 | 168.15 µs | 12.91 µs |
| 100,000 | 1,759.21 µs | 11.75 µs |
| 216,000 | 5,013.36 µs | 14.97 µs |

The host was shared with other validation jobs, so absolute timings are noisy.
The timed sampled batch allocates approximately 18,800 B in 10 allocations in
both versions. This excludes retention setup and does not claim an allocation
or retained-memory improvement. It demonstrates removal of the live-backlog
scan, not the ticket's application P99 acceptance criterion.

## Collector handoff and acceptance experiment

Use `TRACE_SAMPLING_ENABLED=false` / `--trace-sampling-enabled=false`, keep the
OTLP exporter enabled, and set `OTEL_TRACES_SAMPLER=always_on`. A success ratio of
`1` still installs the wrapper; disabling it removes the wrapper entirely.
Ledger's pinned SDK otherwise defaults to parent-based sampling, which can drop
spans under an incoming unsampled parent. `always_on` records Ledger spans even
then, but cannot recover spans already dropped by other services.

The existing Cluster API exposes `spec.monitoring.traces.sampling.enabled`; use
`spec.extraEnv` for the SDK sampler environment variable. The main Formance
Operator forwards the corresponding `LedgerConfiguration.spec.cluster` fields.
No new Ledger flag or Operator implementation is required for this handoff.
Verify the rendered pod configuration and deployed Operator version before use.

The external collector deployment must provide:

- An exhaustive input stream and a separate spanmetrics branch before tail
  sampling. Count each span once and preserve metric-stream identity across
  collector replicas. Sampling first biases counts, error rates, and latency
  distributions.
- Trace-ID routing so all spans of a trace reach the same tail-sampling instance,
  including spans from different Ledger nodes and upstream services.
- OR policies for error status, `error=true`, and presence of the exception span
  attributes supported locally, plus approximately 10% probabilistic success
  retention. Specify exception-event behavior deliberately.
- A pinned collector distribution/version with compatible policies and sizing
  for `decision_wait`, `num_traces`, expected trace rate, decision caches,
  maximum trace size, memory limiter, and export queues.
- A measured upper bound on span arrival skew and trace duration. A late error
  after a drop decision cannot recover earlier spans. For unbounded streams,
  explicitly retain that class or use bounded trace boundaries; a 30-second
  collector wait alone does not guarantee completeness.

Pin images, rendered configuration, workload, and collector topology. On fresh
equivalent `m8a.large` and `r8a.large` environments compare: telemetry disabled;
logs/metrics/profiling held fixed with traces disabled; local 10% sampler;
wrapper disabled with exhaustive export; and wrapper disabled with collector
tail sampling. Keep all other controls fixed, randomize/repeat run order, and
compare the two-minute warm-up and a long steady run at the same 8,000 TPS target.
Record achieved TPS and errors as well as P99: acceptance is no more than 10%
P99 overhead relative to the no-telemetry baseline, not merely equal offered TPS.

Measure Ledger CPU/RSS/heap/GC, throttling, Pebble compaction, SDK queue losses,
OTLP serialization/network volume, and collector CPU/memory/early trace drops,
late spans, export failures, and sampling-decision latency. Existing recording
and snapshot costs can remain significant after local sampling is disabled.

Inject traces with known span IDs: successes; child-first/parent-error-later;
error attributes; unsampled incoming parents; cross-node traces; errors before
and after the configured decision window; and long streams. Compare exact
exported span-ID sets for errors, the statistical success retention rate, and
pre-sampling metric counts/histograms against exhaustive input. Exercise queue
saturation, collector restart, and replica routing changes. Do not claim the
full ticket accepted until these external conditions and the P99 target pass.

Primary collector references:
[tail sampling](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/tailsamplingprocessor),
[spanmetrics](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/connector/spanmetricsconnector).
