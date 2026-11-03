# Trace sampling

## Requirement and decision

[EN-1588](https://formance-team.atlassian.net/browse/EN-1588) reports substantial
CPU, memory, and request-latency overhead when exporting telemetry at 8,000 TPS.
The former `ErrorAwareSamplingExporter` kept non-sampled spans in Ledger for a
30-second error-discovery window and scanned retained traces on each export.
Optimizing those scans reduced CPU cost but retained the application-local
buffer and duplicated sampling configuration between Ledger and the SDK.

Ledger now uses the standard SDK exporter directly. The custom exporter,
retention state, legacy flags, and Operator `monitoring.traces.sampling` configuration
have been removed. Tail-sampling retention belongs to the collector. This
removes the local sampler's CPU and memory cost, but does not eliminate SDK
recording/snapshot allocation, batching, serialization, or transport overhead.
The ticket's end-to-end P99 and error-completeness targets still require the
collector rollout and controlled load tests described below.

## SDK and error signals

`cmd/server/server.go` configures tracing through
`tracesModuleFromFlags`, which composes `observefx.TracesModuleFromFlags` with
an optional standard SDK `WithSampler` provider option. There is no Ledger
sampling decorator. The
OpenTelemetry SDK creates and exports batches according to its standard configuration.
To send the complete Ledger span stream, keep the OTLP exporter enabled and set:

```bash
OTEL_TRACES_SAMPLER=always_on
```

The pinned SDK otherwise defaults to parent-based sampling, which can drop spans
under an incoming unsampled parent. `always_on` records Ledger spans even then,
while preserving trace IDs and parent relationships. It cannot recover spans
already dropped by upstream services. Other standard SDK sampling policies remain
available, but sampling before export deprives the collector of those spans and
biases metrics derived from them.

Error instrumentation is preserved. For example, `auth.logAuthFailure` still
records the error status, authentication attributes, and an exception event.
Children can reach the collector in earlier batches before a parent ends with an
error; Ledger does not withhold or retrospectively filter them. Collector policies
must inspect the emitted signal locations: exception events are distinct from
span-level `exception.type` / `exception.message` attributes.

SDK/exporter queues and retry/shutdown behavior still apply. Queue saturation,
transport failure, process crashes, and upstream sampling can lose spans. Removing
local sampling is not an exactly-once delivery guarantee or a guarantee of
complete distributed traces in the presence of those losses.

`TestAuthFailureExportsChildrenBeforeParentError` exercises the standard batch
export path with an unsampled incoming parent: the child is flushed before the
error exists, then the parent is exported with error status, attributes, exception
event, and intact trace/parent IDs. The CLI test rejects the removed flags, and
Operator tests verify standard sampler configuration passes through `extraEnv`.

## First-class configuration and deployment

Operators need to bound trace volume before export without a custom retention
mechanism. `spec.monitoring.traces.sampler` selects a built-in SDK policy and
`samplerArg` carries its string argument to `OTEL_TRACES_SAMPLER_ARG`. Supported
policies are `always_on`, `always_off`, `traceidratio`, `parentbased_always_on`,
`parentbased_always_off`, and `parentbased_traceidratio`.

Typed ratio policies require an explicit finite probability in `[0,1]`; other
policies reject arguments. The schema and controller validate these settings.
Controller validation rejects combining typed sampling with raw
`OTEL_TRACES_SAMPLER*` variables or the server's `TRACE_SAMPLER*` aliases in
`extraEnv`, including indirect Secret/ConfigMap sources, before
changing workloads. A corrected spec resumes reconciliation. When the typed
fields are omitted, raw values pass through and the SDK owns parsing/fallback.
No typed default is materialized, preserving existing deployments and the
`parentbased_always_on` SDK default.

Standalone server `--trace-sampler` / `--trace-sampler-arg` flags (also bound from
`TRACE_SAMPLER` / `TRACE_SAMPLER_ARG`) apply the same strict validation. CLI
values override their corresponding typed environment values. Selecting either
typed setting replaces the entire raw SDK sampler pair via `WithSampler`;
with neither selected, no sampler option is supplied and the SDK reads the raw
`OTEL_TRACES_SAMPLER*` variables. This preserves raw-only SDK fallback behavior,
including the missing ratio argument default of `1.0`. No environment mutation,
local sampler implementation, span retention, or per-export trace scan is added.
Configuration/validation work is bounded at startup; sampling cost is that of
the SDK's built-in sampler per span. No business state, Raft apply, stored
format, service protocol revision or published RPC is changed.

`TestServerTraceSamplerSDK` exercises actual Fx provider construction, recording
and export for all six policies, raw environment fallback, flag precedence,
parent sampling decisions and deterministic fractional trace IDs. Operator tests
cover rendering, invalid specs, conflicts and real API-server schema admission.
The existing child-before-parent-error regression continues to exercise the
standard exporter without retention.

The former error-aware flags and nested `monitoring.traces.sampling` object
remain unsupported. Install matching Operator/Cluster CRDs and use the
[deployment examples](../../../../ops/deployment.md#collector-side-trace-sampling).
The main Formance Operator must bump the shared API dependency and regenerate
its `LedgerConfiguration` CRD/schema to expose the typed fields under
`spec.cluster`; that cross-repository rollout is separate.

This repository does not deploy the production collector pipeline. Verify its
version, rendered configuration, routing, capacity, and policies before relying
on collector-side error/success retention. Sending all Ledger spans without
collector sampling increases the downstream trace volume.

## Collector responsibilities

The collector deployment must provide:

- An exhaustive input stream and a separate spanmetrics branch before tail
  sampling. Count each span once and preserve metric-stream identity across
  replicas. Sampling first biases counts, error rates, and latency distributions.
- Trace-ID routing so all spans of a trace reach the same tail-sampling instance,
  including spans from different Ledger nodes and upstream services.
- OR policies for error status, `error=true`, and exception information, plus
  approximately 10% probabilistic success retention. Include exception events
  and span attributes deliberately; a status-only policy does not cover every
  error representation.
- A pinned collector distribution/version and sizing for `decision_wait`,
  `num_traces`, expected trace rate, decision caches, maximum trace size, memory
  limiter, and export queues.
- A measured upper bound on span arrival skew and trace duration. A late error
  after a drop decision cannot recover earlier spans. For unbounded streams,
  explicitly retain that class or use bounded trace boundaries; a finite wait
  alone does not guarantee completeness.

## Acceptance experiment

Pin images, rendered configuration, workload, and collector topology. On fresh
equivalent `m8a.large` and `r8a.large` environments compare: telemetry disabled;
logs/metrics/profiling held fixed with traces disabled; exhaustive OTLP trace
export; and exhaustive export with collector tail sampling. Historical local
sampler measurements and their exact source baseline are recorded in EN-1588.
Keep other controls fixed, randomize/repeat run order, and compare the two-minute
warm-up and a long steady run at the same 8,000 TPS target.

Record achieved TPS and errors as well as P99: acceptance is no more than 10%
P99 overhead relative to the no-telemetry baseline, not merely equal offered TPS.
Measure Ledger CPU/RSS/heap/GC, throttling, Pebble compaction, SDK queue losses,
OTLP serialization/network volume, and collector CPU/memory/early trace drops,
late spans, export failures, and sampling-decision latency. Existing recording
and snapshot costs can remain significant after local sampling is removed.

Inject known span IDs for successes, child-first/parent-error-later, error
attributes/events, unsampled incoming parents, cross-node traces, errors before
and after the configured decision window, and long streams. Compare exact
exported span-ID sets for errors, the statistical success retention rate, and
pre-sampling metric counts/histograms against exhaustive input. Exercise queue
saturation, collector restart, and replica routing changes. Do not mark the full
ticket accepted until these external conditions and the P99 target pass.

Primary collector references:
[tail sampling](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/tailsamplingprocessor),
[spanmetrics](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/connector/spanmetricsconnector).
