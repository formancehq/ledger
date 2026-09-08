# Trace sampling

## Requirement and decision

[EN-1588](https://formance-team.atlassian.net/browse/EN-1588) reports substantial
CPU, memory, and request-latency overhead when exporting telemetry at 8,000 TPS.
The former `ErrorAwareSamplingExporter` kept non-sampled spans in Ledger for a
30-second error-discovery window and scanned retained traces on each export.
Optimizing those scans reduced CPU cost but retained the application-local
buffer and duplicated sampling configuration between Ledger and the SDK.

Ledger now uses the standard SDK exporter directly. The custom exporter,
retention state, flags, and Operator `monitoring.traces.sampling` configuration
have been removed. Tail-sampling retention belongs to the collector. This
removes the local sampler's CPU and memory cost, but does not eliminate SDK
recording/snapshot allocation, batching, serialization, or transport overhead.
The ticket's end-to-end P99 and error-completeness targets still require the
collector rollout and controlled load tests described below.

## SDK and error signals

`cmd/server/server.go` configures tracing through
`observefx.TracesModuleFromFlags`, with no Ledger sampling decorator. The
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

## Deployment configuration

The Cluster API uses `spec.monitoring.traces` for OTLP export and `spec.extraEnv`
for `OTEL_TRACES_SAMPLER`. The main Formance Operator forwards the corresponding
`LedgerConfiguration.spec.cluster` fields. See the concrete
[deployment example](../../../../ops/deployment.md#collector-side-trace-sampling).

Remove the former Ledger sampling flags, custom environment variables, and
`monitoring.traces.sampling` objects from deployment configuration. Install the
matching Operator/CRDs. The standard SDK variable is now the only sampling
control needed inside Ledger for the collector path; no replacement custom flag
or compatibility layer is provided for unreleased v3 configuration.

When the main Formance Operator bumps its dependency on this module, its tests
that construct/assert the removed `TracesConfig.Sampling` field must be updated.
Regenerate its `LedgerConfiguration` CRD/Helm schema and copied Cluster CRD test
fixtures from the new shared types. Those cross-repository changes are separate
from the embedded Operator/API cleanup in this repository.

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
