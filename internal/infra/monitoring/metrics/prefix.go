// Package metrics holds the ledger's metric naming defaults.
//
// Instrument names are namespaced by go-libs: the metrics module wraps the
// injected metric.MeterProvider with metrics.NewPrefixedMeterProvider using
// the --otel-metrics-prefix flag (env OTEL_METRICS_PREFIX). The global
// MeterProvider stays the raw SDK provider, so OpenTelemetry
// semantic-convention instrumentation (go.*, process.*, system.*, http.*,
// rpc.*) keeps its upstream names.
package metrics

// DefaultPrefix is the ledger's default for --otel-metrics-prefix. Following
// the OpenTelemetry naming recommendation for application-specific names
// (https://opentelemetry.io/docs/specs/semconv/general/naming/), it groups
// the ledger's own metrics (raft.*, cache.*, wal.*, …) under one namespace
// when several services share the same metrics backend. The dashboard
// generator in misc/devenv/monitoring-dashboards mirrors it; see
// TestNamingPolicyMatchesDashboards.
const DefaultPrefix = "formance.ledger"
