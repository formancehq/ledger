package metrics

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"
)

// Factory is a metric.MeterProvider decorator that applies a [Naming]
// policy and a namespace prefix to every instrument created via meters
// it hands out.
//
// OpenTelemetry auto-instrumentation (Go runtime, process, host,
// HTTP) targets the *global* MeterProvider, which is left as the raw
// SDK provider — those metrics therefore bypass this factory and
// keep their canonical semconv names regardless of mode or prefix.
// The flags only affect metrics our own code emits via the injected
// meter provider.
type Factory struct {
	embedded.MeterProvider

	inner  metric.MeterProvider
	naming Naming
	prefix string
}

// NewFactory wraps inner with the given naming policy and prefix. An
// empty prefix disables the namespace; see [ParsePrefix].
func NewFactory(inner metric.MeterProvider, naming Naming, prefix string) *Factory {
	return &Factory{inner: inner, naming: naming, prefix: prefix}
}

// Meter returns a meter that rewrites instrument names according to
// the factory's policy. When the policy is the identity (NamingOTel
// without prefix) the upstream meter is returned directly.
func (f *Factory) Meter(name string, opts ...metric.MeterOption) metric.Meter {
	inner := f.inner.Meter(name, opts...)
	if f.naming == NamingOTel && f.prefix == "" {
		return inner
	}

	return &renamingMeter{Meter: inner, naming: f.naming, prefix: f.prefix}
}
