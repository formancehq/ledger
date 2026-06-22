package metrics

import "go.opentelemetry.io/otel/metric"

// renamingMeter forwards every call to its embedded metric.Meter
// after rewriting the instrument name through [transformName]. The
// embedded metric.Meter pulls in all forward-compatible methods
// (RegisterCallback, observers, etc.) so we only override the
// synchronous and observable instrument constructors.
//
// The wrapper is created once per meter (at instrument-registration
// time); after that the recorded values are written directly to the
// underlying instrument with no extra indirection.
type renamingMeter struct {
	metric.Meter

	naming Naming
}

func (m *renamingMeter) rewrite(name string) string {
	return transformName(name, m.naming)
}

func (m *renamingMeter) Int64Counter(name string, opts ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return m.Meter.Int64Counter(m.rewrite(name), opts...)
}

func (m *renamingMeter) Int64UpDownCounter(name string, opts ...metric.Int64UpDownCounterOption) (metric.Int64UpDownCounter, error) {
	return m.Meter.Int64UpDownCounter(m.rewrite(name), opts...)
}

func (m *renamingMeter) Int64Histogram(name string, opts ...metric.Int64HistogramOption) (metric.Int64Histogram, error) {
	return m.Meter.Int64Histogram(m.rewrite(name), opts...)
}

func (m *renamingMeter) Int64Gauge(name string, opts ...metric.Int64GaugeOption) (metric.Int64Gauge, error) {
	return m.Meter.Int64Gauge(m.rewrite(name), opts...)
}

func (m *renamingMeter) Int64ObservableCounter(name string, opts ...metric.Int64ObservableCounterOption) (metric.Int64ObservableCounter, error) {
	return m.Meter.Int64ObservableCounter(m.rewrite(name), opts...)
}

func (m *renamingMeter) Int64ObservableUpDownCounter(name string, opts ...metric.Int64ObservableUpDownCounterOption) (metric.Int64ObservableUpDownCounter, error) {
	return m.Meter.Int64ObservableUpDownCounter(m.rewrite(name), opts...)
}

func (m *renamingMeter) Int64ObservableGauge(name string, opts ...metric.Int64ObservableGaugeOption) (metric.Int64ObservableGauge, error) {
	return m.Meter.Int64ObservableGauge(m.rewrite(name), opts...)
}

func (m *renamingMeter) Float64Counter(name string, opts ...metric.Float64CounterOption) (metric.Float64Counter, error) {
	return m.Meter.Float64Counter(m.rewrite(name), opts...)
}

func (m *renamingMeter) Float64UpDownCounter(name string, opts ...metric.Float64UpDownCounterOption) (metric.Float64UpDownCounter, error) {
	return m.Meter.Float64UpDownCounter(m.rewrite(name), opts...)
}

func (m *renamingMeter) Float64Histogram(name string, opts ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	return m.Meter.Float64Histogram(m.rewrite(name), opts...)
}

func (m *renamingMeter) Float64Gauge(name string, opts ...metric.Float64GaugeOption) (metric.Float64Gauge, error) {
	return m.Meter.Float64Gauge(m.rewrite(name), opts...)
}

func (m *renamingMeter) Float64ObservableCounter(name string, opts ...metric.Float64ObservableCounterOption) (metric.Float64ObservableCounter, error) {
	return m.Meter.Float64ObservableCounter(m.rewrite(name), opts...)
}

func (m *renamingMeter) Float64ObservableUpDownCounter(name string, opts ...metric.Float64ObservableUpDownCounterOption) (metric.Float64ObservableUpDownCounter, error) {
	return m.Meter.Float64ObservableUpDownCounter(m.rewrite(name), opts...)
}

func (m *renamingMeter) Float64ObservableGauge(name string, opts ...metric.Float64ObservableGaugeOption) (metric.Float64ObservableGauge, error) {
	return m.Meter.Float64ObservableGauge(m.rewrite(name), opts...)
}
