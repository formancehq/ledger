package tracing

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// MeterWithAttributes wraps a metric.Meter so that the given attributes are
// injected as *datapoint* attributes (i.e. real metric labels) on every
// measurement recorded by the synchronous instruments it creates. Caller-
// supplied attributes on each Record/Add are preserved and take precedence on
// key conflicts.
//
// Why this exists: OpenTelemetry instrumentation-scope attributes are dropped
// by the OTLP -> Prometheus/VictoriaMetrics translation (they are not promoted
// to labels). Carrying per-ledger identity only on the meter scope therefore
// collapsed every ledger's series into one, so a single time series ended up
// holding the conflicting cumulative values of many ledgers. VictoriaMetrics
// read the downward jumps as counter resets, which made rate() on cumulative
// series such as CreateTransaction_count produce garbage. Promoting the
// identity to a datapoint attribute turns it into a proper label, yielding one
// monotonic series per (ledger, operation).
//
// Coverage: all synchronous instruments (Int64/Float64 Counter,
// UpDownCounter, Histogram, Gauge) inject the attributes. Asynchronous
// (observable) instruments are NOT covered: their callbacks would have to apply
// the attributes at observation time, which this wrapper cannot do. If a future
// metric needs the labels on an observable instrument, attach them explicitly
// in its callback.
func MeterWithAttributes(meter metric.Meter, attrs ...attribute.KeyValue) metric.Meter {
	return attrMeter{
		Meter: meter,
		opt:   metric.WithAttributeSet(attribute.NewSet(attrs...)),
	}
}

// attrMeter embeds metric.Meter so it satisfies the full interface (including
// the embedded.Meter compatibility marker); it overrides the synchronous
// instrument constructors to wrap the returned instruments. Asynchronous
// constructors fall through to the wrapped meter unchanged.
type attrMeter struct {
	metric.Meter
	opt metric.MeasurementOption
}

func (m attrMeter) Int64Counter(name string, options ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	i, err := m.Meter.Int64Counter(name, options...)
	if err != nil {
		return i, err
	}
	return attrInt64Counter{Int64Counter: i, opt: m.opt}, nil
}

func (m attrMeter) Int64UpDownCounter(name string, options ...metric.Int64UpDownCounterOption) (metric.Int64UpDownCounter, error) {
	i, err := m.Meter.Int64UpDownCounter(name, options...)
	if err != nil {
		return i, err
	}
	return attrInt64UpDownCounter{Int64UpDownCounter: i, opt: m.opt}, nil
}

func (m attrMeter) Int64Histogram(name string, options ...metric.Int64HistogramOption) (metric.Int64Histogram, error) {
	i, err := m.Meter.Int64Histogram(name, options...)
	if err != nil {
		return i, err
	}
	return attrInt64Histogram{Int64Histogram: i, opt: m.opt}, nil
}

func (m attrMeter) Int64Gauge(name string, options ...metric.Int64GaugeOption) (metric.Int64Gauge, error) {
	i, err := m.Meter.Int64Gauge(name, options...)
	if err != nil {
		return i, err
	}
	return attrInt64Gauge{Int64Gauge: i, opt: m.opt}, nil
}

func (m attrMeter) Float64Counter(name string, options ...metric.Float64CounterOption) (metric.Float64Counter, error) {
	i, err := m.Meter.Float64Counter(name, options...)
	if err != nil {
		return i, err
	}
	return attrFloat64Counter{Float64Counter: i, opt: m.opt}, nil
}

func (m attrMeter) Float64UpDownCounter(name string, options ...metric.Float64UpDownCounterOption) (metric.Float64UpDownCounter, error) {
	i, err := m.Meter.Float64UpDownCounter(name, options...)
	if err != nil {
		return i, err
	}
	return attrFloat64UpDownCounter{Float64UpDownCounter: i, opt: m.opt}, nil
}

func (m attrMeter) Float64Histogram(name string, options ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	i, err := m.Meter.Float64Histogram(name, options...)
	if err != nil {
		return i, err
	}
	return attrFloat64Histogram{Float64Histogram: i, opt: m.opt}, nil
}

func (m attrMeter) Float64Gauge(name string, options ...metric.Float64GaugeOption) (metric.Float64Gauge, error) {
	i, err := m.Meter.Float64Gauge(name, options...)
	if err != nil {
		return i, err
	}
	return attrFloat64Gauge{Float64Gauge: i, opt: m.opt}, nil
}

// recordOpts prepends the injected option so caller options (applied later)
// win on key conflicts. The len==0 fast path avoids an allocation on the hot
// path, which is the common case for these instruments.
func recordOpts(opt metric.MeasurementOption, options []metric.RecordOption) []metric.RecordOption {
	if len(options) == 0 {
		return []metric.RecordOption{opt}
	}
	return append([]metric.RecordOption{opt}, options...)
}

func addOpts(opt metric.MeasurementOption, options []metric.AddOption) []metric.AddOption {
	if len(options) == 0 {
		return []metric.AddOption{opt}
	}
	return append([]metric.AddOption{opt}, options...)
}

type attrInt64Counter struct {
	metric.Int64Counter
	opt metric.MeasurementOption
}

func (c attrInt64Counter) Add(ctx context.Context, incr int64, options ...metric.AddOption) {
	c.Int64Counter.Add(ctx, incr, addOpts(c.opt, options)...)
}

type attrInt64UpDownCounter struct {
	metric.Int64UpDownCounter
	opt metric.MeasurementOption
}

func (c attrInt64UpDownCounter) Add(ctx context.Context, incr int64, options ...metric.AddOption) {
	c.Int64UpDownCounter.Add(ctx, incr, addOpts(c.opt, options)...)
}

type attrInt64Histogram struct {
	metric.Int64Histogram
	opt metric.MeasurementOption
}

func (h attrInt64Histogram) Record(ctx context.Context, incr int64, options ...metric.RecordOption) {
	h.Int64Histogram.Record(ctx, incr, recordOpts(h.opt, options)...)
}

type attrInt64Gauge struct {
	metric.Int64Gauge
	opt metric.MeasurementOption
}

func (g attrInt64Gauge) Record(ctx context.Context, value int64, options ...metric.RecordOption) {
	g.Int64Gauge.Record(ctx, value, recordOpts(g.opt, options)...)
}

type attrFloat64Counter struct {
	metric.Float64Counter
	opt metric.MeasurementOption
}

func (c attrFloat64Counter) Add(ctx context.Context, incr float64, options ...metric.AddOption) {
	c.Float64Counter.Add(ctx, incr, addOpts(c.opt, options)...)
}

type attrFloat64UpDownCounter struct {
	metric.Float64UpDownCounter
	opt metric.MeasurementOption
}

func (c attrFloat64UpDownCounter) Add(ctx context.Context, incr float64, options ...metric.AddOption) {
	c.Float64UpDownCounter.Add(ctx, incr, addOpts(c.opt, options)...)
}

type attrFloat64Histogram struct {
	metric.Float64Histogram
	opt metric.MeasurementOption
}

func (h attrFloat64Histogram) Record(ctx context.Context, incr float64, options ...metric.RecordOption) {
	h.Float64Histogram.Record(ctx, incr, recordOpts(h.opt, options)...)
}

type attrFloat64Gauge struct {
	metric.Float64Gauge
	opt metric.MeasurementOption
}

func (g attrFloat64Gauge) Record(ctx context.Context, value float64, options ...metric.RecordOption) {
	g.Float64Gauge.Record(ctx, value, recordOpts(g.opt, options)...)
}
