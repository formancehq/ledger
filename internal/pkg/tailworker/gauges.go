package tailworker

import (
	"context"
	"fmt"
	"sync/atomic"

	"go.opentelemetry.io/otel/metric"
)

// computeLag returns how far indexed trails sourceLast, clamped at 0 so a
// momentarily-ahead indexed cursor never reports a negative lag.
func computeLag(indexed, sourceLast uint64) int64 {
	return max(int64(sourceLast)-int64(indexed), 0)
}

// RegisterTailGauges registers the standard tail-worker progress gauges under
// the given namespace and returns the registration (Unregister on Stop):
//
//	{ns}.last_indexed_sequence   - indexed.Load()
//	{ns}.{source}_last_sequence  - sourceLast.Load()
//	{ns}.lag                     - max(sourceLast-indexed, 0)
//
// source names the upstream sequence (e.g. "audit", "pebble") so the middle
// gauge preserves each worker's established metric name.
func RegisterTailGauges(meter metric.Meter, ns, source string, indexed, sourceLast *atomic.Uint64) (metric.Registration, error) {
	indexedGauge, err := meter.Int64ObservableGauge(ns+".last_indexed_sequence",
		metric.WithDescription("Last sequence indexed"))
	if err != nil {
		return nil, err
	}

	sourceGauge, err := meter.Int64ObservableGauge(fmt.Sprintf("%s.%s_last_sequence", ns, source),
		metric.WithDescription("Last sequence available upstream"))
	if err != nil {
		return nil, err
	}

	lagGauge, err := meter.Int64ObservableGauge(ns+".lag",
		metric.WithDescription("Sequences the worker is behind upstream"))
	if err != nil {
		return nil, err
	}

	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		idx := indexed.Load()
		src := sourceLast.Load()
		o.ObserveInt64(indexedGauge, int64(idx))
		o.ObserveInt64(sourceGauge, int64(src))
		o.ObserveInt64(lagGauge, computeLag(idx, src))

		return nil
	}, indexedGauge, sourceGauge, lagGauge)
}
