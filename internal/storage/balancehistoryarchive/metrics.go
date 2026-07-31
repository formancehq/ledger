package balancehistoryarchive

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/metric"
)

type archiveMetrics struct {
	cacheHits     metric.Int64Counter
	cacheMisses   metric.Int64Counter
	fetchDuration metric.Int64Histogram
	registration  metric.Registration
}

func newArchiveMetrics(meter metric.Meter, cache *cache) (*archiveMetrics, error) {
	cacheHits, err := meter.Int64Counter(
		"balancehistory.archive.cache.hits",
		metric.WithDescription("Verified local balance-history archive cache hits"),
		metric.WithUnit("{hit}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history archive cache hit counter: %w", err)
	}

	cacheMisses, err := meter.Int64Counter(
		"balancehistory.archive.cache.misses",
		metric.WithDescription("Balance-history archive lookups requiring cold fetch coordination"),
		metric.WithUnit("{miss}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history archive cache miss counter: %w", err)
	}

	fetchDuration, err := meter.Int64Histogram(
		"balancehistory.archive.fetch.duration",
		metric.WithDescription("Cold balance-history archive fetch duration"),
		metric.WithUnit("us"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history archive fetch duration histogram: %w", err)
	}

	cacheBytes, err := meter.Int64ObservableGauge(
		"balancehistory.archive.cache.bytes",
		metric.WithDescription("Bytes currently retained in the local balance-history archive cache"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history archive cache bytes gauge: %w", err)
	}

	registration, err := meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		observer.ObserveInt64(cacheBytes, cache.stats().Bytes)

		return nil
	}, cacheBytes)
	if err != nil {
		return nil, fmt.Errorf("registering balance history archive cache metrics: %w", err)
	}

	return &archiveMetrics{
		cacheHits:     cacheHits,
		cacheMisses:   cacheMisses,
		fetchDuration: fetchDuration,
		registration:  registration,
	}, nil
}

func (m *archiveMetrics) recordLookup(ctx context.Context, hit bool) {
	if hit {
		m.cacheHits.Add(ctx, 1)

		return
	}

	m.cacheMisses.Add(ctx, 1)
}

func (m *archiveMetrics) recordFetch(ctx context.Context, started time.Time) {
	m.fetchDuration.Record(ctx, time.Since(started).Microseconds())
}

func (m *archiveMetrics) close() error {
	return m.registration.Unregister()
}
