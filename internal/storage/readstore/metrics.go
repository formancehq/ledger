package readstore

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RegisterMetrics registers Pebble internal metrics with the given meter.
func (s *Store) RegisterMetrics(m metric.Meter) (metric.Registration, error) {
	levelBytes, err := m.Int64ObservableGauge(
		"readindex.level.bytes",
		metric.WithDescription("Total bytes in each Pebble level"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating readindex.level.bytes gauge: %w", err)
	}

	memtableBytes, err := m.Int64ObservableGauge(
		"readindex.memtable.bytes",
		metric.WithDescription("Current memtable size in bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating readindex.memtable.bytes gauge: %w", err)
	}

	cacheHits, err := m.Int64ObservableGauge(
		"readindex.cache.hits",
		metric.WithDescription("Block cache hits"),
		metric.WithUnit("{hits}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating readindex.cache.hits gauge: %w", err)
	}

	cacheMisses, err := m.Int64ObservableGauge(
		"readindex.cache.misses",
		metric.WithDescription("Block cache misses"),
		metric.WithUnit("{misses}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating readindex.cache.misses gauge: %w", err)
	}

	return m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		metrics := s.db.Metrics()

		// Per-level sizes.
		for i, level := range metrics.Levels {
			o.ObserveInt64(levelBytes, level.TablesSize,
				metric.WithAttributes(attribute.Int("level", i)))
		}

		o.ObserveInt64(memtableBytes, int64(metrics.MemTable.Size))
		o.ObserveInt64(cacheHits, metrics.BlockCache.Hits)
		o.ObserveInt64(cacheMisses, metrics.BlockCache.Misses)

		return nil
	}, levelBytes, memtableBytes, cacheHits, cacheMisses)
}
