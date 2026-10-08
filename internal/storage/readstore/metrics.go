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
		"readindex.level.size",
		metric.WithDescription("Total bytes in each Pebble level"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating readindex.level.size gauge: %w", err)
	}

	memtableBytes, err := m.Int64ObservableGauge(
		"readindex.memtable.size",
		metric.WithDescription("Current memtable size in bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating readindex.memtable.size gauge: %w", err)
	}

	cacheHits, err := m.Int64ObservableCounter(
		"readindex.cache.hits",
		metric.WithDescription("Block cache hits"),
		metric.WithUnit("{hit}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating readindex.cache.hits counter: %w", err)
	}

	cacheMisses, err := m.Int64ObservableCounter(
		"readindex.cache.misses",
		metric.WithDescription("Block cache misses"),
		metric.WithUnit("{miss}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating readindex.cache.misses counter: %w", err)
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
