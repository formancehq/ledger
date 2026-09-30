package usagestore

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RegisterMetrics registers RocksDB metrics for the usage store.
// Mirrors readstore.Store.RegisterMetrics with a "usagestore." namespace so
// operators can see LSM state, cache hit rates, and memtable pressure per
// secondary store.
func (s *Store) RegisterMetrics(m metric.Meter) (metric.Registration, error) {
	levelBytes, err := m.Int64ObservableGauge(
		"usagestore.level.bytes",
		metric.WithDescription("Total bytes in each RocksDB level"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating usagestore.level.bytes gauge: %w", err)
	}

	memtableBytes, err := m.Int64ObservableGauge(
		"usagestore.memtable.bytes",
		metric.WithDescription("Current memtable size in bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating usagestore.memtable.bytes gauge: %w", err)
	}

	cacheHits, err := m.Int64ObservableGauge(
		"usagestore.cache.hits",
		metric.WithDescription("Block cache hits"),
		metric.WithUnit("{hits}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating usagestore.cache.hits gauge: %w", err)
	}

	cacheMisses, err := m.Int64ObservableGauge(
		"usagestore.cache.misses",
		metric.WithDescription("Block cache misses"),
		metric.WithUnit("{misses}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating usagestore.cache.misses gauge: %w", err)
	}

	return m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		levelSizes := map[int]int64{}
		for _, file := range s.db.GetLiveFilesMetaData() {
			levelSizes[file.Level] += file.Size
		}
		for level, size := range levelSizes {
			o.ObserveInt64(levelBytes, size, metric.WithAttributes(attribute.Int("level", level)))
		}
		if size, ok := s.db.GetIntProperty("rocksdb.cur-size-all-mem-tables"); ok {
			o.ObserveInt64(memtableBytes, int64(size))
		}
		if hits, ok := s.db.GetIntProperty("rocksdb.block-cache-hit"); ok {
			o.ObserveInt64(cacheHits, int64(hits))
		}
		if misses, ok := s.db.GetIntProperty("rocksdb.block-cache-miss"); ok {
			o.ObserveInt64(cacheMisses, int64(misses))
		}

		return nil
	}, levelBytes, memtableBytes, cacheHits, cacheMisses)
}
