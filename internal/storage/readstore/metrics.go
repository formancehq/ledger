package readstore

import (
	"context"
	"fmt"
	"math"

	"go.opentelemetry.io/otel/metric"
)

// RegisterMetrics samples the available RocksDB read-index properties.
func (s *Store) RegisterMetrics(m metric.Meter) (metric.Registration, error) {
	type property struct {
		name, description, key string
		gauge                  metric.Int64ObservableGauge
	}
	specs := []property{
		{name: "readindex.memtable.bytes", description: "Current memtable size in bytes", key: "rocksdb.cur-size-all-mem-tables"},
		{name: "readindex.cache.bytes", description: "Block cache usage in bytes", key: "rocksdb.block-cache-usage"},
		{name: "readindex.compaction.pending.bytes", description: "Estimated pending compaction bytes", key: "rocksdb.estimate-pending-compaction-bytes"},
	}
	instruments := make([]metric.Observable, 0, len(specs))
	for i := range specs {
		gauge, err := m.Int64ObservableGauge(specs[i].name, metric.WithDescription(specs[i].description), metric.WithUnit("By"))
		if err != nil {
			return nil, fmt.Errorf("creating %s gauge: %w", specs[i].name, err)
		}
		specs[i].gauge = gauge
		instruments = append(instruments, gauge)
	}
	return m.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		for _, spec := range specs {
			if value, ok := s.db.Raw().GetIntProperty(spec.key); ok && value <= math.MaxInt64 {
				observer.ObserveInt64(spec.gauge, int64(value))
			}
		}
		return nil
	}, instruments...)
}
