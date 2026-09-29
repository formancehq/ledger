package dal

import (
	"context"
	"fmt"
	"math"

	"go.opentelemetry.io/otel/metric"
)

// RegisterMetrics samples RocksDB properties while holding the Store lifecycle
// lock. An unavailable property is omitted rather than reported as zero.
func (s *Store) RegisterMetrics(m metric.Meter) (metric.Registration, error) {
	type observation struct {
		property string
		gauge    metric.Int64ObservableGauge
	}
	observations := make([]observation, 0, len(rocksDBProperties))
	instruments := make([]metric.Observable, 0, len(rocksDBProperties))
	for _, property := range rocksDBProperties {
		gauge, err := m.Int64ObservableGauge(property.name,
			metric.WithDescription(property.description), metric.WithUnit(property.unit))
		if err != nil {
			return nil, fmt.Errorf("creating %s gauge: %w", property.name, err)
		}
		observations = append(observations, observation{property: property.property, gauge: gauge})
		instruments = append(instruments, gauge)
	}

	return m.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		s.dbMu.RLock()
		defer s.dbMu.RUnlock()
		db := s.getDB()
		if db == nil {
			return nil
		}
		raw := db.Raw()
		for _, item := range observations {
			if value, ok := raw.GetIntProperty(item.property); ok && value <= math.MaxInt64 {
				observer.ObserveInt64(item.gauge, int64(value))
			}
		}
		return nil
	}, instruments...)
}
