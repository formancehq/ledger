package readstore

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RegisterMetrics registers bbolt internal metrics with the given meter.
// The returned Registration must be unregistered when the store is closed
// to avoid a metric callback referencing a closed database.
func (s *Store) RegisterMetrics(m metric.Meter) (metric.Registration, error) {
	freelistPages, err := m.Int64ObservableGauge(
		"readindex.freelist.pages",
		metric.WithDescription("Number of free and pending-free pages in the bbolt freelist"),
		metric.WithUnit("{pages}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating readindex.freelist.pages gauge: %w", err)
	}

	freelistBytes, err := m.Int64ObservableGauge(
		"readindex.freelist.bytes",
		metric.WithDescription("Total bytes allocated in free pages"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating readindex.freelist.bytes gauge: %w", err)
	}

	txPageReads, err := m.Int64ObservableGauge(
		"readindex.tx.page_reads_total",
		metric.WithDescription("Cumulative number of page reads across all transactions"),
		metric.WithUnit("{pages}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating readindex.tx.page_reads_total gauge: %w", err)
	}

	txPageWrites, err := m.Int64ObservableGauge(
		"readindex.tx.page_writes_total",
		metric.WithDescription("Cumulative number of page writes across all transactions"),
		metric.WithUnit("{pages}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating readindex.tx.page_writes_total gauge: %w", err)
	}

	return m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		stats := s.db.Stats()
		pageSize := int64(s.db.Info().PageSize)

		o.ObserveInt64(freelistPages, int64(stats.FreePageN), metric.WithAttributes(attribute.String("type", "free")))
		o.ObserveInt64(freelistPages, int64(stats.PendingPageN), metric.WithAttributes(attribute.String("type", "pending")))
		o.ObserveInt64(freelistBytes, int64(stats.FreeAlloc))
		o.ObserveInt64(txPageReads, stats.TxStats.PageCount)

		if pageSize > 0 {
			o.ObserveInt64(txPageWrites, stats.TxStats.PageAlloc/pageSize)
		}

		return nil
	}, freelistPages, freelistBytes, txPageReads, txPageWrites)
}
