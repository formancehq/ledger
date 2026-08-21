package balancehistorystore

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type metricsRegistrar struct {
	*storeCore
}

// RegisterMetrics exposes bounded-cardinality physical and logical store
// metrics. It deliberately carries no ledger, account, asset, or timestamp
// labels.
func (s *metricsRegistrar) RegisterMetrics(meter metric.Meter) (metric.Registration, error) {
	levelBytes, err := meter.Int64ObservableGauge(
		"balancehistory.store.level.bytes",
		metric.WithDescription("Physical bytes in each balance-history Pebble level"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history level bytes gauge: %w", err)
	}

	segments, err := meter.Int64ObservableGauge(
		"balancehistory.store.segments",
		metric.WithDescription("Immutable logical balance-history segments by level"),
		metric.WithUnit("{segment}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history segments gauge: %w", err)
	}

	entries, err := meter.Int64ObservableGauge(
		"balancehistory.store.summary_entries",
		metric.WithDescription("Temporal prefix-summary entries by logical level"),
		metric.WithUnit("{entry}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history summary entries gauge: %w", err)
	}

	memtableBytes, err := meter.Int64ObservableGauge(
		"balancehistory.store.memtable.bytes",
		metric.WithDescription("Current balance-history memtable bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history memtable gauge: %w", err)
	}

	compactionDebt, err := meter.Int64ObservableGauge(
		"balancehistory.store.compaction_debt.bytes",
		metric.WithDescription("Estimated physical Pebble compaction debt"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history compaction debt gauge: %w", err)
	}

	cacheHits, err := meter.Int64ObservableGauge(
		"balancehistory.store.cache.hits",
		metric.WithDescription("Balance-history Pebble block-cache hits"),
		metric.WithUnit("{hit}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history cache hits gauge: %w", err)
	}

	cacheMisses, err := meter.Int64ObservableGauge(
		"balancehistory.store.cache.misses",
		metric.WithDescription("Balance-history Pebble block-cache misses"),
		metric.WithUnit("{miss}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history cache misses gauge: %w", err)
	}

	return meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		physical := s.db.Metrics()
		for level, values := range physical.Levels {
			observer.ObserveInt64(levelBytes, values.TablesSize, metric.WithAttributes(attribute.Int("level", level)))
		}
		observer.ObserveInt64(memtableBytes, int64(physical.MemTable.Size))
		observer.ObserveInt64(compactionDebt, int64(physical.Compact.EstimatedDebt))
		observer.ObserveInt64(cacheHits, physical.BlockCache.Hits)
		observer.ObserveInt64(cacheMisses, physical.BlockCache.Misses)

		manifest, manifestErr := s.Manifest()
		if manifestErr != nil {
			return manifestErr
		}
		byLevelSegments := make(map[uint32]int64)
		byLevelEntries := make(map[uint32]int64)
		for _, segment := range manifest.Segments {
			byLevelSegments[segment.Level]++
			byLevelEntries[segment.Level] += int64(segment.EntryCount)
		}
		for level, count := range byLevelSegments {
			attrs := metric.WithAttributes(attribute.Int("level", int(level)))
			observer.ObserveInt64(segments, count, attrs)
			observer.ObserveInt64(entries, byLevelEntries[level], attrs)
		}

		return nil
	}, levelBytes, segments, entries, memtableBytes, compactionDebt, cacheHits, cacheMisses)
}
