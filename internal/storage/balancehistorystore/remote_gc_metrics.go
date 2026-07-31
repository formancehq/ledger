package balancehistorystore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"
)

type remoteGCMetrics struct {
	instrumentMu   sync.RWMutex
	listDuration   metric.Float64Histogram
	deleteDuration metric.Float64Histogram

	queueObjects            atomic.Int64
	queueBytes              atomic.Int64
	oldestQueuedAge         atomic.Int64
	inventoryObjects        atomic.Int64
	inventoryBytes          atomic.Int64
	deletedObjects          atomic.Int64
	deletedBytes            atomic.Int64
	activeViewBlockedCycles atomic.Int64
	listFailures            atomic.Int64
	deleteFailures          atomic.Int64
	lastCompletedInventory  atomic.Int64
}

// RegisterMetrics exposes label-free remote-GC inventory, queue, safety, and
// operation signals. A collector accepts one registration for its lifetime.
func (m *remoteGCMetrics) register(meter metric.Meter) (metric.Registration, error) {
	if meter == nil {
		return nil, errors.New("balance history remote GC meter is required")
	}

	m.instrumentMu.Lock()
	defer m.instrumentMu.Unlock()
	if m.listDuration != nil || m.deleteDuration != nil {
		return nil, errors.New("balance history remote GC metrics are already registered")
	}

	inventoryObjects, err := meter.Int64ObservableGauge(
		"balancehistory.remote_gc.inventory.objects",
		metric.WithDescription("Objects found in the last completed owned-namespace inventory"),
		metric.WithUnit("{object}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history remote GC inventory objects gauge: %w", err)
	}
	inventoryBytes, err := meter.Int64ObservableGauge(
		"balancehistory.remote_gc.inventory.bytes",
		metric.WithDescription("Bytes found in the last completed owned-namespace inventory"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history remote GC inventory bytes gauge: %w", err)
	}
	queueObjects, err := meter.Int64ObservableGauge(
		"balancehistory.remote_gc.queue.objects",
		metric.WithDescription("Durable remote-GC candidates awaiting retirement or deletion"),
		metric.WithUnit("{object}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history remote GC queue objects gauge: %w", err)
	}
	queueBytes, err := meter.Int64ObservableGauge(
		"balancehistory.remote_gc.queue.bytes",
		metric.WithDescription("Bytes in the durable remote-GC candidate queue"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history remote GC queue bytes gauge: %w", err)
	}
	oldestQueuedAge, err := meter.Int64ObservableGauge(
		"balancehistory.remote_gc.queue.oldest_age",
		metric.WithDescription("Age of the oldest durable remote-GC candidate"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history remote GC oldest queue age gauge: %w", err)
	}
	activeViewBlocked, err := meter.Int64ObservableCounter(
		"balancehistory.remote_gc.blocked.active_view.cycles",
		metric.WithDescription("Collection calls that failed closed because at least one View was active"),
		metric.WithUnit("{cycle}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history remote GC active-view blocked counter: %w", err)
	}
	deletedObjects, err := meter.Int64ObservableCounter(
		"balancehistory.remote_gc.deleted.objects",
		metric.WithDescription("Logical current remote archive objects deleted and durably acknowledged"),
		metric.WithUnit("{object}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history remote GC deleted objects counter: %w", err)
	}
	deletedBytes, err := meter.Int64ObservableCounter(
		"balancehistory.remote_gc.deleted.bytes",
		metric.WithDescription("Logical current remote archive bytes deleted and durably acknowledged"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history remote GC deleted bytes counter: %w", err)
	}
	listFailures, err := meter.Int64ObservableCounter(
		"balancehistory.remote_gc.list.failures",
		metric.WithDescription("Owned-namespace remote listing failures"),
		metric.WithUnit("{failure}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history remote GC list failures counter: %w", err)
	}
	deleteFailures, err := meter.Int64ObservableCounter(
		"balancehistory.remote_gc.delete.failures",
		metric.WithDescription("Idempotent remote object deletion failures"),
		metric.WithUnit("{failure}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history remote GC delete failures counter: %w", err)
	}
	lastCompletedInventory, err := meter.Int64ObservableGauge(
		"balancehistory.remote_gc.last_completed_inventory.timestamp",
		metric.WithDescription("Unix timestamp of the last durably completed owned-namespace inventory"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history remote GC last completed inventory gauge: %w", err)
	}
	listDuration, err := meter.Float64Histogram(
		"balancehistory.remote_gc.list.duration",
		metric.WithDescription("Remote owned-namespace listing duration"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history remote GC list duration histogram: %w", err)
	}
	deleteDuration, err := meter.Float64Histogram(
		"balancehistory.remote_gc.delete.duration",
		metric.WithDescription("Remote idempotent deletion duration"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history remote GC delete duration histogram: %w", err)
	}

	registration, err := meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		observer.ObserveInt64(inventoryObjects, m.inventoryObjects.Load())
		observer.ObserveInt64(inventoryBytes, m.inventoryBytes.Load())
		observer.ObserveInt64(queueObjects, m.queueObjects.Load())
		observer.ObserveInt64(queueBytes, m.queueBytes.Load())
		observer.ObserveInt64(oldestQueuedAge, m.oldestQueuedAge.Load())
		observer.ObserveInt64(activeViewBlocked, m.activeViewBlockedCycles.Load())
		observer.ObserveInt64(deletedObjects, m.deletedObjects.Load())
		observer.ObserveInt64(deletedBytes, m.deletedBytes.Load())
		observer.ObserveInt64(listFailures, m.listFailures.Load())
		observer.ObserveInt64(deleteFailures, m.deleteFailures.Load())
		observer.ObserveInt64(lastCompletedInventory, m.lastCompletedInventory.Load())

		return nil
	},
		inventoryObjects,
		inventoryBytes,
		queueObjects,
		queueBytes,
		oldestQueuedAge,
		activeViewBlocked,
		deletedObjects,
		deletedBytes,
		listFailures,
		deleteFailures,
		lastCompletedInventory,
	)
	if err != nil {
		return nil, fmt.Errorf("registering balance history remote GC metrics callback: %w", err)
	}
	m.listDuration = listDuration
	m.deleteDuration = deleteDuration

	return registration, nil
}

func (m *remoteGCMetrics) recordList(ctx context.Context, elapsed time.Duration, err error) {
	m.instrumentMu.RLock()
	duration := m.listDuration
	m.instrumentMu.RUnlock()
	if duration != nil {
		duration.Record(ctx, elapsed.Seconds())
	}
	if err != nil {
		m.listFailures.Add(1)
	}
}

func (m *remoteGCMetrics) recordDelete(ctx context.Context, elapsed time.Duration, err error) {
	m.instrumentMu.RLock()
	duration := m.deleteDuration
	m.instrumentMu.RUnlock()
	if duration != nil {
		duration.Record(ctx, elapsed.Seconds())
	}
	if err != nil {
		m.deleteFailures.Add(1)
	}
}
