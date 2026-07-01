// Package usagebuilder tails the FSM audit chain and materialises the usage
// projections consumed by the housekeeping API: per-Numscript-template
// invocation counters (count + lastUsed) and per-ledger event counters
// (postings, reverts, numscript executions, references).
//
// The projection is not part of the FSM authoritative state — it lives in a
// dedicated secondary Pebble instance (usagestore) and is rebuildable from
// cursor=0 on demand.
//
// The subsystem reads from the audit chain (AuditEntry + AuditItem in
// ZoneCold) rather than the log stream because the audit item carries the
// raw serialized order — the only place where the Numscript reference
// survives past apply (the log's CreatedTransaction payload does not).
// For posting/revert counts, we fetch the specific log referenced by
// AuditItem.LogSequence — a single Get on the hot Pebble cache.
//
// Runs on every node; each replica maintains its own cursor (last consumed
// audit sequence). Eventually consistent with the FSM: reads may lag by up
// to one tick interval (100 ms) plus batch drain time.
package usagebuilder

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/usagestore"
)

// DefaultBatchSize is the default number of audit entries per Pebble batch commit.
const DefaultBatchSize = 200

// Builder tails the FSM audit chain and populates the usagestore projections.
// Runs as a background goroutine on all nodes (not leader-only). Progress is
// stored in the usagestore itself under [0xFE][0x01].
type Builder struct {
	pebbleStore   *dal.Store
	usageStore    *usagestore.Store
	notifications *signal.Notifications
	logger        logging.Logger
	meter         metric.Meter
	w             worker.Worker

	batchSize int

	lastProcessedAuditSeq atomic.Uint64
	pebbleLastAuditSeq    atomic.Uint64
	entriesProcessed      atomic.Uint64
	metricsRegistration   metric.Registration
}

// NewBuilder wires the usagebuilder subsystem. Notifications is injected via
// constructor rather than a setter to keep the fx graph explicit — see
// feedback_constructor_injection in the project memory.
func NewBuilder(
	pebbleStore *dal.Store,
	usageStore *usagestore.Store,
	notifications *signal.Notifications,
	logger logging.Logger,
	meter metric.Meter,
	batchSize int,
) *Builder {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &Builder{
		pebbleStore:   pebbleStore,
		usageStore:    usageStore,
		notifications: notifications,
		logger:        logger.WithFields(map[string]any{"cmp": "usage-builder"}),
		meter:         meter,
		batchSize:     batchSize,
	}
}

// Start begins the background loop and registers OTEL metrics.
func (b *Builder) Start() {
	if reg, err := b.registerMetrics(); err == nil {
		b.metricsRegistration = reg
	}

	b.w = worker.New()
	b.w.RunCtx(b.loop)
}

// Stop gracefully stops the background loop and unregisters OTEL metrics.
func (b *Builder) Stop() {
	b.w.Stop()

	if b.metricsRegistration != nil {
		_ = b.metricsRegistration.Unregister()
	}
}

// LastProcessedAuditSequence returns the last audit sequence consumed (from
// the atomic cache — same value as usagestore.ReadProgress but without a
// Pebble Get).
func (b *Builder) LastProcessedAuditSequence() uint64 {
	return b.lastProcessedAuditSeq.Load()
}

// PebbleLastAuditSequence returns the last known Pebble audit sequence (from
// the atomic cache).
func (b *Builder) PebbleLastAuditSequence() uint64 {
	return b.pebbleLastAuditSeq.Load()
}

// registerMetrics registers observable gauges for the usagebuilder.
func (b *Builder) registerMetrics() (metric.Registration, error) {
	lastProcessedGauge, err := b.meter.Int64ObservableGauge(
		"usage.builder.last_processed_audit_sequence",
		metric.WithDescription("Last audit sequence consumed by the usagebuilder"),
	)
	if err != nil {
		return nil, err
	}

	pebbleLastGauge, err := b.meter.Int64ObservableGauge(
		"usage.builder.pebble_last_audit_sequence",
		metric.WithDescription("Last audit sequence in Pebble"),
	)
	if err != nil {
		return nil, err
	}

	lagGauge, err := b.meter.Int64ObservableGauge(
		"usage.builder.lag",
		metric.WithDescription("Number of audit entries the usagebuilder is behind Pebble"),
	)
	if err != nil {
		return nil, err
	}

	entriesProcessedGauge, err := b.meter.Int64ObservableGauge(
		"usage.builder.entries_processed_total",
		metric.WithDescription("Total number of audit entries consumed since process start"),
	)
	if err != nil {
		return nil, err
	}

	return b.meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			processed := int64(b.lastProcessedAuditSeq.Load())
			pebbleLast := int64(b.pebbleLastAuditSeq.Load())

			lag := max(pebbleLast-processed, 0)

			o.ObserveInt64(lastProcessedGauge, processed)
			o.ObserveInt64(pebbleLastGauge, pebbleLast)
			o.ObserveInt64(lagGauge, lag)
			o.ObserveInt64(entriesProcessedGauge, int64(b.entriesProcessed.Load()))

			return nil
		},
		lastProcessedGauge,
		pebbleLastGauge,
		lagGauge,
		entriesProcessedGauge,
	)
}

// loop is the main goroutine driven by Start(). Reads cursor from the usage
// store, catches up on any pending audit entries, then tails via a 100 ms
// ticker plus the LogCommitted notification (fires whenever the FSM
// advances, which is also when the audit chain advances).
func (b *Builder) loop(ctx context.Context) {
	cursor, err := b.usageStore.ReadProgress()
	if err != nil {
		b.logger.Errorf("Failed to read usage progress: %v", err)

		return
	}

	b.lastProcessedAuditSeq.Store(cursor)

	// Seed pebble last audit sequence. Handle closed immediately to release
	// the RLock — keeping it open would deadlock with RestoreCheckpoint
	// (write lock) when processAuditEntries takes a new RLock.
	var pebbleLast uint64
	if handle, err := b.pebbleStore.NewDirectReadHandle(); err != nil {
		b.logger.Errorf("Failed to create read handle: %v", err)

		return
	} else {
		if v, err := query.ReadLastAuditSequence(handle); err == nil {
			pebbleLast = v
			b.pebbleLastAuditSeq.Store(v)
		}

		_ = handle.Close()
	}

	b.logger.WithFields(map[string]any{
		"cursor":     cursor,
		"pebbleLast": pebbleLast,
		"gap":        int64(pebbleLast) - int64(cursor),
	}).Infof("Usage builder started")

	// Initial catch-up: time-bounded iterations to release the Pebble
	// snapshot between passes (same rationale as indexbuilder catchUpBudget).
	const catchUpBudget = 5 * time.Second

	prevCursor := cursor
	savedBatchSize := b.batchSize
	b.batchSize = max(b.batchSize, 2_000)

	for {
		select {
		case <-ctx.Done():
			b.batchSize = savedBatchSize

			return
		default:
		}

		before := cursor
		deadline := time.Now().Add(catchUpBudget)

		if cursor, err = b.processAuditEntries(ctx, cursor, deadline); err != nil {
			b.logger.Errorf("Error during initial catch-up: %v", err)

			break
		}

		if cursor == before {
			break
		}
	}

	b.batchSize = savedBatchSize

	if cursor > prevCursor {
		b.logger.WithFields(map[string]any{
			"from":    prevCursor,
			"to":      cursor,
			"entries": cursor - prevCursor,
		}).Infof("Initial catch-up complete")
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			b.logger.Infof("Usage builder stopped")

			return
		case <-b.notifications.LogCommitted.C():
		case <-ticker.C:
		}

		// Fast path: skip Pebble iterator when nothing new has landed. The
		// LastSequence atomic tracks the last LOG sequence — a strict lower
		// bound on the last audit sequence (audit entries are written in
		// the same batch as their logs). Comparing against our audit cursor
		// is conservative: we may wake up spuriously if the last batch had
		// only audit updates (rare), but we never miss real work.
		if cached := b.notifications.LastSequence.Load(); cached != 0 && cached <= cursor {
			continue
		}

		if cursor, err = b.processAuditEntries(ctx, cursor, time.Time{}); err != nil {
			b.logger.Errorf("Error processing audit entries: %v", err)
		}
	}
}
