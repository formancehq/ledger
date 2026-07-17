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
// to one tick interval plus batch drain time.
package usagebuilder

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/pkg/tailworker"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/usagestore"
)

// DefaultBatchSize is the default number of audit entries per Pebble batch commit.
const DefaultBatchSize = 200

// TickInterval is the steady-state polling interval. Same rationale as the
// audit indexer: the audit sequence advances on every proposal (including
// failures that emit no log), so a ticker is what guarantees pickup.
const TickInterval = 100 * time.Millisecond

// catchUpBudget bounds how long a single processAuditEntries invocation
// holds a Pebble snapshot during boot-time catch-up. Between slices the
// snapshot is released so compactions can proceed on long-history stores.
const catchUpBudget = 5 * time.Second

// Builder tails the FSM audit chain and populates the usagestore projections.
// Runs as a background goroutine on all nodes (not leader-only). Progress is
// stored in the usagestore itself under [0xFE][0x01].
type Builder struct {
	pebbleStore   *dal.Store
	usageStore    *usagestore.Store
	notifications *signal.Notifications
	logger        logging.Logger
	meter         metric.Meter

	batchSize int

	// lastProcessedAuditSeq mirrors usagestore.ReadProgress() and is
	// updated on every successful commit — the atomic hint lets external
	// readers (metrics, tests) avoid a Pebble Get.
	lastProcessedAuditSeq atomic.Uint64
	// pebbleLastAuditSeq is the highest audit sequence in the main store,
	// resampled on each tick for the lag gauge.
	pebbleLastAuditSeq atomic.Uint64

	tw  *tailworker.TailWorker
	reg metric.Registration
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

// Start launches the background tail loop and registers OTEL gauges.
func (b *Builder) Start() {
	if reg, err := tailworker.RegisterTailGauges(
		b.meter, "usage.builder", "audit", &b.lastProcessedAuditSeq, &b.pebbleLastAuditSeq,
	); err == nil {
		b.reg = reg
	}

	// Wake on FSM commit signals when available. The guard is defensive:
	// steady-state always wires notifications, but a nil value must not
	// panic (e.g. a builder constructed in a test without a live FSM).
	var wake <-chan struct{}
	if b.notifications != nil {
		wake = b.notifications.LogCommitted.C()
	}

	b.tw = tailworker.New(tailworker.Config{
		Name:   "usage-builder",
		Logger: b.logger,
		Ticker: TickInterval,
		Wake:   wake,
		Boot:   b.boot,
		Tick:   b.tick,
	})
	b.tw.Start()
}

// Stop halts the tail loop and unregisters metrics.
func (b *Builder) Stop() {
	if b.tw != nil {
		b.tw.Stop()
	}
	if b.reg != nil {
		_ = b.reg.Unregister()
	}
}

// LastProcessedAuditSequence returns the last audit sequence consumed
// (atomic hint — same value as usagestore.ReadProgress but without a
// Pebble Get). Exposed for tests and health checks.
func (b *Builder) LastProcessedAuditSequence() uint64 {
	return b.lastProcessedAuditSeq.Load()
}

// PebbleLastAuditSequence returns the last known main-store audit sequence
// (atomic hint refreshed each tick).
func (b *Builder) PebbleLastAuditSequence() uint64 {
	return b.pebbleLastAuditSeq.Load()
}

// boot runs once before the tail loop: seed both atomics from the persisted
// state and drain the reachable backlog with a bigger batch size so the
// steady-state loop starts already caught up. A cursor-read error aborts the
// loop (returned to tailworker, which logs and stops); a catch-up error is
// logged and swallowed so steady-state indexing still starts.
func (b *Builder) boot(ctx context.Context) error {
	cursor, err := b.usageStore.ReadProgress()
	if err != nil {
		return fmt.Errorf("reading usage progress: %w", err)
	}

	b.lastProcessedAuditSeq.Store(cursor)

	pebbleLast, sampleErr := b.sampleAuditHead()
	if sampleErr == nil {
		b.pebbleLastAuditSeq.Store(pebbleLast)

		// Rollback detection before the catch-up: if the primary store was
		// restored beneath the persisted cursor, drop the projection and rewind
		// to 0 so the fold below replays into a clean store.
		if cursor, err = b.resetIfRolledBack(cursor, pebbleLast); err != nil {
			return err
		}
	}

	b.logger.WithFields(map[string]any{
		"cursor":     cursor,
		"pebbleLast": pebbleLast,
		"gap":        int64(pebbleLast) - int64(cursor),
	}).Infof("Usage builder started")

	// Initial catch-up — time-bounded slices so the Pebble snapshot is
	// released between passes. Larger batch size so bootstrap commits are
	// coalesced.
	prevCursor := cursor
	savedBatchSize := b.batchSize
	b.batchSize = max(b.batchSize, 2_000)
	defer func() { b.batchSize = savedBatchSize }()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		before := cursor
		deadline := time.Now().Add(catchUpBudget)

		cursor, err = b.processAuditEntries(ctx, cursor, deadline)
		if err != nil {
			b.logger.Errorf("initial catch-up: %v", err)

			break
		}

		if cursor == before {
			break
		}
	}

	if cursor > prevCursor {
		b.logger.WithFields(map[string]any{
			"from":    prevCursor,
			"to":      cursor,
			"entries": cursor - prevCursor,
		}).Infof("Initial catch-up complete")
	}

	return nil
}

// tick runs one steady-state iteration: refresh the audit-head gauge, detect a
// primary-store rollback, then drain any pending audit entries from the cursor
// forward.
//
// In steady state the persisted cursor sits at or behind the audit head: the
// chain is append-only and RestoreCheckpoint normally only advances a node that
// is behind (IsStoreUpToDate gates sync on LastAppliedIndex >= SnapshotIndex).
// But a restore that rolls the primary store back BENEATH the persisted cursor
// leaves the cursor ahead of the audit head; a forward-only fold would then
// no-op and leave the projection permanently over-counted — and, because the
// usagestore is a peer store outside checker scope (invariant #8), nothing would
// surface the drift. resetIfRolledBack wipes the projection + cursor and rewinds
// to 0 so this tick replays into a clean store (docs: usagebuilder.md rollback).
func (b *Builder) tick(ctx context.Context) error {
	cursor := b.lastProcessedAuditSeq.Load()

	if last, err := b.sampleAuditHead(); err == nil {
		b.pebbleLastAuditSeq.Store(last)

		if cursor, err = b.resetIfRolledBack(cursor, last); err != nil {
			return err
		}
	}

	_, err := b.processAuditEntries(ctx, cursor, time.Time{})

	return err
}

// resetIfRolledBack detects a primary-store rollback beneath the usage cursor —
// the persisted cursor sitting AHEAD of the current audit head — and, when found,
// wipes every counter/template row + the progress cursor (usagestore.Reset) so
// the caller replays the projection from audit sequence 0. It returns the cursor
// to resume from: 0 after a reset, or the unchanged cursor otherwise. A reset
// also rewinds the lastProcessedAuditSeq atomic so external readers observe the
// rewind. This is the online reconvergence path the usagebuilder relies on in
// 3.0 (offline drop-and-rebuild is deferred to 3.1 — see usagebuilder.md).
func (b *Builder) resetIfRolledBack(cursor, auditHead uint64) (uint64, error) {
	if cursor <= auditHead {
		return cursor, nil
	}

	b.logger.WithFields(map[string]any{
		"cursor":    cursor,
		"auditHead": auditHead,
	}).Infof("usage cursor ahead of audit head — primary store rolled back; resetting usage projection and replaying from audit sequence 0")

	if err := b.usageStore.Reset(); err != nil {
		return cursor, fmt.Errorf("resetting usage projection after rollback: %w", err)
	}

	b.lastProcessedAuditSeq.Store(0)

	return 0, nil
}

// sampleAuditHead opens a short-lived read handle to read the current audit
// head. The handle is closed immediately so RestoreCheckpoint's write lock
// is not blocked during idle ticks.
func (b *Builder) sampleAuditHead() (uint64, error) {
	handle, err := b.pebbleStore.NewDirectReadHandle()
	if err != nil {
		return 0, err
	}

	defer func() { _ = handle.Close() }()

	return query.ReadLastAuditSequence(handle)
}
