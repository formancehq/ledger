package auditindexer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// DefaultBatchSize is the number of audit entries indexed per readstore batch.
const DefaultBatchSize = 1000

// Config tunes the audit indexer.
type Config struct {
	// BatchSize is the maximum number of audit entries processed per batch.
	// Defaults to DefaultBatchSize when zero.
	BatchSize int

	// RebuildThreshold triggers a full drop+rebuild on boot when the cursor
	// lags the last audit sequence by more than this many entries (0 disables
	// gap-based rebuild). See shouldRebuildOnBoot.
	RebuildThreshold uint64

	// Disabled prevents ProcessOnce from doing any work when true.
	Disabled bool
}

// TickInterval is the steady-state polling interval. The audit sequence
// advances on every proposal (including failures, which emit no log), so a
// ticker — not a log signal — is what guarantees pickup. Lag is eventual.
const TickInterval = 200 * time.Millisecond

// Indexer tails the Audit zone of the main store and maintains the readstore
// audit secondary index. It runs on all nodes independently; progress is
// per-replica (no Raft coordination).
type Indexer struct {
	cfg       Config
	store     *dal.Store
	readStore *readstore.Store
	logger    logging.Logger
	meter     metric.Meter

	batchSize int

	// lastIndexed holds the sequence number the indexer has committed to the
	// readstore in this process lifetime. It is a snapshot hint — the
	// authoritative value is always readStore.ReadAuditProgress().
	lastIndexed atomic.Uint64

	// auditLast holds the last known audit sequence in the main store.
	// Updated on each tick by lastAuditSequence(); used only for metric gauges.
	auditLast atomic.Uint64

	// w manages the background goroutine lifecycle.
	w worker.Worker

	// reg is the OTEL metric registration; unregistered on Stop.
	reg metric.Registration
}

// New constructs an Indexer. It does not start any background processing.
func New(cfg Config, store *dal.Store, rs *readstore.Store, logger logging.Logger, meter metric.Meter) *Indexer {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &Indexer{
		cfg:       cfg,
		store:     store,
		readStore: rs,
		logger:    logger.WithFields(map[string]any{"cmp": "audit-indexer"}),
		meter:     meter,
		batchSize: batchSize,
	}
}

// ProcessOnce indexes all audit entries after the persisted cursor, committing
// one readstore batch per batchSize entries, and returns the cursor it reached.
// It is safe to call concurrently, but callers are expected to serialise calls
// in practice (the background loop in Task 7 does so naturally).
func (i *Indexer) ProcessOnce(ctx context.Context) (uint64, error) {
	if i.cfg.Disabled {
		cursor, err := i.readStore.ReadAuditProgress()
		if err != nil {
			return 0, fmt.Errorf("reading audit progress: %w", err)
		}

		return cursor, nil
	}

	cursor, err := i.readStore.ReadAuditProgress()
	if err != nil {
		return 0, fmt.Errorf("reading audit progress: %w", err)
	}

	for {
		// Honor shutdown between batches: worker.Stop() blocks on this loop
		// returning, so without this check draining a large backlog (or a
		// sustained write stream, where advanced stays true) would stall
		// graceful shutdown until fully caught up.
		if err := ctx.Err(); err != nil {
			return cursor, err
		}

		next, advanced, err := i.processBatch(ctx, cursor)
		if err != nil {
			return cursor, err
		}

		cursor = next
		if !advanced {
			break
		}
	}

	i.lastIndexed.Store(cursor)

	return cursor, nil
}

// Rebuild drops the audit index and the cursor, then replays from the earliest
// surviving audit entry. Used by ledgerctl and by boot auto-rebuild.
func (i *Indexer) Rebuild(ctx context.Context) error {
	// Drop the index and reset the cursor in a single batch so the operation is
	// crash-atomic: a torn write leaves either (old index, old cursor) or (empty
	// index, cursor 0). The latter deterministically re-triggers boot rebuild
	// (shouldRebuildOnBoot) and steady-state ProcessOnce self-heals from 0, so
	// the index can never be left permanently empty with a stale high cursor.
	batch := i.readStore.NewBatch()
	defer func() { _ = batch.Cancel() }()
	if err := i.readStore.DropAuditIndexInBatch(batch); err != nil {
		return err
	}
	if err := i.readStore.WriteAuditProgress(batch, 0); err != nil {
		return err
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("resetting audit index: %w", err)
	}
	i.lastIndexed.Store(0)

	_, err := i.ProcessOnce(ctx)

	return err
}

// shouldRebuildOnBoot reports whether boot should drop+rebuild instead of an
// incremental catch-up: cursor missing (0) with entries present, or the gap
// exceeds the configured threshold.
func (i *Indexer) shouldRebuildOnBoot(cursor, last uint64) bool {
	if cursor == 0 && last > 0 {
		return true
	}
	if i.cfg.RebuildThreshold > 0 && last > cursor && last-cursor > i.cfg.RebuildThreshold {
		return true
	}

	return false
}

// processBatch indexes up to batchSize audit entries whose sequence is strictly
// greater than after, commits a single readstore batch, and returns the new
// cursor and whether at least one entry was processed.
func (i *Indexer) processBatch(ctx context.Context, after uint64) (uint64, bool, error) {
	handle, err := i.store.NewDirectReadHandle()
	if err != nil {
		return after, false, fmt.Errorf("opening read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	cur, err := query.ReadAuditEntries(ctx, handle, &after)
	if err != nil {
		return after, false, fmt.Errorf("reading audit entries after %d: %w", after, err)
	}
	defer func() { _ = cur.Close() }()

	batch := i.readStore.NewBatch()
	// Cancel releases the underlying Pebble batch on every early return (caught
	// up with count == 0, or any read/build error). It is a no-op once the batch
	// is committed, so the success path below is unaffected. Without this, the
	// steady-state idle tick (count == 0, every TickInterval) leaks a batch.
	defer func() { _ = batch.Cancel() }()
	kb := dal.NewKeyBuilder()
	emit := func(key []byte) error { return batch.SetBytes(key, nil) }

	cursor := after
	count := 0

	for count < i.batchSize {
		entry, err := cur.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return after, false, fmt.Errorf("iterating audit entries: %w", err)
		}

		items, err := query.ReadAuditItems(ctx, handle, entry.GetSequence())
		if err != nil {
			return after, false, fmt.Errorf("reading audit items for seq %d: %w", entry.GetSequence(), err)
		}

		if err := appendEntryKeys(kb, emit, entry, items); err != nil {
			return after, false, fmt.Errorf("building index keys for seq %d: %w", entry.GetSequence(), err)
		}

		cursor = entry.GetSequence()
		count++
	}

	if count == 0 {
		return after, false, nil
	}

	if err := i.readStore.WriteAuditProgress(batch, cursor); err != nil {
		return after, false, fmt.Errorf("writing audit progress %d: %w", cursor, err)
	}

	if err := batch.Commit(); err != nil {
		return after, false, fmt.Errorf("committing audit index batch at seq %d: %w", cursor, err)
	}

	i.lastIndexed.Store(cursor)

	return cursor, true, nil
}

// lastAuditSequence reads the highest audit sequence currently in the main
// store. Returns 0 when the store is empty.
func (i *Indexer) lastAuditSequence() (uint64, error) {
	handle, err := i.store.NewDirectReadHandle()
	if err != nil {
		return 0, fmt.Errorf("opening read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	return query.ReadLastAuditSequence(handle)
}

// Start launches the background indexing loop (no-op if disabled).
func (i *Indexer) Start() {
	if i.cfg.Disabled {
		i.logger.Infof("Audit indexer disabled")

		return
	}
	if reg, err := i.registerMetrics(); err == nil {
		i.reg = reg
	}
	i.w = worker.New()
	i.w.RunCtx(i.loop)
}

// Stop halts the loop and unregisters metrics.
func (i *Indexer) Stop() {
	if i.cfg.Disabled {
		return
	}
	i.w.Stop()
	if i.reg != nil {
		_ = i.reg.Unregister()
	}
}

func (i *Indexer) loop(ctx context.Context) {
	cursor, err := i.readStore.ReadAuditProgress()
	if err != nil {
		i.logger.Errorf("read audit cursor: %v", err)

		return
	}
	if last, err := i.lastAuditSequence(); err == nil {
		i.auditLast.Store(last)
		if i.shouldRebuildOnBoot(cursor, last) {
			i.logger.WithFields(map[string]any{"cursor": cursor, "last": last}).Infof("Audit index rebuild on boot")
			if err := i.Rebuild(ctx); err != nil && !errors.Is(err, context.Canceled) {
				i.logger.Errorf("audit index boot rebuild: %v", err)
			}
		}
	}

	ticker := time.NewTicker(TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if last, err := i.lastAuditSequence(); err == nil {
			i.auditLast.Store(last)
		}
		if _, err := i.ProcessOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			i.logger.Errorf("audit indexing: %v", err)
		}
	}
}

func (i *Indexer) registerMetrics() (metric.Registration, error) {
	indexed, err := i.meter.Int64ObservableGauge("audit_index.last_indexed_sequence",
		metric.WithDescription("Last audit sequence indexed"))
	if err != nil {
		return nil, err
	}
	last, err := i.meter.Int64ObservableGauge("audit_index.audit_last_sequence",
		metric.WithDescription("Last audit sequence in the store"))
	if err != nil {
		return nil, err
	}
	lag, err := i.meter.Int64ObservableGauge("audit_index.lag",
		metric.WithDescription("Audit entries the index is behind"))
	if err != nil {
		return nil, err
	}

	return i.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		idx := int64(i.lastIndexed.Load())
		al := int64(i.auditLast.Load())
		o.ObserveInt64(indexed, idx)
		o.ObserveInt64(last, al)
		o.ObserveInt64(lag, max(al-idx, 0))

		return nil
	}, indexed, last, lag)
}
