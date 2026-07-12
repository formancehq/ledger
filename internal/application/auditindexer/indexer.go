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

	"github.com/formancehq/ledger/v3/internal/pkg/tailworker"
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

	// restoreGen is the store restore generation this indexer last processed
	// under (see dal.Store.RestoreGeneration). Seeded on boot, re-read every
	// tick: a change means the primary store was rolled back beneath the cursor
	// at runtime, so a full rebuild is forced regardless of where the audit head
	// landed relative to the cursor — the position-based cursorAheadOfHead /
	// RebuildThreshold signals can be erased by a catch-up race.
	restoreGen atomic.Uint64

	// tw drives the steady-state tail loop (boot + ticker).
	tw *tailworker.TailWorker

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
	// Snapshot the restore generation up-front: a RestoreCheckpoint that lands
	// while this rebuild is replaying bumps past this value, so the next
	// processTick re-detects it instead of the rebuild silently swallowing it.
	i.restoreGen.Store(i.store.RestoreGeneration())

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
	if cursorAheadOfHead(cursor, last) {
		return true
	}
	if i.cfg.RebuildThreshold > 0 && last > cursor && last-cursor > i.cfg.RebuildThreshold {
		return true
	}

	return false
}

// cursorAheadOfHead reports the post-restore rollback signature: the persisted
// cursor sits beyond the current audit head. This is only possible after a
// main-store restore/rollback to an earlier checkpoint with the read index
// retained (e.g. follower sync via SynchronizeWithLeader/RestoreCheckpoint).
// Steady-state ProcessOnce would scan past the surviving (lower-seq) entries
// forever, so both boot and the steady-state tick force a full rebuild here
// (which resets the cursor to 0 and re-indexes from the earliest survivor).
func cursorAheadOfHead(cursor, last uint64) bool { return cursor > last }

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

	// Wake any live filtered-audit reader blocked in WaitForAuditSequence so it
	// picks up the just-committed cursor immediately, instead of waiting for an
	// unrelated log-index NotifyProgress or the next tick.
	i.readStore.NotifyProgress()

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
	if reg, err := tailworker.RegisterTailGauges(i.meter, "audit_index", "audit", &i.lastIndexed, &i.auditLast); err == nil {
		i.reg = reg
	}
	i.tw = tailworker.New(tailworker.Config{
		Name:   "audit-indexer",
		Logger: i.logger,
		Ticker: TickInterval,
		Boot:   i.boot,
		Tick:   i.processTick,
	})
	i.tw.Start()
}

// Stop halts the loop and unregisters metrics.
func (i *Indexer) Stop() {
	if i.cfg.Disabled {
		return
	}
	i.tw.Stop()
	if i.reg != nil {
		_ = i.reg.Unregister()
	}
}

// boot runs once before the tail loop: it seeds the audit-head gauge and, when
// the persisted cursor is missing / lagging beyond threshold / ahead of the
// head, performs a full drop+rebuild. A cursor read error aborts the loop
// (returned to tailworker, which logs and stops); a rebuild error is logged
// and swallowed so steady-state indexing still starts.
func (i *Indexer) boot(ctx context.Context) error {
	// Snapshot the restore generation before any processing so processTick can
	// detect a runtime rollback that lands after boot.
	i.restoreGen.Store(i.store.RestoreGeneration())

	cursor, err := i.readStore.ReadAuditProgress()
	if err != nil {
		return fmt.Errorf("read audit cursor: %w", err)
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

	return nil
}

// processTick runs one steady-state iteration: refresh the audit-head gauge,
// rebuild if the persisted cursor has overtaken the audit head, otherwise index
// incrementally. The cursor-ahead check is what catches a runtime main-store
// restore (SynchronizeWithLeader/RestoreCheckpoint) that drops the audit head
// below the persisted cursor while this loop is already running — the boot
// shouldRebuildOnBoot check runs only once, before the ticker, so without this
// re-check ProcessOnce would scan past every surviving entry forever. Only the
// cursor-ahead condition is re-evaluated per tick, not the full
// shouldRebuildOnBoot: its RebuildThreshold branch is a boot-only heuristic that
// would spuriously trigger a full rebuild whenever a normal burst exceeds the
// threshold between ticks.
func (i *Indexer) processTick(ctx context.Context) error {
	// Restore-generation change is the authoritative rollback signal: it fires
	// on any RestoreCheckpoint regardless of where the audit head landed, so it
	// catches the catch-up race that erases the cursor>head position signal
	// (restore below cursor, head re-grows past the old cursor before this tick
	// re-samples). Rebuild re-syncs restoreGen, so this fires once per restore.
	if gen := i.store.RestoreGeneration(); gen != i.restoreGen.Load() {
		i.logger.WithFields(map[string]any{"from": i.restoreGen.Load(), "to": gen}).
			Infof("Audit index rebuild: primary store restore detected (generation changed)")

		return i.Rebuild(ctx)
	}

	if last, err := i.lastAuditSequence(); err == nil {
		i.auditLast.Store(last)

		cursor, cursorErr := i.readStore.ReadAuditProgress()
		if cursorErr == nil && cursorAheadOfHead(cursor, last) {
			i.logger.WithFields(map[string]any{"cursor": cursor, "last": last}).
				Infof("Audit index rebuild: cursor overtook audit head")

			return i.Rebuild(ctx)
		}
	}

	_, err := i.ProcessOnce(ctx)

	return err
}
