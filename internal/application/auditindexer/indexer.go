package auditindexer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

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

	// RebuildThreshold is reserved for future use (full-rebuild triggering).
	RebuildThreshold uint64

	// Disabled prevents ProcessOnce from doing any work when true.
	Disabled bool
}

// Indexer tails the Audit zone of the main store and maintains the readstore
// audit secondary index. It runs on all nodes independently; progress is
// per-replica (no Raft coordination).
type Indexer struct {
	cfg       Config
	store     *dal.Store
	readStore *readstore.Store
	logger    logging.Logger
	// meter is retained for future gauge metrics added in the background-loop task.
	meter metric.Meter

	batchSize int

	// lastIndexed holds the sequence number the indexer has committed to the
	// readstore in this process lifetime. It is a snapshot hint — the
	// authoritative value is always readStore.ReadAuditProgress().
	lastIndexed atomic.Uint64
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
	if err := i.readStore.DropAuditIndex(); err != nil {
		return err
	}
	batch := i.readStore.NewBatch()
	if err := i.readStore.WriteAuditProgress(batch, 0); err != nil {
		return err
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("resetting audit cursor: %w", err)
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
