package indexbuilder

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// DefaultBatchSize is the default number of log entries per Pebble batch
// commit. Can be overridden via --read-index-batch-size.
const DefaultBatchSize = 1000

// Proposer submits a technical Raft proposal (no orders, no preload
// payload) and blocks until the FSM applies. Implemented in bootstrap
// via a closure that routes the proposal through preload.RunWithPreload
// with empty Needs.
type Proposer interface {
	Propose(ctx context.Context, cmd *raftcmdpb.Proposal) error
}

// Builder tails the system log and populates the Pebble read store indexes.
// It runs as a background goroutine on ALL nodes (not just the leader).
// Progress is stored locally in Pebble (no Raft needed).
//
// When a new index is created, the builder also backfills historical data.
// Only the leader proposes IndexReady through Raft when backfill completes.
type Builder struct {
	pebbleStore   *dal.Store
	readStore     *readstore.Store
	logger        logging.Logger
	meter         metric.Meter
	notifications *signal.Notifications
	proposer      Proposer
	isLeader      func() bool
	w             worker.Worker

	lastIndexedSeq      atomic.Uint64
	pebbleLastSeq       atomic.Uint64
	logsIndexed         atomic.Uint64
	metricsRegistration metric.Registration

	// Per-ledger index configuration cache.
	indexConfig map[string]*ledgerIndexConfig

	// Ledger name → ID cache, populated from CreateLedger logs.
	ledgerNameToID map[string]uint32

	// Active backfill tasks for BUILDING indexes.
	backfillTasks []*backfillTask

	// Active schema rewrite tasks for deferred SetMetadataFieldType processing.
	schemaRewriteTasks []*schemaRewriteTask

	// Batch size for normal index processing and backfill.
	batchSize int

	// Max time budget per tick for backfill processing (default 50ms).
	backfillBudget time.Duration

	// Round-robin index for fair scheduling across backfill tasks.
	nextBackfillIdx int

	// Audit sync for transient account filtering.
	lastAuditSeq uint64

	// Reusable scratch objects to reduce allocations in the hot loop.
	kb       *dal.KeyBuilder
	wb       *readstore.WriteBatch
	accounts map[string]struct{}
}

// NewBuilder creates a new index builder.
// batchSize controls how many log entries are buffered per Pebble batch commit.
// Use 0 for the default (DefaultBatchSize).
func NewBuilder(
	pebbleStore *dal.Store,
	readStore *readstore.Store,
	logger logging.Logger,
	meter metric.Meter,
	batchSize int,
) *Builder {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &Builder{
		pebbleStore:    pebbleStore,
		readStore:      readStore,
		logger:         logger.WithFields(map[string]any{"cmp": "index-builder"}),
		meter:          meter,
		batchSize:      batchSize,
		backfillBudget: 50 * time.Millisecond,
		indexConfig:    make(map[string]*ledgerIndexConfig),
		ledgerNameToID: make(map[string]uint32),
		kb:             dal.NewKeyBuilder(),
		wb:             readstore.NewWriteBatch(),
		accounts:       make(map[string]struct{}, 64),
	}
}

// SetNotifications sets the dedicated Notifications signal for the builder.
func (b *Builder) SetNotifications(n *signal.Notifications) {
	b.notifications = n
}

// SetProposer sets the Raft proposer and leader check function.
// Must be called before Start.
func (b *Builder) SetProposer(p Proposer, isLeader func() bool) {
	b.proposer = p
	b.isLeader = isLeader
}

// Start begins the background index-building loop and registers OTEL metrics.
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

// LastIndexedSequence returns the last indexed sequence (from the atomic cache).
func (b *Builder) LastIndexedSequence() uint64 {
	return b.lastIndexedSeq.Load()
}

// PebbleLastSequence returns the last known Pebble sequence (from the atomic cache).
func (b *Builder) PebbleLastSequence() uint64 {
	return b.pebbleLastSeq.Load()
}

// registerMetrics registers observable gauges for index builder progress.
func (b *Builder) registerMetrics() (metric.Registration, error) {
	lastIndexedGauge, err := b.meter.Int64ObservableGauge(
		"index.builder.last_indexed_sequence",
		metric.WithDescription("Last log sequence indexed in Pebble read store"),
	)
	if err != nil {
		return nil, err
	}

	pebbleLastGauge, err := b.meter.Int64ObservableGauge(
		"index.builder.pebble_last_sequence",
		metric.WithDescription("Last log sequence in Pebble"),
	)
	if err != nil {
		return nil, err
	}

	lagGauge, err := b.meter.Int64ObservableGauge(
		"index.builder.lag",
		metric.WithDescription("Number of logs the index builder is behind Pebble"),
	)
	if err != nil {
		return nil, err
	}

	logsIndexedGauge, err := b.meter.Int64ObservableGauge(
		"index.builder.logs_indexed_total",
		metric.WithDescription("Total number of logs indexed since process start"),
	)
	if err != nil {
		return nil, err
	}

	return b.meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			indexed := int64(b.lastIndexedSeq.Load())
			pebbleLast := int64(b.pebbleLastSeq.Load())

			lag := max(pebbleLast-indexed, 0)

			o.ObserveInt64(lastIndexedGauge, indexed)
			o.ObserveInt64(pebbleLastGauge, pebbleLast)
			o.ObserveInt64(lagGauge, lag)
			o.ObserveInt64(logsIndexedGauge, int64(b.logsIndexed.Load()))

			return nil
		},
		lastIndexedGauge,
		pebbleLastGauge,
		lagGauge,
		logsIndexedGauge,
	)
}

func (b *Builder) loop(ctx context.Context) {
	// ctx is supplied by Worker.RunCtx and is cancelled by Stop(). It
	// flows to all blocking operations (Pebble reads, iterators) so they
	// are interrupted promptly on shutdown instead of blocking until the
	// operation completes naturally. For internal helpers that still
	// take a stop <-chan struct{}, ctx.Done() is passed at the boundary
	// (same signal, same semantics).
	stop := ctx.Done()

	// Initialize index config cache from Pebble before processing any logs.
	b.initIndexConfig(ctx)

	cursor, err := b.readStore.LastIndexedSequence()
	if err != nil {
		b.logger.Errorf("Failed to read progress: %v", err)

		return
	}

	b.lastIndexedSeq.Store(cursor)

	// Recover audit progress.
	if auditSeq, err := b.readStore.ReadAuditProgress(); err == nil {
		b.lastAuditSeq = auditSeq
	}

	// Seed pebble last sequence. The handle is closed immediately after use
	// to release the RLock — keeping it open would deadlock with
	// RestoreCheckpoint (write lock) when processLogs tries to take a new RLock.
	var pebbleLast uint64
	if handle, err := b.pebbleStore.NewDirectReadHandle(); err != nil {
		b.logger.Errorf("Failed to create read handle: %v", err)

		return
	} else {
		if v, err := query.ReadLastSequence(handle); err == nil {
			pebbleLast = v
			b.pebbleLastSeq.Store(v)
		}

		_ = handle.Close()
	}
	b.logger.WithFields(map[string]any{
		"cursor":     cursor,
		"pebbleLast": pebbleLast,
		"gap":        int64(pebbleLast) - int64(cursor),
		"backfills":  len(b.backfillTasks),
	}).Infof("Index builder started")

	// Initial catch-up: process all pending logs before entering the main loop.
	// Use a larger batch size to reduce fsync overhead, and strip BUILDING
	// indexes from the config since their ranges will be covered by backfill
	// tasks. This avoids millions of redundant Pebble writes.
	//
	// The catch-up is split into time-bounded iterations (catchUpBudget) so
	// that the Pebble ReadHandle (snapshot) is released between iterations.
	// Without this, a single unbounded processLogs call holds a snapshot for
	// the entire catch-up duration (potentially hours on large stores),
	// preventing Pebble from garbage-collecting obsolete SSTs and causing
	// zombie files and pinned keys to accumulate.
	const catchUpBudget = 5 * time.Second
	prevCursor := cursor
	savedBatchSize := b.batchSize
	b.batchSize = max(b.batchSize, 10_000)
	restoreIndexes := b.stripBuildingIndexes()

	for {
		select {
		case <-ctx.Done():
			restoreIndexes()
			b.batchSize = savedBatchSize

			return
		default:
		}

		before := cursor
		deadline := time.Now().Add(catchUpBudget)

		if cursor, err = b.processLogs(ctx, cursor, deadline); err != nil {
			b.logger.Errorf("Error during initial catch-up: %v", err)

			break
		}

		if cursor == before {
			break
		}
	}

	restoreIndexes()
	b.batchSize = savedBatchSize

	if cursor > prevCursor {
		b.logger.WithFields(map[string]any{
			"from": prevCursor,
			"to":   cursor,
			"logs": cursor - prevCursor,
		}).Infof("Initial catch-up complete")
	}

	b.processBackgroundTasks(ctx, stop, cursor)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			b.logger.Infof("Index builder stopped")

			return
		case <-b.notifications.LogCommitted.C():
		case <-ticker.C:
		}

		// Fast path: skip Pebble iterator + batch commit when the FSM
		// hasn't advanced past our cursor.
		logsProcessed := false

		if cached := b.notifications.LastSequence.Load(); cached == 0 || cached > cursor {
			// When background tasks are active, cap normal processing so they
			// get their fair share of each tick.
			var logDeadline time.Time
			if len(b.backfillTasks) > 0 || len(b.schemaRewriteTasks) > 0 {
				logDeadline = time.Now().Add(b.backfillBudget)
			}

			prevCursor := cursor
			if cursor, err = b.processLogs(ctx, cursor, logDeadline); err != nil {
				b.logger.Errorf("Error processing logs: %v", err)
			}

			logsProcessed = cursor > prevCursor
		}

		// When processLogs had nothing to do (cluster idle), give backfills
		// a much larger budget — the full tick interval instead of just 50ms.
		if !logsProcessed {
			b.backfillBudget = 90 * time.Millisecond
		} else {
			b.backfillBudget = 50 * time.Millisecond
		}

		b.processBackgroundTasks(ctx, stop, cursor)

		// Always wake WaitForSequence waiters so they can re-check progress.
		// Without this, a waiter that enters Wait() between the last
		// NotifyProgress (inside processLogs) and the next tick would miss
		// the broadcast and block until new logs arrive.
		b.readStore.NotifyProgress()
	}
}
