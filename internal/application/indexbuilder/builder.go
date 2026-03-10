package indexbuilder

import (
	"context"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.opentelemetry.io/otel/metric"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/signal"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
)

// DefaultBatchSize is the default number of log entries per bbolt write
// transaction. Can be overridden via --read-index-batch-size.
const DefaultBatchSize = 1000

// Proposer proposes Raft commands to the cluster.
// Implemented by a thin adapter around *node.Node (bootstrap.NodeProposer).
type Proposer interface {
	ProposeOrders(orders ...*raftcmdpb.Order) error
}

// Builder tails the system log and populates the bbolt read store indexes.
// It runs as a background goroutine on ALL nodes (not just the leader).
// Progress is stored locally in bbolt (no Raft needed).
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

	// Reusable scratch objects to reduce allocations in the hot loop.
	kb       *dal.KeyBuilder
	wb       *readstore.WriteBatch
	accounts map[string]struct{}
}

// NewBuilder creates a new index builder.
// batchSize controls how many log entries are buffered per bbolt write transaction.
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
	b.w.Run(b.loop)
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
		metric.WithDescription("Last log sequence indexed in bbolt"),
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

func (b *Builder) loop(stop <-chan struct{}) {
	// Initialize index config cache from Pebble before processing any logs.
	b.initIndexConfig()

	cursor, err := b.readStore.LastIndexedSequence()
	if err != nil {
		b.logger.Errorf("Failed to read progress: %v", err)

		return
	}

	b.lastIndexedSeq.Store(cursor)

	// Seed pebble last sequence.
	if pebbleLast, err := query.ReadLastSequence(b.pebbleStore); err == nil {
		b.pebbleLastSeq.Store(pebbleLast)
	}

	// Log both the bbolt cursor and the Pebble last sequence for diagnostics.
	pebbleLast, _ := query.ReadLastSequence(b.pebbleStore)
	b.logger.WithFields(map[string]any{
		"cursor":     cursor,
		"pebbleLast": pebbleLast,
		"gap":        int64(pebbleLast) - int64(cursor),
		"backfills":  len(b.backfillTasks),
	}).Infof("Index builder started")

	// Initial catch-up (unbounded — backfills haven't started yet).
	// Use a larger batch size to reduce fsync overhead, and strip BUILDING
	// indexes from the config since their ranges will be covered by backfill
	// tasks. This avoids millions of redundant bbolt writes.
	prevCursor := cursor
	savedBatchSize := b.batchSize
	b.batchSize = max(b.batchSize, 10_000)
	restoreIndexes := b.stripBuildingIndexes()

	if cursor, err = b.processLogs(cursor, time.Time{}); err != nil {
		b.logger.Errorf("Error during initial catch-up: %v", err)
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

	b.processBackgroundTasks(stop, cursor)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			b.logger.Infof("Index builder stopped")

			return
		case <-b.notifications.LogCommitted.C():
		case <-ticker.C:
		}

		// Fast path: skip Pebble iterator + bbolt transaction when the FSM
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
			if cursor, err = b.processLogs(cursor, logDeadline); err != nil {
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

		b.processBackgroundTasks(stop, cursor)
	}
}

// persistProgress writes the index builder's cursor to bbolt within an
// existing write transaction.
func (b *Builder) persistProgress(tx *bolt.Tx, seq uint64) error {
	return b.readStore.WriteProgress(tx, seq)
}
