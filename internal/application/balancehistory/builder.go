package balancehistory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	domainhistory "github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/pkg/tailworker"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

const (
	// DefaultBatchSize bounds the number of complete proposals held in memory
	// and published in one immutable level-zero history segment.
	DefaultBatchSize = 200
	// TickInterval retries transient source/store failures and guarantees
	// progress if a post-commit notification is ever missed. LogCommitted
	// notifications drive the normal low-latency path; the ticker is only a
	// correctness backstop.
	TickInterval = 200 * time.Millisecond
	// DefaultBackfillYield bounds shared-disk pressure during boot catch-up.
	// It applies only between bounded boot batches; steady-state ticker
	// processing never sleeps.
	DefaultBackfillYield = 5 * time.Millisecond
	// DefaultDurabilityInterval bounds the NoSync suffix which may be replayed
	// after process or power loss. Logical publication remains batch-atomic.
	DefaultDurabilityInterval = 5 * time.Second
)

// Builder tails the authoritative audit source into one local, rebuildable
// balance-history peer store. It runs independently on every replica and never
// participates in the FSM apply path.
type Builder struct {
	source        Source
	store         *balancehistorystore.Store
	notifications *signal.Notifications
	clusterID     string
	logger        logging.Logger
	meter         metric.Meter

	batchSize           int
	compactionThreshold int
	backfillYield       time.Duration
	durabilityInterval  time.Duration

	lastProcessedAuditSequence atomic.Uint64
	sourceHeadAuditSequence    atomic.Uint64
	sourceHeadLogSequence      atomic.Uint64
	lastDurableAuditSequence   atomic.Uint64
	durabilitySyncFailures     atomic.Uint64
	durabilitySyncUnhealthy    atomic.Bool
	rebuildFromGenesis         atomic.Bool
	rebuilding                 atomic.Bool
	sourceMissing              atomic.Bool
	ready                      atomic.Bool

	durabilityMu       sync.Mutex
	lastDurabilitySync time.Time
	lastDurabilityErr  error
	durabilitySync     func() error
	durabilityNow      func() time.Time
	processingMetrics  builderProcessingMetrics
	idempotencyProbe   idempotencyReductionProbe
	projectionLedgers  []string
	configurationBuild bool

	tw   *tailworker.TailWorker
	regs []metric.Registration
}

type builderProcessingMetrics struct {
	effects       metric.Int64Counter
	postings      metric.Int64Counter
	publications  metric.Int64Counter
	rebuilds      metric.Int64Counter
	resets        metric.Int64Counter
	batchDuration metric.Int64Histogram
	batchSize     metric.Int64Histogram
	publishLag    metric.Int64Histogram
}

// NewBuilder wires one asynchronous balance-history projection builder.
// Background maintenance owns logical segment compaction. backfillYield
// controls cooperative pauses between boot
// batches; values <= 0 select DefaultBackfillYield. durabilityInterval bounds
// asynchronous WAL durability; values <= 0 select DefaultDurabilityInterval.
// Notifications is optional because the ticker is the correctness
// backstop for no-log and failed proposals.
func NewBuilder(
	source Source,
	store *balancehistorystore.Store,
	notifications *signal.Notifications,
	clusterID string,
	logger logging.Logger,
	meter metric.Meter,
	batchSize int,
	compactionThreshold int,
	backfillYield time.Duration,
	durabilityInterval time.Duration,
) *Builder {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	if compactionThreshold <= 1 {
		compactionThreshold = balancehistorystore.DefaultSegmentCompactionThreshold
	}
	if backfillYield <= 0 {
		backfillYield = DefaultBackfillYield
	}
	if durabilityInterval <= 0 {
		durabilityInterval = DefaultDurabilityInterval
	}

	now := time.Now
	builder := &Builder{
		source:              source,
		store:               store,
		notifications:       notifications,
		clusterID:           clusterID,
		logger:              logger.WithFields(map[string]any{"cmp": "balance-history-builder"}),
		meter:               meter,
		batchSize:           batchSize,
		compactionThreshold: compactionThreshold,
		backfillYield:       backfillYield,
		durabilityInterval:  durabilityInterval,
		lastDurabilitySync:  now(),
		durabilityNow:       now,
	}
	if store != nil {
		builder.durabilitySync = store.SyncWAL
	}
	if meter != nil {
		processingMetrics, err := newBuilderProcessingMetrics(meter)
		if err != nil {
			builder.logger.Errorf("registering balance history processing metrics: %v", err)
		} else {
			builder.processingMetrics = processingMetrics
		}
	}

	return builder
}

func newBuilderProcessingMetrics(meter metric.Meter) (builderProcessingMetrics, error) {
	effects, err := meter.Int64Counter(
		"balancehistory.builder.effects.processed",
		metric.WithDescription("Normalized monetary effects published by the balance-history builder"),
		metric.WithUnit("{effect}"),
	)
	if err != nil {
		return builderProcessingMetrics{}, fmt.Errorf("creating balance history effects counter: %w", err)
	}
	postings, err := meter.Int64Counter(
		"balancehistory.builder.postings.processed",
		metric.WithDescription("Resolved postings published by the balance-history builder"),
		metric.WithUnit("{posting}"),
	)
	if err != nil {
		return builderProcessingMetrics{}, fmt.Errorf("creating balance history postings counter: %w", err)
	}
	publications, err := meter.Int64Counter(
		"balancehistory.builder.publications",
		metric.WithDescription("Atomic balance-history manifest publications"),
		metric.WithUnit("{publication}"),
	)
	if err != nil {
		return builderProcessingMetrics{}, fmt.Errorf("creating balance history publications counter: %w", err)
	}
	rebuilds, err := meter.Int64Counter(
		"balancehistory.builder.rebuilds",
		metric.WithDescription("Full-prefix balance-history rebuilds started"),
		metric.WithUnit("{rebuild}"),
	)
	if err != nil {
		return builderProcessingMetrics{}, fmt.Errorf("creating balance history rebuild counter: %w", err)
	}
	resets, err := meter.Int64Counter(
		"balancehistory.builder.resets",
		metric.WithDescription("Successful balance-history projection resets"),
		metric.WithUnit("{reset}"),
	)
	if err != nil {
		return builderProcessingMetrics{}, fmt.Errorf("creating balance history reset counter: %w", err)
	}
	batchDuration, err := meter.Int64Histogram(
		"balancehistory.builder.batch.duration",
		metric.WithDescription("End-to-end processing duration of a published balance-history batch"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 500, 1000, 5000, 10000, 50000, 100000,
			500000, 1000000, 5000000, 30000000,
		),
	)
	if err != nil {
		return builderProcessingMetrics{}, fmt.Errorf("creating balance history batch duration histogram: %w", err)
	}
	batchSize, err := meter.Int64Histogram(
		"balancehistory.builder.batch.proposals",
		metric.WithDescription("Complete audit proposals per balance-history publication"),
		metric.WithUnit("{proposal}"),
		metric.WithExplicitBucketBoundaries(1, 10, 50, 100, 200, 500, 1000),
	)
	if err != nil {
		return builderProcessingMetrics{}, fmt.Errorf("creating balance history batch proposal histogram: %w", err)
	}
	publishLag, err := meter.Int64Histogram(
		"balancehistory.builder.publish_lag",
		metric.WithDescription("Elapsed time from the latest audit entry in a published batch to its balance-history manifest publication"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(
			0, 10, 100, 1000, 5000, 30000, 300000, 3600000,
			86400000, 604800000, 2592000000, 15552000000,
		),
	)
	if err != nil {
		return builderProcessingMetrics{}, fmt.Errorf("creating balance history publish lag histogram: %w", err)
	}

	return builderProcessingMetrics{
		effects:       effects,
		postings:      postings,
		publications:  publications,
		rebuilds:      rebuilds,
		resets:        resets,
		batchDuration: batchDuration,
		batchSize:     batchSize,
		publishLag:    publishLag,
	}, nil
}

func (m builderProcessingMetrics) recordPublication(
	ctx context.Context,
	startedAt time.Time,
	proposalCount int,
	effectCount int,
) {
	if m.effects != nil {
		m.effects.Add(ctx, int64(effectCount))
	}
	if m.postings != nil {
		// Every normalized posting is represented by exactly two effects: the
		// source output and destination input.
		m.postings.Add(ctx, int64(effectCount/2))
	}
	if m.publications != nil {
		m.publications.Add(ctx, 1)
	}
	if m.batchDuration != nil {
		m.batchDuration.Record(ctx, time.Since(startedAt).Microseconds())
	}
	if m.batchSize != nil {
		m.batchSize.Record(ctx, int64(proposalCount))
	}
}

func (m builderProcessingMetrics) recordRebuild() {
	ctx := context.Background()
	// One rebuild transition corresponds to exactly one successful projection
	// reset and one rebuild start. Keep both counters paired and single-shot.
	if m.resets != nil {
		m.resets.Add(ctx, 1)
	}
	if m.rebuilds != nil {
		m.rebuilds.Add(ctx, 1)
	}
}

// Start launches the tail loop and registers the standard progress/head/lag
// gauges. Manifest resumability and the authoritative source relationship are
// checked by boot before Ready can become true.
func (b *Builder) Start() {
	b.ready.Store(false)
	if b.meter != nil {
		if registration, err := tailworker.RegisterTailGauges(
			b.meter,
			"balancehistory.builder",
			"audit",
			&b.lastProcessedAuditSequence,
			&b.sourceHeadAuditSequence,
		); err == nil {
			b.regs = append(b.regs, registration)
		} else {
			b.logger.Errorf("registering balance history builder metrics: %v", err)
		}
		if registration, err := b.registerDurabilityMetrics(); err == nil {
			b.regs = append(b.regs, registration)
		} else {
			b.logger.Errorf("registering balance history durability metrics: %v", err)
		}
	}

	var wake <-chan struct{}
	if b.notifications != nil {
		wake = b.notifications.LogCommitted.C()
	}

	b.tw = tailworker.New(tailworker.Config{
		Name:   "balance-history-builder",
		Logger: b.logger,
		Ticker: TickInterval,
		Wake:   wake,
		Boot:   b.boot,
		Tick:   b.tick,
	})
	b.tw.Start()
}

// Stop waits for the tail loop, makes the final asynchronous suffix durable,
// then unregisters metrics. The runtime closes the store only after this
// method returns.
func (b *Builder) Stop() error {
	b.ready.Store(false)
	defer b.ready.Store(false)

	if b.tw != nil {
		b.tw.Stop()
		b.tw = nil
	}
	syncErr := b.syncDurability(true)
	if syncErr != nil {
		// Keep the failure instruments registered while the runtime remains
		// open for a retry of the final WAL barrier.
		return syncErr
	}

	unregisterErrors := make([]error, 0, len(b.regs))
	for _, registration := range b.regs {
		unregisterErrors = append(unregisterErrors, registration.Unregister())
	}
	b.regs = nil

	return errors.Join(unregisterErrors...)
}

func (b *Builder) registerDurabilityMetrics() (metric.Registration, error) {
	durableSequence, err := b.meter.Int64ObservableGauge(
		"balancehistory.builder.last_durable_audit_sequence",
		metric.WithDescription("Latest balance-history audit watermark covered by a successful WAL barrier"),
		metric.WithUnit("{sequence}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history durable sequence gauge: %w", err)
	}
	syncFailures, err := b.meter.Int64ObservableCounter(
		"balancehistory.builder.durability_sync_failures",
		metric.WithDescription("Failed balance-history WAL durability barriers"),
		metric.WithUnit("{failure}"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history durability failure counter: %w", err)
	}
	syncError, err := b.meter.Int64ObservableGauge(
		"balancehistory.builder.durability_sync_error",
		metric.WithDescription("Whether the latest required balance-history WAL barrier failed"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balance history durability error gauge: %w", err)
	}

	return b.meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		observer.ObserveInt64(durableSequence, int64(b.lastDurableAuditSequence.Load()))
		observer.ObserveInt64(syncFailures, int64(b.durabilitySyncFailures.Load()))
		if b.durabilitySyncUnhealthy.Load() {
			observer.ObserveInt64(syncError, 1)
		} else {
			observer.ObserveInt64(syncError, 0)
		}

		return nil
	}, durableSequence, syncFailures, syncError)
}

// LastProcessedAuditSequence returns the last atomically published audit
// watermark without reading Pebble.
func (b *Builder) LastProcessedAuditSequence() uint64 {
	return b.lastProcessedAuditSequence.Load()
}

// SourceHeadAuditSequence returns the most recently sampled source head.
func (b *Builder) SourceHeadAuditSequence() uint64 {
	return b.sourceHeadAuditSequence.Load()
}

// LastDurableAuditSequence is the greatest published audit watermark covered
// by a successful WAL barrier in this process.
func (b *Builder) LastDurableAuditSequence() uint64 {
	return b.lastDurableAuditSequence.Load()
}

// LastDurabilityError returns the most recent WAL barrier failure. A later
// successful retry clears it.
func (b *Builder) LastDurabilityError() error {
	b.durabilityMu.Lock()
	defer b.durabilityMu.Unlock()

	return b.lastDurabilityErr
}

// Ready reports whether this process has reconciled the persisted projection
// with a successfully read authoritative source head, reached that head, made
// the repair prefix durable, and completed the required structural checks.
// It is deliberately in-memory: every process restart must prove the source
// relationship again before the provider may expose persisted history.
func (b *Builder) Ready() bool {
	return b.ready.Load()
}

func (b *Builder) syncDurability(force bool) error {
	b.durabilityMu.Lock()
	defer b.durabilityMu.Unlock()

	if b.durabilitySync == nil {
		return errors.New("balance history durability sync is unavailable")
	}
	now := b.durabilityNow()
	if !force && now.Sub(b.lastDurabilitySync) < b.durabilityInterval {
		return nil
	}
	if err := b.durabilitySync(); err != nil {
		wrapped := fmt.Errorf("syncing balance history durability prefix: %w", err)
		b.lastDurabilityErr = wrapped
		b.durabilitySyncFailures.Add(1)
		b.durabilitySyncUnhealthy.Store(true)

		return wrapped
	}

	b.lastDurabilitySync = now
	b.lastDurabilityErr = nil
	b.durabilitySyncUnhealthy.Store(false)
	b.lastDurableAuditSequence.Store(b.lastProcessedAuditSequence.Load())

	return nil
}

// boot restores the locally validated manifest and reducer state, repairs a
// primary rollback by resetting the projection, then drains bounded snapshots
// until it reaches one observed source head. Projection reads remain closed
// until the rebuilt prefix is complete and durable.
func (b *Builder) boot(ctx context.Context) error {
	b.ready.Store(false)
	if b.source == nil {
		return errors.New("balance history builder source is nil")
	}
	if b.store == nil {
		return errors.New("balance history builder store is nil")
	}
	if b.clusterID == "" {
		return errors.New("balance history builder cluster id is empty")
	}
	manifest, err := b.store.Manifest()
	if err != nil {
		if handledErr := b.handleBuildError(fmt.Errorf("reading balance history manifest: %w", err)); handledErr != nil {
			return b.swallowBootError(handledErr)
		}
		manifest, err = b.store.Manifest()
		if err != nil {
			return b.swallowBootError(fmt.Errorf("reading balance history manifest after recovery reset: %w", err))
		}
	}
	b.projectionLedgers = append(b.projectionLedgers[:0], manifest.Ledgers...)
	if _, err := reducerFromManifest(manifest); err != nil {
		return b.swallowBootError(err)
	}
	if err := b.restoreRebuildState(manifest); err != nil {
		return b.swallowBootError(err)
	}
	manifest, err = b.store.Manifest()
	if err != nil {
		return b.swallowBootError(fmt.Errorf("reading balance history manifest after recovery-state restore: %w", err))
	}
	if !b.rebuilding.Load() && manifest.Version > 0 && !manifest.SourceComplete {
		// A durable partial projection cannot become complete by tailing its
		// suffix. The next successful source probe rebuilds from genesis while
		// reads remain BUILDING (or SOURCE_MISSING when that marker exists).
		b.rebuildFromGenesis.Store(true)
	}
	b.lastProcessedAuditSequence.Store(manifest.AuditWatermark)
	b.lastDurableAuditSequence.Store(manifest.AuditWatermark)

	for {
		caughtUp, err := b.processOnce(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			if syncErr := b.syncDurability(false); syncErr != nil {
				err = errors.Join(err, syncErr)
			}

			return b.swallowBootError(err)
		}
		if caughtUp {
			if err := b.syncDurability(true); err != nil {
				return b.swallowBootError(err)
			}
			if err := b.compactBeforeReady(ctx); err != nil {
				return b.swallowBootError(err)
			}
			if err := b.completeCaughtUpHistory(ctx); err != nil {
				return b.swallowBootError(err)
			}
			if err := b.markReadyAfterReconciliation(); err != nil {
				return b.swallowBootError(err)
			}

			return nil
		}
		if err := b.syncDurability(false); err != nil {
			return b.swallowBootError(err)
		}
		if err := b.waitForBackfillYield(ctx); err != nil {
			return err
		}
	}
}

func (b *Builder) waitForBackfillYield(ctx context.Context) error {
	timer := time.NewTimer(b.backfillYield)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (b *Builder) tick(ctx context.Context) error {
	for {
		caughtUp, buildErr := b.processOnce(ctx)
		repairing := b.rebuilding.Load() || b.sourceMissing.Load()
		readyAfterBuild := b.ready.Load()
		forceSync := caughtUp && (!readyAfterBuild || repairing)
		syncErr := b.syncDurability(forceSync)
		if buildErr != nil {
			b.ready.Store(false)

			return errors.Join(b.handleBuildError(buildErr), syncErr)
		}
		if syncErr != nil {
			b.ready.Store(false)

			return syncErr
		}
		if !caughtUp {
			// One wake drains every already-visible bounded source batch. This
			// removes the old batchSize/TickInterval throughput ceiling while
			// keeping each publication and allocation bounded by one batch.
			b.ready.Store(false)

			continue
		}
		if readyAfterBuild && !repairing {
			return nil
		}
		if err := b.compactBeforeReady(ctx); err != nil {
			b.ready.Store(false)

			return err
		}
		if err := b.completeCaughtUpHistory(ctx); err != nil {
			b.ready.Store(false)

			return err
		}
		if err := b.markReadyAfterReconciliation(); err != nil {
			b.ready.Store(false)

			return err
		}

		return nil
	}
}

// compactBeforeReady prevents a boot or repair backfill from exposing a
// manifest with an arbitrarily large segment fan-out. Compaction is entirely
// local and rebuildable; readiness opens only after no level has enough runs
// for another configured merge.
func (b *Builder) compactBeforeReady(ctx context.Context) error {
	for {
		compacted, err := b.store.CompactContext(ctx, b.compactionThreshold)
		if err != nil {
			return fmt.Errorf("compacting balance history before readiness: %w", err)
		}
		if !compacted {
			return nil
		}
	}
}

// swallowBootError records a fail-closed readiness state but deliberately
// returns nil for non-cancellation errors. tailworker aborts permanently when
// Boot returns an error, so all source and local-I/O failures must fall through
// to the steady 200 ms fallback retry loop.
func (b *Builder) swallowBootError(err error) error {
	b.ready.Store(false)
	if errors.Is(err, context.Canceled) {
		return err
	}
	if handledErr := b.handleBuildError(err); handledErr != nil {
		b.logger.Errorf("balance history builder boot will retry: %v", handledErr)
	}

	return nil
}

// handleBuildError translates proven data failures into persistent readiness
// markers. Successfully persisted markers consume the error: the API exposes
// the fail-closed state and tailworker keeps retrying without treating it as a
// fatal lifecycle error. Unclassified operational failures are returned so the
// worker logs them, then retries on the next wake/tick.
func (b *Builder) handleBuildError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return err
	}
	b.ready.Store(false)

	var (
		quarantined        *balancehistorystore.ErrQuarantined
		corrupt            *balancehistorystore.ErrCorrupt
		unsupportedFormat  *balancehistorystore.ErrUnsupportedFormat
		unsupportedReduce  *balancehistorystore.ErrUnsupportedReducer
		sourceGap          *balancehistorystore.ErrSourceGap
		invalidSource      *ErrSourceInvalid
		missingSource      *ErrSourceMissing
		missingStoreSource *balancehistorystore.ErrSourceMissing
	)
	if errors.As(err, &quarantined) {
		return b.beginCorruptionRebuild(err)
	}
	if errors.As(err, &corrupt) ||
		errors.As(err, &unsupportedFormat) ||
		errors.As(err, &unsupportedReduce) ||
		errors.As(err, &sourceGap) ||
		errors.As(err, &invalidSource) {
		return b.beginCorruptionRebuild(err)
	}
	if errors.As(err, &missingSource) || errors.As(err, &missingStoreSource) {
		// A missing range invalidates the assumption that the current suffix is
		// enough. Once the source answers again, rebuild from audit sequence 1.
		b.rebuildFromGenesis.Store(true)
		if b.rebuilding.Load() {
			// REBUILDING already has higher fail-closed priority than
			// SOURCE_MISSING. Keep that durable marker and retry the source.
			return nil
		}
		if markErr := b.store.MarkSourceMissing(err.Error()); markErr != nil {
			return errors.Join(err, fmt.Errorf("marking balance history source missing: %w", markErr))
		}
		b.sourceMissing.Store(true)

		return nil
	}

	return err
}

func (b *Builder) beginCorruptionRebuild(cause error) error {
	b.ready.Store(false)
	if quarantineErr := b.store.Quarantine(cause.Error()); quarantineErr != nil {
		return errors.Join(cause, fmt.Errorf("quarantining balance history: %w", quarantineErr))
	}
	if resetErr := b.store.ResetForConfiguration(b.projectionLedgers); resetErr != nil {
		return errors.Join(cause, fmt.Errorf("resetting quarantined balance history for rebuild: %w", resetErr))
	}
	b.processingMetrics.recordRebuild()
	b.rebuilding.Store(true)
	b.sourceMissing.Store(false)
	b.rebuildFromGenesis.Store(false)
	b.lastProcessedAuditSequence.Store(0)
	b.lastDurableAuditSequence.Store(0)

	return nil
}

// restoreRebuildState restores fail-closed markers after process restart. A
// quarantine or interrupted rebuild is restarted from genesis.
func (b *Builder) restoreRebuildState(manifest balancehistorystore.Manifest) error {
	view, err := b.store.OpenView(manifest.LogWatermark)
	if err == nil {
		return view.Close()
	}

	var missing *balancehistorystore.ErrSourceMissing
	if errors.As(err, &missing) {
		b.ready.Store(false)
		b.sourceMissing.Store(true)
		b.rebuildFromGenesis.Store(true)

		return nil
	}
	var quarantined *balancehistorystore.ErrQuarantined
	if !errors.As(err, &quarantined) {
		// BUILDING is a normal non-corruption state.
		return nil
	}

	return b.beginCorruptionRebuild(err)
}

func (b *Builder) completeCaughtUpHistory(_ context.Context) error {
	manifest, err := b.store.Manifest()
	if err != nil {
		return fmt.Errorf("reading caught-up balance history manifest: %w", err)
	}
	if !manifest.SourceComplete {
		return nil
	}
	headAudit := b.sourceHeadAuditSequence.Load()
	headLog := b.sourceHeadLogSequence.Load()
	if manifest.AuditWatermark < headAudit || manifest.LogWatermark < headLog {
		return fmt.Errorf(
			"balance history cannot become ready before pinned head: manifest audit/log=(%d,%d), head=(%d,%d)",
			manifest.AuditWatermark,
			manifest.LogWatermark,
			headAudit,
			headLog,
		)
	}
	if !b.rebuilding.Load() && !b.sourceMissing.Load() {
		return nil
	}
	if b.rebuilding.Load() {
		if err := b.store.CompleteRebuild(headAudit, headLog); err != nil {
			return fmt.Errorf("completing balance history rebuild: %w", err)
		}
		b.rebuilding.Store(false)
		b.configurationBuild = false

		return nil
	}
	if b.sourceMissing.Load() {
		if err := b.store.ClearFailure(headAudit, headLog); err != nil {
			return fmt.Errorf("marking caught-up balance history ready: %w", err)
		}
		b.sourceMissing.Store(false)
	}

	return nil
}

func (b *Builder) markReadyAfterReconciliation() error {
	manifest, err := b.store.Manifest()
	if err != nil {
		return fmt.Errorf("reading reconciled balance history manifest: %w", err)
	}
	headAudit := b.sourceHeadAuditSequence.Load()
	headLog := b.sourceHeadLogSequence.Load()
	if !manifest.SourceComplete {
		return &balancehistorystore.ErrBuilding{
			Current: manifest.LogWatermark,
			Target:  headLog,
		}
	}
	if manifest.AuditWatermark != headAudit || manifest.LogWatermark != headLog {
		return fmt.Errorf(
			"balance history reconciliation is not at source head: manifest audit/log=(%d,%d), head=(%d,%d)",
			manifest.AuditWatermark,
			manifest.LogWatermark,
			headAudit,
			headLog,
		)
	}
	if b.rebuilding.Load() || b.sourceMissing.Load() || b.rebuildFromGenesis.Load() {
		return errors.New("balance history reconciliation still has a pending repair state")
	}

	b.ready.Store(true)
	reachAntithesisBalanceHistoryReconciled(manifest.Version, headAudit, headLog)

	return nil
}

// processOnce owns one source snapshot and publishes at most one bounded
// proposal batch. Every verification/reduction step uses temporary state; an
// error before Publish leaves both the manifest and the in-memory hints at the
// prior durable cursor.
func (b *Builder) processOnce(ctx context.Context) (bool, error) {
	startedAt := time.Now()
	publishedProposals := 0
	publishedEffects := 0
	defer func() {
		if publishedProposals > 0 {
			b.processingMetrics.recordPublication(
				ctx,
				startedAt,
				publishedProposals,
				publishedEffects,
			)
		}
	}()

	manifest, err := b.store.Manifest()
	if err != nil {
		return false, fmt.Errorf("reading balance history manifest: %w", err)
	}

	repairFromGenesis := b.rebuildFromGenesis.Load()
	if repairFromGenesis && (manifest.Version > 0 || manifest.AuditWatermark > 0 || len(manifest.Segments) > 0) {
		// A destructive reset requires a successful source probe first. This is
		// the exceptional repair path; steady-state processing obtains the head
		// from Source.Read's single pinned snapshot below.
		head, err := b.source.Head(ctx)
		if err != nil {
			return false, fmt.Errorf("reading balance history source head before repair: %w", err)
		}
		if head.AuditSequence > 0 && len(head.AuditHash) == 0 {
			return false, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"source audit head %d has no hash",
				head.AuditSequence,
			)}
		}
		b.sourceHeadAuditSequence.Store(head.AuditSequence)
		b.sourceHeadLogSequence.Store(head.LogSequence)
		b.ready.Store(false)
		// Reset only after a successful source probe. This preserves the
		// persistent SOURCE_MISSING marker while the source is still entirely
		// unreachable, and invalidates all old views before rebuild begins.
		reset := b.store.Reset
		switch {
		case b.rebuilding.Load():
			reset = func() error { return b.store.ResetForConfiguration(b.projectionLedgers) }
		case b.sourceMissing.Load():
			reset = b.store.ResetForSourceRepair
		}
		if err := reset(); err != nil {
			return false, fmt.Errorf("resetting balance history before source repair rebuild: %w", err)
		}
		b.rebuildFromGenesis.Store(false)
		b.processingMetrics.recordRebuild()
		manifest, err = b.store.Manifest()
		if err != nil {
			return false, fmt.Errorf("reading reset balance history manifest: %w", err)
		}
		b.projectionLedgers = append(b.projectionLedgers[:0], manifest.Ledgers...)
		b.lastProcessedAuditSequence.Store(0)
		b.lastDurableAuditSequence.Store(0)
		b.idempotencyProbe.Reset()
	}

	reducer, err := reducerFromManifest(manifest)
	if err != nil {
		return false, err
	}
	if manifest.AuditWatermark > 0 && len(manifest.AuditHash) == 0 {
		return false, &ErrSourceInvalid{Detail: fmt.Sprintf(
			"manifest audit watermark %d has no predecessor hash",
			manifest.AuditWatermark,
		)}
	}

	after := positionFromManifest(manifest)
	batch, err := b.source.Read(ctx, after, b.batchSize)
	if err != nil {
		if recovered, recoveryErr := b.recoverSourceRollback(ctx, manifest, err); recoveryErr != nil {
			return false, recoveryErr
		} else if recovered {
			return false, nil
		}

		return false, fmt.Errorf("reading balance history source after audit %d: %w", after.AuditSequence, err)
	}
	if repairFromGenesis {
		// For an already-empty repair store, the successful Read itself is the
		// non-destructive source probe. Do not reset the batch just read on the
		// following iteration.
		b.rebuildFromGenesis.Store(false)
	}
	b.sourceHeadAuditSequence.Store(batch.Head.AuditSequence)
	b.sourceHeadLogSequence.Store(batch.Head.LogSequence)
	if batch.Head.AuditSequence > 0 && len(batch.Head.AuditHash) == 0 {
		return false, &ErrSourceInvalid{Detail: fmt.Sprintf(
			"source audit head %d has no hash",
			batch.Head.AuditSequence,
		)}
	}
	manifest, reset, err := b.resetIfRolledBack(manifest, batch.Head)
	if err != nil {
		return false, err
	}
	if reset {
		b.resetInMemoryCursor()

		return false, nil
	}

	if len(batch.Proposals) == 0 {
		if !batch.Next.equal(after) {
			return false, &ErrSourceInvalid{Detail: "empty source batch advanced its cursor"}
		}
		if batch.Head.AuditSequence > after.AuditSequence || batch.Head.LogSequence > after.LogSequence {
			return false, &ErrSourceMissing{Detail: fmt.Sprintf(
				"source returned no proposal after audit %d before head %d",
				after.AuditSequence,
				batch.Head.AuditSequence,
			)}
		}
		if !manifest.SourceComplete && after.equal(Position{}) && batch.Head.equal(after) {
			nextManifest, err := b.store.Publish(balancehistorystore.Publication{
				Coverage:     balancehistorystore.Coverage{SourceComplete: true},
				ReducerState: reducer.State(),
			})
			if err != nil {
				return false, fmt.Errorf("publishing complete empty balance history source: %w", err)
			}
			b.lastProcessedAuditSequence.Store(nextManifest.AuditWatermark)
		}

		return true, nil
	}

	effects, next, reducerState, err := reduceVerifiedBatch(
		b.clusterID,
		reducer,
		after,
		batch,
	)
	if err != nil {
		return false, err
	}
	caughtUp := next.AuditSequence >= batch.Head.AuditSequence
	configurationChanged := !slices.Equal(reducerState.Enabled, manifest.Ledgers)
	if configurationChanged && (!b.configurationBuild || caughtUp) {
		if err := b.beginConfigurationRebuild(reducerState.Enabled); err != nil {
			return false, err
		}

		return false, nil
	}

	sourceComplete := manifest.SourceComplete
	if !sourceComplete && manifest.AuditWatermark == 0 && batch.Proposals[0].Entry.GetSequence() == 1 {
		sourceComplete = true
	}

	nextManifest, err := b.store.Publish(balancehistorystore.Publication{
		Effects: effects,
		Coverage: balancehistorystore.Coverage{
			AuditSequence:  next.AuditSequence,
			LogSequence:    next.LogSequence,
			AuditHash:      append([]byte(nil), next.AuditHash...),
			SourceComplete: sourceComplete,
		},
		ReducerState: reducerState,
	})
	if err != nil {
		return false, fmt.Errorf("publishing balance history through audit %d: %w", next.AuditSequence, err)
	}
	b.idempotencyProbe.RecordPublished(batch.Proposals)

	b.lastProcessedAuditSequence.Store(nextManifest.AuditWatermark)
	publishedProposals = len(batch.Proposals)
	publishedEffects = len(effects)
	b.observePublishLag(ctx, batch.Proposals[len(batch.Proposals)-1].Entry)

	return caughtUp, nil
}

func (b *Builder) recoverSourceRollback(
	ctx context.Context,
	manifest balancehistorystore.Manifest,
	readErr error,
) (bool, error) {
	var (
		missing *ErrSourceMissing
		invalid *ErrSourceInvalid
	)
	if !errors.As(readErr, &missing) && !errors.As(readErr, &invalid) {
		return false, nil
	}
	head, err := b.source.Head(ctx)
	if err != nil {
		return false, nil
	}
	b.sourceHeadAuditSequence.Store(head.AuditSequence)
	b.sourceHeadLogSequence.Store(head.LogSequence)
	_, reset, err := b.resetIfRolledBack(manifest, head)
	if err != nil {
		return false, errors.Join(readErr, err)
	}
	if !reset {
		return false, nil
	}
	b.resetInMemoryCursor()

	return true, nil
}

func (b *Builder) resetInMemoryCursor() {
	b.lastProcessedAuditSequence.Store(0)
	b.lastDurableAuditSequence.Store(0)
	b.idempotencyProbe.Reset()
}

func (b *Builder) beginConfigurationRebuild(ledgers []string) error {
	ledgers = append([]string(nil), ledgers...)
	slices.Sort(ledgers)
	b.ready.Store(false)
	if err := b.store.ResetForConfiguration(ledgers); err != nil {
		return fmt.Errorf("resetting balance history for client configuration: %w", err)
	}
	b.processingMetrics.recordRebuild()
	b.projectionLedgers = ledgers
	b.configurationBuild = true
	b.rebuilding.Store(true)
	b.sourceMissing.Store(false)
	b.rebuildFromGenesis.Store(false)
	b.lastProcessedAuditSequence.Store(0)
	b.lastDurableAuditSequence.Store(0)
	b.idempotencyProbe.Reset()

	return nil
}

func (b *Builder) observePublishLag(ctx context.Context, entry *auditpb.AuditEntry) {
	if b.processingMetrics.publishLag == nil || entry == nil || entry.GetTimestamp() == nil {
		return
	}

	lag := max(b.durabilityNow().Sub(entry.GetTimestamp().AsTime().Time), 0)
	b.processingMetrics.publishLag.Record(ctx, lag.Milliseconds())
}

func (b *Builder) resetIfRolledBack(
	manifest balancehistorystore.Manifest,
	head Position,
) (balancehistorystore.Manifest, bool, error) {
	rolledBack := head.AuditSequence < manifest.AuditWatermark ||
		head.LogSequence < manifest.LogWatermark
	diverged := head.AuditSequence == manifest.AuditWatermark &&
		head.AuditSequence > 0 &&
		len(head.AuditHash) > 0 &&
		len(manifest.AuditHash) > 0 &&
		!bytes.Equal(head.AuditHash, manifest.AuditHash)
	if !rolledBack && !diverged {
		return manifest, false, nil
	}
	b.ready.Store(false)
	detail := fmt.Sprintf(
		"source rollback detected: manifest audit/log=(%d,%d), source audit/log=(%d,%d), hashDiverged=%t",
		manifest.AuditWatermark,
		manifest.LogWatermark,
		head.AuditSequence,
		head.LogSequence,
		diverged,
	)
	if !b.rebuilding.Load() {
		if err := b.store.MarkSourceMissing(detail); err != nil {
			return balancehistorystore.Manifest{}, false, fmt.Errorf("marking balance history unavailable before rollback reset: %w", err)
		}
		b.sourceMissing.Store(true)
	}

	b.logger.WithFields(map[string]any{
		"manifestAudit": manifest.AuditWatermark,
		"manifestLog":   manifest.LogWatermark,
		"sourceAudit":   head.AuditSequence,
		"sourceLog":     head.LogSequence,
		"hashDiverged":  diverged,
	}).Infof("balance history source rolled back; resetting and rebuilding local projection")

	reset := b.store.Reset
	switch {
	case b.rebuilding.Load():
		reset = b.store.ResetForRebuild
	case b.sourceMissing.Load():
		reset = b.store.ResetForSourceRepair
	}
	if err := reset(); err != nil {
		return balancehistorystore.Manifest{}, false, fmt.Errorf("resetting balance history after source rollback: %w", err)
	}
	b.processingMetrics.recordRebuild()
	resetManifest, err := b.store.Manifest()
	if err != nil {
		return balancehistorystore.Manifest{}, false, fmt.Errorf("reading reset balance history manifest: %w", err)
	}

	return resetManifest, true, nil
}

func positionFromManifest(manifest balancehistorystore.Manifest) Position {
	return Position{
		AuditSequence: manifest.AuditWatermark,
		LogSequence:   manifest.LogWatermark,
		AuditHash:     append([]byte(nil), manifest.AuditHash...),
	}
}

func reducerFromManifest(manifest balancehistorystore.Manifest) (*domainhistory.Reducer, error) {
	reducer, err := domainhistory.NewReducerFromState(manifest.ReducerState)
	if err != nil {
		return nil, &ErrSourceInvalid{Detail: fmt.Sprintf(
			"restoring reducer at audit %d: %v",
			manifest.AuditWatermark,
			err,
		)}
	}
	state := manifest.ReducerState
	if state.HasLast && (state.Last.AuditSequence > manifest.AuditWatermark || state.Last.LogSequence > manifest.LogWatermark) {
		return nil, &ErrSourceInvalid{Detail: fmt.Sprintf(
			"reducer position (%d,%d) exceeds manifest watermarks (%d,%d)",
			state.Last.AuditSequence,
			state.Last.LogSequence,
			manifest.AuditWatermark,
			manifest.LogWatermark,
		)}
	}
	reducer.SetProjectedLedgers(manifest.Ledgers)

	return reducer, nil
}

func reduceVerifiedBatch(
	clusterID string,
	reducer *domainhistory.Reducer,
	after Position,
	batch Batch,
) ([]domainhistory.Effect, Position, domainhistory.State, error) {
	effects := make([]domainhistory.Effect, 0)
	next := Position{
		AuditSequence: after.AuditSequence,
		LogSequence:   after.LogSequence,
		AuditHash:     append([]byte(nil), after.AuditHash...),
	}
	lastHash := append([]byte(nil), after.AuditHash...)
	var hashBuffer []byte
	generators := make(map[uint32]processing.HashGenerator, 2)

	for proposalIndex, proposal := range batch.Proposals {
		entry := proposal.Entry
		expectedAuditSequence := next.AuditSequence + 1
		if entry == nil || entry.GetSequence() != expectedAuditSequence {
			return nil, after, domainhistory.State{}, &ErrSourceMissing{Detail: fmt.Sprintf(
				"proposal %d has audit sequence %d, want %d",
				proposalIndex,
				entry.GetSequence(),
				expectedAuditSequence,
			)}
		}
		if len(proposal.Items) != len(proposal.Logs) || uint32(len(proposal.Items)) != entry.GetOrderCount() {
			return nil, after, domainhistory.State{}, &ErrSourceMissing{Detail: fmt.Sprintf(
				"audit sequence %d has %d items, %d logs, and declared order count %d",
				entry.GetSequence(),
				len(proposal.Items),
				len(proposal.Logs),
				entry.GetOrderCount(),
			)}
		}

		hashSlices, err := auditHashSlices(entry, proposal.Items)
		if err != nil {
			return nil, after, domainhistory.State{}, err
		}
		generator := generators[entry.GetHashVersion()]
		if generator == nil {
			generator = processing.NewHashGenerator(commonpb.HashAlgorithm(entry.GetHashVersion()), clusterID)
			generators[entry.GetHashVersion()] = generator
		}
		var computedHash []byte
		hashBuffer, computedHash = generator.Compute(hashBuffer, lastHash, hashSlices)
		if !bytes.Equal(computedHash, entry.GetHash()) {
			return nil, after, domainhistory.State{}, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"audit hash chain mismatch at sequence %d: stored=%x computed=%x",
				entry.GetSequence(),
				entry.GetHash(),
				computedHash,
			)}
		}

		minLogSequence, maxLogSequence, err := freshLogRange(entry, next.LogSequence)
		if err != nil {
			return nil, after, domainhistory.State{}, err
		}
		seenFresh := make(map[uint64]struct{})
		for itemIndex, item := range proposal.Items {
			if item == nil || item.GetOrderIndex() != uint32(itemIndex) {
				return nil, after, domainhistory.State{}, &ErrSourceInvalid{Detail: fmt.Sprintf(
					"audit sequence %d has invalid item at position %d",
					entry.GetSequence(),
					itemIndex,
				)}
			}
			logSequence := item.GetLogSequence()
			if entry.GetFailure() != nil && logSequence != 0 {
				return nil, after, domainhistory.State{}, &ErrSourceInvalid{Detail: fmt.Sprintf(
					"failed audit sequence %d item %d references log %d",
					entry.GetSequence(),
					itemIndex,
					logSequence,
				)}
			}
			if maxLogSequence == 0 && logSequence != 0 {
				return nil, after, domainhistory.State{}, &ErrSourceInvalid{Detail: fmt.Sprintf(
					"audit sequence %d item %d references log %d with an empty fresh range",
					entry.GetSequence(),
					itemIndex,
					logSequence,
				)}
			}
			if maxLogSequence > 0 && logSequence > maxLogSequence {
				return nil, after, domainhistory.State{}, &ErrSourceInvalid{Detail: fmt.Sprintf(
					"audit sequence %d item %d references log %d beyond fresh range maximum %d",
					entry.GetSequence(),
					itemIndex,
					logSequence,
					maxLogSequence,
				)}
			}
			if logSequence == 0 && proposal.Logs[itemIndex] != nil {
				return nil, after, domainhistory.State{}, &ErrSourceInvalid{Detail: fmt.Sprintf(
					"audit sequence %d item %d has a log payload without a log sequence",
					entry.GetSequence(),
					itemIndex,
				)}
			}
			fresh := minLogSequence > 0 && logSequence >= minLogSequence && logSequence <= maxLogSequence
			if !fresh {
				continue
			}
			if proposal.Logs[itemIndex] == nil {
				return nil, after, domainhistory.State{}, &ErrSourceMissing{Detail: fmt.Sprintf(
					"audit sequence %d fresh log %d is missing",
					entry.GetSequence(),
					logSequence,
				)}
			}
			if _, exists := seenFresh[logSequence]; exists {
				return nil, after, domainhistory.State{}, &ErrSourceInvalid{Detail: fmt.Sprintf(
					"audit sequence %d fresh log %d appears more than once",
					entry.GetSequence(),
					logSequence,
				)}
			}
			seenFresh[logSequence] = struct{}{}

			logEffects, err := reducer.Reduce(domainhistory.Position{
				AuditSequence: entry.GetSequence(),
				OrderIndex:    item.GetOrderIndex(),
				LogSequence:   logSequence,
			}, proposal.Logs[itemIndex])
			if err != nil {
				return nil, after, domainhistory.State{}, &ErrSourceInvalid{Detail: fmt.Sprintf(
					"reducing audit sequence %d item %d log %d: %v",
					entry.GetSequence(),
					itemIndex,
					logSequence,
					err,
				)}
			}
			effects = append(effects, logEffects...)
		}
		if minLogSequence > 0 && uint64(len(seenFresh)) != maxLogSequence-minLogSequence+1 {
			return nil, after, domainhistory.State{}, &ErrSourceMissing{Detail: fmt.Sprintf(
				"audit sequence %d does not contain every fresh log in [%d,%d]",
				entry.GetSequence(),
				minLogSequence,
				maxLogSequence,
			)}
		}

		next.AuditSequence = entry.GetSequence()
		if maxLogSequence > 0 {
			next.LogSequence = maxLogSequence
		}
		next.AuditHash = append(next.AuditHash[:0], entry.GetHash()...)
		lastHash = entry.GetHash()
	}

	if batch.Next.AuditSequence != next.AuditSequence || batch.Next.LogSequence != next.LogSequence {
		return nil, after, domainhistory.State{}, &ErrSourceInvalid{Detail: fmt.Sprintf(
			"source next position is (%d,%d), verified position is (%d,%d)",
			batch.Next.AuditSequence,
			batch.Next.LogSequence,
			next.AuditSequence,
			next.LogSequence,
		)}
	}
	if len(batch.Next.AuditHash) > 0 && !bytes.Equal(batch.Next.AuditHash, next.AuditHash) {
		return nil, after, domainhistory.State{}, &ErrSourceInvalid{Detail: fmt.Sprintf(
			"source next hash %x differs from verified hash %x at audit %d",
			batch.Next.AuditHash,
			next.AuditHash,
			next.AuditSequence,
		)}
	}
	if batch.Head.AuditSequence < next.AuditSequence || batch.Head.LogSequence < next.LogSequence {
		return nil, after, domainhistory.State{}, &ErrSourceInvalid{Detail: "source batch head is behind its verified next position"}
	}

	return effects, next, reducer.State(), nil
}

func auditHashSlices(entry *auditpb.AuditEntry, items []*auditpb.AuditItem) ([][]byte, error) {
	if len(entry.GetItems()) > 0 {
		return nil, &ErrSourceInvalid{Detail: fmt.Sprintf(
			"audit sequence %d embeds %d unbound items in its header",
			entry.GetSequence(),
			len(entry.GetItems()),
		)}
	}
	header, err := state.BuildHashedHeaderPayload(entry)
	if err != nil {
		return nil, &ErrSourceInvalid{Detail: fmt.Sprintf(
			"audit sequence %d cannot rebuild its hash header: %v",
			entry.GetSequence(),
			err,
		)}
	}

	slices := make([][]byte, 0, len(items)+1)
	slices = append(slices, header)
	for index, item := range items {
		if item == nil {
			return nil, &ErrSourceMissing{Detail: fmt.Sprintf(
				"audit sequence %d is missing item %d",
				entry.GetSequence(),
				index,
			)}
		}
		slices = append(slices, state.BuildPerItemPayload(item))
	}

	return slices, nil
}

func freshLogRange(entry *auditpb.AuditEntry, currentLogWatermark uint64) (uint64, uint64, error) {
	if entry.GetFailure() != nil {
		return 0, 0, nil
	}
	success := entry.GetSuccess()
	if success == nil {
		return 0, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
			"audit sequence %d has no outcome",
			entry.GetSequence(),
		)}
	}
	minLogSequence := success.GetMinLogSequence()
	maxLogSequence := success.GetMaxLogSequence()
	if (minLogSequence == 0) != (maxLogSequence == 0) {
		return 0, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
			"audit sequence %d has partial fresh log range [%d,%d]",
			entry.GetSequence(),
			minLogSequence,
			maxLogSequence,
		)}
	}
	if minLogSequence > maxLogSequence {
		return 0, 0, &ErrSourceInvalid{Detail: fmt.Sprintf(
			"audit sequence %d has descending fresh log range [%d,%d]",
			entry.GetSequence(),
			minLogSequence,
			maxLogSequence,
		)}
	}
	if minLogSequence > 0 && minLogSequence != currentLogWatermark+1 {
		return 0, 0, &ErrSourceMissing{Detail: fmt.Sprintf(
			"audit sequence %d starts fresh log range at %d after watermark %d",
			entry.GetSequence(),
			minLogSequence,
			currentLogWatermark,
		)}
	}

	return minLogSequence, maxLogSequence, nil
}
