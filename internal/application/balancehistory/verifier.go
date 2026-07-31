package balancehistory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	domainhistory "github.com/formancehq/ledger/v3/internal/domain/balancehistory"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
)

const (
	// DefaultVerifierInterval keeps integrity checking off the write path while
	// bounding how long a latent peer-store fault can remain undetected.
	DefaultVerifierInterval = 15 * time.Minute
	// DefaultVerifierBatchSize bounds one authoritative source snapshot held by
	// a verification replay. Proposals are never split across batches.
	DefaultVerifierBatchSize = DefaultBatchSize
	// DefaultVerifierReplayEvery runs the full source-to-served semantic proof
	// after one day at the default 15-minute interval and then once per day.
	// Bounded physical store and source-head checks run on every other periodic
	// pass, and explicit Verify/Certify are always full.
	DefaultVerifierReplayEvery = 96
	// DefaultVerifierSampleArchiveParts bounds the cold-storage work performed
	// by each short verifier tick. The cursor rotates over every referenced
	// archive part, so repeated ticks eventually cover the complete cold set.
	DefaultVerifierSampleArchiveParts = 1
)

// VerifierConfig controls periodic verification. ReplayEvery deterministically
// samples expensive source replays: 1 replays every periodic pass, N replays
// every Nth pass. Short passes verify manifest structure, source-head sanity,
// and a rotating bounded set of physical targets across hot runs and cold
// archive parts. Verify always performs a full physical and semantic replay.
type VerifierConfig struct {
	Interval           time.Duration
	BatchSize          int
	ReplayEvery        uint64
	SampleArchiveParts int
	ReplayYield        time.Duration
	// ScratchParent isolates the temporary authoritative replay store. An empty
	// value uses the operating-system temporary directory for direct library
	// users; production wiring places it on the balance-history volume.
	ScratchParent string
}

// DefaultVerifierConfig returns the fail-closed production defaults.
func DefaultVerifierConfig() VerifierConfig {
	return VerifierConfig{
		Interval:           DefaultVerifierInterval,
		BatchSize:          DefaultVerifierBatchSize,
		ReplayEvery:        DefaultVerifierReplayEvery,
		SampleArchiveParts: DefaultVerifierSampleArchiveParts,
		ReplayYield:        DefaultBackfillYield,
	}
}

// HistoryVerifier independently proves that the rebuildable history peer store
// is both physically intact and logically derived from the authoritative audit
// source. It never writes projection data; its only mutations are fail-closed
// SOURCE_MISSING and quarantine markers maintained by the store.
type HistoryVerifier struct {
	source    Source
	store     *balancehistorystore.Store
	clusterID string
	logger    logging.Logger
	config    VerifierConfig
	metrics   historyVerifierMetrics

	guard chan struct{}
	// coldSampleOffset is protected by guard. It is an opaque cursor returned
	// by the store and advances only after a successful bounded verification.
	coldSampleOffset  uint64
	verifyStoreFull   func(context.Context) error
	verifyStoreSample func(
		context.Context,
		uint64,
		int,
	) (balancehistorystore.VerificationStats, error)

	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	done        chan struct{}

	periodicSequence atomic.Uint64
	verifiedRuns     atomic.Uint64
	failures         atomic.Uint64
	lastSuccessUnix  atomic.Int64
}

type historyVerifierMetrics struct {
	verified         metric.Int64Counter
	failures         metric.Int64Counter
	duration         metric.Float64Histogram
	lastSuccess      metric.Int64Gauge
	archiveParts     metric.Int64Counter
	archiveBytes     metric.Int64Counter
	physicalDuration metric.Float64Histogram
}

// NewHistoryVerifier creates an on-demand and periodic verifier. The source
// must expose the complete proposal stream from genesis through the manifest
// watermark.
func NewHistoryVerifier(
	source Source,
	store *balancehistorystore.Store,
	clusterID string,
	logger logging.Logger,
	meter metric.Meter,
	config VerifierConfig,
) (*HistoryVerifier, error) {
	if source == nil {
		return nil, errors.New("balance history verifier source is nil")
	}
	if store == nil {
		return nil, errors.New("balance history verifier store is nil")
	}
	if clusterID == "" {
		return nil, errors.New("balance history verifier cluster id is empty")
	}
	if config.Interval <= 0 {
		config.Interval = DefaultVerifierInterval
	}
	if config.BatchSize <= 0 {
		config.BatchSize = DefaultVerifierBatchSize
	}
	if config.ReplayEvery == 0 {
		config.ReplayEvery = DefaultVerifierReplayEvery
	}
	if config.SampleArchiveParts <= 0 {
		config.SampleArchiveParts = DefaultVerifierSampleArchiveParts
	}
	if config.ReplayYield <= 0 {
		config.ReplayYield = DefaultBackfillYield
	}
	scratchParent, err := prepareVerifierScratchParent(config.ScratchParent)
	if err != nil {
		return nil, err
	}
	config.ScratchParent = scratchParent

	metrics, err := newHistoryVerifierMetrics(meter)
	if err != nil {
		return nil, err
	}

	verifier := &HistoryVerifier{
		source:            source,
		store:             store,
		clusterID:         clusterID,
		logger:            logger.WithFields(map[string]any{"cmp": "balance-history-verifier"}),
		config:            config,
		metrics:           metrics,
		guard:             make(chan struct{}, 1),
		verifyStoreFull:   store.VerifyContext,
		verifyStoreSample: store.VerifyBoundedContext,
	}
	verifier.guard <- struct{}{}

	return verifier, nil
}

func prepareVerifierScratchParent(parent string) (string, error) {
	if parent == "" {
		return "", nil
	}

	parent = filepath.Clean(parent)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return "", fmt.Errorf("creating balance history verifier scratch parent: %w", err)
	}
	info, err := os.Stat(parent)
	if err != nil {
		return "", fmt.Errorf("checking balance history verifier scratch parent: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("balance history verifier scratch parent is not a directory: %s", parent)
	}

	probe, err := os.MkdirTemp(parent, ".permission-check-*")
	if err != nil {
		return "", fmt.Errorf("checking balance history verifier scratch parent permissions: %w", err)
	}
	if err := os.Remove(probe); err != nil {
		return "", fmt.Errorf("removing balance history verifier scratch permission check: %w", err)
	}

	return parent, nil
}

func newHistoryVerifierMetrics(meter metric.Meter) (historyVerifierMetrics, error) {
	if meter == nil {
		return historyVerifierMetrics{}, nil
	}

	verified, err := meter.Int64Counter(
		"balancehistory.verifier.runs",
		metric.WithDescription("Successful balance-history integrity verification runs"),
		metric.WithUnit("{run}"),
	)
	if err != nil {
		return historyVerifierMetrics{}, fmt.Errorf("creating balance history verifier runs counter: %w", err)
	}
	failures, err := meter.Int64Counter(
		"balancehistory.verifier.failures",
		metric.WithDescription("Failed balance-history integrity verification runs"),
		metric.WithUnit("{failure}"),
	)
	if err != nil {
		return historyVerifierMetrics{}, fmt.Errorf("creating balance history verifier failures counter: %w", err)
	}
	duration, err := meter.Float64Histogram(
		"balancehistory.verifier.duration",
		metric.WithDescription("Balance-history integrity verification duration"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return historyVerifierMetrics{}, fmt.Errorf("creating balance history verifier duration histogram: %w", err)
	}
	lastSuccess, err := meter.Int64Gauge(
		"balancehistory.verifier.last_success",
		metric.WithDescription("Unix timestamp of the last successful balance-history integrity verification"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return historyVerifierMetrics{}, fmt.Errorf("creating balance history verifier last success gauge: %w", err)
	}
	archiveParts, err := meter.Int64Counter(
		"balancehistory.verifier.archive.parts",
		metric.WithDescription("Cold archive parts physically verified by bounded balance-history verifier passes"),
		metric.WithUnit("{part}"),
	)
	if err != nil {
		return historyVerifierMetrics{}, fmt.Errorf("creating balance history verifier archive parts counter: %w", err)
	}
	archiveBytes, err := meter.Int64Counter(
		"balancehistory.verifier.archive.bytes",
		metric.WithDescription("Cold archive bytes physically verified by bounded balance-history verifier passes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return historyVerifierMetrics{}, fmt.Errorf("creating balance history verifier archive bytes counter: %w", err)
	}
	physicalDuration, err := meter.Float64Histogram(
		"balancehistory.verifier.physical.duration",
		metric.WithDescription("Balance-history physical store verification duration by bounded scope"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return historyVerifierMetrics{}, fmt.Errorf("creating balance history verifier physical duration histogram: %w", err)
	}

	return historyVerifierMetrics{
		verified:         verified,
		failures:         failures,
		duration:         duration,
		lastSuccess:      lastSuccess,
		archiveParts:     archiveParts,
		archiveBytes:     archiveBytes,
		physicalDuration: physicalDuration,
	}, nil
}

// Start launches one immediate bounded verification followed by periodic
// checks. The first full replay runs when the periodic sequence reaches
// ReplayEvery (immediately only when ReplayEvery is 1). It is idempotent; a
// stopped verifier may be started again.
func (v *HistoryVerifier) Start() {
	v.lifecycleMu.Lock()
	defer v.lifecycleMu.Unlock()

	if v.cancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	v.cancel = cancel
	v.done = done
	go v.run(ctx, done)
}

// Stop cancels an in-flight periodic pass and waits for it to release its
// source snapshots. It does not cancel a caller-owned on-demand Verify.
func (v *HistoryVerifier) Stop() {
	v.lifecycleMu.Lock()
	cancel := v.cancel
	done := v.done
	v.lifecycleMu.Unlock()
	if cancel == nil {
		return
	}

	cancel()
	<-done

	v.lifecycleMu.Lock()
	if v.done == done {
		v.cancel = nil
		v.done = nil
	}
	v.lifecycleMu.Unlock()
}

func (v *HistoryVerifier) run(ctx context.Context, done chan<- struct{}) {
	defer close(done)

	v.runPeriodic(ctx)
	ticker := time.NewTicker(v.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			v.runPeriodic(ctx)
		}
	}
}

func (v *HistoryVerifier) runPeriodic(ctx context.Context) {
	sequence := v.periodicSequence.Add(1)
	fullReplay := sequence%v.config.ReplayEvery == 0
	if err := v.verifyObserved(ctx, fullReplay, 0, 0); err != nil && !errors.Is(err, context.Canceled) {
		v.logger.Errorf("verifying balance history: %v", err)
	}
}

// Verify performs a complete on-demand verification: physical checksums, audit
// hash chain, reducer state, every normalized effect, and the chained logical
// digest through the pinned manifest watermark.
func (v *HistoryVerifier) Verify(ctx context.Context) error {
	return v.verifyObserved(ctx, true, 0, 0)
}

// Certify performs the same complete semantic proof as Verify while also
// requiring the pinned manifest to cover the builder's caught-up source head.
// It deliberately leaves SOURCE_MISSING or REBUILDING persisted: the builder
// clears the appropriate marker only after this method succeeds and its WAL
// barrier for the same required prefix has completed.
func (v *HistoryVerifier) Certify(
	ctx context.Context,
	requiredAuditSequence uint64,
	requiredLogSequence uint64,
) error {
	return v.verifyObserved(
		ctx,
		true,
		requiredAuditSequence,
		requiredLogSequence,
	)
}

// VerifiedRuns returns the number of successful full or sampled passes. It is
// primarily useful to health reporting; it carries no unbounded dimensions.
func (v *HistoryVerifier) VerifiedRuns() uint64 {
	return v.verifiedRuns.Load()
}

// Failures returns the number of failed verification passes.
func (v *HistoryVerifier) Failures() uint64 {
	return v.failures.Load()
}

// LastSuccessUnix returns the Unix timestamp of the last successful pass, or
// zero before the first success.
func (v *HistoryVerifier) LastSuccessUnix() int64 {
	return v.lastSuccessUnix.Load()
}

func (v *HistoryVerifier) verifyObserved(
	ctx context.Context,
	fullReplay bool,
	requiredAuditSequence uint64,
	requiredLogSequence uint64,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-v.guard:
	}
	defer func() { v.guard <- struct{}{} }()

	startedAt := time.Now()
	err := v.verify(
		ctx,
		fullReplay,
		requiredAuditSequence,
		requiredLogSequence,
	)
	duration := time.Since(startedAt).Seconds()
	if v.metrics.duration != nil {
		v.metrics.duration.Record(ctx, duration)
	}
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			v.failures.Add(1)
			if v.metrics.failures != nil {
				v.metrics.failures.Add(ctx, 1)
			}
		}

		return err
	}

	now := time.Now().Unix()
	v.verifiedRuns.Add(1)
	v.lastSuccessUnix.Store(now)
	if v.metrics.verified != nil {
		v.metrics.verified.Add(ctx, 1)
	}
	if v.metrics.lastSuccess != nil {
		v.metrics.lastSuccess.Record(ctx, now)
	}

	return nil
}

func (v *HistoryVerifier) verify(
	ctx context.Context,
	fullReplay bool,
	requiredAuditSequence uint64,
	requiredLogSequence uint64,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := v.verifyPhysicalStore(ctx, fullReplay); err != nil {
		return err
	}

	manifest, err := v.store.Manifest()
	if err != nil {
		return fmt.Errorf("reading balance history manifest for verification: %w", err)
	}
	if fullReplay && manifest.SourceComplete {
		return v.verifyPinned(
			ctx,
			max(manifest.LogWatermark, requiredLogSequence),
			requiredAuditSequence,
			requiredLogSequence,
		)
	}

	head, err := v.source.Head(ctx)
	if err != nil {
		return fmt.Errorf("reading authoritative balance history head: %w", err)
	}

	if err := v.validateHead(manifest, head); err != nil {
		return err
	}
	if !fullReplay {
		// A sampled pass never clears SOURCE_MISSING: only a complete replay can
		// prove that every intermediate proposal is present again.
		return nil
	}
	if manifest.AuditWatermark == 0 {
		return &balancehistorystore.ErrBuilding{
			Current: manifest.LogWatermark,
			Target:  head.LogSequence,
		}
	}

	return v.markSourceMissing(&ErrSourceMissing{Detail: fmt.Sprintf(
		"manifest at audit %d does not prove a complete source prefix",
		manifest.AuditWatermark,
	)})
}

func (v *HistoryVerifier) verifyPhysicalStore(ctx context.Context, full bool) error {
	startedAt := time.Now()
	scope := "sample"
	var (
		stats balancehistorystore.VerificationStats
		err   error
	)
	if full {
		scope = "full"
		err = v.verifyStoreFull(ctx)
	} else {
		stats, err = v.verifyStoreSample(
			ctx,
			v.coldSampleOffset,
			v.config.SampleArchiveParts,
		)
	}
	if v.metrics.physicalDuration != nil {
		v.metrics.physicalDuration.Record(
			ctx,
			time.Since(startedAt).Seconds(),
			metric.WithAttributes(attribute.String("scope", scope)),
		)
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		var missing *balancehistorystore.ErrSourceMissing
		if errors.As(err, &missing) {
			return v.markSourceMissing(err)
		}

		return fmt.Errorf("verifying balance history physical store: %w", err)
	}
	if full {
		return nil
	}

	v.coldSampleOffset = stats.NextOffset
	if v.metrics.archiveParts != nil && stats.ArchiveParts > 0 {
		v.metrics.archiveParts.Add(ctx, int64(stats.ArchiveParts))
	}
	if v.metrics.archiveBytes != nil && stats.ArchiveBytes > 0 {
		const maxMetricValue = uint64(1<<63 - 1)
		archiveBytes := min(stats.ArchiveBytes, maxMetricValue)
		v.metrics.archiveBytes.Add(ctx, int64(archiveBytes))
	}

	return nil
}

func (v *HistoryVerifier) verifyPinned(
	ctx context.Context,
	minLogSequence uint64,
	requiredAuditSequence uint64,
	requiredLogSequence uint64,
) (retErr error) {
	actualView, err := v.store.OpenVerificationView(ctx, minLogSequence)
	if err != nil {
		return fmt.Errorf("opening pinned balance history verification view: %w", err)
	}
	defer func() {
		if err := actualView.Close(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("closing pinned balance history verification view: %w", err))
		}
	}()

	manifest := actualView.Manifest()
	if manifest.AuditWatermark < requiredAuditSequence {
		return &ErrSourceMissing{Detail: fmt.Sprintf(
			"pinned manifest audit watermark %d is behind required certification watermark %d",
			manifest.AuditWatermark,
			requiredAuditSequence,
		)}
	}
	if manifest.LogWatermark < requiredLogSequence {
		return &balancehistorystore.ErrBehind{
			Required: requiredLogSequence,
			Current:  manifest.LogWatermark,
		}
	}
	head, err := v.source.Head(ctx)
	if err != nil {
		return fmt.Errorf("reading authoritative balance history head: %w", err)
	}
	if err := v.validateHead(manifest, head); err != nil {
		return err
	}

	scratch, scratchDir, err := v.newScratchStore()
	if err != nil {
		return err
	}
	defer func() {
		if err := closeScratchStore(scratch, scratchDir); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}()

	digest, position, reducerState, err := v.replay(ctx, manifest, scratch)
	if err != nil {
		return v.markReplayFailure(err)
	}
	if position.AuditSequence != manifest.AuditWatermark || position.LogSequence != manifest.LogWatermark {
		return v.quarantine(fmt.Sprintf(
			"authoritative replay ended at (%d,%d), manifest declares (%d,%d)",
			position.AuditSequence,
			position.LogSequence,
			manifest.AuditWatermark,
			manifest.LogWatermark,
		))
	}
	if !bytes.Equal(position.AuditHash, manifest.AuditHash) {
		return v.quarantine(fmt.Sprintf(
			"authoritative audit hash %x differs from manifest hash %x at sequence %d",
			position.AuditHash,
			manifest.AuditHash,
			manifest.AuditWatermark,
		))
	}
	if digest != manifest.LogicalDigest {
		return v.quarantine(fmt.Sprintf(
			"authoritative logical digest %x differs from manifest digest %x at audit %d",
			digest,
			manifest.LogicalDigest,
			manifest.AuditWatermark,
		))
	}
	if !reflect.DeepEqual(reducerState, manifest.ReducerState) {
		return v.quarantine(fmt.Sprintf(
			"authoritative reducer state differs from manifest state at audit %d",
			manifest.AuditWatermark,
		))
	}

	scratchView, err := scratch.OpenView(manifest.LogWatermark)
	if err != nil {
		return fmt.Errorf("opening replayed balance history verification view: %w", err)
	}
	defer func() {
		if err := scratchView.Close(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("closing replayed balance history verification view: %w", err))
		}
	}()

	actualSemanticDigest, err := actualView.SemanticDigest(ctx)
	if err != nil {
		return fmt.Errorf("computing served balance history semantic digest: %w", err)
	}
	replayedSemanticDigest, err := scratchView.SemanticDigest(ctx)
	if err != nil {
		return fmt.Errorf("computing replayed balance history semantic digest: %w", err)
	}
	if actualSemanticDigest != replayedSemanticDigest {
		return v.quarantine(fmt.Sprintf(
			"served semantic digest %x differs from authoritative replay digest %x at audit %d",
			actualSemanticDigest,
			replayedSemanticDigest,
			manifest.AuditWatermark,
		))
	}

	return nil
}

func (v *HistoryVerifier) newScratchStore() (*balancehistorystore.Store, string, error) {
	dir, err := os.MkdirTemp(
		v.config.ScratchParent,
		"balance-history-verifier-*",
	)
	if err != nil {
		return nil, "", fmt.Errorf("creating balance history verification scratch directory: %w", err)
	}

	config := balancehistorystore.DefaultConfig()
	config.CacheSize = 8 << 20
	config.MemTableSize = 8 << 20
	config.MemTableStopWritesThreshold = 2
	config.MaxConcurrentCompactions = 1
	scratch, err := balancehistorystore.New(dir, v.logger, config)
	if err != nil {
		if removeErr := os.RemoveAll(dir); removeErr != nil {
			err = errors.Join(err, fmt.Errorf("removing failed balance history verification scratch directory: %w", removeErr))
		}

		return nil, "", fmt.Errorf("opening balance history verification scratch store: %w", err)
	}

	return scratch, dir, nil
}

func closeScratchStore(store *balancehistorystore.Store, dir string) error {
	closeErr := store.Close()
	removeErr := os.RemoveAll(dir)
	if closeErr == nil && removeErr == nil {
		return nil
	}

	return errors.Join(
		wrapNonNilError("closing balance history verification scratch store", closeErr),
		wrapNonNilError("removing balance history verification scratch directory", removeErr),
	)
}

func wrapNonNilError(operation string, err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%s: %w", operation, err)
}

func (v *HistoryVerifier) validateHead(manifest balancehistorystore.Manifest, head Position) error {
	if manifest.AuditWatermark > 0 && len(manifest.AuditHash) == 0 {
		return v.quarantine(fmt.Sprintf(
			"manifest audit watermark %d has no audit hash",
			manifest.AuditWatermark,
		))
	}
	if head.AuditSequence > 0 && len(head.AuditHash) == 0 {
		return v.markSourceMissing(&ErrSourceInvalid{Detail: fmt.Sprintf(
			"authoritative head at audit %d has no audit hash",
			head.AuditSequence,
		)})
	}
	if head.AuditSequence < manifest.AuditWatermark || head.LogSequence < manifest.LogWatermark {
		return v.markSourceMissing(&ErrSourceMissing{Detail: fmt.Sprintf(
			"authoritative head (%d,%d) is behind manifest (%d,%d)",
			head.AuditSequence,
			head.LogSequence,
			manifest.AuditWatermark,
			manifest.LogWatermark,
		)})
	}
	if head.AuditSequence == manifest.AuditWatermark {
		if head.LogSequence != manifest.LogWatermark {
			return v.markSourceMissing(&ErrSourceInvalid{Detail: fmt.Sprintf(
				"authoritative and manifest audit watermark %d have log watermarks %d and %d",
				manifest.AuditWatermark,
				head.LogSequence,
				manifest.LogWatermark,
			)})
		}
	}

	return nil
}

func (v *HistoryVerifier) replay(
	ctx context.Context,
	manifest balancehistorystore.Manifest,
	scratch *balancehistorystore.Store,
) ([32]byte, Position, domainhistory.State, error) {
	return v.replayInto(
		ctx,
		manifest.AuditWatermark,
		scratch,
		Position{},
		[32]byte{},
		domainhistory.NewReducer(),
	)
}

func (v *HistoryVerifier) replayInto(
	ctx context.Context,
	targetAuditSequence uint64,
	scratch *balancehistorystore.Store,
	position Position,
	digest [32]byte,
	reducer *domainhistory.Reducer,
) ([32]byte, Position, domainhistory.State, error) {
	for position.AuditSequence < targetAuditSequence {
		if err := ctx.Err(); err != nil {
			return [32]byte{}, Position{}, domainhistory.State{}, err
		}

		remaining := targetAuditSequence - position.AuditSequence
		batchSize := v.config.BatchSize
		if remaining < uint64(batchSize) {
			batchSize = int(remaining)
		}
		batch, err := v.source.Read(ctx, position, batchSize)
		if err != nil {
			return [32]byte{}, Position{}, domainhistory.State{}, fmt.Errorf(
				"reading authoritative source after audit %d: %w",
				position.AuditSequence,
				err,
			)
		}
		if len(batch.Proposals) == 0 {
			return [32]byte{}, Position{}, domainhistory.State{}, &ErrSourceMissing{Detail: fmt.Sprintf(
				"authoritative source returned no proposal after audit %d before manifest watermark %d",
				position.AuditSequence,
				targetAuditSequence,
			)}
		}

		effects, next, _, err := reduceVerifiedBatch(v.clusterID, reducer, position, batch)
		if err != nil {
			return [32]byte{}, Position{}, domainhistory.State{}, err
		}
		if next.AuditSequence <= position.AuditSequence || next.AuditSequence > targetAuditSequence {
			return [32]byte{}, Position{}, domainhistory.State{}, &ErrSourceInvalid{Detail: fmt.Sprintf(
				"authoritative replay advanced from audit %d to %d with target %d",
				position.AuditSequence,
				next.AuditSequence,
				targetAuditSequence,
			)}
		}

		digest, err = balancehistorystore.AdvanceLogicalDigest(
			digest,
			position.AuditSequence,
			next.AuditSequence,
			effects,
		)
		if err != nil {
			return [32]byte{}, Position{}, domainhistory.State{}, fmt.Errorf(
				"computing authoritative logical digest through audit %d: %w",
				next.AuditSequence,
				err,
			)
		}
		if _, err := scratch.Publish(balancehistorystore.Publication{
			Effects: effects,
			Coverage: balancehistorystore.Coverage{
				AuditSequence:  next.AuditSequence,
				LogSequence:    next.LogSequence,
				AuditHash:      append([]byte(nil), next.AuditHash...),
				SourceComplete: true,
			},
			ReducerState: reducer.State(),
		}); err != nil {
			return [32]byte{}, Position{}, domainhistory.State{}, &scratchProjectionError{
				err: fmt.Errorf("publishing replay through audit %d: %w", next.AuditSequence, err),
			}
		}
		position = next
		if position.AuditSequence < targetAuditSequence {
			if err := v.waitForReplayYield(ctx); err != nil {
				return [32]byte{}, Position{}, domainhistory.State{}, err
			}
		}
	}
	if targetAuditSequence == 0 {
		if _, err := scratch.Publish(balancehistorystore.Publication{
			Coverage: balancehistorystore.Coverage{
				SourceComplete: true,
			},
			ReducerState: reducer.State(),
		}); err != nil {
			return [32]byte{}, Position{}, domainhistory.State{}, &scratchProjectionError{
				err: fmt.Errorf("publishing empty authoritative replay: %w", err),
			}
		}
	}

	return digest, position, reducer.State(), nil
}

func (v *HistoryVerifier) waitForReplayYield(ctx context.Context) error {
	timer := time.NewTimer(v.config.ReplayYield)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type scratchProjectionError struct {
	err error
}

func (e *scratchProjectionError) Error() string {
	return "building balance history verification scratch projection: " + e.err.Error()
}

func (e *scratchProjectionError) Unwrap() error {
	return e.err
}

func (v *HistoryVerifier) markReplayFailure(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var scratchErr *scratchProjectionError
	if errors.As(err, &scratchErr) {
		return err
	}

	return v.markSourceMissing(err)
}

func (v *HistoryVerifier) markSourceMissing(cause error) error {
	detail := cause.Error()
	if err := v.store.MarkSourceMissing(detail); err != nil {
		return errors.Join(cause, fmt.Errorf("marking balance history source missing: %w", err))
	}

	return cause
}

func (v *HistoryVerifier) quarantine(detail string) error {
	if err := v.store.Quarantine(detail); err != nil {
		return errors.Join(
			&balancehistorystore.ErrQuarantined{Detail: detail},
			fmt.Errorf("persisting balance history quarantine: %w", err),
		)
	}

	return &balancehistorystore.ErrQuarantined{Detail: detail}
}
