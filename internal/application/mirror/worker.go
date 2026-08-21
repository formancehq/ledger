package mirror

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	v2 "github.com/formancehq/ledger/v3/internal/adapter/v2"
	"github.com/formancehq/ledger/v3/internal/adapter/v2/celrewrite"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/pkg/vtmarshal"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const (
	defaultBatchSize    = 100
	defaultPollInterval = 5 * time.Second
	initialBackoff      = 1 * time.Second
	maxBackoff          = 60 * time.Second
	backoffMultiplier   = 2.0
)

// prefetchResult holds the result of a background log fetch started during
// the previous batch's Raft wait. afterID records the source position the
// fetch was issued from, so a stale prefetch (the position moved because of
// an error or a boundary re-read) can be discarded.
type prefetchResult struct {
	logs     []v2.V2Log
	hasMore  bool
	err      error
	duration time.Duration
	afterID  uint64
}

// Worker continuously fetches v2 logs for a single mirror ledger and proposes
// them via Raft. It is started/stopped by the Manager based on leadership.
type Worker struct {
	ledgerName     string
	batchSize      int
	source         v2.Source
	rewriter       *celrewrite.Rewriter
	store          *dal.Store
	proposer       Proposer
	builder        *plan.Builder
	logger         logging.Logger
	sourceLogCount uint64
	// sourceHeadObserved records that GetLatestLogID has answered at least
	// once, making sourceLogCount == 0 an observed empty source rather than
	// "never asked". Without it publishIdleStatus cannot tell the two apart,
	// and an empty source would never get its status published (EN-1773).
	sourceHeadObserved bool
	// lastPublishedSourceHead is the source head most recently persisted via a
	// MirrorSyncUpdate (either bundled with an ingest batch or published on its
	// own by an idle, caught-up worker). It gates publishIdleStatus so an idle
	// mirror does not re-propose the same value every poll tick (EN-1773).
	lastPublishedSourceHead uint64
	// statusClearConfirmed records that a MirrorSyncUpdate carrying
	// ClearError was confirmed applied since the last error report, tracking
	// "the persisted status needs clearing" independently from "the head
	// needs publishing". Both are required: a source that recovers without
	// producing a new log leaves the head unchanged, so a head-only gate
	// would suppress the clear and the API would keep serving the stale
	// error (EN-1773).
	//
	// The zero value is deliberately false: a worker that has just started
	// cannot know whether an error is persisted, so it publishes once rather
	// than assuming the status is clean. One idle proposal per worker start,
	// and it is idempotent.
	statusClearConfirmed bool

	notify  signal.Signal
	w       worker.Worker
	backoff time.Duration // current backoff duration (0 = no backoff)
	// lastAppliedV2LogID caches LedgerBoundaries.last_mirror_v2_log_id, the
	// sole durable authority for the ingestion position. It is an
	// optimisation only: it is seeded from the boundary, advanced solely
	// after confirmed FSM application, and dropped on any batch error so the
	// next tick re-reads the authority (EN-1513).
	lastAppliedV2LogID uint64
	nextTxID           uint64 // next transaction ID, from the same boundary read
	boundariesLoaded   bool
	prefetchCh         chan prefetchResult // pending prefetch from previous batch

	// Metrics
	ledgerAttr        attribute.KeyValue
	fetchDuration     metric.Int64Histogram
	translateDuration metric.Int64Histogram
	preloadDuration   metric.Int64Histogram
	proposeDuration   metric.Int64Histogram
	fsmWaitDuration   metric.Int64Histogram
	batchDuration     metric.Int64Histogram
	commandSize       metric.Int64Histogram
	logsIngested      metric.Int64Counter
	batchTotal        metric.Int64Counter
}

// NewWorker creates a new mirror Worker for the given ledger.
func NewWorker(
	ledgerName string,
	batchSize int,
	source v2.Source,
	rewriter *celrewrite.Rewriter,
	store *dal.Store,
	proposer Proposer,
	builder *plan.Builder,
	logger logging.Logger,
	meterProvider metric.MeterProvider,
) *Worker {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	meter := meterProvider.Meter("mirror")

	durationBuckets := metric.WithExplicitBucketBoundaries(
		0, 1000, 5000, 20000, 100000, 500000, 2000000,
	)
	sizeBuckets := metric.WithExplicitBucketBoundaries(
		0, 512, 2048, 8192, 32768, 131072, 524288,
	)

	fetchDuration, _ := meter.Int64Histogram("mirror.fetch.duration",
		metric.WithUnit("us"), durationBuckets)
	translateDuration, _ := meter.Int64Histogram("mirror.translate.duration",
		metric.WithUnit("us"), durationBuckets)
	preloadDuration, _ := meter.Int64Histogram("mirror.preload.duration",
		metric.WithUnit("us"), durationBuckets)
	proposeDuration, _ := meter.Int64Histogram("mirror.propose.duration",
		metric.WithUnit("us"), durationBuckets)
	fsmWaitDuration, _ := meter.Int64Histogram("mirror.fsm_wait.duration",
		metric.WithUnit("us"), durationBuckets)
	batchDuration, _ := meter.Int64Histogram("mirror.batch.duration",
		metric.WithUnit("us"), durationBuckets)
	commandSize, _ := meter.Int64Histogram("mirror.command.size",
		metric.WithUnit("By"), sizeBuckets)
	logsIngested, _ := meter.Int64Counter("mirror.logs.ingested",
		metric.WithUnit("1"))
	batchTotal, _ := meter.Int64Counter("mirror.batch.total",
		metric.WithUnit("1"))

	return &Worker{
		ledgerName: ledgerName,
		batchSize:  batchSize,
		source:     source,
		rewriter:   rewriter,
		store:      store,
		proposer:   proposer,
		builder:    builder,
		logger:     logger.WithFields(map[string]any{"cmp": "mirror-worker", "ledger": ledgerName}),
		notify:     signal.New(),

		ledgerAttr:        attribute.String("ledger", ledgerName),
		fetchDuration:     fetchDuration,
		translateDuration: translateDuration,
		preloadDuration:   preloadDuration,
		proposeDuration:   proposeDuration,
		fsmWaitDuration:   fsmWaitDuration,
		batchDuration:     batchDuration,
		commandSize:       commandSize,
		logsIngested:      logsIngested,
		batchTotal:        batchTotal,
	}
}

// Start begins the background sync loop.
func (w *Worker) Start() {
	w.w = worker.New()
	w.w.RunCtx(w.loop)
}

// Stop gracefully stops the sync loop and closes the source.
func (w *Worker) Stop() {
	w.w.Stop()
	_ = w.source.Close() // best-effort cleanup
}

// Notify signals that new logs may be available (e.g., after a Raft commit).
func (w *Worker) Notify() {
	w.notify.Notify()
}

func (w *Worker) loop(ctx context.Context) {
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	w.refreshSourceHead(ctx)
	w.processLogs(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.notify.C():
			w.processLogs(ctx)
		case <-ticker.C:
			w.refreshSourceHead(ctx)
			w.processLogs(ctx)
		}
	}
}

// refreshSourceHead queries the v2 source for its latest log ID and stores
// it in the worker for inclusion in subsequent cursor reports.
func (w *Worker) refreshSourceHead(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	count, err := w.source.GetLatestLogID(ctx)
	if err != nil {
		w.logger.WithFields(map[string]any{"error": err.Error()}).Errorf("Failed to query source head")

		return
	}

	w.sourceLogCount = count
	w.sourceHeadObserved = true
}

func (w *Worker) processLogs(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Pause while Pebble is in a write stall to let compaction catch up.
		if w.store.IsWriteStalled() {
			w.logger.Infof("Pausing mirror ingestion: Pebble write stall in progress")

			select {
			case <-ctx.Done():
				return
			case <-w.store.WriteStallWaitCh():
			}

			w.logger.Infof("Resuming mirror ingestion: write stall cleared")
		}

		hasMore, err := w.processBatch(ctx)
		if err != nil {
			w.logger.WithFields(map[string]any{"error": err.Error()}).Errorf("Mirror sync error")
			w.reportError(ctx, err.Error())

			// Drop the boundary snapshot: it is a cache over the durable
			// authority, so a failed batch must re-read rather than retry
			// against a possibly-stale in-memory position (EN-1513).
			w.boundariesLoaded = false

			// Apply exponential backoff on persistent errors
			if w.backoff == 0 {
				w.backoff = initialBackoff
			} else {
				w.backoff = min(time.Duration(float64(w.backoff)*backoffMultiplier), maxBackoff)
			}

			select {
			case <-ctx.Done():
			case <-time.After(w.backoff):
			}

			return
		}
		// Reset backoff on success
		w.backoff = 0

		if !hasMore {
			return
		}
	}
}

func (w *Worker) processBatch(ctx context.Context) (bool, error) {
	batchStart := time.Now()
	attrs := metric.WithAttributes(w.ledgerAttr)

	// Load the applied boundary from Pebble once; subsequent batches use the
	// in-memory snapshot. This single read serves BOTH the source position and
	// nextTxID — LedgerBoundaries carries both (EN-1513).
	if !w.boundariesLoaded {
		boundaries, err := w.builder.ReadBoundaries(w.ledgerName)
		if err != nil {
			return false, fmt.Errorf("reading boundaries: %w", err)
		}

		w.lastAppliedV2LogID = boundaries.GetLastMirrorV2LogId()

		if boundaries != nil {
			w.nextTxID = boundaries.GetNextTransactionId()
		} else {
			w.nextTxID = 1
		}

		w.boundariesLoaded = true
	}

	expectedNextLogID := w.lastAppliedV2LogID + 1

	// Use prefetched result if available and valid, otherwise fetch synchronously.
	var (
		v2Logs   []v2.V2Log
		hasMore  bool
		fetchDur time.Duration
	)

	if w.prefetchCh != nil {
		pf := <-w.prefetchCh

		w.prefetchCh = nil
		if pf.err == nil && pf.afterID == w.lastAppliedV2LogID {
			v2Logs = pf.logs
			hasMore = pf.hasMore
			fetchDur = pf.duration
		}
	}

	if v2Logs == nil {
		fetchStart := time.Now()

		fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		var err error

		v2Logs, hasMore, err = w.source.FetchLogs(fetchCtx, w.lastAppliedV2LogID, w.batchSize)
		if err != nil {
			return false, err
		}

		fetchDur = time.Since(fetchStart)
	}

	w.fetchDuration.Record(ctx, fetchDur.Microseconds(), attrs)

	if len(v2Logs) == 0 {
		// Fully caught up: the ingest path (which normally bundles the source
		// head and the error clear into its data proposal) never runs, so
		// publish them on their own. Without this a correctly restored, idle
		// mirror reports SYNCING forever because RebuildDelta reconstructs
		// last_mirror_v2_log_id but not SubPLMirrorSourceHead, and a source
		// that recovers without producing a new log never gets its persisted
		// error cleared (EN-1773).
		w.publishIdleStatus(ctx)

		return false, nil
	}

	w.logsIngested.Add(ctx, int64(len(v2Logs)), attrs)

	expectedNextTxID := w.nextTxID

	// Translate v2 logs to v3 orders
	translateStart := time.Now()

	orders, _, newNextTxID, err := v2.TranslateBatch(w.ledgerName, v2Logs, expectedNextLogID, expectedNextTxID, w.rewriter)
	if err != nil {
		return false, err
	}

	w.nextTxID = newNextTxID

	w.translateDuration.Record(ctx, time.Since(translateStart).Microseconds(), attrs)

	if len(orders) == 0 {
		return hasMore, nil
	}

	// Build proposal with orders and preloads for cache population
	cmd := commands.NewCommand(orders...)
	cmd.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentMirror)

	preloadStart := time.Now()

	aggregate, perOrder := w.extractMirrorNeeds(cmd)

	// Merge the source-head/status update into the data proposal to avoid a
	// second Raft round-trip. The FSM processes TechnicalUpdates on any
	// proposal (machine.go). The applied position is NOT carried here — the
	// FSM derives it from the ingest orders themselves (EN-1513).
	lastV2LogID := v2Logs[len(v2Logs)-1].ID
	cmd.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{
		Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{
			MirrorSync: &raftcmdpb.MirrorSyncUpdate{
				LedgerName:     w.ledgerName,
				ClearError:     true,
				SourceLogCount: w.sourceLogCount,
			},
		},
	}}

	// One WriteOperation per Order + one for the mirror-sync TU. The
	// mirror-sync TU reads Registry.Ledgers[w.ledgerName] in applyMirrorSyncUpdate.
	tuNeeds := plan.NewCoverage()
	tuNeeds.Add(dal.SubAttrLedger, domain.LedgerKey{Name: w.ledgerName}.Bytes())

	// Roll the mirror-sync TU's need into the batch aggregate — Build no
	// longer recomputes it from operations.
	aggregate.Merge(tuNeeds)

	operations := make([]plan.WriteOperation, 0, len(orders)+1)
	cmdOrders := cmd.GetOrders()
	for i := range orders {
		// coverage_bits moved to OrderTechnical; init nil-safely before Build
		// fills it through the pointer.
		if cmdOrders[i].GetTechnical() == nil {
			cmdOrders[i].Technical = &raftcmdpb.OrderTechnical{}
		}
		operations = append(operations, plan.WriteOperation{
			Coverage: perOrder[i],
			Target:   &cmdOrders[i].Technical.CoverageBits,
		})
	}

	operations = append(operations, plan.WriteOperation{
		Coverage: tuNeeds,
		Target:   &cmd.GetTechnicalUpdates()[0].CoverageBits,
	})

	build, err := w.builder.Build(aggregate, operations)
	if err != nil {
		build.ReleaseLoaders()

		return false, fmt.Errorf("building preloads: %w", err)
	}

	w.preloadDuration.Record(ctx, time.Since(preloadStart).Microseconds(), attrs)

	// Run preload + propose via the shared runner. Mirror is a
	// single-shot caller (no concurrent admissions sharing loaders),
	// so we release loaders immediately after the runner returns.
	marshalFn := func(c *raftcmdpb.Proposal) ([]byte, error) {
		data, err := marshalMirrorCommand(c)
		if err != nil {
			return nil, err
		}

		w.commandSize.Record(ctx, int64(len(data)), attrs)

		return data, nil
	}

	runResult, err := w.builder.Run(ctx, cmd, build, marshalFn, w.proposer)
	if err != nil {
		return false, err
	}

	runResult.Guard.ReleaseLoaders()

	proposal := runResult.Proposal
	fsmFuture := runResult.FSMFuture

	// Start prefetching the next batch while waiting for Raft consensus.
	// The goroutine writes to a buffered channel and always exits, even if
	// nobody reads the result (e.g., on stop).
	var nextPrefetchCh chan prefetchResult
	if hasMore {
		nextPrefetchCh = make(chan prefetchResult, 1)
		nextAfterID := lastV2LogID

		go func() {
			start := time.Now()

			// Derive from the worker's ctx so Worker.Stop() unblocks the
			// source fetch — without this, drainPrefetch can wait up to
			// 30s on a fetch that won't return any faster.
			fCtx, fCancel := context.WithTimeout(ctx, 30*time.Second)
			defer fCancel()

			logs, more, fetchErr := w.source.FetchLogs(fCtx, nextAfterID, w.batchSize)
			nextPrefetchCh <- prefetchResult{
				logs:     logs,
				hasMore:  more,
				err:      fetchErr,
				duration: time.Since(start),
				afterID:  nextAfterID,
			}
		}()
	}

	// Wait for Raft acceptance (proposal enqueued by leader).
	if _, err := proposal.Wait(ctx); err != nil {
		w.drainPrefetch(nextPrefetchCh)

		return false, err
	}
	// Preserve the "Propose + Wait" semantic of this metric: the
	// runner exposes the wall-clock instant just before its
	// proposer.Propose call, so subtracting now gives the Raft
	// queue-insertion + commit-acceptance duration.
	w.proposeDuration.Record(ctx, time.Since(runResult.ProposeStartTime).Microseconds(), attrs)

	// Wait for FSM application and check for business errors.
	// Without this, the cursor would advance past entries that failed to process.
	fsmWaitStart := time.Now()
	result, fsmErr := fsmFuture.Wait(ctx)

	w.fsmWaitDuration.Record(ctx, time.Since(fsmWaitStart).Microseconds(), attrs)

	if fsmErr != nil {
		w.drainPrefetch(nextPrefetchCh)
		w.batchTotal.Add(ctx, 1, attrs, metric.WithAttributes(attribute.String("status", "error")))

		return false, fmt.Errorf("FSM apply: %w", fsmErr)
	}

	if result.Error != nil {
		w.drainPrefetch(nextPrefetchCh)
		w.batchTotal.Add(ctx, 1, attrs, metric.WithAttributes(attribute.String("status", "error")))

		return false, fmt.Errorf("FSM apply: %w", result.Error)
	}

	w.batchTotal.Add(ctx, 1, attrs, metric.WithAttributes(attribute.String("status", "success")))
	w.batchDuration.Record(ctx, time.Since(batchStart).Microseconds(), attrs)

	// Advance the in-memory position so the next batch skips the Pebble read.
	// Only reached after BOTH Raft acceptance and successful FSM application.
	w.lastAppliedV2LogID = lastV2LogID
	w.prefetchCh = nextPrefetchCh

	// The applied proposal carried this source head AND ClearError in its
	// MirrorSyncUpdate, so record both as published to keep a later idle tick
	// from re-proposing them.
	w.lastPublishedSourceHead = w.sourceLogCount
	w.statusClearConfirmed = true

	return hasMore, nil
}

// drainPrefetch waits for a background prefetch goroutine to complete,
// discarding its result. This prevents goroutine leaks on error paths.
func (w *Worker) drainPrefetch(ch chan prefetchResult) {
	if ch != nil {
		<-ch
	}
}

// publishIdleStatus proposes a standalone MirrorSyncUpdate for an idle, fully
// caught-up mirror, carrying the observed source head and clearing any
// persisted error. The ingest path bundles both into its data proposal, but a
// caught-up worker fetches no logs and would otherwise propose nothing —
// leaving SubPLMirrorSourceHead at 0 after a restore, so ReadMirrorSyncProgress
// reports SYNCING indefinitely until a new source log arrives, and leaving a
// recorded error in place even after the source recovers (EN-1773).
//
// The two conditions are tracked separately because they move independently: a
// source that recovers without producing a new log leaves the head unchanged,
// so gating on the head alone would suppress the error clear forever. A head
// that was never observed carries no information and is skipped — but an
// observed head of 0 (an empty source) is a legitimate value that must still
// be able to clear an error, which is why sourceHeadObserved exists rather
// than a sourceLogCount == 0 test.
func (w *Worker) publishIdleStatus(ctx context.Context) {
	if !w.sourceHeadObserved {
		return
	}

	if w.statusClearConfirmed && w.sourceLogCount == w.lastPublishedSourceHead {
		return
	}

	// A zero SourceLogCount is "no write" for that field (write_set.go), so an
	// empty source still gets its error cleared without inventing a head.
	update := &raftcmdpb.MirrorSyncUpdate{
		LedgerName:     w.ledgerName,
		ClearError:     true,
		SourceLogCount: w.sourceLogCount,
	}

	if !w.proposeMirrorSync(ctx, update, "mirror idle status publish") {
		return
	}

	w.lastPublishedSourceHead = w.sourceLogCount
	w.statusClearConfirmed = true
}

func (w *Worker) reportError(ctx context.Context, message string) {
	update := &raftcmdpb.MirrorSyncUpdate{
		LedgerName: w.ledgerName,
		Error: &commonpb.MirrorSyncError{
			Message:    message,
			OccurredAt: &commonpb.Timestamp{Data: uint64(libtime.Now().UnixMicro())},
		},
	}

	// Marked dirty BEFORE the propose, reading the bit as "an error may now be
	// persisted". proposeMirrorSync reports confirmation, not application: both
	// its waits abandon on context cancellation (futures.Future.Wait returns
	// ctx.Err() on an unresolved future) and abandoning a wait does not
	// un-commit a Raft entry, so a committed error report can return false.
	// Deferring the write to a confirmed apply would leave the bit true with an
	// error in the store, and publishIdleStatus would then suppress the clear
	// for as long as the source head does not move. Being wrong this way costs
	// one idempotent idle publish; being wrong the other way costs a permanent
	// error on a healthy mirror — the same asymmetry the field's zero value
	// already resolves in this direction (EN-1773).
	w.statusClearConfirmed = false

	w.proposeMirrorSync(ctx, update, "mirror error report")
}

// proposeMirrorSync proposes a technical-only MirrorSyncUpdate and reports
// whether it was confirmed applied. It is the shared spine of the two
// standalone mirror-status proposals — the idle publish and the error report —
// which differ only in their payload.
//
// label names the operation in the failure logs. The bool return is what makes
// publishIdleStatus's bookkeeping safe: only a confirmed application may
// advance the published state, or a rejected proposal would suppress the retry
// on the next tick. reportError ignores it on purpose — it marks the status
// dirty up front, because for that direction an unconfirmed-but-applied
// proposal is the dangerous case.
func (w *Worker) proposeMirrorSync(ctx context.Context, update *raftcmdpb.MirrorSyncUpdate, label string) bool {
	cmd := &raftcmdpb.Proposal{
		Date:           &commonpb.Timestamp{Data: uint64(libtime.Now().UnixMicro())},
		CallerSnapshot: commands.SystemCallerSnapshot(commands.ComponentMirror),
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{MirrorSync: update},
		}},
	}

	// applyMirrorSyncUpdate reads Registry.Ledgers through the FSM-side Plan,
	// so the TU must declare the ledger key or the gate rejects the read and
	// the update silently skips — the FSM would emit no audit entry and the
	// status would never reach the store. Same coverage as the ingest path.
	needs := plan.NewCoverage()
	needs.Add(dal.SubAttrLedger, domain.LedgerKey{Name: w.ledgerName}.Bytes())

	operations := []plan.WriteOperation{{
		Coverage: needs,
		Target:   &cmd.GetTechnicalUpdates()[0].CoverageBits,
	}}

	logFailure := func(reason string, err error) {
		w.logger.WithFields(map[string]any{
			"error":     err.Error(),
			"operation": label,
		}).Errorf("Mirror sync update %s", reason)
	}

	build, err := w.builder.Build(needs, operations)
	if err != nil {
		if build != nil {
			build.ReleaseLoaders()
		}

		logFailure("preload build failed", err)

		return false
	}

	runResult, err := w.builder.Run(ctx, cmd, build, marshalMirrorCommand, w.proposer)
	if err != nil {
		logFailure("could not be proposed", err)

		return false
	}

	runResult.Guard.ReleaseLoaders()

	// Wait for Raft acceptance THEN FSM apply. Without the FSM wait the FSM
	// could reject the proposal (ErrStaleProposal on tracker drift, for
	// example) and this would return as if it had succeeded.
	if _, err := runResult.Proposal.Wait(ctx); err != nil {
		logFailure("rejected by Raft", err)

		return false
	}

	result, fsmErr := runResult.FSMFuture.Wait(ctx)
	if fsmErr != nil {
		logFailure("rejected by FSM", fsmErr)

		return false
	}

	if result.Error != nil {
		logFailure("apply returned a business error", result.Error)

		return false
	}

	return true
}

// marshalMirrorCommand marshals a proposal command into a newly allocated byte
// slice using a pooled buffer. The returned slice is safe for Raft retention.
func marshalMirrorCommand(cmd *raftcmdpb.Proposal) ([]byte, error) {
	return vtmarshal.MarshalCopy(cmd)
}

// extractMirrorNeeds builds plan.Coverage from a mirror proposal's orders.
// Returns the proposal-wide aggregate Coverage alongside a parallel slice with
// one Coverage per order, used to compute Order.coverage_bits after
// Build. Mirror only touches ledger info, boundaries, volumes and
// account metadata.
func (w *Worker) extractMirrorNeeds(cmd *raftcmdpb.Proposal) (*plan.Coverage, []*plan.Coverage) {
	aggregate := plan.NewCoverage()
	perOrder := make([]*plan.Coverage, len(cmd.GetOrders()))

	ledgerBytes := domain.LedgerKey{Name: w.ledgerName}.Bytes()

	addAccountMetadata := func(p *plan.Coverage, account, key string) {
		p.Add(dal.SubAttrMetadata, domain.MetadataKey{
			AccountKey: domain.AccountKey{LedgerName: w.ledgerName, Account: account},
			Key:        key,
		}.Bytes())
	}
	addTx := func(p *plan.Coverage, txID uint64) {
		p.Add(dal.SubAttrTransaction, domain.TransactionKey{LedgerName: w.ledgerName, ID: txID}.Bytes())
	}

	for orderIdx, order := range cmd.GetOrders() {
		p := plan.NewCoverage()
		p.Add(dal.SubAttrLedger, ledgerBytes)
		p.Add(dal.SubAttrBoundary, ledgerBytes)

		mi := order.GetLedgerScoped().GetMirrorIngest()
		if mi == nil {
			perOrder[orderIdx] = p
			aggregate.Merge(p)

			continue
		}

		var postings []*commonpb.Posting
		if ct := mi.GetEntry().GetCreatedTransaction(); ct != nil {
			postings = ct.GetPostings()
		} else if rt := mi.GetEntry().GetRevertedTransaction(); rt != nil {
			postings = rt.GetReversePostings()
		}

		for _, posting := range postings {
			for _, volKey := range []domain.VolumeKey{
				{AccountKey: domain.AccountKey{LedgerName: w.ledgerName, Account: posting.GetSource()}, Asset: posting.GetAsset()},
				{AccountKey: domain.AccountKey{LedgerName: w.ledgerName, Account: posting.GetDestination()}, Asset: posting.GetAsset()},
			} {
				p.Add(dal.SubAttrVolume, volKey.Bytes())
			}
		}

		// Preload account metadata for previous value capture in logs.
		if ct := mi.GetEntry().GetCreatedTransaction(); ct != nil {
			for account, mm := range ct.GetAccountMetadata() {
				for key := range mm.GetValues() {
					addAccountMetadata(p, account, key)
				}
			}
		}

		if sm := mi.GetEntry().GetSavedMetadata(); sm != nil {
			switch target := sm.GetTarget().GetTarget().(type) {
			case *commonpb.Target_Account:
				for key := range sm.GetMetadata() {
					addAccountMetadata(p, target.Account.GetAddr(), key)
				}
			case *commonpb.Target_TransactionId:
				addTx(p, target.TransactionId)
			}
		}

		if dm := mi.GetEntry().GetDeletedMetadata(); dm != nil {
			switch target := dm.GetTarget().GetTarget().(type) {
			case *commonpb.Target_Account:
				// Same Del coverage as the admission-side
				// MirrorIngest.DeletedMetadata path (see admission.go) —
				// AttributeCache.Del lazy-fabricates the Gen0 tombstone
				// from Gen1's tag if a race occurred.
				p.Add(dal.SubAttrMetadata, domain.MetadataKey{
					AccountKey: domain.AccountKey{LedgerName: w.ledgerName, Account: target.Account.GetAddr()},
					Key:        dm.GetKey(),
				}.Bytes())
			case *commonpb.Target_TransactionId:
				addTx(p, target.TransactionId)
			}
		}

		if rt := mi.GetEntry().GetRevertedTransaction(); rt != nil {
			addTx(p, rt.GetRevertedTransactionId())
		}

		perOrder[orderIdx] = p
		aggregate.Merge(p)
	}

	return aggregate, perOrder
}
