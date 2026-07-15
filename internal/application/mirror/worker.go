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
	"github.com/formancehq/ledger/v3/internal/query"
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
// the previous batch's Raft wait. The cursor field is used to validate that
// the prefetch is still valid (cursor hasn't changed due to errors).
type prefetchResult struct {
	logs     []v2.V2Log
	hasMore  bool
	err      error
	duration time.Duration
	cursor   uint64
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

	notify         signal.Signal
	w              worker.Worker
	backoff        time.Duration // current backoff duration (0 = no backoff)
	cursor         uint64        // last known cursor, avoids Pebble read per batch
	nextTxID       uint64        // last known next transaction ID, avoids Pebble read per batch
	cursorLoaded   bool
	nextTxIDLoaded bool
	prefetchCh     chan prefetchResult // pending prefetch from previous batch

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

	// Load cursor from Pebble only once; subsequent batches use the in-memory value.
	if !w.cursorLoaded {
		cursor, err := query.ReadMirrorCursor(w.store, w.ledgerName)
		if err != nil {
			return false, err
		}

		w.cursor = cursor
		w.cursorLoaded = true
	}

	expectedNextLogID := w.cursor + 1
	if w.cursor == 0 {
		expectedNextLogID = 1
	}

	// Use prefetched result if available and valid, otherwise fetch synchronously.
	var (
		v2Logs   []v2.V2Log
		hasMore  bool
		fetchDur time.Duration
	)

	if w.prefetchCh != nil {
		pf := <-w.prefetchCh

		w.prefetchCh = nil
		if pf.err == nil && pf.cursor == w.cursor {
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

		v2Logs, hasMore, err = w.source.FetchLogs(fetchCtx, w.cursor, w.batchSize)
		if err != nil {
			return false, err
		}

		fetchDur = time.Since(fetchStart)
	}

	w.fetchDuration.Record(ctx, fetchDur.Microseconds(), attrs)

	if len(v2Logs) == 0 {
		return false, nil
	}

	w.logsIngested.Add(ctx, int64(len(v2Logs)), attrs)

	// Load NextTransactionId from boundaries only once; subsequent batches use the in-memory value
	// updated by TranslateBatch.
	if !w.nextTxIDLoaded {
		boundaries, err := w.builder.ReadBoundaries(w.ledgerName)
		if err != nil {
			return false, fmt.Errorf("reading boundaries: %w", err)
		}

		if boundaries != nil {
			w.nextTxID = boundaries.GetNextTransactionId()
		} else {
			w.nextTxID = 1
		}

		w.nextTxIDLoaded = true
	}

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

	// Merge cursor update into the data proposal to avoid a second Raft round-trip.
	// The FSM processes TechnicalUpdates on any proposal (machine.go).
	lastV2LogID := v2Logs[len(v2Logs)-1].ID
	cmd.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{
		Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{
			MirrorSync: &raftcmdpb.MirrorSyncUpdate{
				LedgerName:     w.ledgerName,
				Cursor:         lastV2LogID,
				ClearError:     true,
				SourceLogCount: w.sourceLogCount,
			},
		},
	}}

	// One WriteOperation per Order + one for the cursor TU. The cursor
	// TU reads Registry.Ledgers[w.ledgerName] in applyMirrorSyncUpdate.
	tuNeeds := plan.NewCoverage()
	tuNeeds.Add(dal.SubAttrLedger, domain.LedgerKey{Name: w.ledgerName}.Bytes())

	// Roll the cursor TU's need into the batch aggregate — Build no
	// longer recomputes it from operations.
	aggregate.Merge(tuNeeds)

	operations := make([]plan.WriteOperation, 0, len(orders)+1)
	cmdOrders := cmd.GetOrders()
	for i := range orders {
		// coverage_bits moved to OrderTechnical; init nil-safely before Build
		// fills it through the pointer.
		if cmdOrders[i].Technical == nil {
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
		nextCursor := lastV2LogID

		go func() {
			start := time.Now()

			// Derive from the worker's ctx so Worker.Stop() unblocks the
			// source fetch — without this, drainPrefetch can wait up to
			// 30s on a fetch that won't return any faster.
			fCtx, fCancel := context.WithTimeout(ctx, 30*time.Second)
			defer fCancel()

			logs, more, fetchErr := w.source.FetchLogs(fCtx, nextCursor, w.batchSize)
			nextPrefetchCh <- prefetchResult{
				logs:     logs,
				hasMore:  more,
				err:      fetchErr,
				duration: time.Since(start),
				cursor:   nextCursor,
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

	// Update in-memory cursor so next batch skips the Pebble read.
	w.cursor = lastV2LogID
	w.prefetchCh = nextPrefetchCh

	return hasMore, nil
}

// drainPrefetch waits for a background prefetch goroutine to complete,
// discarding its result. This prevents goroutine leaks on error paths.
func (w *Worker) drainPrefetch(ch chan prefetchResult) {
	if ch != nil {
		<-ch
	}
}

func (w *Worker) reportError(ctx context.Context, message string) {
	cmd := &raftcmdpb.Proposal{
		Date:           &commonpb.Timestamp{Data: uint64(libtime.Now().UnixMicro())},
		CallerSnapshot: commands.SystemCallerSnapshot(commands.ComponentMirror),
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{
				MirrorSync: &raftcmdpb.MirrorSyncUpdate{
					LedgerName: w.ledgerName,
					Error: &commonpb.MirrorSyncError{
						Message:    message,
						OccurredAt: &commonpb.Timestamp{Data: uint64(libtime.Now().UnixMicro())},
					},
				},
			},
		}},
	}

	// applyMirrorSyncUpdate reads Registry.Ledgers through the FSM-side
	// Plan. Without a declared Ledgers key the gate rejects the
	// read and the mirror status update silently skips — the FSM would
	// emit no audit entry and the error would never reach the store.
	// One WriteOperation for the error TU with its ledger needs declared.
	needs := plan.NewCoverage()
	needs.Add(dal.SubAttrLedger, domain.LedgerKey{Name: w.ledgerName}.Bytes())

	operations := []plan.WriteOperation{{
		Coverage: needs,
		Target:   &cmd.GetTechnicalUpdates()[0].CoverageBits,
	}}

	build, err := w.builder.Build(needs, operations)
	if err != nil {
		if build != nil {
			build.ReleaseLoaders()
		}

		w.logger.WithFields(map[string]any{"error": err.Error()}).Errorf("Failed to build preloads for mirror error report")

		return
	}

	runResult, err := w.builder.Run(ctx, cmd, build, marshalMirrorCommand, w.proposer)
	if err != nil {
		w.logger.WithFields(map[string]any{"error": err.Error()}).Errorf("Failed to report mirror error")

		return
	}

	runResult.Guard.ReleaseLoaders()

	// Wait for Raft acceptance THEN FSM apply. Without the FSM wait, the
	// FSM could reject the proposal (ErrStaleProposal on tracker drift,
	// for example) and reportError would return as if it succeeded — the
	// mirror error status would never reach the store.
	if _, err := runResult.Proposal.Wait(ctx); err != nil {
		w.logger.WithFields(map[string]any{"error": err.Error()}).Errorf("Mirror error report rejected by Raft")

		return
	}

	result, fsmErr := runResult.FSMFuture.Wait(ctx)
	if fsmErr != nil {
		w.logger.WithFields(map[string]any{"error": fsmErr.Error()}).Errorf("Mirror error report rejected by FSM")

		return
	}

	if result.Error != nil {
		w.logger.WithFields(map[string]any{"error": result.Error.Error()}).Errorf("Mirror error report apply returned business error")
	}
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
