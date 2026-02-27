package mirror

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/adapter/v2"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/commands"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/signal"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	defaultBatchSize  = 100
	defaultPollPeriod = 5 * time.Second
	initialBackoff    = 1 * time.Second
	maxBackoff        = 60 * time.Second
	backoffMultiplier = 2.0
)

// marshalBufPool holds reusable buffers for proto.MarshalToVT to avoid
// repeated buffer growth allocations in the proposal hot path.
var marshalBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4096)
		return &b
	},
}

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
	store          *dal.Store
	proposer       Proposer
	cache          *cache.Cache
	attrs          *attributes.Attributes
	logger         logging.Logger
	sourceLogCount uint64

	notify       signal.Signal
	w            worker.Worker
	backoff      time.Duration       // current backoff duration (0 = no backoff)
	cursor       uint64              // last known cursor, avoids Pebble read per batch
	cursorLoaded bool
	prefetchCh   chan prefetchResult // pending prefetch from previous batch

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
	preloadCacheMiss  metric.Int64Counter
}

// NewWorker creates a new mirror Worker for the given ledger.
func NewWorker(
	ledgerName string,
	batchSize int,
	source v2.Source,
	store *dal.Store,
	proposer Proposer,
	cache *cache.Cache,
	attrs *attributes.Attributes,
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
	preloadCacheMiss, _ := meter.Int64Counter("mirror.preload.cache_miss",
		metric.WithUnit("1"))

	return &Worker{
		ledgerName: ledgerName,
		batchSize:  batchSize,
		source:     source,
		store:      store,
		proposer:   proposer,
		cache:      cache,
		attrs:      attrs,
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
		preloadCacheMiss:  preloadCacheMiss,
	}
}

// Start begins the background sync loop.
func (w *Worker) Start() {
	w.w = worker.New()
	w.w.Run(w.loop)
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

func (w *Worker) loop(stop <-chan struct{}) {
	ticker := time.NewTicker(defaultPollPeriod)
	defer ticker.Stop()

	// Initial source head query + catch-up
	w.refreshSourceHead()
	w.processLogs(stop)

	for {
		select {
		case <-stop:
			return
		case <-w.notify.C():
			w.processLogs(stop)
		case <-ticker.C:
			w.refreshSourceHead()
			w.processLogs(stop)
		}
	}
}

// refreshSourceHead queries the v2 source for its latest log ID and stores
// it in the worker for inclusion in subsequent cursor reports.
func (w *Worker) refreshSourceHead() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	count, err := w.source.GetLatestLogID(ctx)
	if err != nil {
		w.logger.WithFields(map[string]any{"error": err.Error()}).Errorf("Failed to query source head")
		return
	}
	w.sourceLogCount = count
}

func (w *Worker) processLogs(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		hasMore, err := w.processBatch()
		if err != nil {
			w.logger.WithFields(map[string]any{"error": err.Error()}).Errorf("Mirror sync error")
			w.reportError(err.Error())

			// Apply exponential backoff on persistent errors
			if w.backoff == 0 {
				w.backoff = initialBackoff
			} else {
				w.backoff = time.Duration(float64(w.backoff) * backoffMultiplier)
				if w.backoff > maxBackoff {
					w.backoff = maxBackoff
				}
			}
			select {
			case <-stop:
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

func (w *Worker) processBatch() (bool, error) {
	batchStart := time.Now()
	ctx := context.Background()
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

	// TODO: read actual NextTransactionId from boundaries for gap detection
	// For now, pass 1 and let the processor handle it
	expectedNextTxID := uint64(1)

	// Translate v2 logs to v3 orders
	translateStart := time.Now()
	orders, _, _, err := v2.TranslateBatch(w.ledgerName, v2Logs, expectedNextLogID, expectedNextTxID)
	if err != nil {
		return false, err
	}
	w.translateDuration.Record(ctx, time.Since(translateStart).Microseconds(), attrs)

	if len(orders) == 0 {
		return hasMore, nil
	}

	// Build proposal with orders and preloads for cache population
	cmd := commands.NewCommand(orders...)

	preloadStart := time.Now()
	w.buildPreloads(cmd)
	w.preloadDuration.Record(ctx, time.Since(preloadStart).Microseconds(), attrs)

	// Merge cursor update into the data proposal to avoid a second Raft round-trip.
	// The FSM processes MirrorSyncUpdates on any proposal (machine.go).
	lastV2LogID := v2Logs[len(v2Logs)-1].ID
	cmd.MirrorSyncUpdates = []*raftcmdpb.MirrorSyncUpdate{{
		LedgerName:     w.ledgerName,
		Cursor:         lastV2LogID,
		ClearError:     true,
		SourceLogCount: w.sourceLogCount,
	}}

	// Marshal into a pooled buffer to avoid repeated growth allocations.
	// Copy to exact-size slice since Raft retains a reference to proposal data.
	bufp := marshalBufPool.Get().(*[]byte)
	size := cmd.SizeVT()
	buf := *bufp
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	n, err := cmd.MarshalToVT(buf)
	if err != nil {
		*bufp = buf
		marshalBufPool.Put(bufp)
		return false, err
	}
	cmdData := buf[:n]

	w.commandSize.Record(ctx, int64(len(cmdData)), attrs)

	proposalData := make([]byte, len(cmdData))
	copy(proposalData, cmdData)
	*bufp = buf
	marshalBufPool.Put(bufp)

	proposeStart := time.Now()
	proposal := node.NewProposal(cmd.Id, proposalData)
	fsmFuture, err := w.proposer.Propose(proposal)
	if err != nil {
		return false, err
	}

	// Start prefetching the next batch while waiting for Raft consensus.
	// The goroutine writes to a buffered channel and always exits, even if
	// nobody reads the result (e.g., on stop).
	var nextPrefetchCh chan prefetchResult
	if hasMore {
		nextPrefetchCh = make(chan prefetchResult, 1)
		nextCursor := lastV2LogID
		go func() {
			start := time.Now()
			fCtx, fCancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	if _, err := proposal.Wait(); err != nil {
		w.drainPrefetch(nextPrefetchCh)
		return false, err
	}
	w.proposeDuration.Record(ctx, time.Since(proposeStart).Microseconds(), attrs)

	// Wait for FSM application and check for business errors.
	// Without this, the cursor would advance past entries that failed to process.
	fsmWaitStart := time.Now()
	result, fsmErr := fsmFuture.Wait()
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

func (w *Worker) reportError(message string) {
	update := &raftcmdpb.Proposal{
		Date: &commonpb.Timestamp{Data: uint64(libtime.Now().UnixMicro())},
		MirrorSyncUpdates: []*raftcmdpb.MirrorSyncUpdate{{
			LedgerName: w.ledgerName,
			Error: &commonpb.MirrorSyncError{
				Message:    message,
				OccurredAt: &commonpb.Timestamp{Data: uint64(libtime.Now().UnixMicro())},
			},
		}},
	}

	size := update.SizeVT()
	buf := make([]byte, size)
	n, _ := update.MarshalToVT(buf)

	proposal := node.NewProposal(0, buf[:n])
	_, err := w.proposer.Propose(proposal)
	if err != nil {
		w.logger.WithFields(map[string]any{"error": err.Error()}).Errorf("Failed to report mirror error")
		return
	}
	_, _ = proposal.Wait()
}

// buildPreloads populates the command's PreloadSet with ledger info and
// boundaries so the FSM can inject them into the attribute cache. This is
// necessary because mirror proposals bypass the admission layer which
// normally builds preloads. Without this, a cold cache (after restart)
// causes all mirror processing to fail with "ledger does not exist".
func (w *Worker) buildPreloads(cmd *raftcmdpb.Proposal) {
	boundary := w.cache.BaseIndex.Gen0
	cmd.Preload.LastPersistedIndex = boundary

	canonicalKey := domain.LedgerKey{Name: w.ledgerName}.Bytes()
	id, tag := attributes.MakeKey(attributes.DefaultSeeds, canonicalKey)
	attrID := &raftcmdpb.AttributeID{Id: id[:], Tag: tag}

	// Preload ledger info if not in cache
	if _, ok := w.cache.Ledgers.Get(id); !ok {
		w.preloadCacheMiss.Add(context.Background(), 1,
			metric.WithAttributes(w.ledgerAttr, attribute.String("type", "ledger")))
		info, err := w.attrs.Ledger.ComputeValue(w.store, ^uint64(0), canonicalKey)
		if err == nil && info != nil {
			cmd.Preload.Preloads = append(cmd.Preload.Preloads, &raftcmdpb.Preload{
				Type: &raftcmdpb.Preload_Ledger{
					Ledger: &raftcmdpb.PreloadLedger{
						Id:   attrID,
						Info: info,
					},
				},
			})
		}
	}

	// Preload boundaries if not in cache (same canonical key, different cache)
	if _, ok := w.cache.Boundaries.Get(id); !ok {
		w.preloadCacheMiss.Add(context.Background(), 1,
			metric.WithAttributes(w.ledgerAttr, attribute.String("type", "boundary")))
		boundaries, err := w.attrs.Boundary.ComputeValue(w.store, ^uint64(0), canonicalKey)
		if err == nil && boundaries != nil {
			cmd.Preload.Preloads = append(cmd.Preload.Preloads, &raftcmdpb.Preload{
				Type: &raftcmdpb.Preload_Boundary{
					Boundary: &raftcmdpb.PreloadBoundary{
						Id:         attrID,
						Boundaries: boundaries,
					},
				},
			})
		}
	}
}
