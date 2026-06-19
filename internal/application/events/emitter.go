package events

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source emitter.go -destination emitter_generated_test.go -typed -package events . Proposer

// Proposer proposes commands to the Raft cluster.
type Proposer interface {
	Propose(ctx context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error)
}

// EmitterConfig holds configuration for the event emitter.
type EmitterConfig struct {
	BatchSize  int
	BatchDelay time.Duration
	Format     Format
	EventTypes map[commonpb.EventType]struct{} // nil or empty = all events
}

const deliveredCursorUpdateTimeout = 2 * time.Second

// DefaultEmitterConfig returns the default emitter configuration.
func DefaultEmitterConfig() EmitterConfig {
	return EmitterConfig{
		BatchSize:  64,
		BatchDelay: 10 * time.Millisecond,
		Format:     FormatJSON,
	}
}

// Emitter tails the global log and publishes domain events to a sink.
// It runs as a background goroutine and is gated by the node's leader status.
// Each Emitter is associated with a named sink and tracks its own cursor.
type Emitter struct {
	store    *dal.Store
	sink     Sink
	sinkName string
	proposer Proposer
	builder  *plan.Builder
	config   EmitterConfig
	logger   logging.Logger

	notify  signal.Signal
	cancel  context.CancelFunc
	stopCh  chan struct{}
	stopped chan struct{}
	started chan struct{}
	mu      sync.Mutex
	running bool

	// startErr is written exactly once by run before started is closed. WaitStarted
	// reads it only after observing that close, which provides the happens-before edge.
	startErr error

	// Reusable proposal struct for proposeSinkUpdate (single-goroutine, no
	// lock needed). The marshal output is NOT reused — see vtmarshal.MarshalCopy.
	proposal raftcmdpb.Proposal

	// failure tracks consecutive publish failures so we don't spam Raft
	// (or the sink) when an external dependency is unhealthy. See
	// sink_failure_state.go for the policy.
	failure sinkFailureState

	// now overridable for tests; defaults to time.Now.
	now func() time.Time
}

// NewEmitter creates a new event emitter for a named sink.
func NewEmitter(store *dal.Store, sink Sink, sinkName string, proposer Proposer, builder *plan.Builder, logger logging.Logger, config EmitterConfig) *Emitter {
	if config.BatchSize <= 0 {
		config.BatchSize = 64
	}

	if config.BatchDelay <= 0 {
		config.BatchDelay = 10 * time.Millisecond
	}

	return &Emitter{
		store:    store,
		sink:     sink,
		sinkName: sinkName,
		proposer: proposer,
		builder:  builder,
		config:   config,
		logger:   logger.WithFields(map[string]any{"cmp": "event-emitter", "sink": sinkName}),
		notify:   signal.New(),
		now:      time.Now,
	}
}

// WaitStarted waits until the emitter has completed its startup read.
func (e *Emitter) WaitStarted(ctx context.Context) error {
	select {
	case <-e.started:
		return e.startErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Notify sends a non-blocking signal that new logs are available.
func (e *Emitter) Notify() {
	e.notify.Notify()
}

// Start begins the background event emission loop.
// It is idempotent — calling Start on an already-running emitter is a no-op.
func (e *Emitter) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return
	}

	e.running = true
	e.stopCh = make(chan struct{})
	e.stopped = make(chan struct{})
	e.started = make(chan struct{})
	e.startErr = nil

	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel

	go e.run(ctx)
}

// Stop gracefully stops the background emission loop.
// It is idempotent — calling Stop on an already-stopped emitter is a no-op.
func (e *Emitter) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.running {
		return
	}

	e.running = false
	e.cancel()
	close(e.stopCh)
	<-e.stopped
}

func (e *Emitter) run(ctx context.Context) {
	defer close(e.stopped)

	cursor, err := query.ReadSinkCursor(e.store, e.sinkName)
	if err != nil {
		e.logger.Errorf("Failed to read sink cursor: %v", err)
		e.signalStarted(err)

		return
	}

	e.logger.WithFields(map[string]any{"cursor": cursor}).Infof("Event emitter started")
	e.signalStarted(nil)

	ticker := time.NewTicker(e.config.BatchDelay)
	defer ticker.Stop()

	// Queue the first batch through the main loop so startup never waits for
	// full catch-up and stop requests are observed at the same boundary as
	// notification-driven work.
	e.notify.Notify()

	for {
		select {
		case <-e.stopCh:
			e.logger.Infof("Event emitter stopped")

			return
		default:
		}

		select {
		case <-e.stopCh:
			e.logger.Infof("Event emitter stopped")

			return
		case <-e.notify.C():
		case <-ticker.C:
		}

		select {
		case <-e.stopCh:
			e.logger.Infof("Event emitter stopped")

			return
		default:
		}

		var more bool
		cursor, more, err = e.processLogBatch(ctx, cursor)
		if err != nil {
			e.logger.Errorf("Error processing logs: %v", err)
		}

		if more {
			e.notify.Notify()
		}
	}
}

// shouldEmit reports whether an event derived from a log entry should be
// published to this sink. Internal logs (EVENT_TYPE_UNSPECIFIED, e.g.
// AddedEventsSink) are always dropped. When the sink declares an
// EventTypes allow-list, only types in that set are emitted.
func (e *Emitter) shouldEmit(event *eventspb.Event) bool {
	if event.GetType() == commonpb.EventType_EVENT_TYPE_UNSPECIFIED {
		return false
	}

	if len(e.config.EventTypes) == 0 {
		return true
	}

	_, ok := e.config.EventTypes[event.GetType()]

	return ok
}

func (e *Emitter) signalStarted(err error) {
	e.startErr = err
	close(e.started)
}

// processLogBatch reads logs from the store starting after the given cursor,
// publishes at most one batch, and returns the updated cursor position. The
// boolean return is true when more logs may be immediately available.
//
// If a previous publish failed and the failure backoff has not yet
// elapsed, this is a no-op: the cursor is returned unchanged and no
// Raft proposal is made. This prevents the 10ms ticker from spinning
// the sink (and the Raft log) when an external dependency is unhealthy.
func (e *Emitter) processLogBatch(ctx context.Context, cursor uint64) (uint64, bool, error) {
	if !e.failure.shouldRetry(e.now()) {
		return cursor, false, nil
	}

	handle, err := e.store.NewDirectReadHandle()
	if err != nil {
		return cursor, false, fmt.Errorf("creating read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	logsCursor, err := query.ReadLogsSince(ctx, handle, cursor)
	if err != nil {
		return cursor, false, err
	}

	defer func() { _ = logsCursor.Close() }()

	batch := make([]*eventspb.Event, 0, e.config.BatchSize)
	lastPersistedCursor := cursor

	// pendingFilteredCursor remembers the highest sequence of a filtered or
	// internal log scanned while batch != nil. We MUST NOT advance cursor
	// through those logs in-place: if a later publishBatch fails, returning
	// the scan position would skip the unpublished events with lower
	// sequences and lose them as soon as the next successful flush
	// persists the bumped cursor via Raft (#323). Instead we stash the
	// position here and apply it together with batch[last].LogSequence
	// after the next successful flush.
	var pendingFilteredCursor uint64

	for {
		log, err := logsCursor.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return cursor, false, err
		}

		event := LogToEvent(log)

		if !e.shouldEmit(event) {
			if len(batch) == 0 {
				// No pending events with lower sequence: advancing the
				// cursor through this filtered log is safe.
				cursor = log.GetSequence()
			} else {
				// Defer: this log's seq is > every event in the pending
				// batch (scan is sequential), so we apply it only after
				// the batch is durably published.
				pendingFilteredCursor = log.GetSequence()
			}

			continue
		}

		batch = append(batch, event)

		if len(batch) >= e.config.BatchSize {
			err := e.publishBatch(ctx, batch)
			if err != nil {
				return cursor, false, err
			}

			cursor = max(pendingFilteredCursor, batch[len(batch)-1].GetLogSequence())

			return cursor, true, nil
		}
	}

	// Flush remaining events
	if len(batch) > 0 {
		err := e.publishBatch(ctx, batch)
		if err != nil {
			return cursor, false, err
		}

		lastPersistedCursor = batch[len(batch)-1].GetLogSequence()
		if cursor < lastPersistedCursor {
			cursor = lastPersistedCursor
		}
		// Filtered logs deferred during this batch window are now safe to
		// absorb. The end-of-function block below will issue a single
		// trailing Raft update if needed.
		if pendingFilteredCursor > cursor {
			cursor = pendingFilteredCursor
		}
	}

	// If cursor advanced past the last persisted position (due to filtered or
	// skipped logs after the last published event), persist the cursor so we
	// don't re-process these logs on restart.
	if cursor > lastPersistedCursor {
		err := e.proposeSinkUpdate(ctx, &raftcmdpb.EventsSinkUpdate{
			SinkName: e.sinkName,
			Cursor:   cursor,
		})
		if err != nil {
			return cursor, false, err
		}
	}

	return cursor, false, nil
}

func (e *Emitter) publishBatch(ctx context.Context, batch []*eventspb.Event) error {
	err := e.sink.Publish(ctx, batch)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		// Report the error via Raft (best-effort), with dedup/backoff.
		e.reportError(ctx, err)

		return err
	}

	// Sink is healthy again — reset failure bookkeeping before we
	// propose the cursor advance so a follow-up failure starts from
	// a clean state.
	e.failure.recordSuccess()

	// Advance cursor via Raft and clear any previous error
	lastSeq := batch[len(batch)-1].GetLogSequence()

	// A delivered batch should get a best-effort cursor advance even if Stop
	// races the proposal. Keep it bounded so leadership loss cannot wait forever.
	updateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deliveredCursorUpdateTimeout)
	defer cancel()

	err = e.proposeSinkUpdate(updateCtx, &raftcmdpb.EventsSinkUpdate{
		SinkName:   e.sinkName,
		Cursor:     lastSeq,
		ClearError: true,
	})
	if err != nil {
		return err
	}

	if e.logger.Enabled(logging.TraceLevel) {
		e.logger.WithFields(map[string]any{
			"count":    len(batch),
			"last_seq": lastSeq,
		}).Tracef("Published event batch")
	}

	return nil
}

// reportError records a publish failure and proposes a sink error
// status via Raft when the failure is worth reporting (first failure,
// message changed, or remind interval elapsed — see sinkFailureState).
// The Raft proposal itself remains best-effort: failures to propose
// are logged but do not propagate.
func (e *Emitter) reportError(ctx context.Context, publishErr error) {
	now := e.now()

	if !e.failure.recordFailure(now, publishErr) {
		// Same error within the remind interval — skip the Raft
		// roundtrip. The SinkStatus already carries this message
		// from the previous report.
		return
	}

	update := &raftcmdpb.EventsSinkUpdate{
		SinkName: e.sinkName,
		Error: &commonpb.SinkError{
			Message:    publishErr.Error(),
			OccurredAt: commonpb.NewTimestamp(libtime.New(now)),
		},
	}

	if err := e.proposeSinkUpdate(ctx, update); err != nil {
		e.logger.Errorf("Failed to report sink error via Raft: %v", err)
	}
}

// maxSinkUpdateStaleRetries bounds the number of times proposeSinkUpdate
// will retry an ErrStaleProposal rejection before giving up. Stale
// rejections fire when the IndexTracker is inflated from a dropped
// proposal (typically a leadership transition); a fresh PredictedIndex
// is computed on every re-attempt, so once the tracker catches up the
// next try succeeds. Matches bootstrap.proposeTechnical's bound.
const maxSinkUpdateStaleRetries = 5

// proposeSinkUpdate proposes a Raft command to update per-sink state.
// Routed through Builder.Run so PredictedIndex, IndexTracker mutex
// ordering and the (fast-path) no-preload flow are identical to every
// other proposer.
//
// Stale rejections must be retried here, not returned to the caller:
// publishBatch has already delivered the events to the external sink
// before this function is reached, so a returned error makes the
// emitter restart from the unchanged cursor and re-publish the same
// batch. Mirror bootstrap.proposeTechnical's bounded retry.
func (e *Emitter) proposeSinkUpdate(ctx context.Context, update *raftcmdpb.EventsSinkUpdate) error {
	var lastErr error

	for range maxSinkUpdateStaleRetries {
		err := e.proposeSinkUpdateOnce(ctx, update)
		if err == nil {
			return nil
		}

		if !errors.Is(err, domain.ErrStaleProposal) {
			return err
		}

		lastErr = err
	}

	return fmt.Errorf("proposeSinkUpdate: giving up after %d stale retries: %w", maxSinkUpdateStaleRetries, lastErr)
}

func (e *Emitter) proposeSinkUpdateOnce(ctx context.Context, update *raftcmdpb.EventsSinkUpdate) error {
	// Reset every per-attempt field so Run assigns a fresh ID and
	// PredictedIndex; the previous stale rejection left them populated.
	e.proposal.Reset()
	e.proposal.Id = commands.GenerateRandomID()
	e.proposal.ExecutionPlan = nil
	e.proposal.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{
		Kind: &raftcmdpb.TechnicalUpdate_EventsSink{EventsSink: update},
	}}

	// One WriteOperation per TU. applyEventsSinkUpdate reads no cache
	// state, so Needs stays nil and the runner takes the fast path
	// (tracker mutex held just long enough to inject PredictedIndex +
	// proposer.Propose).
	operations := []plan.WriteOperation{{
		SetCoverage: func(bits []byte) {
			e.proposal.GetTechnicalUpdates()[0].CoverageBits = bits
		},
	}}

	build, err := e.builder.Build(operations)
	if err != nil {
		if build != nil {
			build.ReleaseLoaders()
		}

		return fmt.Errorf("building preloads for sink update: %w", err)
	}

	result, err := e.builder.Run(
		ctx, &e.proposal, build,
		func(c *raftcmdpb.Proposal) ([]byte, error) { return c.MarshalVT() },
		e.proposer,
	)
	if err != nil {
		return err
	}

	result.Guard.ReleaseLoaders()

	if _, err := result.Proposal.WaitContext(ctx); err != nil {
		return fmt.Errorf("waiting for raft acceptance: %w", err)
	}

	applyResult, err := result.FSMFuture.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("waiting for FSM apply: %w", err)
	}

	// The FSM apply succeeded transport-wise but may have rejected the
	// proposal as a business error (ErrStaleProposal from a stale
	// IndexTracker after leadership churn, ErrCoverageMiss on a
	// malformed plan, etc.). Wrap with %w so the caller retry loop
	// can detect ErrStaleProposal via errors.Is.
	if applyResult.Error != nil {
		return fmt.Errorf("applying sink update: %w", applyResult.Error)
	}

	return nil
}
