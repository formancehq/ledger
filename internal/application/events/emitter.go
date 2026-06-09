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

	"github.com/formancehq/ledger/v3/internal/infra/node"
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
	config   EmitterConfig
	logger   logging.Logger

	notify  signal.Signal
	cancel  context.CancelFunc
	stopCh  chan struct{}
	stopped chan struct{}
	ready   chan struct{}
	mu      sync.Mutex
	running bool

	// Reusable state for proposeSinkUpdate (single-goroutine, no lock needed).
	proposal   raftcmdpb.Proposal
	marshalBuf []byte

	// failure tracks consecutive publish failures so we don't spam Raft
	// (or the sink) when an external dependency is unhealthy. See
	// sink_failure_state.go for the policy.
	failure sinkFailureState

	// now overridable for tests; defaults to time.Now.
	now func() time.Time
}

// NewEmitter creates a new event emitter for a named sink.
func NewEmitter(store *dal.Store, sink Sink, sinkName string, proposer Proposer, logger logging.Logger, config EmitterConfig) *Emitter {
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
		config:   config,
		logger:   logger.WithFields(map[string]any{"cmp": "event-emitter", "sink": sinkName}),
		notify:   signal.New(),
		now:      time.Now,
	}
}

// Ready returns a channel that is closed when the emitter goroutine has completed
// its initial catch-up and entered the main select loop.
func (e *Emitter) Ready() <-chan struct{} {
	return e.ready
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
	e.ready = make(chan struct{})

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

		return
	}

	e.logger.WithFields(map[string]any{"cursor": cursor}).Infof("Event emitter started")

	// Initial catch-up: process any logs since the cursor
	if cursor, err = e.processLogs(ctx, cursor); err != nil {
		e.logger.Errorf("Error during initial catch-up: %v", err)
	}

	// Signal that the emitter is ready to receive notifications.
	close(e.ready)

	ticker := time.NewTicker(e.config.BatchDelay)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			e.logger.Infof("Event emitter stopped")

			return
		case <-e.notify.C():
			if cursor, err = e.processLogs(ctx, cursor); err != nil {
				e.logger.Errorf("Error processing logs: %v", err)
			}
		case <-ticker.C:
			if cursor, err = e.processLogs(ctx, cursor); err != nil {
				e.logger.Errorf("Error processing logs (poll): %v", err)
			}
		}
	}
}

// processLogs reads logs from the store starting after the given cursor,
// publishes them, and returns the updated cursor position.
//
// If a previous publish failed and the failure backoff has not yet
// elapsed, this is a no-op: the cursor is returned unchanged and no
// Raft proposal is made. This prevents the 10ms ticker from spinning
// the sink (and the Raft log) when an external dependency is unhealthy.
func (e *Emitter) processLogs(ctx context.Context, cursor uint64) (uint64, error) {
	if !e.failure.shouldRetry(e.now()) {
		return cursor, nil
	}

	handle, err := e.store.NewDirectReadHandle()
	if err != nil {
		return cursor, fmt.Errorf("creating read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	logsCursor, err := query.ReadLogsSince(ctx, handle, cursor)
	if err != nil {
		return cursor, err
	}

	defer func() { _ = logsCursor.Close() }()

	batch := make([]*eventspb.Event, 0, e.config.BatchSize)
	lastPersistedCursor := cursor

	for {
		log, err := logsCursor.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return cursor, err
		}

		event := LogToEvent(log)
		// Skip internal logs (e.g. AddedEventsSink) that don't produce domain events.
		if event.GetType() == commonpb.EventType_EVENT_TYPE_UNSPECIFIED {
			cursor = log.GetSequence()

			continue
		}
		// Apply per-sink event type filter (empty = all events).
		if len(e.config.EventTypes) > 0 {
			if _, ok := e.config.EventTypes[event.GetType()]; !ok {
				cursor = log.GetSequence()

				continue
			}
		}

		batch = append(batch, event)

		if len(batch) >= e.config.BatchSize {
			err := e.publishBatch(ctx, batch)
			if err != nil {
				return cursor, err
			}

			cursor = batch[len(batch)-1].GetLogSequence()
			lastPersistedCursor = cursor
			batch = batch[:0]
		}
	}

	// Flush remaining events
	if len(batch) > 0 {
		err := e.publishBatch(ctx, batch)
		if err != nil {
			return cursor, err
		}

		lastPersistedCursor = batch[len(batch)-1].GetLogSequence()
		if cursor < lastPersistedCursor {
			cursor = lastPersistedCursor
		}
	}

	// If cursor advanced past the last persisted position (due to filtered or
	// skipped logs after the last published event), persist the cursor so we
	// don't re-process these logs on restart.
	if cursor > lastPersistedCursor {
		err := e.proposeSinkUpdate(&raftcmdpb.EventsSinkUpdate{
			SinkName: e.sinkName,
			Cursor:   cursor,
		})
		if err != nil {
			return cursor, err
		}
	}

	return cursor, nil
}

func (e *Emitter) publishBatch(ctx context.Context, batch []*eventspb.Event) error {
	err := e.sink.Publish(ctx, batch)
	if err != nil {
		// Report the error via Raft (best-effort), with dedup/backoff.
		e.reportError(err)

		return err
	}

	// Sink is healthy again — reset failure bookkeeping before we
	// propose the cursor advance so a follow-up failure starts from
	// a clean state.
	e.failure.recordSuccess()

	// Advance cursor via Raft and clear any previous error
	lastSeq := batch[len(batch)-1].GetLogSequence()

	err = e.proposeSinkUpdate(&raftcmdpb.EventsSinkUpdate{
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
func (e *Emitter) reportError(publishErr error) {
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

	if err := e.proposeSinkUpdate(update); err != nil {
		e.logger.Errorf("Failed to report sink error via Raft: %v", err)
	}
}

// proposeSinkUpdate proposes a Raft command to update per-sink state.
func (e *Emitter) proposeSinkUpdate(update *raftcmdpb.EventsSinkUpdate) error {
	cmdID := commands.GenerateRandomID()

	e.proposal.Reset()
	e.proposal.Id = cmdID
	e.proposal.EventsSinkUpdates = []*raftcmdpb.EventsSinkUpdate{update}

	size := e.proposal.SizeVT()
	if cap(e.marshalBuf) < size {
		e.marshalBuf = make([]byte, size)
	} else {
		e.marshalBuf = e.marshalBuf[:size]
	}

	n, err := e.proposal.MarshalToVT(e.marshalBuf)
	if err != nil {
		return err
	}

	future, err := e.proposer.Propose(context.Background(), node.NewProposal(cmdID, e.marshalBuf[:n]))
	if err != nil {
		return err
	}

	// Wait for the update to be applied by the FSM
	_, err = future.Wait()

	return err
}
