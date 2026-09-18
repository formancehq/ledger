package events

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// resolvedFuture returns a Future already resolved with a zero-value
// ApplyResult so MockProposer.Propose can hand back something Wait()-able.
func resolvedFuture() *futures.Future[state.ApplyResult] {
	f := futures.New[state.ApplyResult]()
	f.Resolve(state.ApplyResult{}, nil)

	return f
}

// TestEmitter_PublishBatch_ReportError_Dedups exercises the wiring of
// sinkFailureState through publishBatch + reportError. It guards the
// integration point that the unit-level sinkFailureState tests don't
// reach: that reportError actually consults recordFailure and skips
// the Raft proposal when dedup says so.
func TestEmitter_PublishBatch_ReportError_Dedups(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	sink := NewMockSink(ctrl)
	proposer := NewMockProposer(ctrl)
	logger := logging.Testing()

	publishErr := errors.New("connection refused")
	sink.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(publishErr).AnyTimes()

	// Expect exactly two proposals across the run: the first failure,
	// then one more after the remind interval elapses. gomock fails
	// the test if Propose is called any other number of times.
	proposer.EXPECT().
		Propose(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, p *node.Proposal) (*futures.Future[state.ApplyResult], error) {
			p.Resolve(nil, nil)

			return resolvedFuture(), nil
		}).
		Times(2)

	builder, store := newTestBuilder(t)
	emitter := NewEmitter(store, sink, "test", proposer, builder, logger, DefaultEmitterConfig())

	clock := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)
	emitter.now = func() time.Time { return clock }

	batch := []*eventspb.Event{{LogSequence: 1}}

	// First failure: must propose.
	require.Error(t, emitter.publishBatch(context.Background(), batch))

	// 50 more identical failures within the remind interval: must dedup.
	for range 50 {
		clock = clock.Add(100 * time.Millisecond)

		require.Error(t, emitter.publishBatch(context.Background(), batch))
	}

	// Past the remind interval: must propose once more so the
	// SinkError timestamp doesn't get stuck.
	clock = clock.Add(sinkFailureRemindInterval + time.Second)
	require.Error(t, emitter.publishBatch(context.Background(), batch))
}

// TestEmitter_PublishBatch_ReportError_ReportsOnMessageChange asserts a
// changed error message bypasses dedup even within the remind interval.
func TestEmitter_PublishBatch_ReportError_ReportsOnMessageChange(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	sink := NewMockSink(ctrl)
	proposer := NewMockProposer(ctrl)
	logger := logging.Testing()

	gomock.InOrder(
		sink.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(errors.New("err A")),
		sink.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(errors.New("err B")),
	)
	proposer.EXPECT().
		Propose(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, p *node.Proposal) (*futures.Future[state.ApplyResult], error) {
			p.Resolve(nil, nil)

			return resolvedFuture(), nil
		}).
		Times(2)

	builder, store := newTestBuilder(t)
	emitter := NewEmitter(store, sink, "test", proposer, builder, logger, DefaultEmitterConfig())

	clock := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)
	emitter.now = func() time.Time { return clock }

	batch := []*eventspb.Event{{LogSequence: 1}}

	require.Error(t, emitter.publishBatch(context.Background(), batch))

	// Same call again but the sink now returns a different message,
	// still inside the remind interval — must propose again.
	clock = clock.Add(time.Second)
	require.Error(t, emitter.publishBatch(context.Background(), batch))
}

// TestEmitter_PublishBatch_RecoverClearsFailureState asserts that a
// successful Publish calls recordSuccess (via the cursor-advance path),
// so the next failure is reported even if the message matches what was
// previously deduped.
func TestEmitter_PublishBatch_RecoverClearsFailureState(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	sink := NewMockSink(ctrl)
	proposer := NewMockProposer(ctrl)
	logger := logging.Testing()

	publishErr := errors.New("connection refused")
	gomock.InOrder(
		sink.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(publishErr), // 1: fail
		sink.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil),        // 2: recover
		sink.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(publishErr), // 3: fail with same msg
	)

	// Expect: 1 error proposal + 1 cursor-advance proposal + 1 error
	// proposal = 3 total. If recordSuccess didn't reset, the 3rd would
	// be deduped against the 1st and Times(3) would fail.
	proposer.EXPECT().
		Propose(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, p *node.Proposal) (*futures.Future[state.ApplyResult], error) {
			p.Resolve(nil, nil)

			return resolvedFuture(), nil
		}).
		Times(3)

	builder, store := newTestBuilder(t)
	emitter := NewEmitter(store, sink, "test", proposer, builder, logger, DefaultEmitterConfig())
	emitter.now = func() time.Time { return time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC) }

	batch := []*eventspb.Event{{LogSequence: 1}}

	require.Error(t, emitter.publishBatch(context.Background(), batch))   // fail
	require.NoError(t, emitter.publishBatch(context.Background(), batch)) // recover (resets state)
	require.Error(t, emitter.publishBatch(context.Background(), batch))   // same msg, must report fresh
}

// This covers the actual HTTP failure through the Raft submission boundary,
// where the adapter's diagnostic is persisted verbatim as the public status.
func TestEmitterHTTPFailurePersistsUsefulSanitizedDiagnostic(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic(http.ErrAbortHandler) }))
	defer server.Close()
	sink, err := NewHTTPSink(HTTPSinkConfig{Endpoint: "http://alice:EXAMPLE_PASSWORD@" + server.URL[len("http://"):] + "/events?api_key=EXAMPLE_API_KEY", Format: FormatProto})
	require.NoError(t, err)
	defer func() { require.NoError(t, sink.Close()) }()
	proposer := NewMockProposer(gomock.NewController(t))
	proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, p *node.Proposal) (*futures.Future[state.ApplyResult], error) {
		var proposal raftcmdpb.Proposal
		require.NoError(t, proposal.UnmarshalVT(p.Data()))
		require.Len(t, proposal.GetTechnicalUpdates(), 1)
		message := proposal.GetTechnicalUpdates()[0].GetEventsSink().GetError().GetMessage()
		require.Contains(t, message, "posting event seq=1")
		require.Contains(t, message, server.URL[len("http://"):])
		require.Contains(t, message, "EOF")
		require.NotContains(t, message, "EXAMPLE_PASSWORD")
		require.NotContains(t, message, "EXAMPLE_API_KEY")
		p.Resolve(nil, nil)

		return resolvedFuture(), nil
	}).Times(1)
	builder, store := newTestBuilder(t)
	emitter := NewEmitter(store, sink, "test", proposer, builder, logging.Testing(), DefaultEmitterConfig())
	require.Error(t, emitter.publishBatch(context.Background(), []*eventspb.Event{{LogSequence: 1}}))
}
