package plan

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// recordingProposer drives SubmitTechnical's downstream phases
// deterministically. It records every proposal and can answer
// Propose with stale rejections, hang Raft acceptance, or hang FSM
// apply, according to its mode fields.
type recordingProposer struct {
	tracker *node.IndexTracker

	mu          sync.Mutex
	proposals   []*node.Proposal
	staleRemain int
	alwaysStale bool
	holdRaft    bool // leave the proposal's Raft-acceptance future unresolved
	holdFSM     bool // resolve Raft acceptance but leave FSM apply unresolved
}

func (p *recordingProposer) Propose(_ context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	p.mu.Lock()
	p.proposals = append(p.proposals, proposal)
	p.mu.Unlock()

	// Mirror node.Node.Propose: advancing the tracker on every submit is
	// what makes a fresh PredictedIndex observable on each retry.
	if p.tracker != nil {
		p.tracker.Increment(1)
	}

	if !p.holdRaft {
		proposal.Resolve(nil, nil)
	}

	f := futures.New[state.ApplyResult]()

	switch {
	case p.holdFSM:
		// Leave f unresolved so FSMFuture.Wait blocks until ctx cancel.
	case p.alwaysStale:
		f.Resolve(state.ApplyResult{Error: &domain.BusinessError{Err: domain.ErrStaleProposal}}, nil)
	default:
		p.mu.Lock()
		stale := p.staleRemain > 0
		if stale {
			p.staleRemain--
		}
		p.mu.Unlock()

		if stale {
			f.Resolve(state.ApplyResult{Error: &domain.BusinessError{Err: domain.ErrStaleProposal}}, nil)
		} else {
			f.Resolve(state.ApplyResult{}, nil)
		}
	}

	return f, nil
}

func (p *recordingProposer) capturedProposals(t *testing.T) []*raftcmdpb.Proposal {
	t.Helper()

	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]*raftcmdpb.Proposal, 0, len(p.proposals))
	for _, proposal := range p.proposals {
		decoded := &raftcmdpb.Proposal{}
		require.NoError(t, decoded.UnmarshalVT(proposal.Data()))
		out = append(out, decoded)
	}

	return out
}

func (p *recordingProposer) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.proposals)
}

func newSubmitTestBuilder(t *testing.T, tracker *node.IndexTracker) *Builder {
	t.Helper()

	logger := logging.Testing()
	store, err := dal.NewStore(t.TempDir(), logger, noop.Meter{}, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	c, err := cache.New(1000, noop.Meter{})
	require.NoError(t, err)

	return NewBuilder(tracker, c, attributes.New(), store, nil, logger, 0)
}

// TestSubmitTechnical_RetriesStaleThenSucceeds pins the shared bounded
// retry: one ErrStaleProposal rejection is retried internally rather
// than surfaced, and every attempt carries a fresh command ID and a
// fresh PredictedIndex.
func TestSubmitTechnical_RetriesStaleThenSucceeds(t *testing.T) {
	t.Parallel()

	tracker := node.NewIndexTracker(1)
	builder := newSubmitTestBuilder(t, tracker)
	proposer := &recordingProposer{tracker: tracker, staleRemain: 1}
	cmd := &raftcmdpb.Proposal{}

	err := SubmitTechnical(context.Background(), builder, proposer, cmd, nil, "test")
	require.NoError(t, err)
	require.Equal(t, 2, proposer.callCount())

	captured := proposer.capturedProposals(t)
	require.Len(t, captured, 2)

	first, second := captured[0], captured[1]
	require.NotZero(t, first.GetId(), "first attempt must get a fresh command ID")
	require.NotZero(t, second.GetId(), "second attempt must get a fresh command ID")
	require.NotEqual(t, first.GetId(), second.GetId(), "each retry must use a fresh command ID")
	require.NotZero(t, first.GetPredictedIndex(), "first attempt must get a predicted index")
	require.NotZero(t, second.GetPredictedIndex(), "second attempt must get a predicted index")
	require.Greater(t, second.GetPredictedIndex(), first.GetPredictedIndex(),
		"retry must recompute PredictedIndex against the advanced tracker")
}

// TestSubmitTechnical_ExhaustsStaleRetryBound pins the hard stop when
// every attempt is rejected as stale: the error wraps ErrStaleProposal
// (errors.Is works) and the retry loop makes exactly maxTechnicalStaleRetries
// attempts.
func TestSubmitTechnical_ExhaustsStaleRetryBound(t *testing.T) {
	t.Parallel()

	builder := newSubmitTestBuilder(t, node.NewIndexTracker(1))
	proposer := &recordingProposer{alwaysStale: true}

	err := SubmitTechnical(context.Background(), builder, proposer, &raftcmdpb.Proposal{}, nil, "test")
	require.Error(t, err)
	require.ErrorIs(t, err, domain.ErrStaleProposal)
	require.Equal(t, maxTechnicalStaleRetries, proposer.callCount())
	require.Contains(t, err.Error(), "giving up after 5 stale retries")
}

// TestSubmitTechnical_CancelBeforeRaftAcceptance pins that a context
// cancellation while waiting for Raft acceptance unwinds instead of
// hanging, and preserves the cancellation cause via errors.Is.
func TestSubmitTechnical_CancelBeforeRaftAcceptance(t *testing.T) {
	t.Parallel()

	builder := newSubmitTestBuilder(t, node.NewIndexTracker(1))
	proposer := &recordingProposer{holdRaft: true}

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- SubmitTechnical(ctx, builder, proposer, &raftcmdpb.Proposal{}, nil, "test")
	}()

	// Wait until Propose has been reached (Raft acceptance outstanding).
	require.Eventually(t, func() bool { return proposer.callCount() == 1 }, time.Second, time.Millisecond)
	cancel()

	err := <-errCh
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.Contains(t, err.Error(), "waiting for raft acceptance")
}

// TestSubmitTechnical_CancelAfterRaftAcceptance pins that cancellation
// between Raft acceptance and FSM apply unwinds the FSM wait instead of
// hanging.
func TestSubmitTechnical_CancelAfterRaftAcceptance(t *testing.T) {
	t.Parallel()

	builder := newSubmitTestBuilder(t, node.NewIndexTracker(1))
	proposer := &recordingProposer{holdFSM: true}

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- SubmitTechnical(ctx, builder, proposer, &raftcmdpb.Proposal{}, nil, "test")
	}()

	require.Eventually(t, func() bool { return proposer.callCount() == 1 }, time.Second, time.Millisecond)
	cancel()

	err := <-errCh
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.Contains(t, err.Error(), "waiting for FSM apply")
}

// TestSubmitTechnical_ReleasesLoaders pins that a successful submission
// (through the slow preload path, not the empty no-op token) releases its
// loader keys exactly once, so later preloads of the same key do a real
// load again.
func TestSubmitTechnical_ReleasesLoaders(t *testing.T) {
	t.Parallel()

	tracker := node.NewIndexTracker(1)
	builder := newSubmitTestBuilder(t, tracker)
	proposer := &recordingProposer{tracker: tracker}

	refKey := domain.TransactionReferenceKey{LedgerName: "test", Reference: "fresh-ref"}
	expectedID, _ := attributes.MakeKey(refKey.Bytes())

	// Calibrate: Build on an absent key actually tracks the key in the
	// loader (CacheMiss load). Release it so SubmitTechnical below
	// starts from a clean loader.
	needs := NewCoverage()
	needs.Add(dal.SubAttrReference, refKey.Bytes())
	build, err := builder.Build(needs, []WriteOperation{{Coverage: needs}})
	require.NoError(t, err)
	require.NotEmpty(t, build.token.Tracked, "calibration build must track the preload key")

	build.ReleaseLoaders()

	// SubmitTechnical must load, then release, the same key.
	cmd := &raftcmdpb.Proposal{}
	require.NoError(t, SubmitTechnical(context.Background(), builder, proposer, cmd, []WriteOperation{{Coverage: needs}}, "test"))
	require.Equal(t, 1, proposer.callCount())

	reload, err := builder.loaders.References.LoadOrWait(expectedID, 0, 1, func() (*commonpb.TransactionReferenceValue, error) {
		return nil, nil
	})
	require.NoError(t, err)
	require.True(t, reload.FromLoad, "loader must release the preload key after SubmitTechnical returns")
}
