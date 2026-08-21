package mirror

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	v2 "github.com/formancehq/ledger/v3/internal/adapter/v2"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// newTestBuilder spins up a minimal plan.Builder against a temp Pebble store
// so worker tests can exercise the boundary-derived resume position. The
// store is closed on test cleanup.
func newTestBuilder(t *testing.T) (*plan.Builder, *dal.Store) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	testCache, err := cache.New(100, meter)
	require.NoError(t, err)

	return plan.NewBuilder(node.NewIndexTracker(1), testCache, attributes.New(), store, nil, logger, 0), store
}

// writeBoundaries persists a LedgerBoundaries row so a worker constructed
// afterwards resumes from it.
func writeBoundaries(t *testing.T, store *dal.Store, ledgerName string, b *raftcmdpb.LedgerBoundaries) {
	t.Helper()

	attrs := attributes.New()
	session := store.OpenWriteSession()
	_, err := attrs.Boundary.Set(session, []byte(ledgerName), b)
	require.NoError(t, err)
	require.NoError(t, session.Commit())
}

// newWorkerForTest builds a Worker wired to a mock source, with no proposer.
// It is only safe for tests that never reach a propose path. An empty fetch is
// no longer sufficient on its own: processBatch calls publishIdleStatus, which
// proposes unless sourceHeadObserved is false — and it is false here only
// because these tests never call refreshSourceHead. Any test that does needs
// newWorkerWithProposer instead, or plan.Builder.Run dereferences the nil
// Proposer (runner.go).
func newWorkerForTest(t *testing.T, ledgerName string, source v2.Source, store *dal.Store, builder *plan.Builder) *Worker {
	t.Helper()

	ctx := logging.TestingContext()

	return NewWorker(
		ledgerName, 100, source, nil, store, nil, builder,
		logging.FromContext(ctx), noop.NewMeterProvider(),
	)
}

// recordedProposal is one proposal a stubProposer intercepted, already
// unmarshaled so tests assert on the MirrorSyncUpdate payload rather than on
// call counts alone.
type recordedProposal struct {
	cmd    *raftcmdpb.Proposal
	update *raftcmdpb.MirrorSyncUpdate
}

// stubProposal outcomes. Each names the stage a proposal is failed at, so a
// test can pin that the caller's published state does not advance and the next
// tick retries.
type proposeOutcome int

const (
	proposeApplied  proposeOutcome = iota // Raft accepts, FSM applies cleanly
	proposeRunError                       // proposer.Propose itself fails
	proposeRaftReject
	proposeFSMError
	proposeBusinessError
	// proposeWaitAbandoned models the one outcome the other four cannot: the
	// entry is committed and will apply on every node, but the caller never
	// learns it. Neither future is resolved, so a caller waiting on a
	// cancelled context gets ctx.Err() back and proposeMirrorSync reports
	// false for a proposal that did take effect.
	proposeWaitAbandoned
)

// stubProposer stands in for the Raft proposer on the happy path: it resolves
// both the Raft-acceptance future and the FSM future so a worker's
// propose-and-wait sequence completes in-process, with no Raft and no FSM.
// errProposer (manager_test.go) is its failure-only sibling, kept separate
// because the manager tests only ever need the error path.
type stubProposer struct {
	mu        sync.Mutex
	outcome   proposeOutcome
	proposals []recordedProposal
}

func (s *stubProposer) Propose(_ context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	s.mu.Lock()

	outcome := s.outcome

	cmd := &raftcmdpb.Proposal{}
	if err := cmd.UnmarshalVT(proposal.Data()); err == nil {
		rec := recordedProposal{cmd: cmd}
		for _, tu := range cmd.GetTechnicalUpdates() {
			if sync := tu.GetMirrorSync(); sync != nil {
				rec.update = sync
			}
		}

		s.proposals = append(s.proposals, rec)
	}

	s.mu.Unlock()

	if outcome == proposeRunError {
		return nil, errors.New("stub proposer: propose rejected")
	}

	fsmFuture := futures.New[state.ApplyResult]()

	switch outcome {
	case proposeWaitAbandoned:
		// Deliberately resolves nothing.
	case proposeRaftReject:
		proposal.Resolve(nil, errors.New("stub proposer: raft rejected"))
	case proposeFSMError:
		proposal.Resolve(nil, nil)
		fsmFuture.Resolve(state.ApplyResult{}, errors.New("stub proposer: fsm apply failed"))
	case proposeBusinessError:
		proposal.Resolve(nil, nil)
		fsmFuture.Resolve(state.ApplyResult{Error: errors.New("stub proposer: business rule rejected")}, nil)
	case proposeApplied, proposeRunError:
		proposal.Resolve(nil, nil)
		fsmFuture.Resolve(state.ApplyResult{}, nil)
	}

	return fsmFuture, nil
}

// setOutcome switches the stage the next proposals fail at, so one worker can
// reach a confirmed steady state before the failure under test.
func (s *stubProposer) setOutcome(outcome proposeOutcome) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.outcome = outcome
}

// recorded snapshots the intercepted proposals under the stub's lock.
func (s *stubProposer) recorded() []recordedProposal {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]recordedProposal(nil), s.proposals...)
}

// newWorkerWithProposer builds a Worker wired to a stub proposer so the
// propose-and-wait paths (idle status publication, error reporting) can be
// exercised without Raft.
func newWorkerWithProposer(
	t *testing.T,
	ledgerName string,
	source v2.Source,
	store *dal.Store,
	builder *plan.Builder,
	proposer Proposer,
) *Worker {
	t.Helper()

	ctx := logging.TestingContext()

	return NewWorker(
		ledgerName, 100, source, nil, store, proposer, builder,
		logging.FromContext(ctx), noop.NewMeterProvider(),
	)
}
