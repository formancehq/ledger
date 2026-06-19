package events

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// staleThenSuccessProposer rejects the first Propose with
// ErrStaleProposal (a leadership-churn / IndexTracker drift outcome)
// and resolves every subsequent call as a normal Raft acceptance + FSM
// success. proposeSinkUpdate must catch the stale rejection internally
// and retry; otherwise the emitter loop would restart from the
// unchanged cursor and re-publish the events publishBatch already
// delivered to the external sink.
type staleThenSuccessProposer struct {
	calls int
}

func (p *staleThenSuccessProposer) Propose(_ context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	p.calls++
	proposal.Resolve(nil, nil)

	f := futures.New[state.ApplyResult]()
	if p.calls == 1 {
		f.Resolve(state.ApplyResult{Error: &domain.BusinessError{Err: domain.ErrStaleProposal}}, nil)
	} else {
		f.Resolve(state.ApplyResult{}, nil)
	}

	return f, nil
}

// TestProposeSinkUpdate_RetriesStaleProposal pins the bounded retry
// loop introduced after publishBatch was promoted to deliver events
// BEFORE this function runs. Without the retry, an ErrStaleProposal
// from leadership churn would propagate up and the emitter's outer
// loop would re-deliver the same batch from the unchanged cursor.
func TestProposeSinkUpdate_RetriesStaleProposal(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	sink := NewMockSink(ctrl)

	prop := &staleThenSuccessProposer{}
	builder, store := newTestBuilder(t)
	emitter := NewEmitter(store, sink, "test-sink", prop, builder, logging.Testing(), DefaultEmitterConfig())

	err := emitter.proposeSinkUpdate(context.Background(), &raftcmdpb.EventsSinkUpdate{
		SinkName: "test-sink",
		Cursor:   42,
	})
	require.NoError(t, err, "ErrStaleProposal on the first attempt must be retried, not surfaced")
	require.Equal(t, 2, prop.calls, "exactly one retry was needed")
}
