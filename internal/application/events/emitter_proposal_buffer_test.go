package events

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// capturingProposer keeps the raw proposal slice (not a copy) so the test
// can detect mutation of bytes Raft would otherwise still be holding.
type capturingProposer struct {
	captured [][]byte
}

func (p *capturingProposer) Propose(_ context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	p.captured = append(p.captured, proposal.Data())

	// Resolve the Raft-acceptance future embedded in the proposal so
	// callers that wait on it (Builder.Run consumers) don't hang. The
	// real etcd/raft Node does this when the entry is committed.
	proposal.Resolve(nil, nil)

	f := futures.New[state.ApplyResult]()
	f.Resolve(state.ApplyResult{}, nil)

	return f, nil
}

// TestProposeSinkUpdate_DoesNotMutateEarlierProposalBytes pins the fix
// for #311. etcd/raft retains the proposal slice in its in-memory log and
// may re-read it to replicate the entry to a slow follower after local
// apply. proposeSinkUpdate previously marshaled into a per-Emitter
// reusable buffer and handed that buffer to Propose — a follow-up call
// then overwrote the same backing array, corrupting the bytes a lagging
// follower would eventually receive. Switching to vtmarshal.MarshalCopy
// fixes it because MarshalCopy returns a freshly allocated slice each
// call.
func TestProposeSinkUpdate_DoesNotMutateEarlierProposalBytes(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	sink := NewMockSink(ctrl)

	prop := &capturingProposer{}
	builder, store := newTestBuilder(t)
	emitter := NewEmitter(store, sink, "test-sink", prop, builder, logging.Testing(), DefaultEmitterConfig())

	const firstCursor uint64 = 42
	const secondCursor uint64 = 99

	require.NoError(t, emitter.proposeSinkUpdate(context.Background(), &raftcmdpb.EventsSinkUpdate{
		SinkName: "test-sink",
		Cursor:   firstCursor,
	}))

	require.NoError(t, emitter.proposeSinkUpdate(context.Background(), &raftcmdpb.EventsSinkUpdate{
		SinkName: "test-sink",
		Cursor:   secondCursor,
	}))

	require.Len(t, prop.captured, 2)

	// Replay the FIRST captured slice — it must still decode to the first
	// proposal, not the second. Without the fix the second Marshal overwrote
	// the shared buffer and this assertion fails (either unmarshal error or
	// Cursor=secondCursor).
	first := &raftcmdpb.Proposal{}
	require.NoError(t, first.UnmarshalVT(prop.captured[0]))
	require.Len(t, first.GetTechnicalUpdates(), 1)
	require.Equal(t, firstCursor, first.GetTechnicalUpdates()[0].GetEventsSink().GetCursor(),
		"first proposal bytes were overwritten by the second proposeSinkUpdate (#311)")

	second := &raftcmdpb.Proposal{}
	require.NoError(t, second.UnmarshalVT(prop.captured[1]))
	require.Len(t, second.GetTechnicalUpdates(), 1)
	require.Equal(t, secondCursor, second.GetTechnicalUpdates()[0].GetEventsSink().GetCursor())
}
