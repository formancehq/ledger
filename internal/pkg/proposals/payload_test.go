package proposals

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestReadersPreserveIntentAndCoverage(t *testing.T) {
	t.Parallel()
	order := &raftcmdpb.Order{Technical: &raftcmdpb.OrderTechnical{CoverageBits: []byte{7}}}
	update := &raftcmdpb.TechnicalUpdate{CoverageBits: []byte{2}}
	for _, legacy := range []bool{false, true} {
		p := &raftcmdpb.Proposal{}
		if legacy {
			p.Orders = []*raftcmdpb.Order{order}
			p.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{update}
		} else {
			p.Payload = &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{Orders: []*raftcmdpb.Order{order}, TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{update}}}
		}
		before := p.CloneVT()
		require.Same(t, order, BatchOrders(p)[0])
		require.Same(t, update, TechnicalUpdates(p)[0])
		require.Equal(t, 1, IntentCount(p))
		require.True(t, proto.Equal(before, p))
	}
}

func TestPolicyIsNotAnExecutionOrder(t *testing.T) {
	t.Parallel()
	p := &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_SetClusterPolicy{SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{}}}
	require.Empty(t, BatchOrders(p))
	require.Empty(t, TechnicalUpdates(p))
	require.Equal(t, 1, IntentCount(p))
	require.Zero(t, IntentCount(&raftcmdpb.Proposal{}))
}
