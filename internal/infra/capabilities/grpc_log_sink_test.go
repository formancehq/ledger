package capabilities

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestGrpcLogSinkRequiresCapability(t *testing.T) {
	t.Parallel()
	config := &commonpb.SinkConfig{Type: &commonpb.SinkConfig_GrpcLog{GrpcLog: &commonpb.GrpcLogSinkConfig{Ledger: "ledger", ClientId: "client"}}}
	add := &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
		Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{AddEventsSink: &raftcmdpb.AddEventsSinkOrder{Config: config}},
	}}}
	ack := &raftcmdpb.TechnicalUpdate{Kind: &raftcmdpb.TechnicalUpdate_GrpcLogSinkAck{GrpcLogSinkAck: &raftcmdpb.GrpcLogSinkAck{SinkName: "sink", Incarnation: 1, LogId: 2}}}
	for _, proposal := range []*raftcmdpb.Proposal{
		{Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{Orders: []*raftcmdpb.Order{add}}}},
		{Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{ack}}}},
	} {
		required, err := RequiredCapabilities(proposal)
		require.NoError(t, err)
		require.Equal(t, []CapabilityID{GrpcAcknowledgedLogSink}, required)
	}
}

func TestGrpcLogSinkActivationWaitsForLearner(t *testing.T) {
	t.Parallel()
	members := []Member{
		{NodeID: 1, InstanceID: []byte("voter"), Role: Voter, Reachable: true, Capabilities: []CapabilityID{GrpcAcknowledgedLogSink}},
		{NodeID: 2, InstanceID: []byte("learner"), Role: Learner, Reachable: true},
	}
	ready, err := Ready(SupportedIDs(), members)
	require.NoError(t, err)
	require.Empty(t, ready)
	decision := Evaluate(State{Activations: map[CapabilityID]bool{GrpcAcknowledgedLogSink: true}},
		[]CapabilityID{GrpcAcknowledgedLogSink}, false, members)
	require.False(t, decision.Allowed)
	require.Equal(t, uint64(2), decision.Blockers[0].NodeID)
}
