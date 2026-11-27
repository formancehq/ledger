package state

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestGrpcLogSinkAckFencesRecreatedSink(t *testing.T) {
	t.Parallel()
	fsm, dataStore, _ := newTestMachine(t)
	key := domain.SinkConfigKey{Name: "log-sink"}
	stored := &commonpb.SinkConfig{Name: "log-sink", Type: &commonpb.SinkConfig_GrpcLog{GrpcLog: &commonpb.GrpcLogSinkConfig{
		Ledger: "ledger", ClientId: "client", Incarnation: 42,
	}}}
	seed := dataStore.OpenWriteSession()
	_, _, err := fsm.Registry.SinkConfigs.PutWithCache(seed, 0, key.Bytes(), stored)
	require.NoError(t, err)
	require.NoError(t, seed.Commit())

	attrID, _ := attributes.MakeKey(key.Bytes())
	plan := &raftcmdpb.ExecutionPlan{LastPersistedIndex: fsm.Registry.Cache.BaseIndex.Gen0,
		Attributes: []*raftcmdpb.AttributeCoverage{declareTestPlan(attrID, dal.SubAttrSinkConfig)}}
	proposal := &raftcmdpb.Proposal{Id: 1, Date: &commonpb.Timestamp{Data: 1700000000}, ExecutionPlan: plan,
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{Kind: &raftcmdpb.TechnicalUpdate_GrpcLogSinkAck{
			GrpcLogSinkAck: &raftcmdpb.GrpcLogSinkAck{SinkName: "log-sink", Incarnation: 41, LogId: 7},
		}}}}
	sealProposal(proposal)
	fsm.writeSet.Reset(proposal.GetDate())
	scopeFactory := NewScopeFactory(fsm.writeSet, plan, fsm.logger, fsm.preloadMissCounter, proposal.GetId())
	batch := dataStore.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()
	err = fsm.applyTechnicalUpdates(scopeFactory, batch, proposal.GetId(), proposal)
	require.True(t, errors.Is(err, ErrGrpcLogSinkStale), "old incarnation must not advance the recreated sink")
	require.Empty(t, fsm.State.GrpcLogSinkCursors)

	proposal.TechnicalUpdates[0].Kind = &raftcmdpb.TechnicalUpdate_GrpcLogSinkAck{
		GrpcLogSinkAck: &raftcmdpb.GrpcLogSinkAck{SinkName: "log-sink", Incarnation: 42, LogId: 8},
	}
	require.NoError(t, fsm.applyTechnicalUpdates(scopeFactory, batch, proposal.GetId(), proposal))
	require.NoError(t, batch.Commit())
	handle, err := dataStore.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()
	cursor, err := query.ReadGrpcLogSinkCursor(handle, 42)
	require.NoError(t, err)
	require.Equal(t, uint64(8), cursor)
	cursor, err = query.ReadGrpcLogSinkCursor(handle, 41)
	require.NoError(t, err)
	require.Zero(t, cursor)
	fsm.State.GrpcLogSinkCursors = map[uint64]uint64{}
	require.NoError(t, NewRecovery(fsm, dataStore).RecoverState())
	require.Equal(t, uint64(8), fsm.State.GrpcLogSinkCursors[42], "recovery must restore the monotonic apply fence")
}
