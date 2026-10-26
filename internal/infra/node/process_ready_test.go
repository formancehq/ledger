package node

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// The peer may see a Ready message only after its entries and HardState are
// durable. Local append/apply responses and committed configuration changes
// must still reach their respective owners after the split.
func TestProcessReadyPersistsBeforeSendingAndDefersLocalWork(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)
	transport := NewMockTransport(gomock.NewController(t))
	n, err := NewNode(NodeConfig{
		NodeID: 1, AdvertiseAddr: "node-1:7000", ServiceAdvertiseAddr: "node-1:8000",
		InstanceID: []byte("0000000000000001"), TickInterval: time.Hour,
		ProcessingTickInterval: time.Hour, MaintenanceInterval: time.Hour,
	}, transport, setup.applier, logging.Testing(), noop.NewMeterProvider().Meter("process-ready"),
		setup.wal, setup.fsm, setup.applier.recovery, setup.applier.synchronizer,
		newTestMembership(t), setup.responseSink)
	require.NoError(t, err)

	cc := &raftpb.ConfChangeV2{}
	data, err := proto.Marshal(cc)
	require.NoError(t, err)
	entry := &raftpb.Entry{Index: proto.Uint64(1), Term: proto.Uint64(1), Type: new(raftpb.EntryConfChangeV2), Data: data}
	appendResp := msg(raftpb.MsgStorageAppendResp, raft.LocalAppendThread, 1)
	applyResp := msg(raftpb.MsgStorageApplyResp, raft.LocalApplyThread, 1)
	outbound := msg(raftpb.MsgApp, 1, 2)

	transport.EXPECT().Send([]*raftpb.Message{outbound}, (<-chan struct{})(setup.stop)).Do(func([]*raftpb.Message, <-chan struct{}) {
		hs, _, stateErr := setup.wal.InitialState()
		require.NoError(t, stateErr)
		require.Equal(t, uint64(1), hs.GetTerm())
		entries, entriesErr := setup.wal.Entries(1, 2, 0)
		require.NoError(t, entriesErr)
		require.Equal(t, []*raftpb.Entry{entry}, entries)
	})

	rd := raft.Ready{
		HardState: &raftpb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(1)},
		Entries:   []*raftpb.Entry{entry}, CommittedEntries: []*raftpb.Entry{entry},
		Messages: []*raftpb.Message{
			outbound,
			{Type: new(raftpb.MsgStorageAppend), To: proto.Uint64(raft.LocalAppendThread), Responses: []*raftpb.Message{appendResp}},
			{Type: new(raftpb.MsgStorageApply), To: proto.Uint64(raft.LocalApplyThread), Responses: []*raftpb.Message{applyResp}},
		},
	}
	result, err := n.processReady(context.Background(), setup.stop, rd)
	require.NoError(t, err)
	require.Equal(t, []*raftpb.Message{applyResp}, result.applyResponses)
	require.Len(t, result.confChanges, 1)
	require.Equal(t, uint64(1), result.confChanges[0].index)
	require.True(t, proto.Equal(cc, result.confChanges[0].change))
	require.Equal(t, []*raftpb.Message{appendResp}, <-n.localResponseCh)
}
