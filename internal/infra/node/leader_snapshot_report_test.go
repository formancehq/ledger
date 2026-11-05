package node

import (
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

// A leader that sends a MsgSnap and then compacts its log past that snapshot
// before the peer acknowledges it must keep replicating to the peer. etcd/raft
// leaves StateSnapshot only on MsgSnapStatus or on an acknowledgement at or
// past the leader's first retained index; without the leader-side
// ReportSnapshot the late acknowledgement is ignored and the peer receives no
// entry for the rest of the term (the out-of-sync election and lost-follower
// E2E stalls). The same report also covers a MsgSnap lost in transit.
func TestFinishReadyReportsSentSnapshotSoCompactedLeaderKeepsReplicating(t *testing.T) {
	t.Parallel()

	const (
		leaderID = uint64(1)
		peerID   = uint64(2)
	)

	setup := newTestApplierSetup(t)
	transport := NewMockTransport(gomock.NewController(t))
	transport.EXPECT().Send(gomock.Any()).AnyTimes()
	n, err := NewNode(NodeConfig{
		NodeID: leaderID, AdvertiseAddr: "node-1:7000", ServiceAdvertiseAddr: "node-1:8000",
		InstanceID: []byte("0000000000000001"), TickInterval: time.Hour,
		ProcessingTickInterval: time.Hour, MaintenanceInterval: time.Hour,
	}, transport, setup.applier, logging.Testing(), noop.NewMeterProvider().Meter("leader-snapshot-report"),
		setup.wal, setup.fsm, setup.applier.recovery, setup.applier.synchronizer,
		newTestMembership(t), setup.responseSink)
	require.NoError(t, err)

	// The leader's log starts after a snapshot at index 8; the peer is a
	// learner so the single voter commits on its own.
	conf := &raftpb.ConfState{Voters: []uint64{leaderID}, Learners: []uint64{peerID}}
	storage := raft.NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
		Index: proto.Uint64(8), Term: proto.Uint64(1), ConfState: conf,
	}}))
	rn, err := raft.NewRawNode(&raft.Config{
		ID: leaderID, ElectionTick: 10, HeartbeatTick: 1, Storage: storage,
		MaxInflightMsgs: 256, MaxSizePerMsg: 1024 * 1024,
		Logger: NewLoggerAdapter(logging.Testing()),
	})
	require.NoError(t, err)
	n.rawNode = rn

	// drain persists and advances every pending Ready, returning the messages
	// addressed to the peer.
	drain := func() []*raftpb.Message {
		t.Helper()
		var toPeer []*raftpb.Message
		for rn.HasReady() {
			rd := rn.Ready()
			if !raft.IsEmptyHardState(rd.HardState) {
				require.NoError(t, storage.SetHardState(rd.HardState))
			}
			require.NoError(t, storage.Append(rd.Entries))
			for _, m := range rd.Messages {
				if m.GetTo() == peerID {
					toPeer = append(toPeer, m)
				}
			}
			rn.Advance(rd)
		}

		return toPeer
	}
	fromPeer := func(typ raftpb.MessageType, fields func(*raftpb.Message)) {
		t.Helper()
		m := &raftpb.Message{
			Type: typ.Enum(), From: proto.Uint64(peerID), To: proto.Uint64(leaderID),
			Term: new(rn.Status().GetTerm()),
		}
		if fields != nil {
			fields(m)
		}
		require.NoError(t, rn.Step(m))
	}

	require.NoError(t, rn.Campaign())
	drain()
	require.Equal(t, raft.StateLeader, rn.Status().RaftState)

	// The peer is alive but its log ends at 5, before the leader's first
	// retained index: the leader must send a snapshot.
	fromPeer(raftpb.MsgHeartbeatResp, nil)
	var probe *raftpb.Message
	for _, m := range drain() {
		if m.GetType() == raftpb.MsgApp {
			probe = m
		}
	}
	require.NotNil(t, probe, "leader must probe the peer")
	fromPeer(raftpb.MsgAppResp, func(m *raftpb.Message) {
		m.Index = new(probe.GetIndex())
		m.Reject = new(true)
		m.RejectHint = proto.Uint64(5)
		m.LogTerm = proto.Uint64(1)
	})

	require.True(t, rn.HasReady())
	rd := rn.Ready()
	require.Empty(t, rd.CommittedEntries)
	var sent *raftpb.Message
	for _, m := range rd.Messages {
		if m.GetTo() == peerID && m.GetType() == raftpb.MsgSnap {
			sent = m
		}
	}
	require.NotNil(t, sent, "leader must send a snapshot to a peer behind its first index")
	require.Equal(t, uint64(8), sent.GetSnapshot().GetMetadata().GetIndex())
	require.NoError(t, storage.Append(rd.Entries))
	rn.Advance(rd)

	// Production path: the Ready's messages go through sendReadyMessages,
	// then the orchestrate goroutine runs finishReady.
	result := readyResult{rd: rd}
	n.sendReadyMessages(setup.stop, rd, &result)
	require.Equal(t, []uint64{peerID}, result.snapshotRecipients)
	require.NoError(t, n.finishReady(result, setup.stop))

	// The leader keeps writing and its maintenance compacts past the snapshot
	// it sent before the peer's acknowledgement arrives.
	for range 3 {
		require.NoError(t, rn.Propose([]byte("entry")))
		drain()
	}
	compactedAt := rn.Status().GetCommit()
	require.Greater(t, compactedAt, uint64(9))
	_, err = storage.CreateSnapshot(compactedAt, conf, nil)
	require.NoError(t, err)
	require.NoError(t, storage.Compact(compactedAt))

	fromPeer(raftpb.MsgAppResp, func(m *raftpb.Message) { m.Index = proto.Uint64(8) })
	fromPeer(raftpb.MsgHeartbeatResp, nil)

	var resent *raftpb.Message
	for _, m := range drain() {
		if m.GetType() == raftpb.MsgSnap {
			resent = m
		}
	}
	require.NotNil(t, resent, "leader must resume replication to the peer instead of staying paused in StateSnapshot")
	require.Equal(t, compactedAt, resent.GetSnapshot().GetMetadata().GetIndex())
}

func TestSnapshotRecipients(t *testing.T) {
	t.Parallel()

	require.Empty(t, snapshotRecipients([]*raftpb.Message{
		{Type: raftpb.MsgApp.Enum(), To: proto.Uint64(2)},
		{Type: raftpb.MsgHeartbeat.Enum(), To: proto.Uint64(3)},
	}))
	require.Equal(t, []uint64{2, 3}, snapshotRecipients([]*raftpb.Message{
		{Type: raftpb.MsgSnap.Enum(), To: proto.Uint64(2)},
		{Type: raftpb.MsgApp.Enum(), To: proto.Uint64(4)},
		{Type: raftpb.MsgSnap.Enum(), To: proto.Uint64(3)},
	}))
}
