package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// Keep the real Raft request, snapshot, heartbeat and apply lifecycle while
// selectively losing only the follower snapshot acknowledgement.
func TestSnapshotStatusRestoresQuiescentFollowerProgress(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"missing", "finish", "failure"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			nodes := map[uint64]*raft.RawNode{}
			stores := map[uint64]*raft.MemoryStorage{}
			applied := map[uint64]uint64{}
			readIndex := uint64(0)
			deliveredSnapshot := false
			isolateOne, dropSnapshotAck := true, true
			for _, id := range []uint64{1, 2, 3} {
				s := raft.NewMemoryStorage()
				index := uint64(36)
				if id == 1 {
					index = 35
				}
				require.NoError(t, s.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: new(index), Term: proto.Uint64(1), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}}}}))
				if id != 1 {
					var entries []*raftpb.Entry
					for i := uint64(37); i <= 39; i++ {
						entries = append(entries, &raftpb.Entry{Index: new(i), Term: proto.Uint64(2), Type: raftpb.EntryNormal.Enum()})
					}
					require.NoError(t, s.Append(entries))
					index = 39
				}
				require.NoError(t, s.SetHardState(&raftpb.HardState{Term: proto.Uint64(2), Commit: new(index)}))
				n, err := raft.NewRawNode(&raft.Config{ID: id, Storage: s, Applied: index, ElectionTick: 10, HeartbeatTick: 1, MaxSizePerMsg: 1024 * 1024, MaxInflightMsgs: 256})
				require.NoError(t, err)
				nodes[id], stores[id], applied[id] = n, s, index
			}
			pump := func() {
				t.Helper()
				for range 200 {
					progress := false
					for _, id := range []uint64{1, 2, 3} {
						n, s := nodes[id], stores[id]
						if !n.HasReady() {
							continue
						}
						progress = true
						rd := n.Ready()
						if !raft.IsEmptySnap(rd.Snapshot) {
							require.NoError(t, s.ApplySnapshot(rd.Snapshot))
							applied[id] = max(applied[id], rd.Snapshot.GetMetadata().GetIndex())
							if id == 1 {
								deliveredSnapshot = true
							}
						}
						if !raft.IsEmptyHardState(rd.HardState) {
							require.NoError(t, s.SetHardState(rd.HardState))
						}
						require.NoError(t, s.Append(rd.Entries))
						for _, e := range rd.CommittedEntries {
							applied[id] = max(applied[id], e.GetIndex())
						}
						for _, rs := range rd.ReadStates {
							if id == 1 {
								readIndex = rs.Index
							}
						}
						for _, m := range rd.Messages {
							if isolateOne && (m.GetFrom() == 1 || m.GetTo() == 1) {
								continue
							}
							// Retain append rejections so the leader discovers the compacted
							// prefix. Lose only the successful snapshot acknowledgement.
							if dropSnapshotAck && deliveredSnapshot && m.GetFrom() == 1 && m.GetTo() == 3 && m.GetType() == raftpb.MsgAppResp && !m.GetReject() {
								continue
							}
							require.NoError(t, nodes[m.GetTo()].Step(m))
						}
						n.Advance(rd)
					}
					if !progress {
						return
					}
				}
				t.Fatal("message pump did not quiesce within 200 deterministic rounds")
			}
			require.NoError(t, nodes[3].Campaign())
			pump()
			require.Equal(t, raft.StateLeader, nodes[3].Status().RaftState)
			require.Equal(t, uint64(40), nodes[3].Status().GetCommit())
			isolateOne = false
			nodes[3].Tick()
			pump()
			require.True(t, deliveredSnapshot)
			require.Equal(t, tracker.StateSnapshot, nodes[3].Status().Progress[1].State)
			// Model independently fetched checkpoint39 without inventing a Raft term.
			applied[1] = 39
			nodes[3].ReportUnreachable(1)
			for range 5 {
				nodes[3].Tick()
				pump()
			}
			require.Equal(t, tracker.StateSnapshot, nodes[3].Status().Progress[1].State)
			nodes[1].ReadIndex([]byte("diagnostic-read"))
			pump()
			require.Equal(t, uint64(40), readIndex)
			require.Equal(t, uint64(39), applied[1])
			t.Logf("before report: state=%s readIndex=%d businessApplied=%d raftApplied=%d", nodes[3].Status().Progress[1].State, readIndex, applied[1], nodes[1].Status().Applied)
			dropSnapshotAck = false
			switch mode {
			case "finish":
				routeSnapshotStatusThroughOrchestrator(t, nodes[3], SnapshotStatus{PeerID: 1, Status: raft.SnapshotFinish})
			case "failure":
				routeSnapshotStatusThroughOrchestrator(t, nodes[3], SnapshotStatus{PeerID: 1, Status: raft.SnapshotFailure})
			}
			for range 5 {
				nodes[3].Tick()
				pump()
			}
			// No proposals or rescue writes after the initial current-term no-op40.
			require.Equal(t, uint64(40), nodes[3].Status().GetCommit())
			t.Logf("after %s report: state=%s readIndex=%d businessApplied=%d raftApplied=%d", mode, nodes[3].Status().Progress[1].State, readIndex, applied[1], nodes[1].Status().Applied)
			if mode == "missing" {
				require.Equal(t, tracker.StateSnapshot, nodes[3].Status().Progress[1].State)
				require.Less(t, applied[1], readIndex, "RED behavior: unresolved FSM horizon after healed quiescence")
			} else {
				require.GreaterOrEqual(t, applied[1], readIndex)
				require.Equal(t, tracker.StateReplicate, nodes[3].Status().Progress[1].State)
			}
		})
	}
}

// Give the real orchestrator exclusive ownership while the transport delivers
// its result. Reading progress through execClusterCommand avoids cross-thread
// RawNode access. Stop and join before returning ownership to the message pump.
func routeSnapshotStatusThroughOrchestrator(t *testing.T, rn *raft.RawNode, result SnapshotStatus) {
	t.Helper()
	setup := newTestApplierSetup(t)
	require.NoError(t, setup.wal.UpdateSnapshotConfState(&raftpb.ConfState{Voters: []uint64{3}}))
	tr := NewMockTransport(gomock.NewController(t))
	statuses := make(chan SnapshotStatus)
	tr.EXPECT().SnapshotStatus().AnyTimes().Return((<-chan SnapshotStatus)(statuses))
	tr.EXPECT().Unreachable().AnyTimes().Return((<-chan uint64)(nil))
	tr.EXPECT().RecvHighPriority().AnyTimes().Return((<-chan []*raftpb.Message)(nil))
	tr.EXPECT().RecvMediumPriority().AnyTimes().Return((<-chan []*raftpb.Message)(nil))
	tr.EXPECT().RecvLowPriority().AnyTimes().Return((<-chan []*raftpb.Message)(nil))
	n, err := NewNode(NodeConfig{NodeID: 3, AdvertiseAddr: "node-3:7000", ServiceAdvertiseAddr: "node-3:8000", InstanceID: []byte("0000000000000003"), TickInterval: time.Hour, ProcessingTickInterval: time.Hour, MaintenanceInterval: time.Hour}, tr, setup.applier, logging.Testing(), noop.NewMeterProvider().Meter("snapshot-status"), setup.wal, setup.fsm, setup.applier.recovery, setup.applier.synchronizer, newTestMembership(t), setup.responseSink)
	require.NoError(t, err)
	n.rawNode = rn
	n.readies = make(chan raft.Ready, 16)
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- n.orchestrate(t.Context(), stop) }()
	defer func() {
		close(stop)
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("orchestrator did not stop")
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case statuses <- result:
	case <-ctx.Done():
		t.Fatal("orchestrator did not receive sent-snapshot result")
	}
	require.NoError(t, n.execClusterCommand(ctx, false, func() error {
		if got := rn.Status().Progress[result.PeerID].State; got != tracker.StateProbe {
			return fmt.Errorf("transport snapshot result did not reach leader progress: got %s", got)
		}

		return nil
	}))
}
