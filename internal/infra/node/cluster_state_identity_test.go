package node

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestGetClusterStatePreservesNodeIDs(t *testing.T) {
	t.Parallel()
	for _, localID := range []uint64{1, 4294967296, 4294967297} {
		t.Run(strconv.FormatUint(localID, 10), func(t *testing.T) {
			t.Parallel()
			setup := newTestApplierSetup(t)
			voters := []uint64{1, 4294967296, 4294967297}
			require.NoError(t, setup.wal.UpdateSnapshotConfState(&raftpb.ConfState{Voters: voters}))
			rawNode, err := raft.NewRawNode(&raft.Config{
				ID: localID, ElectionTick: 10, HeartbeatTick: 1, Storage: setup.wal,
				MaxSizePerMsg: 1024 * 1024, MaxInflightMsgs: 256, Logger: NewLoggerAdapter(logging.Testing()),
			})
			require.NoError(t, err)
			require.NoError(t, rawNode.Campaign())
			persistReadyAndAdvance(t, rawNode, setup.wal)
			for _, peerID := range voters {
				if peerID == localID {
					continue
				}
				require.NoError(t, rawNode.Step(&raftpb.Message{
					Type: new(raftpb.MsgVoteResp), From: new(peerID), To: new(localID), Term: new(rawNode.Status().GetTerm()),
				}))
				persistReadyAndAdvance(t, rawNode, setup.wal)
				if rawNode.Status().RaftState == raft.StateLeader {
					break
				}
			}
			require.Equal(t, raft.StateLeader, rawNode.Status().RaftState)
			n := &Node{rawNode: rawNode, wal: setup.wal, fsm: setup.fsm, applier: setup.applier,
				config: NodeConfig{NodeID: localID}, clusterCommandCh: make(chan *clusterCommand),
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				select {
				case cmd := <-n.clusterCommandCh:
					cmd.errCh <- cmd.fn()
				case <-ctx.Done():
				}
			}()
			state, err := n.GetClusterState(ctx)
			<-done
			require.NoError(t, err)
			require.Equal(t, localID, state.LocalNodeID())
			require.Equal(t, localID, state.LeaderID())
			require.Equal(t, localID, state.GetRaftStatus().GetLeader())
			ids := make([]uint64, 0, len(state.GetNodes()))
			for _, info := range state.GetNodes() {
				ids = append(ids, info.NodeID())
				require.Equal(t, state.GetRaftStatus().GetProgress()[info.NodeID()], info.GetProgress())
			}
			require.ElementsMatch(t, voters, ids)
		})
	}
}
