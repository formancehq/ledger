package node

import (
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

func TestGetClusterStatePreservesNodeIDs(t *testing.T) {
	t.Parallel()
	for _, id := range []uint64{2, 4294967297} {
		t.Run(strconv.FormatUint(id, 10), func(t *testing.T) {
			t.Parallel()
			w, err := wal.New(t.TempDir(), logging.Testing(), noop.Meter{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, w.Close()) })
			require.NoError(t, w.CreateSnapshot(0, &raftpb.ConfState{Voters: []uint64{id}, Learners: []uint64{1}}, nil))
			rn, err := raft.NewRawNode(&raft.Config{
				ID: id, ElectionTick: 10, HeartbeatTick: 1, Storage: w,
				MaxSizePerMsg: 1024 * 1024, MaxInflightMsgs: 256, Logger: NewLoggerAdapter(logging.Testing()),
			})
			require.NoError(t, err)
			require.NoError(t, rn.Campaign())
			for rn.HasReady() {
				ready := rn.Ready()
				require.NoError(t, w.Append(ready.HardState, ready.Entries))
				rn.Advance(ready)
			}
			require.Equal(t, raft.StateLeader, rn.Status().RaftState)
			n := &Node{
				rawNode: rn, wal: w, fsm: &state.Machine{}, applier: &Applier{status: new(atomic.Int32)},
				config: NodeConfig{NodeID: id}, clusterCommandCh: make(chan *clusterCommand),
			}
			type result struct {
				value *clusterpb.ClusterState
				err   error
			}
			results := make(chan result, 1)
			go func() {
				value, err := n.GetClusterState(t.Context())
				results <- result{value, err}
			}()
			select {
			case cmd := <-n.clusterCommandCh:
				cmd.errCh <- cmd.fn()
			case <-t.Context().Done():
				t.Fatal("cluster state command was not dispatched")
			}
			got := <-results
			require.NoError(t, got.err)
			require.EqualValues(t, id, got.value.GetLeader())
			require.EqualValues(t, id, got.value.GetLocalNode())
			require.Equal(t, id, got.value.GetRaftStatus().GetLeader())
			ids := make([]uint64, 0, len(got.value.GetNodes()))
			for _, info := range got.value.GetNodes() {
				ids = append(ids, info.GetId())
				require.Equal(t, got.value.GetRaftStatus().GetProgress()[info.GetId()], info.GetProgress())
			}
			require.ElementsMatch(t, []uint64{1, id}, ids)
		})
	}
}
