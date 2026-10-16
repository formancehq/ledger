package node

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// electionCluster is a deterministic, single-goroutine three-voter raft
// cluster. Each node ticks only when shouldTickRaft allows it for that node's
// applier status, exactly as the orchestrate loop does. Storage is the
// production WAL, the raft flags that decide elections (PreVote on,
// CheckQuorum off) match Node.Run, and every peer link delivers every message.
//
// Storage writes are synchronous here (Node.Run uses AsyncStorageWrites):
// that changes when vote responses leave the node, not which votes raft
// grants or which nodes campaign.
type electionCluster struct {
	t        *testing.T
	nodes    map[uint64]*raft.RawNode
	wals     map[uint64]wal.WAL
	statuses map[uint64]int32

	// Election traffic, reported when no leader emerges so a failure shows
	// which nodes campaigned and why their pre-votes were refused.
	preVotesSent     map[uint64]int
	preVotesRejected map[uint64]int
}

// electionNodeLog is one node's durable raft state before the cluster starts:
// the term of each log entry from index 1, and the persisted HardState term.
type electionNodeLog struct {
	entryTerms []uint64
	term       uint64
	commit     uint64
	status     int32
}

func newElectionCluster(t *testing.T, logs map[uint64]electionNodeLog) *electionCluster {
	t.Helper()

	voters := make([]uint64, 0, len(logs))
	for id := range logs {
		voters = append(voters, id)
	}

	c := &electionCluster{
		t:        t,
		nodes:    make(map[uint64]*raft.RawNode, len(logs)),
		wals:     make(map[uint64]wal.WAL, len(logs)),
		statuses: make(map[uint64]int32, len(logs)),

		preVotesSent:     make(map[uint64]int, len(logs)),
		preVotesRejected: make(map[uint64]int, len(logs)),
	}

	for id, l := range logs {
		w, err := wal.New(t.TempDir(), logging.Testing(), noop.Meter{})
		require.NoError(t, err)
		t.Cleanup(func() { _ = w.Close() })

		require.NoError(t, w.CreateSnapshot(0, &raftpb.ConfState{Voters: voters}, nil))

		entries := make([]*raftpb.Entry, len(l.entryTerms))
		for i, term := range l.entryTerms {
			entries[i] = &raftpb.Entry{
				Term:  new(term),
				Index: new(uint64(i + 1)),
				Type:  new(raftpb.EntryNormal),
			}
		}
		require.NoError(t, w.Append(&raftpb.HardState{
			Term:   new(l.term),
			Commit: new(l.commit),
		}, entries))

		rawNode, err := raft.NewRawNode(&raft.Config{
			ID:                        id,
			ElectionTick:              10,
			HeartbeatTick:             1,
			Storage:                   w,
			Applied:                   l.commit,
			MaxSizePerMsg:             1024 * 1024,
			MaxInflightMsgs:           256,
			Logger:                    NewLoggerAdapter(logging.Testing()),
			DisableProposalForwarding: true,
			PreVote:                   true,
		})
		require.NoError(t, err)

		c.nodes[id] = rawNode
		c.wals[id] = w
		c.statuses[id] = l.status
	}

	return c
}

// tick advances every node's logical clock by one orchestrate tick, then
// delivers messages until the cluster is quiescent.
func (c *electionCluster) tick() {
	c.t.Helper()

	for _, id := range c.ids() {
		if shouldTickRaft(c.statuses[id]) {
			c.nodes[id].Tick()
		}
	}

	for {
		var inflight []*raftpb.Message
		for _, id := range c.ids() {
			rawNode := c.nodes[id]
			for rawNode.HasReady() {
				rd := rawNode.Ready()
				require.NoError(c.t, c.wals[id].Append(rd.HardState, rd.Entries))
				inflight = append(inflight, rd.Messages...)
				rawNode.Advance(rd)
			}
		}
		if len(inflight) == 0 {
			return
		}
		for _, msg := range inflight {
			switch msg.GetType() {
			case raftpb.MsgPreVote:
				c.preVotesSent[msg.GetFrom()]++
			case raftpb.MsgPreVoteResp:
				if msg.GetReject() {
					c.preVotesRejected[msg.GetTo()]++
				}
			}
			// Mirror orchestrate's stepMessages filter.
			if msg.GetType() == raftpb.MsgTimeoutNow && c.statuses[msg.GetTo()] != statusNormal {
				continue
			}
			err := c.nodes[msg.GetTo()].Step(msg)
			require.NoError(c.t, err, "step %s from %d to %d", msg.GetType(), msg.GetFrom(), msg.GetTo())
		}
	}
}

func (c *electionCluster) leader() uint64 {
	for _, id := range c.ids() {
		if c.nodes[id].Status().RaftState == raft.StateLeader {
			return id
		}
	}

	return raft.None
}

func (c *electionCluster) ids() []uint64 {
	return []uint64{1, 2, 3}
}

func (c *electionCluster) describe() string {
	var out strings.Builder
	for _, id := range c.ids() {
		st := c.nodes[id].Status()
		lastIndex, err := c.wals[id].LastIndex()
		require.NoError(c.t, err)
		fmt.Fprintf(&out, "n%d: %s term=%d lastIndex=%d status=%d preVotesSent=%d preVotesRejected=%d; ",
			id, st.RaftState, st.GetTerm(), lastIndex, c.statuses[id], c.preVotesSent[id], c.preVotesRejected[id])
	}

	return out.String()
}

// TestOutOfSyncVotersStillElectALeader covers the out-of-sync election
// deadlock found by Antithesis on v3.0.0-rc.1: the two voters holding the
// newest raft log were out of sync, so shouldTickRaft kept their election
// timers stopped, and the only ticking voter had an older log that PreVote's
// up-to-date check rejected every round. An out-of-sync node re-synced only
// from a known leader (retryReadySnapshotSync), so nothing broke the loop.
//
// Election safety depends on the raft log, which an out-of-sync node still
// holds, not on its FSM. With every link up, the cluster must elect a leader.
func TestOutOfSyncVotersStillElectALeader(t *testing.T) {
	t.Parallel()

	// 100 election timeouts: randomized timeouts span [10, 20) ticks, so a
	// healthy three-node cluster elects within a few of them.
	const maxTicks = 100 * 10

	// Last entries mirror the traced Antithesis rerun: n1 and n2 end at
	// (term 3, index 3), n3 missed that entry and ends at (term 2, index 2).
	freshLog := []uint64{1, 2, 3}
	staleLog := []uint64{1, 2}

	tests := []struct {
		name   string
		status map[uint64]int32
	}{
		{
			// Control: the harness elects a leader from this log shape.
			name:   "all voters normal",
			status: map[uint64]int32{1: statusNormal, 2: statusNormal, 3: statusNormal},
		},
		{
			// Control: one voter with the newest log ticking is enough.
			name:   "one freshest voter out of sync",
			status: map[uint64]int32{1: statusNormal, 2: statusOutOfSync, 3: statusNormal},
		},
		{
			name:   "two freshest voters out of sync",
			status: map[uint64]int32{1: statusOutOfSync, 2: statusOutOfSync, 3: statusNormal},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newElectionCluster(t, map[uint64]electionNodeLog{
				1: {entryTerms: freshLog, term: 3, commit: 2, status: tt.status[1]},
				2: {entryTerms: freshLog, term: 3, commit: 2, status: tt.status[2]},
				3: {entryTerms: staleLog, term: 2, commit: 2, status: tt.status[3]},
			})

			for range maxTicks {
				c.tick()
				if c.leader() != raft.None {
					break
				}
			}

			require.NotEqual(t, raft.None, c.leader(),
				"no leader after %d ticks with every link up: %s", maxTicks, c.describe())
		})
	}
}
