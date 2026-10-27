package node

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// A lagging follower can receive several completed membership cycles in one
// Ready. All removals must be observed before that Ready is submitted to the
// asynchronous FSM; cleanup therefore cannot clear the first barrier in time
// for the second removal, regardless of how quickly the FSM normally applies.
func TestHistoricalRemovalFollowerReplay(t *testing.T) {
	t.Parallel()

	for _, catchUp := range []bool{true, false} {
		name := "single_ready_catch_up"
		if !catchUp {
			name = "durable_apply_between_changes_control"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				var m *membership.Membership
				setup := newTestApplierSetupWithConfChangeHandler(t, make(LocalResponses, 1024), func(entry *raftpb.Entry, session *dal.WriteSession) error {
					return m.WriteConfChange(entry, session)
				})
				var err error
				m, err = membership.NewMembership(membership.NewPeerStore(setup.store), noopMemTransport{}, noopMemPool{},
					3, "self:7777", "self:8888", []byte("self-instance-id"), logging.Testing(), membership.NoopClusterPolicyProvider)
				require.NoError(t, err)
				oldIdentity := []byte("old-instance-id-")
				newIdentity := []byte("new-instance-id-")
				require.NoError(t, m.Register(2, "old:7777", "old:8888", oldIdentity, nil))

				confState := &raftpb.ConfState{Voters: []uint64{1, 3}, Learners: []uint64{2}}
				require.NoError(t, setup.wal.UpdateSnapshotConfState(confState))
				rawNode, err := raft.NewRawNode(&raft.Config{
					ID: 3, ElectionTick: 10, HeartbeatTick: 1, Storage: setup.wal,
					MaxSizePerMsg: 1024 * 1024, MaxInflightMsgs: 256, Logger: NewLoggerAdapter(logging.Testing()),
				})
				require.NoError(t, err)
				n := &Node{
					rawNode: rawNode, wal: setup.wal, fsm: setup.fsm, applier: setup.applier,
					membership: m, logger: logging.Testing(), indexTracker: NewIndexTracker(1),
					runDone: make(chan struct{}),
				}
				n.confState.Store(confState)
				applierDone := make(chan error, 1)
				go func() { applierDone <- setup.applier.Run(t.Context(), setup.stop) }()
				t.Cleanup(func() {
					close(n.runDone)
					close(setup.stop)
					require.NoError(t, <-applierDone)
				})

				entries := make([]*raftpb.Entry, 0, 3)
				for i, change := range []struct {
					kind    raftpb.ConfChangeType
					payload membership.ConfChangeContext
				}{
					{raftpb.ConfChangeRemoveNode, membership.ConfChangeContext{InstanceID: oldIdentity}},
					{raftpb.ConfChangeAddLearnerNode, membership.ConfChangeContext{
						RaftAddress: "new:7777", ServiceAddress: "new:8888", InstanceID: newIdentity,
					}},
					{raftpb.ConfChangeRemoveNode, membership.ConfChangeContext{InstanceID: newIdentity}},
				} {
					payload, err := membership.MarshalConfChangeContext(change.payload)
					require.NoError(t, err)
					cc := &raftpb.ConfChangeV2{Context: payload, Changes: []*raftpb.ConfChangeSingle{{
						Type: new(change.kind), NodeId: new(uint64(2)),
					}}}
					require.NoError(t, membership.ValidateConfChangeIdentities(cc))
					data, err := proto.Marshal(cc)
					require.NoError(t, err)
					entries = append(entries, &raftpb.Entry{
						Index: new(uint64(i + 1)), Term: new(uint64(1)), Type: new(raftpb.EntryConfChangeV2), Data: data,
					})
				}

				batches := [][]*raftpb.Entry{entries}
				if !catchUp {
					batches = [][]*raftpb.Entry{{entries[0]}, {entries[1]}, {entries[2]}}
				}
				for _, batch := range batches {
					last := batch[len(batch)-1].GetIndex()
					// Deliver actual leader traffic to a real follower, rather than
					// constructing readyResult.confChanges or seeding pendingRemovals.
					previous := batch[0].GetIndex() - 1
					previousTerm := uint64(1)
					if previous == 0 {
						previousTerm = 0
					}
					require.NoError(t, rawNode.Step(&raftpb.Message{
						Type: new(raftpb.MsgApp), From: new(uint64(1)), To: new(uint64(3)), Term: new(uint64(1)),
						Index: new(previous), LogTerm: new(previousTerm), Entries: batch, Commit: new(last),
					}))
					require.Equal(t, raft.StateFollower, rawNode.Status().RaftState)
					require.True(t, rawNode.HasReady())
					rd := rawNode.Ready()
					require.Equal(t, batch, rd.CommittedEntries, "the follower must expose the complete historical batch")
					require.NoError(t, setup.wal.Append(rd.HardState, rd.Entries))
					result := readyResult{rd: rd}
					require.NoError(t, n.collectReadyConfChanges(rd, &result))
					require.Len(t, result.confChanges, len(batch))
					require.Equal(t, previous, n.fsm.LastPersistedIndex())

					// This must accept valid committed history. The buggy tracker
					// returns a conflict for removal 3 versus pending removal 1.
					require.NoError(t, n.finishReady(result, setup.stop),
						"a fresh identity at a reused node number is a separate removal event")
					setup.applier.Drain(setup.stop)
					require.Equal(t, last, n.fsm.LastPersistedIndex())
					rawNode.Advance(rd)
					synctest.Wait()
					require.False(t, n.isRemovalPending(2, oldIdentity) || n.isRemovalPending(2, newIdentity),
						"durable tombstones must release the verified barriers")
				}

				for _, identity := range [][]byte{oldIdentity, newIdentity} {
					removed, err := m.IsRemoved(2, identity)
					require.NoError(t, err)
					require.True(t, removed, "both removed identities must retain their durable tombstones")
				}
				require.NotContains(t, m.PeerAddresses(), uint64(2))
				require.False(t, confStateContainsNode(n.confState.Load(), 2))
			})
		})
	}
}
