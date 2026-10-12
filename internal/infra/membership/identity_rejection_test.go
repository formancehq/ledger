package membership

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestWriteConfChangeRejectsRemovalWithoutContext(t *testing.T) {
	t.Parallel()

	m := newTestMembership(t)
	identity := fixedInstanceID(9)
	require.NoError(t, m.Register(9, "peer:7777", "peer:8888", identity))
	original := m.PeerAddresses()
	data, err := proto.Marshal(&raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{{
		Type: new(raftpb.ConfChangeRemoveNode), NodeId: new(uint64(9)),
	}}})
	require.NoError(t, err)
	session := m.store.OpenWriteSession()
	err = m.WriteConfChange(&raftpb.Entry{Type: new(raftpb.EntryConfChangeV2), Data: data}, session)
	require.ErrorContains(t, err, "ConfChange removal for peer 9 has no context")
	require.NoError(t, session.Commit(), "rejection must not stage a peer deletion")
	rows, err := m.store.LoadAll()
	require.NoError(t, err)
	require.Equal(t, original, rows)
	require.Equal(t, original, m.PeerAddresses())
	removed, err := m.store.IsRemoved(9, identity)
	require.NoError(t, err)
	require.False(t, removed)
}

func TestDeleteRemovedInSessionRejectsInvalidIdentityWithoutDeletingTombstone(t *testing.T) {
	t.Parallel()

	for _, size := range []int{0, 15, 17} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			t.Parallel()
			ps := newTestPeerStore(t)
			identity := fixedInstanceID(7)
			session := ps.OpenWriteSession()
			require.NoError(t, ps.MarkRemoved(session, &raftcmdpb.RemovedMemberEntry{
				NodeId: 7, InstanceId: identity,
			}))
			require.NoError(t, session.Commit())

			session = ps.OpenWriteSession()
			err := ps.DeleteRemovedInSession(session, 7, make([]byte, size))
			require.ErrorContains(t, err, "DeleteRemovedInSession: instance_id must be 16 bytes")
			require.NoError(t, session.Commit())
			removed, err := ps.IsRemoved(7, identity)
			require.NoError(t, err)
			require.True(t, removed, "an invalid forget request must retain the removal barrier")
		})
	}
}
