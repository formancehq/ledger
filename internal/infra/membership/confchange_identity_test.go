package membership

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestWriteConfChangeRejectsIncompleteRegistration(t *testing.T) {
	t.Parallel()
	for _, kind := range []raftpb.ConfChangeType{raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode, raftpb.ConfChangeUpdateNode} {
		for name, payload := range map[string]*ConfChangeContext{
			"absent":                  nil,
			"correlation only":        {ProposalID: "promotion"},
			"identity only":           {InstanceID: fixedInstanceID(1)},
			"missing raft address":    {ServiceAddress: "peer:8888", InstanceID: fixedInstanceID(1)},
			"missing service address": {RaftAddress: "peer:7777", InstanceID: fixedInstanceID(1)},
			"missing identity":        {RaftAddress: "peer:7777", ServiceAddress: "peer:8888"},
		} {
			t.Run(kind.String()+"/"+name, func(t *testing.T) {
				t.Parallel()
				m := newTestMembership(t)
				cc := &raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{{Type: new(kind), NodeId: new(uint64(9))}}}
				if payload != nil {
					var err error
					cc.Context, err = MarshalConfChangeContext(*payload)
					require.NoError(t, err)
				}
				data, err := proto.Marshal(cc)
				require.NoError(t, err)
				session := m.store.OpenWriteSession()
				err = m.WriteConfChange(&raftpb.Entry{Type: new(raftpb.EntryConfChangeV2), Data: data}, session)
				require.ErrorContains(t, err, "invariant: ConfChange")
				// Commit even after rejection: the validator must have staged no row.
				require.NoError(t, session.Commit())
				rows, err := m.store.LoadAll()
				require.NoError(t, err)
				require.Empty(t, rows)
				require.Empty(t, m.PeerAddresses())
			})
		}
	}
}
