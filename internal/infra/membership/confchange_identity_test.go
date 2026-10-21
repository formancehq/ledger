package membership

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestValidateCapabilityEvidenceBindsActivationToRevisionAndIdentity(t *testing.T) {
	ctx := &ConfChangeContext{
		InstanceID:                   bytesN(0x42),
		Capabilities:                 []uint32{1, 2},
		CapabilityMembershipRevision: 7,
	}

	require.NoError(t, ValidateCapabilityEvidence(ctx, true, 7))
	require.ErrorContains(t, ValidateCapabilityEvidence(ctx, true, 8), "does not match active revision")
	require.ErrorContains(t, ValidateCapabilityEvidence(&ConfChangeContext{InstanceID: bytesN(0x42), CapabilityMembershipRevision: 7}, true, 7), "evidence is empty")
	require.NoError(t, ValidateCapabilityEvidence(&ConfChangeContext{InstanceID: bytesN(0x42)}, false, 0))
}

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
				switch name {
				case "absent":
					require.EqualError(t, err, "invariant: ConfChange registration for peer 9 has no payload")
				case "missing identity":
					require.ErrorContains(t, err, "invariant: ConfChange for peer 9 has invalid identity: instance_id must be 16 bytes")
				default:
					require.EqualError(t, err, "invariant: ConfChange registration for peer 9 requires raft and service addresses")
				}
				// Commit even after rejection: the validator must have staged no row.
				require.NoError(t, session.Commit())
				rows, err := m.store.LoadAll()
				require.NoError(t, err)
				require.Empty(t, rows)
			})
		}
	}
}
