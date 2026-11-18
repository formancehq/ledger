package membership

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestWriteConfChangeAllowsRegistrationBeforeInitialPolicy(t *testing.T) {
	m := newTestMembership(t)
	m.capabilityPolicyProvider = func() (*commonpb.ClusterPolicy, error) {
		return nil, nil
	}

	data, err := MarshalConfChangeContext(ConfChangeContext{
		RaftAddress:    "peer:7777",
		ServiceAddress: "peer:8888",
		InstanceID:     fixedInstanceID(1),
	})
	require.NoError(t, err)
	cc := &raftpb.ConfChangeV2{
		Changes: []*raftpb.ConfChangeSingle{{Type: new(raftpb.ConfChangeAddLearnerNode), NodeId: proto.Uint64(9)}},
		Context: data,
	}
	encoded, err := proto.Marshal(cc)
	require.NoError(t, err)

	session := m.store.OpenWriteSession()
	require.NoError(t, m.WriteConfChange(&raftpb.Entry{Type: new(raftpb.EntryConfChangeV2), Data: encoded}, session))
	require.NoError(t, session.Commit())
}

func TestWriteConfChangeFailsClosedOnPolicyReadError(t *testing.T) {
	m := newTestMembership(t)
	m.capabilityPolicyProvider = func() (*commonpb.ClusterPolicy, error) {
		return nil, errors.New("pebble unavailable")
	}

	data, err := MarshalConfChangeContext(ConfChangeContext{
		RaftAddress:    "peer:7777",
		ServiceAddress: "peer:8888",
		InstanceID:     fixedInstanceID(1),
	})
	require.NoError(t, err)
	cc := &raftpb.ConfChangeV2{
		Changes: []*raftpb.ConfChangeSingle{{Type: new(raftpb.ConfChangeAddLearnerNode), NodeId: proto.Uint64(9)}},
		Context: data,
	}
	encoded, err := proto.Marshal(cc)
	require.NoError(t, err)

	session := m.store.OpenWriteSession()
	require.ErrorContains(t, m.WriteConfChange(&raftpb.Entry{Type: new(raftpb.EntryConfChangeV2), Data: encoded}, session), "pebble unavailable")
	require.NoError(t, session.Commit())
}

func TestValidateCapabilityEvidenceRequiresActiveCapabilities(t *testing.T) {
	ctx := &ConfChangeContext{
		InstanceID:   bytesN(0x42),
		Capabilities: []uint32{1, 2},
	}

	require.NoError(t, ValidateCapabilityEvidence(ctx, []uint32{1, 2}))
	require.ErrorContains(t, ValidateCapabilityEvidence(ctx, []uint32{1, 3}), "capability 3")
	require.NoError(t, ValidateCapabilityEvidence(&ConfChangeContext{InstanceID: bytesN(0x42)}, nil))
}

func TestValidateTransitionEvidenceReadsCommittedPolicyBeforeCachedGate(t *testing.T) {
	m := newTestMembership(t)
	m.requiredCapabilities = nil // simulate the post-commit cache update window
	m.capabilityPolicyProvider = func() (*commonpb.ClusterPolicy, error) {
		return &commonpb.ClusterPolicy{
			CapabilityActivations: []*commonpb.CapabilityActivation{{Id: 7, Active: true}},
		}, nil
	}

	err := m.ValidateTransitionEvidence(&ConfChangeContext{
		InstanceID:   fixedInstanceID(1),
		Capabilities: nil,
	})
	require.ErrorContains(t, err, "capability 7 is required")
}

func TestWriteConfChangeRejectsCapabilitiesMissingFromCommittedPolicy(t *testing.T) {
	m := newTestMembership(t)
	m.capabilityPolicyProvider = func() (*commonpb.ClusterPolicy, error) {
		return &commonpb.ClusterPolicy{
			CapabilityActivations: []*commonpb.CapabilityActivation{{Id: 7, Active: true}},
		}, nil
	}

	data, err := MarshalConfChangeContext(ConfChangeContext{
		RaftAddress:    "peer:7777",
		ServiceAddress: "peer:8888",
		InstanceID:     fixedInstanceID(1),
	})
	require.NoError(t, err)
	cc := &raftpb.ConfChangeV2{
		Changes: []*raftpb.ConfChangeSingle{{Type: new(raftpb.ConfChangeAddLearnerNode), NodeId: proto.Uint64(9)}},
		Context: data,
	}
	encoded, err := proto.Marshal(cc)
	require.NoError(t, err)

	session := m.store.OpenWriteSession()
	err = m.WriteConfChange(&raftpb.Entry{Type: new(raftpb.EntryConfChangeV2), Data: encoded}, session)
	require.ErrorContains(t, err, "capability 7 is required")
	require.NoError(t, session.Commit())
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

func TestRequestedCapabilityFencesMembershipRegistration(t *testing.T) {
	t.Parallel()
	for _, kind := range []raftpb.ConfChangeType{raftpb.ConfChangeAddLearnerNode, raftpb.ConfChangeUpdateNode} {
		for _, source := range []string{"committed provider", "effective apply policy"} {
			t.Run(kind.String()+"/"+source, func(t *testing.T) {
				t.Parallel()
				policy := &commonpb.ClusterPolicy{
					CapabilityActivations: []*commonpb.CapabilityActivation{{Id: 7, Requested: true}},
				}
				member := newTestMembership(t)
				member.requiredCapabilities = nil
				member.capabilityPolicyProvider = func() (*commonpb.ClusterPolicy, error) {
					if source == "committed provider" {
						return policy, nil
					}

					return nil, nil
				}
				for _, supported := range []bool{false, true} {
					evidence := &ConfChangeContext{
						RaftAddress: "peer:7777", ServiceAddress: "peer:8888", InstanceID: fixedInstanceID(1),
					}
					if supported {
						evidence.Capabilities = []uint32{7}
					}
					if source == "committed provider" {
						err := member.ValidateTransitionEvidence(evidence)
						if supported {
							require.NoError(t, err)
						} else {
							require.ErrorContains(t, err, "capability 7 is required")
						}
					}
					context, err := MarshalConfChangeContext(*evidence)
					require.NoError(t, err)
					encoded, err := proto.Marshal(&raftpb.ConfChangeV2{
						Changes: []*raftpb.ConfChangeSingle{{Type: new(kind), NodeId: new(uint64(9))}}, Context: context,
					})
					require.NoError(t, err)
					session := member.store.OpenWriteSession()
					entry := &raftpb.Entry{Type: new(raftpb.EntryConfChangeV2), Data: encoded}
					if source == "effective apply policy" {
						err = member.WriteConfChange(entry, session, policy)
					} else {
						err = member.WriteConfChange(entry, session)
					}
					if supported {
						require.NoError(t, err)
					} else {
						require.ErrorContains(t, err, "capability 7 is required")
					}
					require.NoError(t, session.Commit())
				}
			})
		}
	}
}
