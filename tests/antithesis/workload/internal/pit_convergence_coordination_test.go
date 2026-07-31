package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolvePITConvergenceMembersKeepsLearnersAndMissingMembersInDenominator(t *testing.T) {
	t.Parallel()

	snapshot := pitRaftMembershipSnapshot{
		leader: 1,
		members: []pitRaftMemberSnapshot{
			{id: 1, suffrage: "Voter", serviceAddress: "ledger-0:8888"},
			{id: 2, suffrage: "Voter", serviceAddress: "ledger-1:8888"},
			{id: 3, suffrage: "Learner", serviceAddress: "ledger-2:8888"},
		},
	}
	partial := PerNodeConns{
		{Addr: "ledger-0:8888", NodeID: 1},
		{Addr: "ledger-1:8888", NodeID: 2},
	}
	resolved, ok := resolvePITConvergenceMembers(snapshot, partial)
	require.False(t, ok)
	require.Nil(t, resolved)

	complete := append(partial, &PerNodeConn{Addr: "ledger-2:8888", NodeID: 3})
	resolved, ok = resolvePITConvergenceMembers(snapshot, complete)
	require.True(t, ok)
	require.Len(t, resolved, 3)
	require.Equal(t, "Learner", resolved[2].member.suffrage)
}

func TestSamePITRaftMembershipIncludesLeaderSuffrageAndAddress(t *testing.T) {
	t.Parallel()

	base := pitRaftMembershipSnapshot{
		leader: 1,
		members: []pitRaftMemberSnapshot{
			{id: 1, suffrage: "Voter", serviceAddress: "ledger-0:8888"},
			{id: 2, suffrage: "Learner", serviceAddress: "ledger-1:8888"},
		},
	}
	require.True(t, samePITRaftMembership(base, base))

	changedLeader := base
	changedLeader.leader = 2
	require.False(t, samePITRaftMembership(base, changedLeader))

	changedMember := base
	changedMember.members = append([]pitRaftMemberSnapshot(nil), base.members...)
	changedMember.members[1].suffrage = "Voter"
	require.False(t, samePITRaftMembership(base, changedMember))
}
