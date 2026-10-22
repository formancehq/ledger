package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"

	raftmembership "github.com/formancehq/ledger/v3/internal/infra/membership"
)

func TestAllMembersAdvertiseCapabilities(t *testing.T) {
	t.Parallel()

	peer := raftmembership.ConfChangeContext{Capabilities: []uint32{1, 2}}

	require.False(t, allMembersAdvertiseCapabilities(nil))
	require.True(t, allMembersAdvertiseCapabilities(map[uint64]raftmembership.ConfChangeContext{1: peer}))
	require.False(t, allMembersAdvertiseCapabilities(map[uint64]raftmembership.ConfChangeContext{
		1: peer,
		2: {Capabilities: []uint32{1}},
	}))
}
