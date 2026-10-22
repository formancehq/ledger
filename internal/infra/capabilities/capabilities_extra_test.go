package capabilities

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIncarnationValidation(t *testing.T) {
	require.Error(t, ValidateMembers([]Member{{NodeID: 4, InstanceID: []byte("a"), Role: Voter}, {NodeID: 4, InstanceID: []byte("b"), Role: Learner}}))
	require.NoError(t, ValidateMembers([]Member{{NodeID: 4, InstanceID: []byte("a"), Role: Voter}, {NodeID: 5, InstanceID: []byte("b"), Role: Learner}}))
}

func TestReplayFloorIsMonotonic(t *testing.T) {
	floor, err := AdvanceReplayFloor(3, 4)
	require.NoError(t, err)
	require.Equal(t, uint32(4), floor)
	_, err = AdvanceReplayFloor(floor, 2)
	require.Error(t, err)
}

func TestValidateReplayFloorRejectsUnsupportedDowngrade(t *testing.T) {
	require.NoError(t, ValidateReplayFloor(0, 0))
	require.NoError(t, ValidateReplayFloor(CurrentReplayProtocol, CurrentReplayProtocol))

	err := ValidateReplayFloor(CurrentReplayProtocol+1, CurrentReplayProtocol)
	require.ErrorIs(t, err, ErrReplayProtocolUnsupported)
}
