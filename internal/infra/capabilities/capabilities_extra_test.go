package capabilities

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIncarnationValidation(t *testing.T) {
	require.Error(t, ValidateMembers([]Member{{NodeID: 4, InstanceID: []byte("a"), Role: Voter}, {NodeID: 4, InstanceID: []byte("b"), Role: Learner}}))
	require.NoError(t, ValidateMembers([]Member{{NodeID: 4, InstanceID: []byte("a"), Role: Voter}, {NodeID: 5, InstanceID: []byte("b"), Role: Learner}}))
}
