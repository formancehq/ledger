package membership

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestConfChangeProposalID(t *testing.T) {
	t.Parallel()

	t.Run("missing change", func(t *testing.T) {
		t.Parallel()

		proposalID, err := ConfChangeProposalID(nil)
		require.NoError(t, err)
		require.Empty(t, proposalID)
	})

	t.Run("empty context", func(t *testing.T) {
		t.Parallel()

		proposalID, err := ConfChangeProposalID(&raftpb.ConfChangeV2{})
		require.NoError(t, err)
		require.Empty(t, proposalID)
	})

	t.Run("correlation token", func(t *testing.T) {
		t.Parallel()

		ctx, err := MarshalConfChangeContext(ConfChangeContext{ProposalID: "proposal-42"})
		require.NoError(t, err)

		proposalID, err := ConfChangeProposalID(&raftpb.ConfChangeV2{Context: ctx})
		require.NoError(t, err)
		require.Equal(t, "proposal-42", proposalID)
	})

	t.Run("malformed context", func(t *testing.T) {
		t.Parallel()

		proposalID, err := ConfChangeProposalID(&raftpb.ConfChangeV2{Context: []byte("not-json")})
		require.ErrorContains(t, err, "unmarshaling ConfChangeContext")
		require.Empty(t, proposalID)
	})
}
