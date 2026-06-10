package membership

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Membership service unit tests cover the input-validation branches.
// Leader-side mutation (raftTransport.AddPeer + servicePool.AddPeer +
// node.AddLearner) requires a real *node.Node and is covered by the
// e2e cluster suite — input validation alone runs before any of those
// dependencies is touched, which is what we assert here.

func TestService_AddLearner_ValidatesRequest(t *testing.T) {
	t.Parallel()

	// node, raftTransport, servicePool intentionally nil — the
	// validation paths must reject before reaching them.
	s := &Service{}

	cases := []struct {
		name        string
		nodeID      uint64
		raftAddr    string
		serviceAddr string
		wantSubstr  string
	}{
		{"missing node_id", 0, "r:1", "s:1", "node_id"},
		{"missing raft_address", 1, "", "s:1", "raft_address"},
		{"missing service_address", 1, "r:1", "", "service_address"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := s.AddLearner(context.Background(), tc.nodeID, tc.raftAddr, tc.serviceAddr)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantSubstr)
		})
	}
}

func TestService_PromoteLearner_RejectsZeroNodeID(t *testing.T) {
	t.Parallel()

	s := &Service{}

	err := s.PromoteLearner(context.Background(), 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "node_id")
}

func TestService_RemoveNode_RejectsZeroNodeID(t *testing.T) {
	t.Parallel()

	s := &Service{}

	t.Run("consensus path", func(t *testing.T) {
		t.Parallel()
		err := s.RemoveNode(context.Background(), 0, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "node_id")
	})

	t.Run("force path", func(t *testing.T) {
		t.Parallel()
		err := s.RemoveNode(context.Background(), 0, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "node_id")
	})
}
