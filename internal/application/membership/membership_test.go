package membership

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
)

// These tests cover input validation before any dependency is touched.
// membership_node_test.go exercises admission and discovery through a real
// *node.Node; the e2e cluster suite covers inter-node routing.

func TestService_AddLearner_ValidatesRequest(t *testing.T) {
	t.Parallel()

	// node intentionally nil — the
	// validation paths must reject before reaching them.
	s := &Service{}

	cases := []struct {
		name        string
		nodeID      uint64
		raftAddr    string
		serviceAddr string
		instanceID  []byte
		wantSubstr  string
	}{
		{"missing node_id", 0, "r:1", "s:1", []byte("0123456789abcdef"), "node_id"},
		{"missing raft_address", 1, "", "s:1", []byte("0123456789abcdef"), "raft_address"},
		{"missing service_address", 1, "r:1", "", []byte("0123456789abcdef"), "service_address"},
		{"missing instance_id", 1, "r:1", "s:1", nil, "instance_id"},
		{"short instance_id", 1, "r:1", "s:1", []byte("short"), "instance_id"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := s.AddLearner(context.Background(), tc.nodeID, tc.raftAddr, tc.serviceAddr, tc.instanceID)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantSubstr)
		})
	}
}

func TestService_AddLearner_RejectionPreservesServiceRouting(t *testing.T) {
	t.Parallel()

	const (
		nodeID          = uint64(2)
		committedAddr   = "member-2:8080"
		uncommittedAddr = "attacker:8080"
	)

	servicePool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	require.NoError(t, servicePool.AddPeer(nodeID, committedAddr))
	t.Cleanup(func() { require.NoError(t, servicePool.Close()) })

	s := &Service{
		servicePool: servicePool,
		addRaftPeer: func(uint64, string) {},
		addLearner: func(context.Context, uint64, string, string, []byte) error {
			return node.ErrNodeAlreadyInCluster
		},
		logger: logging.Testing(),
	}

	err := s.AddLearner(context.Background(), nodeID, "member-2:7070", uncommittedAddr, nil)
	require.ErrorIs(t, err, node.ErrNodeAlreadyInCluster)
	require.Equal(t, committedAddr, servicePool.GetPeerAddress(nodeID))
}

func TestService_JoinAsLearner_RejectsMissingInstanceID(t *testing.T) {
	t.Parallel()

	err := (&Service{}).JoinAsLearner(context.Background(), 1, "r:1", "s:1", nil)
	require.ErrorContains(t, err, "instance_id")
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
