package bootstrap

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/clusterbootstrappb"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// wrongClusterBootstrapServer rejects every join with the RaftServer's
// cluster-id check status and counts the attempts.
type wrongClusterBootstrapServer struct {
	clusterbootstrappb.UnimplementedClusterBootstrapServiceServer

	attempts atomic.Int32
}

func (s *wrongClusterBootstrapServer) JoinAsLearner(context.Context, *clusterbootstrappb.JoinAsLearnerRequest) (*clusterbootstrappb.JoinAsLearnerResponse, error) {
	s.attempts.Add(1)

	return nil, status.Error(codes.PermissionDenied, "invalid cluster ID")
}

// TestTryAddLearner_FailsFastOnPermissionDenied pins EN-2738: a cluster-id
// mismatch during learner registration is a configuration error, reported
// once as a typed JoinClusterIDError and never retried.
func TestTryAddLearner_FailsFastOnPermissionDenied(t *testing.T) {
	t.Parallel()

	srv := &wrongClusterBootstrapServer{}
	addr := serveBootstrap(t, srv)

	walDir := t.TempDir()
	cfg := Config{
		ClusterID: "wrong-cluster",
		TLSConfig: TLSConfig{Mode: TLSModeDisabled},
		RaftConfig: node.NodeConfig{
			NodeID:        2,
			WalDir:        walDir,
			AdvertiseAddr: "127.0.0.1:7777",
			Peers: []node.Peer{
				{ID: 1, Address: addr, ServiceAddress: "127.0.0.1:8888"},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	err := tryAddLearner(ctx, cfg, cfg.TLSConfig, logging.Testing())
	require.Error(t, err)

	var idErr *JoinClusterIDError
	require.True(t, errors.As(err, &idErr),
		"PermissionDenied during learner registration must surface as *JoinClusterIDError, got %T: %v", err, err)
	require.Equal(t, uint64(1), idErr.PeerID)
	require.Equal(t, addr, idErr.PeerAddress)
	require.Equal(t, "wrong-cluster", idErr.ClusterID)
	require.Equal(t, "invalid cluster ID", idErr.Detail)
	require.Equal(t, int32(1), srv.attempts.Load(), "a cluster-id mismatch must not be retried")
	require.False(t, wal.IsClusterJoined(walDir))
}

// TestJoinClusterIDError_Message pins the actionable wording of the fatal
// cluster-id mismatch error, for discovery (no peer id) and registration.
func TestJoinClusterIDError_Message(t *testing.T) {
	t.Parallel()

	t.Run("peer discovery phase (no peer id yet)", func(t *testing.T) {
		t.Parallel()

		msg := (&JoinClusterIDError{
			PeerAddress: "node-1:7777",
			ClusterID:   "staging-ledger",
			Detail:      "invalid cluster ID",
		}).Error()
		require.Contains(t, msg, "rejected by node-1:7777")
		require.NotContains(t, msg, "peer 0")
		require.Contains(t, msg, "cluster ID mismatch (invalid cluster ID)")
		require.Contains(t, msg, `--cluster-id "staging-ledger"`)
		require.Contains(t, msg, "set --cluster-id to the value configured on the existing cluster nodes")
	})

	t.Run("learner registration", func(t *testing.T) {
		t.Parallel()

		msg := (&JoinClusterIDError{
			PeerID:      1,
			PeerAddress: "node-1:7777",
			ClusterID:   "staging-ledger",
			Detail:      "invalid cluster ID",
		}).Error()
		require.Contains(t, msg, "rejected by peer 1 (node-1:7777)")
		require.Contains(t, msg, "set --cluster-id")
	})
}
