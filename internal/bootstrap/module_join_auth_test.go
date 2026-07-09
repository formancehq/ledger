package bootstrap

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/clusterbootstrappb"
)

// unauthBootstrapServer is a ClusterBootstrapService whose every RPC rejects
// the caller with codes.Unauthenticated, mirroring what the real RaftServer's
// cluster-secret interceptor returns for a missing/wrong secret.
type unauthBootstrapServer struct {
	clusterbootstrappb.UnimplementedClusterBootstrapServiceServer
}

func (unauthBootstrapServer) JoinAsLearner(context.Context, *clusterbootstrappb.JoinAsLearnerRequest) (*clusterbootstrappb.JoinAsLearnerResponse, error) {
	return nil, status.Error(codes.Unauthenticated, "invalid cluster credentials on Raft RPC")
}

// TestTryAddLearner_FailsFastOnUnauthenticated pins EN-1080: learner
// registration must abort immediately on a cluster-secret mismatch with a
// typed JoinAuthError instead of retrying the (mis)configuration forever.
func TestTryAddLearner_FailsFastOnUnauthenticated(t *testing.T) {
	t.Parallel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	clusterbootstrappb.RegisterClusterBootstrapServiceServer(srv, unauthBootstrapServer{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	cfg := Config{
		ClusterID:     "test-cluster",
		ClusterSecret: "wrong-secret",
		TLSConfig:     TLSConfig{Mode: TLSModeDisabled},
		RaftConfig: node.NodeConfig{
			NodeID:        2,
			WalDir:        t.TempDir(),
			AdvertiseAddr: "127.0.0.1:7777",
			Peers: []node.Peer{
				{ID: 1, Address: lis.Addr().String(), ServiceAddress: "127.0.0.1:8888"},
			},
		},
	}

	// Generous deadline: a passing test returns almost immediately; a broken
	// fail-fast would retry until this fires.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	start := time.Now()
	err = tryAddLearner(ctx, cfg, cfg.TLSConfig, logging.Testing())
	elapsed := time.Since(start)

	require.Error(t, err)

	var joinErr *JoinAuthError
	require.True(t, errors.As(err, &joinErr),
		"Unauthenticated during learner registration must surface as *JoinAuthError, got %T: %v", err, err)
	require.True(t, joinErr.HasSecret)
	require.Equal(t, uint64(1), joinErr.PeerID)
	require.Contains(t, err.Error(), "inter-node authentication failed")
	require.Contains(t, err.Error(), "verify the secret matches")

	require.Less(t, elapsed, 5*time.Second,
		"learner registration must abort immediately on Unauthenticated, not retry")
}

// TestJoinAuthError_Message pins the actionable wording of the fatal
// cluster-join authentication error (EN-1080). The message must tell the
// operator exactly which lever to pull, distinguishing the missing-secret
// case (the joining node has no --cluster-secret) from the mismatched-secret
// case (it has one, but the target rejected it).
func TestJoinAuthError_Message(t *testing.T) {
	t.Parallel()

	t.Run("missing secret", func(t *testing.T) {
		t.Parallel()

		err := &JoinAuthError{
			PeerID:      2,
			PeerAddress: "node-1:7777",
			HasSecret:   false,
			Detail:      "missing authorization metadata on Raft RPC",
		}

		msg := err.Error()
		require.Contains(t, msg, "peer 2 (node-1:7777)")
		require.Contains(t, msg, "inter-node authentication failed")
		require.Contains(t, msg, "missing authorization metadata on Raft RPC")
		// Actionable hint for the missing-secret case.
		require.Contains(t, msg, "without --cluster-secret")
		require.Contains(t, msg, "set --cluster-secret")
	})

	t.Run("peer discovery phase (no peer id yet)", func(t *testing.T) {
		t.Parallel()

		// During Phase 1 peer discovery the joining node only knows the
		// --join address, not the target's node id, so PeerID is 0 and the
		// message must fall back to the raw address without a "peer 0 (...)"
		// prefix.
		err := &JoinAuthError{
			PeerAddress: "node-1:7777",
			HasSecret:   false,
			Detail:      "missing authorization metadata on Raft RPC",
		}

		msg := err.Error()
		require.Contains(t, msg, "rejected by node-1:7777")
		require.NotContains(t, msg, "peer 0")
		require.Contains(t, msg, "set --cluster-secret")
	})

	t.Run("mismatched secret", func(t *testing.T) {
		t.Parallel()

		err := &JoinAuthError{
			PeerID:      3,
			PeerAddress: "node-1:7777",
			HasSecret:   true,
			Detail:      "invalid cluster credentials on Raft RPC",
		}

		msg := err.Error()
		require.Contains(t, msg, "peer 3 (node-1:7777)")
		require.Contains(t, msg, "invalid cluster credentials on Raft RPC")
		// Actionable hint for the mismatched-secret case.
		require.Contains(t, msg, "started with --cluster-secret")
		require.Contains(t, msg, "verify the secret matches")
	})
}
