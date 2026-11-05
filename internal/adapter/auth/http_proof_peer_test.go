package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestHTTPProofPeerPreservesAuthDisabledAttribution(t *testing.T) {
	t.Parallel()
	cfg := AuthConfig{ClusterSecret: "peer-secret"}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer peer-secret"))
	ctx, err := EvaluateGRPCCredentials(ctx, cfg)
	require.NoError(t, err)
	require.False(t, IsClusterInternal(ctx))
	verified, err := RequireClusterPeer(ctx, cfg)
	require.NoError(t, err)
	require.True(t, IsHTTPProofForwarder(verified))
	require.False(t, IsClusterInternal(verified), "proof forwarding does not grant general caller-attribution trust")
	require.NotNil(t, ResolveCallerSnapshot(verified).GetAuthDisabled())

	_, err = RequireClusterPeer(ctx, AuthConfig{})
	require.Equal(t, codes.PermissionDenied, status.Code(err), "an absent configured secret cannot authenticate a peer")
	require.False(t, IsHTTPProofForwarder(ctx))
}
