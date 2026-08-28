package testserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestForwardIncomingMetadata(t *testing.T) {
	t.Parallel()

	incoming := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer cluster-secret",
		"cluster-id", "test-cluster",
	))

	outgoing := forwardIncomingMetadata(incoming)
	forwarded, ok := metadata.FromOutgoingContext(outgoing)
	require.True(t, ok)
	require.Equal(t, []string{"Bearer cluster-secret"}, forwarded.Get("authorization"))
	require.Equal(t, []string{"test-cluster"}, forwarded.Get("cluster-id"))
}
