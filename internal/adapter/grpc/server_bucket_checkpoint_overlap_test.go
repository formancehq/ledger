package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
)

// Hold the first reader through the second acquisition, removing scheduler
// timing from the production overlap that triggered Antithesis timeline
// 8282440088460908998. GetTransaction uses this exact store-pair boundary.
func TestOpenCheckpointStoresServesOverlappingReaders(t *testing.T) {
	t.Parallel()
	impl := newCheckpointGateFixture(t)
	_, _, releaseFirst, err := impl.openCheckpointStores(context.Background(), gateCheckpointID)
	require.NoError(t, err)
	defer releaseFirst()

	main, index, releaseSecond, err := impl.openCheckpointStores(context.Background(), gateCheckpointID)
	if err != nil {
		t.Logf("raw=%v; gRPC=%s", err, status.Code(convertToGRPCError(err, testLogger())))
	}
	require.NoError(t, err, "a second reader must succeed while the first reader still holds the frozen checkpoint")
	defer releaseSecond()
	require.NotNil(t, main)
	require.NotNil(t, index)
}
