//go:build antithesis

package bootstrap

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestAntithesisLinearizablePITProbeRequiresOmittedConsistency(t *testing.T) {
	t.Parallel()

	const probeID = "quorum-loss-1"

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		antithesisLinearizablePITProbeMetadataKey,
		probeID,
	))
	require.Equal(t, probeID, antithesisLinearizablePITProbeID(ctx))

	explicitCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		antithesisLinearizablePITProbeMetadataKey,
		probeID,
		consistencyMetadataKey,
		"linearizable",
	))
	require.Empty(t, antithesisLinearizablePITProbeID(explicitCtx))

	duplicateCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		antithesisLinearizablePITProbeMetadataKey,
		probeID,
		antithesisLinearizablePITProbeMetadataKey,
		"other",
	))
	require.Empty(t, antithesisLinearizablePITProbeID(duplicateCtx))
}

func TestAntithesisLinearizablePITProbeUsesBoundedInternalBarrier(t *testing.T) {
	t.Parallel()

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		antithesisLinearizablePITProbeMetadataKey,
		"quorum-loss-2",
	))
	routingCtx, cancel := antithesisLinearizablePITBarrierContext(ctx)
	defer cancel()

	deadline, ok := routingCtx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(antithesisLinearizablePITBarrierTimeout), deadline, 100*time.Millisecond)
}
