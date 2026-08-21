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
		antithesisHistoricalBalanceProbeMetadataKey,
		probeID,
	))
	require.Equal(t, probeID, antithesisHistoricalBalanceProbeID(ctx))

	explicitCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		antithesisHistoricalBalanceProbeMetadataKey,
		probeID,
		consistencyMetadataKey,
		"linearizable",
	))
	require.Empty(t, antithesisHistoricalBalanceProbeID(explicitCtx))

	duplicateCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		antithesisHistoricalBalanceProbeMetadataKey,
		probeID,
		antithesisHistoricalBalanceProbeMetadataKey,
		"other",
	))
	require.Empty(t, antithesisHistoricalBalanceProbeID(duplicateCtx))
}

func TestAntithesisLinearizablePITProbeUsesBoundedInternalBarrier(t *testing.T) {
	t.Parallel()

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		antithesisHistoricalBalanceProbeMetadataKey,
		"quorum-loss-2",
	))
	routingCtx, cancel := antithesisHistoricalBalanceBarrierContext(ctx)
	defer cancel()

	deadline, ok := routingCtx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(antithesisHistoricalBalanceBarrierTimeout), deadline, 100*time.Millisecond)
}
