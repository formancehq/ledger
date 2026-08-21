//go:build antithesis

package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestAntithesisIdempotencyCommitProbeRequiresTaggedKeyedCommit(t *testing.T) {
	t.Parallel()

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		antithesisIdempotencyProbeMetadataKey,
		"probe-1",
	))

	// No keyed committed log means the test-only probe cannot delay an RPC.
	awaitAntithesisIdempotencyCommitProbe(ctx, "", []*commonpb.Log{{Sequence: 1}})
	awaitAntithesisIdempotencyCommitProbe(ctx, "key", nil)
}

func TestAntithesisIdempotencyCommitProbeSignalsThenWaitsForCancellation(t *testing.T) {
	t.Parallel()

	const probeID = "probe-after-commit"

	transport := &aggregateVolumesTransportStream{headerSent: make(chan struct{}, 1)}
	baseCtx, cancel := context.WithCancel(context.Background())
	ctx := metadata.NewIncomingContext(baseCtx, metadata.Pairs(
		antithesisIdempotencyProbeMetadataKey,
		probeID,
	))
	ctx = grpc.NewContextWithServerTransportStream(ctx, transport)
	done := make(chan struct{})
	go func() {
		defer close(done)

		awaitAntithesisIdempotencyCommitProbe(ctx, "key", []*commonpb.Log{{Sequence: 7}})
	}()

	select {
	case <-transport.headerSent:
	case <-time.After(time.Second):
		require.FailNow(t, "post-commit probe did not send its response header")
	}
	require.Equal(t, []string{probeID}, transport.header.Get(antithesisIdempotencyProbeReachedMetadataKey))

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		require.FailNow(t, "post-commit probe did not stop after cancellation")
	}
}
