//go:build antithesis

package grpc

import (
	"context"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

const (
	antithesisIdempotencyProbeMetadataKey        = "x-formance-antithesis-idempotency-probe"
	antithesisIdempotencyProbeReachedMetadataKey = "x-formance-antithesis-idempotency-probe-reached"
	antithesisIdempotencyProbeMaximumWait        = 5 * time.Second
)

// awaitAntithesisIdempotencyCommitProbe is compiled only into the instrumented
// Antithesis SUT. A workload-tagged keyed Apply reaches this function only
// after ctrl.Apply's commit future returned successfully and before receipt or
// response serialization. Sending the response header proves that boundary to
// the workload; waiting for cancellation then makes the client response
// deliberately ambiguous without changing the committed FSM outcome.
func awaitAntithesisIdempotencyCommitProbe(
	ctx context.Context,
	idempotencyKey string,
	logs []*commonpb.Log,
) {
	if idempotencyKey == "" || len(logs) == 0 {
		return
	}

	values := metadata.ValueFromIncomingContext(ctx, antithesisIdempotencyProbeMetadataKey)
	if len(values) != 1 || values[0] == "" {
		return
	}
	probeID := values[0]
	firstLogSequence := logs[0].GetSequence()
	lastLogSequence := logs[len(logs)-1].GetSequence()
	details := map[string]any{
		"probe_id":           probeID,
		"idempotency_key":    idempotencyKey,
		"first_log_sequence": firstLogSequence,
		"last_log_sequence":  lastLogSequence,
		"log_count":          len(logs),
	}

	if err := ggrpc.SendHeader(ctx, metadata.Pairs(
		antithesisIdempotencyProbeReachedMetadataKey,
		probeID,
	)); err != nil {
		return
	}

	assert.Reachable("pit: keyed apply committed before response serialization", details)

	timer := time.NewTimer(antithesisIdempotencyProbeMaximumWait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
