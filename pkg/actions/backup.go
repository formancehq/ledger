package actions

import (
	"context"
	"errors"
	"io"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// CheckStoreResult holds the errors, progress and unverifiable-range events
// from a CheckStore RPC call.
//
// UnverifiableRanges is separate from Errors because a range the checker could
// not authenticate is not a divergence — callers that assert "no integrity
// errors" must still be able to assert "and nothing went unverified", which
// requires the two to be distinguishable (EN-1526).
type CheckStoreResult struct {
	Errors             []*servicepb.CheckStoreError
	Progress           []*servicepb.CheckStoreProgress
	UnverifiableRanges []*servicepb.CheckStoreUnverifiableRange
}

// CollectCheckStoreEvents runs the CheckStore RPC and returns all errors and progress events.
func CollectCheckStoreEvents(ctx context.Context, client servicepb.BucketServiceClient) (*CheckStoreResult, error) {
	stream, err := client.CheckStore(ctx, &servicepb.CheckStoreRequest{})
	if err != nil {
		return nil, err
	}

	result := &CheckStoreResult{}
	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		switch t := event.GetType().(type) {
		case *servicepb.CheckStoreEvent_Error:
			result.Errors = append(result.Errors, t.Error)
		case *servicepb.CheckStoreEvent_Progress:
			result.Progress = append(result.Progress, t.Progress)
		case *servicepb.CheckStoreEvent_UnverifiableRange:
			result.UnverifiableRanges = append(result.UnverifiableRanges, t.UnverifiableRange)
		}
	}

	return result, nil
}
