package actions

import (
	"context"
	"errors"
	"io"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// CheckStoreResult holds the errors and progress events from a CheckStore RPC call.
type CheckStoreResult struct {
	Errors   []*servicepb.CheckStoreError
	Progress []*servicepb.CheckStoreProgress
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
		}
	}

	return result, nil
}
