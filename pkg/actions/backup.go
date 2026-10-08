package actions

import (
	"context"
	"errors"
	"io"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// CheckStoreResult holds the errors and progress events from a CheckStore RPC call.
type CheckStoreResult struct {
	Errors   []*ledgerpb.CheckStoreError
	Progress []*ledgerpb.CheckStoreProgress
}

// CollectCheckStoreEvents runs the CheckStore RPC and returns all errors and progress events.
func CollectCheckStoreEvents(ctx context.Context, client ledgerpb.BucketServiceClient) (*CheckStoreResult, error) {
	stream, err := client.CheckStore(ctx, &ledgerpb.CheckStoreRequest{})
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
		case *ledgerpb.CheckStoreEvent_Error:
			result.Errors = append(result.Errors, t.Error)
		case *ledgerpb.CheckStoreEvent_Progress:
			result.Progress = append(result.Progress, t.Progress)
		}
	}

	return result, nil
}
