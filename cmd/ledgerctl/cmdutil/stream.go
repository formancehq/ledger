package cmdutil

import (
	"errors"
	"io"

	"google.golang.org/grpc"
)

// CollectStream drains a gRPC server-streaming response into a slice.
func CollectStream[T any](stream grpc.ServerStreamingClient[T]) ([]*T, error) {
	var items []*T

	for {
		item, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, err
		}

		items = append(items, item)
	}

	return items, nil
}
