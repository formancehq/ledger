package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	ggrpc "google.golang.org/grpc"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/cursor"
)

// sendCursorToStream drains a cursor into a gRPC server stream,
// closing the cursor when done. The cursor must yield *Res items
// matching the stream's Send(*Res) signature.
//
// It records stream.items_sent on the current span.
func sendCursorToStream[Res any](ctx context.Context, cursor cursor.Cursor[*Res], stream ggrpc.ServerStreamingServer[Res], itemName string) error {
	defer func() {
		_ = cursor.Close()
	}()

	span := trace.SpanFromContext(ctx)

	var count int64

	for {
		item, err := cursor.Next()
		if err != nil {
			span.SetAttributes(attribute.Int64("stream.items_sent", count))

			if errors.Is(err, io.EOF) {
				return nil
			}

			return fmt.Errorf("reading %s: %w", itemName, err)
		}

		if err := stream.Send(item); err != nil {
			span.SetAttributes(attribute.Int64("stream.items_sent", count))

			return fmt.Errorf("sending %s: %w", itemName, err)
		}

		count++
	}
}
