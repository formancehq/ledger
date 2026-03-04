package grpc

import (
	"context"
	"fmt"
	"io"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	ggrpc "google.golang.org/grpc"
)

// sendCursorToStream drains a cursor into a gRPC server stream,
// closing the cursor when done. The cursor must yield *Res items
// matching the stream's Send(*Res) signature.
//
// ctx is used to record streaming progress on the current span.
func sendCursorToStream[Res any](ctx context.Context, cursor dal.Cursor[*Res], stream ggrpc.ServerStreamingServer[Res], itemName string) error {
	defer func() {
		_ = cursor.Close()
	}()

	span := trace.SpanFromContext(ctx)
	var count int64

	for {
		item, err := cursor.Next()
		if err != nil {
			if err == io.EOF {
				span.SetAttributes(attribute.Int64("stream.items_sent", count))
				return nil
			}
			span.SetAttributes(attribute.Int64("stream.items_sent", count))
			return fmt.Errorf("reading %s: %w", itemName, err)
		}
		if err := stream.Send(item); err != nil {
			span.SetAttributes(attribute.Int64("stream.items_sent", count))
			return fmt.Errorf("sending %s: %w", itemName, err)
		}
		count++
	}
}
