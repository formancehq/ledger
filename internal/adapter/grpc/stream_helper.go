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
// It records stream.items_sent on both the ctx span and parentSpan so the
// count is visible even when child spans are lost due to batch export timing.
func sendCursorToStream[Res any](ctx context.Context, parentSpan trace.Span, cursor dal.Cursor[*Res], stream ggrpc.ServerStreamingServer[Res], itemName string) error {
	defer func() {
		_ = cursor.Close()
	}()

	span := trace.SpanFromContext(ctx)
	var count int64

	recordCount := func() {
		attr := attribute.Int64("stream.items_sent", count)
		span.SetAttributes(attr)
		parentSpan.SetAttributes(attr)
	}

	for {
		item, err := cursor.Next()
		if err != nil {
			recordCount()
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("reading %s: %w", itemName, err)
		}
		if err := stream.Send(item); err != nil {
			recordCount()
			return fmt.Errorf("sending %s: %w", itemName, err)
		}
		count++
	}
}
