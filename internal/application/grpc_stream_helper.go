package application

import (
	"fmt"
	"io"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"google.golang.org/grpc"
)

// sendCursorToStream drains a cursor into a gRPC server stream,
// closing the cursor when done. The cursor must yield *Res items
// matching the stream's Send(*Res) signature.
func sendCursorToStream[Res any](cursor dal.Cursor[*Res], stream grpc.ServerStreamingServer[Res], itemName string) error {
	defer func() {
		_ = cursor.Close()
	}()

	for {
		item, err := cursor.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("reading %s: %w", itemName, err)
		}
		if err := stream.Send(item); err != nil {
			return fmt.Errorf("sending %s: %w", itemName, err)
		}
	}
}
