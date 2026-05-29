package grpc

import (
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
)

// GRPCStreamCursor implements cursor.Cursor[To] by reading from a gRPC server stream.
type GRPCStreamCursor[Res, To any] struct {
	client grpc.ServerStreamingClient[Res]
	mapper func(*Res) (To, error)
}

func (c GRPCStreamCursor[Res, To]) Next() (To, error) {
	next, err := c.client.Recv()
	if err != nil {
		if status.Code(err) == codes.Canceled {
			err = io.EOF
		}

		var zero To

		return zero, err
	}

	return c.mapper(next)
}

func (c GRPCStreamCursor[Res, To]) Close() error {
	return c.client.CloseSend()
}

var _ cursor.Cursor[any] = (*GRPCStreamCursor[any, any])(nil)

// NewGRPCStreamCursor creates a cursor that reads from a gRPC server stream and maps each element.
func NewGRPCStreamCursor[Res, To any](client grpc.ServerStreamingClient[Res], mapper func(*Res) (To, error)) cursor.Cursor[To] {
	return GRPCStreamCursor[Res, To]{client: client, mapper: mapper}
}

// NewGRPCIdentityCursor creates a GRPCStreamCursor with an identity mapper.
func NewGRPCIdentityCursor[T any](client grpc.ServerStreamingClient[T]) cursor.Cursor[*T] {
	return NewGRPCStreamCursor(client, func(res *T) (*T, error) {
		return res, nil
	})
}
