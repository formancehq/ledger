package grpcerr

import (
	"context"

	"google.golang.org/grpc"
)

// Conn decorates a grpc.ClientConnInterface so every error a generated client
// surfaces has already been through FromStatusError.
//
// Reconstruction belongs here rather than in the generated clients' callers.
// Every generated method reaches the wire through exactly one of Invoke (unary)
// or NewStream (streaming), so wrapping the connection covers the whole
// BucketService surface — all 37 generated methods, 11 of them server-
// streaming — including the six BucketGrpcClient list methods that return a
// lazy cursor (ListLedgers, ListTransactions, ListAccounts, ListLogs,
// ListAuditEntries, ListIndexes), whose error does not come back from the
// method at all but from a later Recv() on the stream, after the method
// already returned nil. It also covers whatever method is generated next,
// which per-method conversion cannot promise.
type Conn struct {
	inner grpc.ClientConnInterface
}

// NewConn wraps cc so that errors returned through it carry their typed
// identity. The zero benefit case (an error with nothing to reconstruct) costs
// one type assertion.
func NewConn(cc grpc.ClientConnInterface) *Conn {
	return &Conn{inner: cc}
}

var _ grpc.ClientConnInterface = (*Conn)(nil)

func (c *Conn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	return FromStatusError(c.inner.Invoke(ctx, method, args, reply, opts...))
}

func (c *Conn) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	cs, err := c.inner.NewStream(ctx, desc, method, opts...)
	if err != nil {
		return nil, FromStatusError(err)
	}

	return &clientStream{ClientStream: cs}, nil
}

// clientStream converts the errors a stream reports after NewStream returned.
// This is the path that matters for the list endpoints: the leader's rejection
// arrives at Recv(), which is RecvMsg underneath.
type clientStream struct {
	grpc.ClientStream
}

// RecvMsg converts the receive error. io.EOF (a normal stream end) is not a
// status and passes through untouched, as does the codes.Canceled that
// internal/adapter/grpc/cursor.go normalises into io.EOF.
func (s *clientStream) RecvMsg(m any) error {
	return FromStatusError(s.ClientStream.RecvMsg(m))
}

// SendMsg converts the send error too: a stream rejected at the header stage
// reports the leader's status here rather than at NewStream.
func (s *clientStream) SendMsg(m any) error {
	return FromStatusError(s.ClientStream.SendMsg(m))
}
