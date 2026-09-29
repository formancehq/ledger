package grpcerr

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
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

// connStateGetter exposes the state of the connection used for Invoke.
// *grpc.ClientConn satisfies this interface; decorators may forward its state.
type connStateGetter interface {
	GetState() connectivity.State
}

// grpc-go's exact bare client-connection close status. Keep this sentinel
// local because grpc.ErrClientConnClosing is deprecated.
var errClientConnClosing = status.Error(codes.Canceled, "grpc: the client connection is closing")

// ErrPeerConnectionClose is the normalized form of grpc-go's close status.
// It identifies a potentially lost response.
var ErrPeerConnectionClose = status.Error(codes.Unavailable, status.Convert(errClientConnClosing).Message())

// NewConn wraps cc so forwarded errors retain their typed identity. Unary
// close-status normalization additionally requires cc to expose the state of
// the connection used for Invoke through GetState. A raw *grpc.ClientConn does
// so; a decorator must forward that same state. Other ClientConnInterface
// implementations receive error reconstruction only.
func NewConn(cc grpc.ClientConnInterface) *Conn {
	return &Conn{inner: cc}
}

var _ grpc.ClientConnInterface = (*Conn)(nil)

func (c *Conn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	err := c.inner.Invoke(ctx, method, args, reply, opts...)
	// Closing a peer connection interrupts its in-flight RPCs even when the
	// external caller is still waiting. Only grpc-go's exact, unadorned close
	// status on a locally closed connection is a transport interruption:
	// matching the status alone would also match a peer-authored lookalike.
	// Shutdown is observed after Invoke, not atomically with status delivery:
	// an identical peer-authored status followed by local closure is also mapped.
	// Arbitrary Canceled statuses and structured server failures retain their
	// identity.
	// Unavailable does not prove non-commit. The caller must reuse its original
	// idempotency key when retrying a write; forwarding itself does not retry.
	stateConn, hasState := c.inner.(connStateGetter)
	if hasState && stateConn.GetState() == connectivity.Shutdown && ctx.Err() == nil {
		if errors.Is(err, errClientConnClosing) {
			return ErrPeerConnectionClose
		}
	}

	return FromStatusError(err)
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
// status and passes through untouched. Bare Canceled statuses also remain raw
// for the cursor's caller-cancellation policy; decoded Ledger failures retain
// their original status through that policy.
func (s *clientStream) RecvMsg(m any) error {
	return FromStatusError(s.ClientStream.RecvMsg(m))
}

// SendMsg converts the send error too: a stream rejected at the header stage
// reports the leader's status here rather than at NewStream.
func (s *clientStream) SendMsg(m any) error {
	return FromStatusError(s.ClientStream.SendMsg(m))
}
