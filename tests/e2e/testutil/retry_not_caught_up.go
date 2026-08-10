package testutil

import (
	"context"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The aligned read path serves a bounded wait and then rejects with a
// retryable FailedPrecondition while the read-index fold is behind the
// main store (EN-1748). Production clients retry it; these interceptors do
// the same for the test harness, so load-heavy specs that leave a deep fold
// backlog don't fail the next filtered read.
const (
	notCaughtUpReason  = "READ_INDEX_NOT_CAUGHT_UP"
	notCaughtUpBackoff = 200 * time.Millisecond
	notCaughtUpWindow  = 90 * time.Second
)

// isNotCaughtUp matches the server's structured detail, not its message: the
// text is free to change, the ErrorInfo reason is the wire contract.
func isNotCaughtUp(err error) bool {
	s, ok := status.FromError(err)
	if !ok || s.Code() != codes.FailedPrecondition {
		return false
	}

	for _, d := range s.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() == notCaughtUpReason {
			return true
		}
	}

	return false
}

// NotCaughtUpRetryDialOptions returns the dial options that make a client
// retry the read-index freshness precondition. Opt-in per spec: the default
// client fails fast, so an unexpected precondition surfaces as a test failure
// rather than a silent multi-minute stall.
func NotCaughtUpRetryDialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithUnaryInterceptor(NotCaughtUpUnaryInterceptor()),
		grpc.WithStreamInterceptor(NotCaughtUpStreamInterceptor()),
	}
}

// NotCaughtUpUnaryInterceptor retries unary calls rejected by the read-index
// freshness precondition until the fold catches up or the window closes.
func NotCaughtUpUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		deadline := time.Now().Add(notCaughtUpWindow)

		for {
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err == nil || !isNotCaughtUp(err) || !time.Now().Before(deadline) {
				return err
			}

			select {
			case <-ctx.Done():
				return err
			case <-time.After(notCaughtUpBackoff):
			}
		}
	}
}

// NotCaughtUpStreamInterceptor is the streaming counterpart: when a stream
// fails with the freshness precondition before delivering any message, the
// whole stream is re-issued with the recorded request.
func NotCaughtUpStreamInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		inner, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}

		return &notCaughtUpRetryStream{
			ClientStream: inner,
			ctx:          ctx,
			deadline:     time.Now().Add(notCaughtUpWindow),
			newStream: func() (grpc.ClientStream, error) {
				return streamer(ctx, desc, cc, method, opts...)
			},
		}, nil
	}
}

type notCaughtUpRetryStream struct {
	grpc.ClientStream

	ctx       context.Context
	deadline  time.Time
	newStream func() (grpc.ClientStream, error)

	sent      []any
	sendsDone bool
	received  bool
}

func (s *notCaughtUpRetryStream) SendMsg(m any) error {
	s.sent = append(s.sent, m)

	return s.ClientStream.SendMsg(m)
}

func (s *notCaughtUpRetryStream) CloseSend() error {
	s.sendsDone = true

	return s.ClientStream.CloseSend()
}

func (s *notCaughtUpRetryStream) RecvMsg(m any) error {
	for {
		err := s.ClientStream.RecvMsg(m)
		if err == nil {
			s.received = true

			return nil
		}

		// A retry is only sound while the server has delivered nothing.
		if s.received || !isNotCaughtUp(err) || !time.Now().Before(s.deadline) {
			return err
		}

		select {
		case <-s.ctx.Done():
			return err
		case <-time.After(notCaughtUpBackoff):
		}

		replacement, nerr := s.newStream()
		if nerr != nil {
			return err
		}

		// A replay or close failure is terminal: looping would keep polling
		// the dead original stream until the window closes, reporting a real
		// failure as "never caught up" — the one thing this helper exists to
		// distinguish.
		for _, sent := range s.sent {
			if serr := replacement.SendMsg(sent); serr != nil {
				_ = replacement.CloseSend()

				return err
			}
		}

		if s.sendsDone {
			if cerr := replacement.CloseSend(); cerr != nil {
				return err
			}
		}

		s.ClientStream = replacement
	}
}
