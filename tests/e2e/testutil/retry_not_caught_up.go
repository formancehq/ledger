package testutil

import (
	"context"
	"strings"
	"time"

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
	notCaughtUpMarker  = "read index has not caught up"
	notCaughtUpBackoff = 200 * time.Millisecond
	notCaughtUpWindow  = 90 * time.Second
)

func isNotCaughtUp(err error) bool {
	s, ok := status.FromError(err)

	return ok && s.Code() == codes.FailedPrecondition && strings.Contains(s.Message(), notCaughtUpMarker)
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

		replay := true
		for _, sent := range s.sent {
			if serr := replacement.SendMsg(sent); serr != nil {
				replay = false

				break
			}
		}

		if !replay {
			continue
		}

		if s.sendsDone {
			if cerr := replacement.CloseSend(); cerr != nil {
				continue
			}
		}

		s.ClientStream = replacement
	}
}
