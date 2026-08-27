package grpc

import (
	"context"
	"errors"
	"io"
	"testing"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeStream implements just enough of grpc.ServerStreamingClient for
// normalizeStreamEnd: Recv is never called by it, Context drives the verdict.
type fakeStream struct {
	ggrpc.ClientStream

	ctx context.Context
}

func (f *fakeStream) Recv() (*struct{}, error) { return nil, io.EOF }
func (f *fakeStream) Context() context.Context { return f.ctx }

func TestNormalizeStreamEnd(t *testing.T) {
	canceled := status.Error(codes.Canceled, "context canceled")

	t.Run("canceled with live local context propagates", func(t *testing.T) {
		got := normalizeStreamEnd[struct{}](&fakeStream{ctx: context.Background()}, canceled)
		if status.Code(got) != codes.Canceled {
			t.Fatalf("want Canceled, got %v", got)
		}
	})

	t.Run("canceled after local cancellation is EOF", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		got := normalizeStreamEnd[struct{}](&fakeStream{ctx: ctx}, canceled)
		if !errors.Is(got, io.EOF) {
			t.Fatalf("want io.EOF, got %v", got)
		}
	})

	t.Run("other errors pass through", func(t *testing.T) {
		unavailable := status.Error(codes.Unavailable, "conn reset")

		got := normalizeStreamEnd[struct{}](&fakeStream{ctx: context.Background()}, unavailable)
		if status.Code(got) != codes.Unavailable {
			t.Fatalf("want Unavailable, got %v", got)
		}
	})
}
