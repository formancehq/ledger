package grpc

import (
	"context"
	"errors"
	"io"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeStreamEnd(t *testing.T) {
	canceled := status.Error(codes.Canceled, "context canceled")

	t.Run("canceled with live caller context propagates", func(t *testing.T) {
		got := normalizeStreamEnd(context.Background(), canceled)
		if status.Code(got) != codes.Canceled {
			t.Fatalf("want Canceled, got %v", got)
		}
	})

	t.Run("canceled after caller cancellation is EOF", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		got := normalizeStreamEnd(ctx, canceled)
		if !errors.Is(got, io.EOF) {
			t.Fatalf("want io.EOF, got %v", got)
		}
	})

	t.Run("other errors pass through", func(t *testing.T) {
		unavailable := status.Error(codes.Unavailable, "conn reset")

		got := normalizeStreamEnd(context.Background(), unavailable)
		if status.Code(got) != codes.Unavailable {
			t.Fatalf("want Unavailable, got %v", got)
		}
	})
}
