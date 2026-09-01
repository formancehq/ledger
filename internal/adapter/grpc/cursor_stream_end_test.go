package grpc

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeStreamEnd(t *testing.T) {
	canceled := status.Error(codes.Canceled, "context canceled")

	t.Run("canceled with live caller context becomes the Unavailable transient", func(t *testing.T) {
		got := normalizeStreamEnd(context.Background(), canceled)
		if status.Code(got) != codes.Unavailable {
			t.Fatalf("want Unavailable, got %v", got)
		}

		if msg := status.Convert(got).Message(); !strings.Contains(msg, "context canceled") {
			t.Fatalf("the original failure must survive in the message, got %q", msg)
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
