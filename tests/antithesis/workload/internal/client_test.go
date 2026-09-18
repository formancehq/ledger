package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsTolerated_LocalReadinessTimeoutIsInconclusive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "wrapped readiness deadline",
			err:  fmt.Errorf("waiting for index readiness: %w (last error: index still building)", context.DeadlineExceeded),
			want: true,
		},
		{
			name: "wrapped driver cancellation",
			err:  fmt.Errorf("waiting for index readiness: %w", context.Canceled),
			want: true,
		},
		{
			name: "permanent readiness error",
			err:  errors.New("index registry entry is malformed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := IsTolerated(tt.err); got != tt.want {
				t.Fatalf("IsTolerated(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}

func TestUnaryTransportClassificationRemainsNarrow(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		code       codes.Code
		transient  bool
		classified bool
	}{
		{codes.Unavailable, true, true},
		{codes.Canceled, false, true},
		{codes.Unknown, false, false},
		{codes.Internal, false, false},
		{codes.Aborted, false, false},
		{codes.FailedPrecondition, false, true},
	} {
		t.Run(test.code.String(), func(t *testing.T) {
			t.Parallel()
			err := status.Error(test.code, "grpc: the client connection is closing")
			if got := IsTransient(err); got != test.transient {
				t.Fatalf("IsTransient(%v) = %t, want %t", err, got, test.transient)
			}
			if got := IsClassified(err); got != test.classified {
				t.Fatalf("IsClassified(%v) = %t, want %t", err, got, test.classified)
			}
		})
	}
}
