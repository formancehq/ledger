package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
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

func TestRetryableRPCError_SurfacesMaintenance(t *testing.T) {
	t.Parallel()

	maintenance, err := status.New(codes.Unavailable, "maintenance").WithDetails(&errdetails.ErrorInfo{Reason: domain.ErrReasonMaintenanceMode})
	if err != nil {
		t.Fatal(err)
	}
	if retryableRPCError(maintenance.Err()) {
		t.Fatal("maintenance rejection must escape the retry loop")
	}
	if !retryableRPCError(status.Error(codes.Unavailable, "no leader")) {
		t.Fatal("infrastructure unavailability must remain retryable")
	}
}

func TestRetryableRPCErrorAfterAttempt_PreservesAmbiguousCommit(t *testing.T) {
	t.Parallel()

	maintenance, err := status.New(codes.Unavailable, "maintenance").WithDetails(&errdetails.ErrorInfo{Reason: domain.ErrReasonMaintenanceMode})
	if err != nil {
		t.Fatal(err)
	}
	if retryableRPCErrorAfterAttempt(maintenance.Err(), false) {
		t.Fatal("first-attempt maintenance rejection must be observable")
	}
	if !retryableRPCErrorAfterAttempt(maintenance.Err(), true) {
		t.Fatal("maintenance after an ambiguous attempt must keep retrying")
	}
}
