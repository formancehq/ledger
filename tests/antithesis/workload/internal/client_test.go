package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
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

func TestRetryUnaryInterceptor_MaintenanceRequiresAmbiguousAttempt(t *testing.T) {
	t.Parallel()

	maintenanceStatus, err := status.New(codes.Unavailable, "maintenance").WithDetails(&errdetails.ErrorInfo{Reason: domain.ErrReasonMaintenanceMode})
	if err != nil {
		t.Fatal(err)
	}
	maintenance := maintenanceStatus.Err()

	tests := []struct {
		name          string
		errors        []error
		wantAttempts  int
		wantAmbiguous bool
	}{
		{
			name:         "definitive unavailable then maintenance",
			errors:       []error{status.Error(codes.Unavailable, "no leader"), maintenance},
			wantAttempts: 2,
		},
		{
			name:          "ambiguous deadline then maintenance",
			errors:        []error{status.Error(codes.DeadlineExceeded, "response lost"), maintenance},
			wantAttempts:  2,
			wantAmbiguous: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			attempts := 0
			invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				err := tt.errors[attempts]
				attempts++
				return err
			}
			err := retryUnaryInterceptor(len(tt.errors))(context.Background(), "/test.Service/Apply", nil, nil, nil, invoker)
			if attempts != tt.wantAttempts {
				t.Fatalf("attempts = %d, want %d", attempts, tt.wantAttempts)
			}
			if got := IsMaintenanceAfterAmbiguousCommit(err); got != tt.wantAmbiguous {
				t.Fatalf("IsMaintenanceAfterAmbiguousCommit() = %t, want %t", got, tt.wantAmbiguous)
			}
			if !HasErrorReason(err, domain.ErrReasonMaintenanceMode) {
				t.Fatalf("final error = %v, want maintenance reason", err)
			}
		})
	}
}
