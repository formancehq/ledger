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
			name:          "ambiguous peer close then maintenance",
			errors:        []error{status.Error(codes.Unavailable, "grpc: the client connection is closing"), maintenance},
			wantAttempts:  2,
			wantAmbiguous: true,
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

func TestIsAmbiguousCommit_ExcludesStructuredCloseLookalike(t *testing.T) {
	t.Parallel()
	st, err := status.New(codes.Unavailable, "grpc: the client connection is closing").WithDetails(&errdetails.ErrorInfo{Reason: "FUTURE_REASON"})
	if err != nil {
		t.Fatal(err)
	}
	if IsAmbiguousCommit(st.Err()) {
		t.Fatal("structured status is not the bare connection-close category")
	}
	for _, code := range []codes.Code{codes.Canceled, codes.Unknown, codes.Internal, codes.Aborted, codes.FailedPrecondition} {
		if IsAmbiguousCommit(status.Error(code, "grpc: the client connection is closing")) {
			t.Fatalf("unexpected ambiguous code: %v", code)
		}
	}
}
