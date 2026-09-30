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

func TestRetryableRPCError_RetriesMaintenance(t *testing.T) {
	t.Parallel()

	maintenance, err := status.New(codes.Unavailable, "maintenance").WithDetails(&errdetails.ErrorInfo{Reason: domain.ErrReasonMaintenanceMode})
	if err != nil {
		t.Fatal(err)
	}
	// The gate is at admission, ahead of the FSM's idempotency replay, so a
	// committed write can be refused on its retry. Riding the window out is
	// what keeps that write from being recorded as never having happened.
	if !retryableRPCError(maintenance.Err()) {
		t.Fatal("maintenance rejection must stay in the retry loop")
	}
	if !retryableRPCError(status.Error(codes.Unavailable, "no leader")) {
		t.Fatal("infrastructure unavailability must remain retryable")
	}
	if retryableRPCError(status.Error(codes.NotFound, "ledger missing")) {
		t.Fatal("a business answer must still be definitive")
	}
}

func TestRetryUnaryInterceptor_RidesOutMaintenance(t *testing.T) {
	t.Parallel()

	maintenanceStatus, err := status.New(codes.Unavailable, "maintenance").WithDetails(&errdetails.ErrorInfo{Reason: domain.ErrReasonMaintenanceMode})
	if err != nil {
		t.Fatal(err)
	}
	maintenance := maintenanceStatus.Err()

	// The shape that dropped a committed write: the response is lost to a
	// kill, the retry lands inside a maintenance window, and the window then
	// closes. The interceptor must keep going and surface the success.
	errs := []error{
		status.Error(codes.Unavailable, "response lost"),
		maintenance,
		maintenance,
		maintenance,
		nil,
	}

	attempts := 0
	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		e := errs[attempts]
		attempts++

		return e
	}

	if err := retryUnaryInterceptor(len(errs))(context.Background(), "/test.Service/Apply", nil, nil, nil, invoker); err != nil {
		t.Fatalf("interceptor returned %v, want the success behind the window", err)
	}

	if attempts != len(errs) {
		t.Fatalf("attempts = %d, want %d — every maintenance rejection must be retried", attempts, len(errs))
	}
}

func TestRetryUnaryInterceptor_StopsOnDefinitiveError(t *testing.T) {
	t.Parallel()

	attempts := 0
	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		attempts++

		return status.Error(codes.NotFound, "ledger missing")
	}

	err := retryUnaryInterceptor(4)(context.Background(), "/test.Service/Apply", nil, nil, nil, invoker)
	if status.Code(err) != codes.NotFound {
		t.Fatalf("err = %v, want NotFound", err)
	}

	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 — a business answer is not retried", attempts)
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
