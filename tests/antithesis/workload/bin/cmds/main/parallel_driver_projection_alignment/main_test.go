package main

import (
	"errors"
	"testing"

	"github.com/formancehq/ledger/v3/internal/domain"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsInconclusiveProjectionRead_OnlyToleratesReplicaBuildLag(t *testing.T) {
	t.Parallel()

	withReason := func(t *testing.T, reason string) error {
		t.Helper()

		st, err := status.New(codes.FailedPrecondition, reason).WithDetails(&errdetails.ErrorInfo{
			Reason: reason,
			Domain: "ledger",
		})
		if err != nil {
			t.Fatalf("adding ErrorInfo: %v", err)
		}

		return st.Err()
	}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "other replica still building",
			err:  withReason(t, domain.ErrReasonIndexBuilding),
			want: true,
		},
		{
			name: "index missing is a setup defect",
			err:  withReason(t, domain.ErrReasonIndexNotFound),
		},
		{
			name: "unclassified business failure",
			err:  errors.New("permanent prepared-query failure"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := isInconclusiveProjectionRead(tt.err); got != tt.want {
				t.Fatalf("isInconclusiveProjectionRead(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}
