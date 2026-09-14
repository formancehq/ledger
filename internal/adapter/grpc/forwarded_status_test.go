package grpc

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/formancehq/ledger/v3/internal/adapter/grpcerr"
)

func TestForwardedStatusPreservedThroughWrappers(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		code   codes.Code
		reason string
	}{
		{"conflict", codes.FailedPrecondition, "LEDGER_DELETED"},
		{"public internal", codes.Internal, "COVERAGE_MISS"},
		{"unknown aborted", codes.Aborted, "FUTURE_REASON"},
		{"unknown canceled", codes.Canceled, "FUTURE_REASON"},
		{"bare not found", codes.NotFound, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			upstream, err := status.New(tt.code, "safe upstream message").WithDetails(
				&errdetails.RetryInfo{RetryDelay: &durationpb.Duration{Seconds: 7}},
			)
			require.NoError(t, err)
			if tt.reason != "" {
				upstream, err = upstream.WithDetails(&errdetails.ErrorInfo{
					Domain: "ledger", Reason: tt.reason,
				})
				require.NoError(t, err)
			}
			wire := upstream
			for hop := range 2 {
				decoded := grpcerr.FromStatusError(wire.Err())
				wrapped := fmt.Errorf("private routing context: %w", decoded)
				wire = status.Convert(convertToGRPCError(wrapped, testLogger()))
				require.True(t, proto.Equal(upstream.Proto(), wire.Proto()),
					"hop %d changed upstream status: want %v, got %v", hop, upstream.Proto(), wire.Proto())
			}
		})
	}
}
