package grpc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/infra/node"
)

func TestDrainBeginErrorPreservesRetryableStatus(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		want codes.Code
	}{
		{"canceled", fmt.Errorf("waiting: %w", context.Canceled), codes.Canceled},
		{"deadline", fmt.Errorf("waiting: %w", context.DeadlineExceeded), codes.DeadlineExceeded},
		{"leadership lost", fmt.Errorf("waiting: %w", node.ErrLeadershipLost), codes.Unavailable},
		{"queue failure", errors.New("proposal queue full"), codes.Unavailable},
		{"epoch conflict", node.ErrDrainEpochConflict, codes.FailedPrecondition},
		{"epoch reused", node.ErrDrainEpochReused, codes.FailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, status.Code(drainBeginError(tc.err)))
		})
	}
}
