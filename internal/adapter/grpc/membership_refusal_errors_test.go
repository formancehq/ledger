package grpc

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/application/membership"
	"github.com/formancehq/ledger/v3/internal/infra/node"
)

// Refused membership changes must reach the operator with a client status
// code and their reason, not as a sanitized unknown server error.
func TestConvertToGRPCError_RefusedMembershipChange(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		err    error
		code   codes.Code
		reason string
	}{
		{
			name:   "promote non-member",
			err:    fmt.Errorf("promoting learner: %w", fmt.Errorf("node 987654: %w", node.ErrNodeNotInCluster)),
			code:   codes.NotFound,
			reason: "node 987654: node is not a member of the cluster",
		},
		{
			name:   "promote voter",
			err:    fmt.Errorf("promoting learner: %w", fmt.Errorf("node 1: %w", node.ErrNodeAlreadyVoter)),
			code:   codes.FailedPrecondition,
			reason: "node 1: node is already a voter",
		},
		{
			name:   "remove leader",
			err:    fmt.Errorf("removing node: %w", node.ErrCannotRemoveSelf),
			code:   codes.FailedPrecondition,
			reason: "cannot remove the leader node; transfer leadership first",
		},
		{
			name:   "force-remove leader",
			err:    fmt.Errorf("force-removing node: %w", node.ErrCannotRemoveSelf),
			code:   codes.FailedPrecondition,
			reason: "cannot remove the leader node; transfer leadership first",
		},
		{
			name:   "invalid request",
			err:    fmt.Errorf("%w: raft_address is required", membership.ErrInvalidRequest),
			code:   codes.InvalidArgument,
			reason: "raft_address is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, ok := status.FromError(convertToGRPCError(tc.err, testLogger()))
			require.True(t, ok)
			require.Equal(t, tc.code, st.Code())
			require.Contains(t, st.Message(), tc.reason)
			require.NotContains(t, st.Message(), "unknown server error")
		})
	}
}
