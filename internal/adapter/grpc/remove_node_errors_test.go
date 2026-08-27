package grpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/infra/node"
)

func TestConvertToGRPCError_NodeNotInCluster(t *testing.T) {
	t.Parallel()

	for _, err := range []error{
		node.ErrNodeNotInCluster,
		fmt.Errorf("removing node: %w", node.ErrNodeNotInCluster),
	} {
		grpcErr := convertToGRPCError(err, testLogger())
		st, ok := status.FromError(grpcErr)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
		require.NotContains(t, st.Message(), "correlation ID")

		info := extractErrorInfo(t, st)
		require.Equal(t, "RAFT_NODE_NOT_IN_CLUSTER", info.GetReason())
		require.Equal(t, errorDomain, info.GetDomain())
	}
}

func TestConvertToGRPCError_NodeRemovalCommittedButApplyPending(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("removing node: %w", &node.RemoveNodeCommittedError{
		NodeID:         3,
		CommittedIndex: 42,
		Cause:          context.DeadlineExceeded,
	})

	grpcErr := convertToGRPCError(err, testLogger())
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code())
	require.Contains(t, st.Message(), "removal committed at raft index 42")
	require.NotContains(t, st.Message(), "correlation ID")

	info := extractErrorInfo(t, st)
	require.Equal(t, "RAFT_NODE_REMOVAL_COMMITTED", info.GetReason())
	require.Equal(t, "3", info.GetMetadata()["nodeId"])
	require.Equal(t, "42", info.GetMetadata()["committedIndex"])
}
