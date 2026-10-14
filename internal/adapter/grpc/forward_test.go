package grpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
)

// TestResolvePeer pins the split between a node ID that is not a
// cluster member (caller error, NotFound) and a member whose service
// connection is missing (transient, Unavailable). Before the fix, any ID
// without a pooled connection — including a mistyped one — surfaced as
// Unavailable, which clients retry and operators read as an outage.
func TestResolvePeer(t *testing.T) {
	t.Parallel()

	pool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	members := func() []uint64 { return []uint64{1, 2} }
	resolve := func(nodeID uint32) (*grpc.ClientConn, error) {
		return resolvePeer(1, members, pool, nodeID)
	}

	tests := []struct {
		name     string
		nodeID   uint32
		wantErr  error
		wantCode codes.Code
	}{
		{name: "unknown node is not a cluster member", nodeID: 99, wantErr: node.ErrNodeNotInCluster, wantCode: codes.NotFound},
		{name: "member without pooled connection is unreachable", nodeID: 2, wantErr: ErrNodeNotReachable, wantCode: codes.Unavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conn, err := resolve(tt.nodeID)
			require.Nil(t, conn)
			require.ErrorIs(t, err, tt.wantErr)

			st, ok := status.FromError(convertToGRPCError(err, testLogger()))
			require.True(t, ok)
			require.Equal(t, tt.wantCode, st.Code())
		})
	}

	for _, local := range []uint32{0, 1} {
		conn, err := resolve(local)
		require.NoError(t, err)
		require.Nil(t, conn, "node %d must be handled locally", local)
	}
}
