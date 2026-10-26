package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/drivertest"
)

// The real server is the authority for ordering, pagination trailers, audit
// list/point header differences, and the local durable/sync-status contract.
// More than one page of both accounts and audit entries must be consumed.
func TestIdentityOracleLocalServer(t *testing.T) {
	// StartServer changes environment variables, so this test cannot run in
	// parallel. Sentinel checks execute on the real commit path as it is seeded.
	t.Setenv("SENTINEL_MODE", "true")
	ctx, client := drivertest.StartServer(t)
	ledger := internal.PrefixSentinel.WithSuffix("identity-smoke")
	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("identity-create", actions.CreateLedgerAction(ledger, map[string]string{"owner": "sentinel"})))
	require.NoError(t, err)
	for i := range 105 {
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(fmt.Sprintf("identity-%d", i), actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{{
			Source: "world", Destination: fmt.Sprintf("raft-sentinel:%03d", i), Asset: "USD", Color: "BLUE", Amount: commonpb.NewUint256FromUint64(uint64(i + 1)),
		}}, map[string]string{"phase": "committed"})))
		require.NoError(t, err)
	}
	conn, err := grpc.NewClient(os.Getenv("LEDGER_GRPC_ADDR"), grpcprotocol.ClientOption(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	cluster := clusterpb.NewClusterServiceClient(conn)
	state, err := cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
	require.NoError(t, err)
	require.Len(t, state.GetNodes(), 1)
	// The bootstrap advertises the host IP; localhost above reaches exactly
	// that same leased local server. Production candidates dial advertised DNS.
	candidate := &internal.PerNodeConn{Addr: state.GetNodes()[0].GetServiceAddress(), NodeID: state.GetLocalNode(), Bucket: client, Cluster: cluster}
	reports := runOracle(ctx, internal.PerNodeConns{candidate}, oracleConfig{Attempts: 3, ConvergenceWindow: 5 * time.Second, CallTimeout: 5 * time.Second, Wait: pollDelay})
	require.NotEmpty(t, reports)
	last := reports[len(reports)-1]
	require.True(t, last.Qualified, "%+v", reports)
	require.Len(t, last.Observations, 1)
	got := last.Observations[0].State
	require.Equal(t, uint64(106), got.Ledgers[ledger].Accounts)
	require.GreaterOrEqual(t, got.Audit.Count, uint64(106))
	require.True(t, got.Audit.TailMatches, "%+v", got.Audit)
	require.True(t, got.Audit.TipPresent)
	require.NotEmpty(t, got.Audit.TipDigest)
}
