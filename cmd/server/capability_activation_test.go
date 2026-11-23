package server

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"
)

func TestControlStateThroughRunAndRestart(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	lease := testserver.AllocateNodeLease()
	directory := t.TempDir()
	dataDir := filepath.Join(directory, "data")
	instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
		NodeID: 1, ClusterID: "activation-test", Ports: lease.Ports(),
		DataDir: dataDir, WalDir: filepath.Join(directory, "wal"),
	})
	request := func(name string) *servicepb.ApplyRequest {
		return servicepb.UnsignedApplyRequest("", &servicepb.Request{Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{Name: name},
		}})
	}
	readPolicy := func() *commonpb.ClusterPolicy {
		t.Helper()
		store, err := dal.NewStore(dataDir, logging.Testing(), noop.NewMeterProvider().Meter("activation-test"), dal.DefaultConfig())
		require.NoError(t, err)
		defer func() { require.NoError(t, store.Close()) }()
		policy, err := query.ReadClusterPolicy(store)
		require.NoError(t, err)

		return policy
	}
	start := func(extra ...string) (servicepb.BucketServiceClient, clusterpb.ClusterServiceClient, func()) {
		t.Helper()
		current := append([]testservice.Instrumentation{}, instruments...)
		current = append(current, testservice.AppendArgsInstrumentation(append([]string{"--bootstrap"}, extra...)...))
		server := lease.NewService(NewRunCommandWithBindings, testservice.WithInstruments(current...))
		require.NoError(t, server.Start(ctx))
		connection, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", lease.Ports().GRPC()),
			grpc.WithTransportCredentials(insecure.NewCredentials()), grpcprotocol.ClientOption())
		require.NoError(t, err)
		stop := sync.OnceFunc(func() {
			require.NoError(t, connection.Close())
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer stopCancel()
			require.NoError(t, server.Stop(stopCtx))
		})
		t.Cleanup(stop)
		cluster := clusterpb.NewClusterServiceClient(connection)
		require.Eventually(t, func() bool {
			state, err := cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})

			return err == nil && state.GetLeader() != 0
		}, 10*time.Second, 10*time.Millisecond)

		return servicepb.NewBucketServiceClient(connection), cluster, stop
	}
	client, cluster, stop := start()
	testserver.WaitForWriteAdmission(t, ctx, client)
	_, err := client.Apply(ctx, request("before-drain"))
	require.NoError(t, err)
	const epoch = "capability-activation"
	begin, err := cluster.BeginDrain(ctx, &clusterpb.BeginDrainRequest{Epoch: epoch})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		drain, err := cluster.GetDrainStatus(ctx, &clusterpb.GetDrainStatusRequest{})

		return err == nil && drain.GetDrainEpoch() == epoch && drain.GetLastPersistedIndex() >= begin.GetCutoffIndex()
	}, 10*time.Second, 10*time.Millisecond)
	stop()

	client, _, stop = start()
	_, err = client.Apply(ctx, request("drained"))
	require.Error(t, err)
	stop()
	require.Equal(t, []uint32{uint32(capabilities.AccountIncarnationWritesV1)}, capabilities.ActiveIDs(readPolicy()))

	client, cluster, stop = start()
	require.Eventually(t, func() bool {
		_, err := cluster.EndDrain(ctx, &clusterpb.EndDrainRequest{Epoch: epoch})

		return err == nil
	}, 10*time.Second, 10*time.Millisecond)
	func() {
		defer stop()
		_, err = client.Apply(ctx, request("after-drain"))
		require.NoError(t, err)
	}()

	client, _, stop = start()
	testserver.WaitForWriteAdmission(t, ctx, client)
	func() {
		defer stop()
		_, err = client.Apply(ctx, request("after-restart"))
		require.NoError(t, err)
	}()
	policy := readPolicy()
	require.Equal(t, []uint32{uint32(capabilities.AccountIncarnationWritesV1)}, capabilities.ActiveIDs(policy))
	require.Equal(t, uint64(1), policy.GetRevision())
	require.Equal(t, uint64(10), policy.GetQueryCheckpointLimit())
}
