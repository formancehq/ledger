package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// The subprocess sets SDK output before package initialization and runs the
// actual entry point, including the workload's normal client interceptors.
func TestQueryCheckpointDriverProcess(t *testing.T) {
	if os.Getenv("CHECKPOINT_DRIVER_TEST_PROCESS") != "1" {
		return
	}
	if os.Getenv("CHECKPOINT_DRIVER_CANCEL_BEFORE_LIST") == "1" {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		conn, err := grpc.NewClient(os.Getenv("LEDGER_GRPC_ADDR"), grpcprotocol.ClientOption(), grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(func(callCtx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if method == clusterpb.ClusterService_ListQueryCheckpoints_FullMethodName {
					cancel()
					return status.FromContextError(ctx.Err()).Err()
				}
				return invoke(callCtx, method, req, reply, cc, opts...)
			}))
		require.NoError(t, err)
		defer conn.Close()
		runQueryCheckpointDriver(ctx, clusterpb.NewClusterServiceClient(conn), servicepb.NewBucketServiceClient(conn))
		return
	}
	main()
}

type sdkAssertion struct {
	Message     string         `json:"message"`
	DisplayType string         `json:"display_type"`
	Condition   bool           `json:"condition"`
	Hit         bool           `json:"hit"`
	Details     map[string]any `json:"details"`
}

func runCheckpointDriver(t *testing.T, address string, extraEnv ...string) []sdkAssertion {
	t.Helper()
	outputPath := filepath.Join(t.TempDir(), "sdk.jsonl")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestQueryCheckpointDriverProcess$")
	for _, env := range os.Environ() {
		name, _, _ := strings.Cut(env, "=")
		switch name {
		case "CHECKPOINT_DRIVER_TEST_PROCESS", "CHECKPOINT_DRIVER_CANCEL_BEFORE_LIST", "ANTITHESIS_SDK_LOCAL_OUTPUT", "LEDGER_GRPC_ADDR", "LEDGER_NO_RETRY", "LEDGER_RETRY_FOREVER":
			continue
		}
		cmd.Env = append(cmd.Env, env)
	}
	cmd.Env = append(cmd.Env, "CHECKPOINT_DRIVER_TEST_PROCESS=1", "LEDGER_GRPC_ADDR="+address, "ANTITHESIS_SDK_LOCAL_OUTPUT="+outputPath)
	cmd.Env = append(cmd.Env, extraEnv...)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "driver output: %s", output)
	data, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	t.Logf("driver SDK output:\n%s", data)
	var assertions []sdkAssertion
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	for {
		var event struct {
			Assertion *sdkAssertion `json:"antithesis_assert"`
		}
		err := decoder.Decode(&event)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if event.Assertion != nil && event.Assertion.Hit {
			assertions = append(assertions, *event.Assertion)
		}
	}
	require.NotEmpty(t, assertions, "the real driver must emit SDK events")
	return assertions
}

func requireNoCheckpointFindings(t *testing.T, assertions []sdkAssertion) {
	t.Helper()
	for _, assertion := range assertions {
		require.NotEqual(t, "Unreachable", assertion.DisplayType, "%s: %v", assertion.Message, assertion.Details)
		if assertion.DisplayType == "Always" || assertion.DisplayType == "AlwaysOrUnreachable" {
			require.True(t, assertion.Condition, "%s: %v", assertion.Message, assertion.Details)
		}
	}
}

func requireCheckpointEvent(t *testing.T, assertions []sdkAssertion, message string) {
	t.Helper()
	for _, assertion := range assertions {
		if assertion.Message == message && assertion.Condition {
			return
		}
	}
	t.Fatalf("missing successful driver event %q", message)
}

func checkpointTestServer(t *testing.T) (context.Context, string, servicepb.BucketServiceClient, clusterpb.ClusterServiceClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	t.Cleanup(cancel)
	lease := testserver.AllocateNodeLease()
	instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
		NodeID: 1, ClusterID: "checkpoint-driver-capacity", Ports: lease.Ports(),
		WalDir: t.TempDir(), DataDir: t.TempDir(), Output: io.Discard,
	})
	instruments = append(instruments, testserver.WithBootstrap(), testservice.InstrumentationFunc(func(_ context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--query-checkpoint-limit", "10")
		return nil
	}))
	server := lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
	require.NoError(t, server.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		require.NoError(t, server.Stop(stopCtx))
	})
	address := fmt.Sprintf("localhost:%d", lease.Ports().GRPC())
	conn, err := grpc.NewClient(address, grpcprotocol.ClientOption(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	cluster := clusterpb.NewClusterServiceClient(conn)
	client := servicepb.NewBucketServiceClient(conn)
	require.Eventually(t, func() bool {
		state, err := cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		return err == nil && state.GetLeader() != 0
	}, 5*time.Second, 10*time.Millisecond)
	_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("checkpoint-driver", nil)))
	require.NoError(t, err)
	return ctx, address, client, cluster
}

func TestQueryCheckpointDriverCapacityAgainstServer(t *testing.T) {
	t.Parallel()
	ctx, address, client, cluster := checkpointTestServer(t)
	ids := make([]uint64, 0, 10)
	for range 10 {
		id, _, err := actions.CreateQueryCheckpoint(ctx, client)
		require.NoError(t, err)
		require.NotContains(t, ids, id)
		ids = append(ids, id)
	}
	liveIDs := func() []uint64 {
		list, err := cluster.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
		require.NoError(t, err)
		var result []uint64
		for _, checkpoint := range list.GetCheckpoints() {
			result = append(result, checkpoint.GetCheckpointId())
		}
		return result
	}
	require.ElementsMatch(t, ids, liveIDs())
	_, _, err := actions.CreateQueryCheckpoint(ctx, client)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	info := actions.ExtractGRPCErrorInfo(err)
	require.NotNil(t, info)
	require.Equal(t, domain.ErrReasonCheckpointLimitReached, info.GetReason())
	require.Equal(t, "10", info.GetMetadata()["limit"])
	t.Logf("ten acknowledged checkpoints=%v; eleventh create=%v; ErrorInfo=%v", ids, err, info)
	require.ElementsMatch(t, ids, liveIDs())

	assertions := runCheckpointDriver(t, address)
	requireNoCheckpointFindings(t, assertions)
	requireCheckpointEvent(t, assertions, "query checkpoint capacity reached")
	require.ElementsMatch(t, ids, liveIDs(), "capacity handling must not evict another invocation's checkpoints")

	require.NoError(t, actions.DeleteQueryCheckpoint(ctx, client, ids[0]))
	assertions = runCheckpointDriver(t, address)
	requireNoCheckpointFindings(t, assertions)
	requireCheckpointEvent(t, assertions, "query checkpoint lifecycle completed")
	require.ElementsMatch(t, ids[1:], liveIDs(), "the driver must release its own checkpoint and preserve the others")
}
