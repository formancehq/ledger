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
	"sync/atomic"
	"testing"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// Every scenario below is decided by which assertions the subprocess emitted.
// The no-op SDK never opens ANTITHESIS_SDK_LOCAL_OUTPUT, so an unarmed build
// leaves nothing to decide from; CI runs this module with the tag. The guard
// keeps the positive `assert.Enabled` form the instrumentor recognises.
func requireArmedSDK(t *testing.T) {
	t.Helper()
	if assert.Enabled {
		return
	}
	t.Skip("requires -tags enable_antithesis_sdk: the no-op SDK writes no local output")
}

// A subprocess isolates the SDK's process-wide assertion cache and initializes
// its local output before any assertions run. Every RPC reaches a real server;
// only the observed GetAccount balance is changed in the corruption control.
func TestQuiescenceAgainstServer(t *testing.T) {
	t.Parallel()
	requireArmedSDK(t)
	for _, scenario := range []string{"idle", "divergence", "late_write", "busy", "unavailable", "expired", "ambiguous_barrier"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			outputPath := filepath.Join(dir, "assertions.jsonl")
			resultPath := filepath.Join(dir, "result.json")
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestQuiescenceServerProcess$", "-test.v")
			cmd.Env = append(os.Environ(), "QUIESCENCE_SCENARIO="+scenario, "QUIESCENCE_RESULT="+resultPath, "ANTITHESIS_SDK_LOCAL_OUTPUT="+outputPath)
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
			t.Logf("%s", output)
			var result struct {
				Lists    int64
				Barriers int64
				Writes   int64
				Indices  []uint64
			}
			data, err := os.ReadFile(resultPath)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(data, &result))
			require.GreaterOrEqual(t, len(result.Indices), 4)
			require.Equal(t, result.Indices[0]+1, result.Indices[1])
			require.Equal(t, result.Indices[1]+1, result.Indices[2])
			require.Equal(t, result.Indices[2]+1, result.Indices[3])
			assertions, err := os.ReadFile(outputPath)
			require.NoError(t, err)
			diverged := quiescenceAssertionObserved(t, assertions, "list/get balance divergence persisted past quiescence", false)
			matched := quiescenceAssertionObserved(t, assertions, "list/get balance pair verified matching", true)
			switch scenario {
			case "divergence":
				require.EqualValues(t, 1, result.Lists, "own barriers must not cause repeated balance reads")
				require.EqualValues(t, 2, result.Barriers, "each of the two mismatching accounts uses exactly one barrier")
				require.True(t, diverged, "divergence must be reported while the server is idle")
				require.False(t, matched)
			case "idle":
				require.EqualValues(t, 1, result.Lists)
				require.Zero(t, result.Barriers)
				require.False(t, diverged)
				require.True(t, matched)
			case "late_write":
				require.EqualValues(t, 2, result.Lists, "a real late write must require a complete re-read")
				require.EqualValues(t, 2, result.Barriers)
				require.EqualValues(t, 1, result.Writes)
				require.False(t, diverged)
				require.True(t, matched)
			case "busy":
				require.EqualValues(t, 20, result.Lists, "full comparison attempts must be bounded")
				require.EqualValues(t, 20, result.Writes)
				require.EqualValues(t, 40, result.Barriers)
				require.False(t, diverged)
				require.False(t, matched)
			case "unavailable", "expired":
				require.EqualValues(t, 1, result.Lists)
				wantBarriers := int64(20)
				if scenario == "expired" {
					wantBarriers = 1
				}
				require.Equal(t, wantBarriers, result.Barriers)
				require.False(t, diverged, "failed requalification is inconclusive")
				require.False(t, matched)
				require.False(t, quiescenceAssertionObserved(t, assertions, "barrier returned unexpected error", false))
			case "ambiguous_barrier":
				require.EqualValues(t, 2, result.Lists, "a committed barrier with a lost response must force a fresh observation")
				require.EqualValues(t, 3, result.Barriers)
				require.False(t, diverged)
				require.True(t, matched)
			}
		})
	}
}

func quiescenceAssertionObserved(t *testing.T, data []byte, message string, condition bool) bool {
	t.Helper()
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		var record struct {
			Assertion struct {
				Message   string `json:"message"`
				Condition bool   `json:"condition"`
				Hit       bool   `json:"hit"`
			} `json:"antithesis_assert"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if record.Assertion.Hit && record.Assertion.Message == message && record.Assertion.Condition == condition {
			return true
		}
	}
	return false
}

func TestQuiescenceServerProcess(t *testing.T) {
	scenario := os.Getenv("QUIESCENCE_SCENARIO")
	if scenario == "" {
		t.Skip("subprocess only")
	}
	ctx, client, target := quiescenceTestServer(t)
	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("L", nil), oracletest.TxReqRefL("L", "seed", "world", "users:0", "USD", 10)))
	require.NoError(t, err)
	var indices []uint64
	for range 4 {
		barrier, err := client.Barrier(ctx, &servicepb.BarrierRequest{})
		require.NoError(t, err)
		indices = append(indices, barrier.GetCommitIndex())
	}
	t.Logf("real idle Barrier indices: %v", indices)
	q := waitForQuiescence(ctx, client, 0)
	require.NotZero(t, q)
	var lists, barriers, writes atomic.Int64
	checkCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn, err := grpc.NewClient(target, grpcprotocol.ClientOption(), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			if strings.HasSuffix(method, "/ListAccounts") && lists.Add(1) > 25 {
				cancel() // Safety stop makes an unbounded-recursion mutation fail quickly.
			}
			return streamer(ctx, desc, cc, method, opts...)
		}),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if strings.HasSuffix(method, "/Barrier") {
				attempt := barriers.Add(1)
				if scenario == "unavailable" {
					return status.Error(codes.Unavailable, "injected barrier outage")
				}
				if scenario == "expired" {
					// Exercise an actually expired child RPC deadline without sleeps.
					expired, stop := context.WithDeadline(ctx, time.Now().Add(-time.Second))
					defer stop()
					err := invoker(expired, method, req, reply, cc, opts...)
					cancel()
					return err
				}
				if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
					return err
				}
				if scenario == "ambiguous_barrier" && attempt == 1 {
					return status.Error(codes.Unavailable, "lost response after real barrier commit")
				}
				return nil
			}
			if strings.HasSuffix(method, "/GetAccount") {
				// ListAccounts has fully completed before GetAccount. Apply a real
				// transaction in this interval, once or on every full comparison.
				if (scenario == "late_write" && writes.Load() == 0) || (scenario == "busy" && writes.Load() < lists.Load()) {
					_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", oracletest.TxReqRefL("L", fmt.Sprintf("late-%d", writes.Load()), "world", "users:0", "USD", 1)))
					if err != nil {
						return err
					}
					writes.Add(1)
				}
			}
			if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
				return err
			}
			if strings.HasSuffix(method, "/GetAccount") && (scenario == "divergence" || scenario == "unavailable" || scenario == "expired" || (scenario == "ambiguous_barrier" && lists.Load() == 1)) {
				account := reply.(*commonpb.Account)
				require.NotEmpty(t, account.Volumes)
				account.Volumes[0].Volumes.Balance = "999"
			}
			return nil
		}))
	require.NoError(t, err)
	defer conn.Close()
	checkVolumesConsistent(checkCtx, servicepb.NewBucketServiceClient(conn), "L", q)
	// Server state remains correct even when its observed response was changed.
	account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: "L", Address: "users:0"})
	require.NoError(t, err)
	require.Equal(t, fmt.Sprint(10+writes.Load()), account.FindVolume("USD", "").GetBalance())
	result, err := json.Marshal(struct {
		Lists    int64
		Barriers int64
		Writes   int64
		Indices  []uint64
	}{lists.Load(), barriers.Load(), writes.Load(), indices})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(os.Getenv("QUIESCENCE_RESULT"), result, 0600))
	t.Logf("result: %s", result)
}

func quiescenceTestServer(t *testing.T) (context.Context, servicepb.BucketServiceClient, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	lease := testserver.AllocateNodeLease()
	instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{NodeID: 1, ClusterID: "quiescence", Ports: lease.Ports(), WalDir: t.TempDir(), DataDir: t.TempDir(), Output: io.Discard})
	instruments = append(instruments, testserver.WithBootstrap())
	server := lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
	require.NoError(t, server.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		require.NoError(t, server.Stop(stopCtx))
	})
	target := fmt.Sprintf("localhost:%d", lease.Ports().GRPC())
	conn, err := grpc.NewClient(target, grpcprotocol.ClientOption(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	cluster := clusterpb.NewClusterServiceClient(conn)
	require.Eventually(t, func() bool {
		state, err := cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		return err == nil && state.GetLeader() != 0
	}, 5*time.Second, 10*time.Millisecond)
	return ctx, servicepb.NewBucketServiceClient(conn), target
}
