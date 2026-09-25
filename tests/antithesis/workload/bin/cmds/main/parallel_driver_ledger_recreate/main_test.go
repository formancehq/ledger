package main

import (
	"bufio"
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
	workloadinternal "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// Capture actual SDK evaluations in a child: SDK output is opened during init
// and outcomes are deduplicated for the lifetime of a process.
func TestLedgerDeletionScenarioContract(t *testing.T) {
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk for SDK emission")
	}
	if mode := os.Getenv("LEDGER_DELETION_SCENARIO_CHILD"); mode != "" {
		var injected atomic.Bool
		ctx, client := deletionTestServer(t, scenarioInterceptor(t, mode, &injected), scenarioStreams(mode, &injected))
		runScenario(ctx, client, 174236, 3)
		if mode != "healthy" {
			require.True(t, injected.Load(), "sensitivity trigger was not exercised: %s", mode)
		}
		if mode == "healthy" || mode == "stream-recv" || mode == "cleanup-failure" {
			_, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: fmt.Sprintf("lrecreate-other-%016x", uint64(174236))})
			if mode == "cleanup-failure" {
				require.NoError(t, err, "failed cleanup must leave the isolation ledger visible")
			} else {
				require.Equal(t, codes.NotFound, status.Code(err), "isolation ledger must be cleaned up on success and early return")
			}
		}
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("probe-tombstone", actions.CreateLedgerAction("lrecreate-174236", nil)))
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.True(t, workloadinternal.IsLedgerDeleted(err), "%v", err)
		t.Logf("real-server same-name create: %v; reason=%s", err, workloadinternal.ErrorReason(err))
		return
	}
	t.Parallel()
	cases := []struct{ mode, failure string }{
		{"healthy", ""},
		{"recreate-before", "deleted ledger name remains permanently reserved"},
		{"recreate-after", "deleted ledger name remains permanently reserved"},
		{"deleted-read", "deleted ledger is hidden from ledger reads"},
		{"deleted-transaction-point", "deleted ledger hides predecessor transaction point reads"},
		{"deleted-account-point", "deleted ledger hides predecessor account point reads"},
		{"deleted-transactions-row", "deleted ledger exposes no predecessor transactions"},
		{"deleted-transactions-eof", "deleted ledger exposes no predecessor transactions"},
		{"deleted-accounts-row", "deleted ledger exposes no predecessor accounts"},
		{"deleted-accounts-eof", "deleted ledger exposes no predecessor accounts"},
		{"missing-marker-log", "ledger deletion acknowledged transaction includes its created log"},
		{"deleted-write", "deleted ledger rejects new transaction writes"},
		{"reference-leak", "other ledger never exposes predecessor transactions"},
		{"account-leak", "other ledger never exposes predecessor account activity"},
		{"reuse-conflict", "predecessor references are reusable in another ledger"},
		{"reuse-permanent", "predecessor references are reusable in another ledger"},
		{"reuse-unavailable", ""},
		{"reuse-deadline", ""},
		{"reuse-canceled", ""},
		{"reuse-context-deadline", ""},
		{"stream-initial", "ledger deletion scenario has no unexpected operation errors"},
		{"stream-recv", "ledger deletion scenario has no unexpected operation errors"},
		{"ambiguous-delete", ""},
		{"cleanup-failure", ""},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			observations := runScenarioProcess(t, tc.mode)
			if tc.failure != "" {
				require.Contains(t, observations[tc.failure], false, "injected violation must fail its exact SDK property")
				if tc.mode == "recreate-after" {
					require.Equal(t, []bool{true, false}, observations[tc.failure], "a later violation must remain visible after the initial passing probe")
				}
				if tc.mode == "recreate-before" {
					require.Equal(t, []bool{false}, observations[tc.failure])
				}
				if tc.mode == "reuse-conflict" || tc.mode == "reuse-permanent" {
					require.Equal(t, []bool{false}, observations["predecessor reference accepted by another ledger"])
					require.Equal(t, []bool{false}, observations[tc.failure])
				}
				return
			}
			if tc.mode == "reuse-unavailable" || tc.mode == "reuse-deadline" || tc.mode == "reuse-canceled" || tc.mode == "reuse-context-deadline" {
				require.Contains(t, observations["other ledger never exposes predecessor account activity"], true)
				require.NotContains(t, observations, "predecessor reference accepted by another ledger", "inconclusive reuse must not be observed as accepted or rejected")
				require.NotContains(t, observations, "predecessor references are reusable in another ledger")
				return
			}
			if tc.mode == "ambiguous-delete" {
				require.NotContains(t, observations, "deleted ledger name remains permanently reserved")
				require.NotContains(t, observations, "other ledger never exposes predecessor transactions")
				require.NotContains(t, observations, "predecessor reference accepted by another ledger")
				return
			}
			for _, message := range []string{
				"deleted ledger name remains permanently reserved",
				"deleted ledger is hidden from ledger reads",
				"deleted ledger hides predecessor transaction point reads",
				"deleted ledger hides predecessor account point reads",
				"deleted ledger exposes no predecessor transactions",
				"deleted ledger exposes no predecessor accounts",
				"deleted ledger rejects new transaction writes",
				"other ledger never exposes predecessor transactions",
				"other ledger never exposes predecessor account activity",
				"predecessor references are reusable in another ledger",
				"predecessor reference accepted by another ledger",
				"ledger deletion scenario has no unexpected operation errors",
				"ledger deletion acknowledged transaction includes its created log",
			} {
				require.Contains(t, observations[message], true, "scenario did not reach a passing oracle: %s", message)
				require.NotContains(t, observations[message], false, "healthy scenario failed: %s", message)
			}
		})
	}
}

func runScenarioProcess(t *testing.T, mode string) map[string][]bool {
	t.Helper()
	output := filepath.Join(t.TempDir(), "assertions.jsonl")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLedgerDeletionScenarioContract$", "-test.v")
	cmd.Env = append(os.Environ(), "LEDGER_DELETION_SCENARIO_CHILD="+mode, "ANTITHESIS_SDK_LOCAL_OUTPUT="+output)
	result, err := cmd.CombinedOutput()
	t.Logf("%s", result)
	require.NoError(t, err)
	if mode == "cleanup-failure" {
		require.Contains(t, string(result), "cleanup: delete isolation ledger failed (transient, expected under faults)")
	}
	f, err := os.Open(output)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	observed := map[string][]bool{}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		var event struct {
			Assertion *struct {
				Message   string `json:"message"`
				Condition bool   `json:"condition"`
				Hit       bool   `json:"hit"`
			} `json:"antithesis_assert"`
		}
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &event))
		if a := event.Assertion; a != nil && a.Hit {
			observed[a.Message] = append(observed[a.Message], a.Condition)
		}
	}
	require.NoError(t, scanner.Err())
	return observed
}

// Intercept only the selected response or add real ledger state at a precise
// stage. All setup, deletion, reads and reference conflicts use the real service.
func scenarioInterceptor(t *testing.T, mode string, injected *atomic.Bool) grpc.UnaryClientInterceptor {
	var predecessor *commonpb.Transaction
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if mode == "deleted-read" && strings.HasSuffix(method, "/GetLedger") {
			injected.Store(true)
			return nil
		}
		if mode == "deleted-transaction-point" && strings.HasSuffix(method, "/GetTransaction") {
			require.NotNil(t, predecessor)
			read := req.(*servicepb.GetTransactionRequest)
			require.Equal(t, "lrecreate-174236", read.GetLedger())
			require.Equal(t, predecessor.GetId(), read.GetTransactionId())
			reply.(*servicepb.GetTransactionResponse).Transaction = predecessor
			injected.Store(true)
			return nil
		}
		if mode == "deleted-account-point" && strings.HasSuffix(method, "/GetAccount") {
			read := req.(*servicepb.GetAccountRequest)
			require.Equal(t, "lrecreate-174236", read.GetLedger())
			require.Equal(t, "lrec-old:174236:0", read.GetAddress())
			reply.(*commonpb.Account).Address = read.GetAddress()
			injected.Store(true)
			return nil
		}
		apply, ok := req.(*servicepb.ApplyRequest)
		if !ok {
			return invoke(ctx, method, req, reply, cc, opts...)
		}
		key := apply.GetUnsigned().GetIdempotencyKey()
		if mode == "cleanup-failure" && strings.HasSuffix(key, "-cleanup-other") {
			injected.Store(true)
			return status.Error(codes.Unavailable, "injected cleanup failure")
		}
		if (mode == "recreate-before" || mode == "recreate-after" || mode == "deleted-write") && strings.HasSuffix(key, "-"+mode) {
			injected.Store(true)
			return nil
		}
		if strings.HasSuffix(key, "-reuse") {
			switch mode {
			case "reuse-unavailable", "reuse-deadline", "reuse-canceled", "reuse-context-deadline":
				injected.Store(true)
				switch mode {
				case "reuse-unavailable":
					return status.Error(codes.Unavailable, "injected unavailable reuse")
				case "reuse-deadline":
					return status.Error(codes.DeadlineExceeded, "injected ambiguous reuse")
				case "reuse-canceled":
					return context.Canceled
				default:
					return fmt.Errorf("injected local reuse timeout: %w", context.DeadlineExceeded)
				}
			}
			if mode == "reuse-permanent" {
				injected.Store(true)
				return status.Error(codes.InvalidArgument, "injected permanent reuse failure")
			}
			if mode == "reuse-conflict" {
				_, err := createTx(ctx, servicepb.NewBucketServiceClient(cc), "inject-conflict", fmt.Sprintf("lrecreate-other-%016x", uint64(174236)), "lrec-174236-0", "conflict-writer")
				require.NoError(t, err)
				injected.Store(true)
			}
		}
		err := invoke(ctx, method, req, reply, cc, opts...)
		if err != nil {
			return err
		}
		if strings.HasSuffix(key, "-seed-0") {
			predecessor = workloadinternal.ExtractCreatedTransaction(reply.(*servicepb.ApplyResponse)).GetTransaction()
		}
		if mode == "missing-marker-log" && strings.HasSuffix(key, "-marker") {
			reply.(*servicepb.ApplyResponse).Logs = nil
			injected.Store(true)
		}
		if mode == "ambiguous-delete" && strings.HasSuffix(key, "-delete") {
			injected.Store(true)
			return status.Error(codes.DeadlineExceeded, "delete committed but response lost")
		}
		if strings.HasSuffix(key, "-marker") && (mode == "reference-leak" || mode == "account-leak") {
			ref, account := "unrelated-ref", "lrec-old:174236:0"
			if mode == "reference-leak" {
				ref, account = "lrec-174236-0", "unrelated-account"
			}
			_, err := createTx(ctx, servicepb.NewBucketServiceClient(cc), "inject-leak", fmt.Sprintf("lrecreate-other-%016x", uint64(174236)), ref, account)
			require.NoError(t, err)
			injected.Store(true)
		}
		return nil
	}
}

func scenarioStreams(mode string, injected *atomic.Bool) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}
		return &scenarioStream{ClientStream: stream, mode: mode, injected: injected}, nil
	}
}

type scenarioStream struct {
	grpc.ClientStream
	mode                            string
	injected                        *atomic.Bool
	target, deletedTarget, received bool
}

func (s *scenarioStream) SendMsg(message any) error {
	if req, ok := message.(*servicepb.ListTransactionsRequest); ok {
		s.target = strings.HasPrefix(req.GetLedger(), "lrecreate-other-")
		s.deletedTarget = req.GetLedger() == "lrecreate-174236" && strings.HasPrefix(s.mode, "deleted-transactions-")
	}
	if req, ok := message.(*servicepb.ListAccountsRequest); ok {
		s.deletedTarget = req.GetLedger() == "lrecreate-174236" && strings.HasPrefix(s.mode, "deleted-accounts-")
	}
	if s.target && s.mode == "stream-initial" {
		s.injected.Store(true)
		return status.Error(codes.FailedPrecondition, "injected initial stream failure")
	}
	return s.ClientStream.SendMsg(message)
}

func (s *scenarioStream) RecvMsg(message any) error {
	if s.deletedTarget {
		s.injected.Store(true)
		if strings.HasSuffix(s.mode, "-eof") {
			return io.EOF
		}
		switch row := message.(type) {
		case *commonpb.Transaction:
			row.Reference = "lrec-174236-0"
		case *commonpb.Account:
			row.Address = "lrec-old:174236:0"
		}
		return nil
	}
	if s.target && s.received && s.mode == "stream-recv" {
		s.injected.Store(true)
		return status.Error(codes.Internal, "injected failure after valid row")
	}
	err := s.ClientStream.RecvMsg(message)
	if err == nil {
		s.received = true
	}
	return err
}

func deletionTestServer(t *testing.T, unary grpc.UnaryClientInterceptor, stream grpc.StreamClientInterceptor) (context.Context, servicepb.BucketServiceClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	lease := testserver.AllocateNodeLease()
	instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{NodeID: 1, ClusterID: "deletion-workload", Ports: lease.Ports(), WalDir: t.TempDir(), DataDir: t.TempDir(), Output: io.Discard})
	instruments = append(instruments, testserver.WithBootstrap(), testserver.WithSentinelMode())
	server := lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
	require.NoError(t, server.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		require.NoError(t, server.Stop(stopCtx))
	})
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", lease.Ports().GRPC()), grpcprotocol.ClientOption(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(unary), grpc.WithStreamInterceptor(stream))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	cluster := clusterpb.NewClusterServiceClient(conn)
	require.Eventually(t, func() bool {
		state, err := cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		return err == nil && state.GetLeader() != 0
	}, 5*time.Second, 10*time.Millisecond)
	return ctx, servicepb.NewBucketServiceClient(conn)
}
