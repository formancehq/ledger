package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	workloadinternal "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// This runs the actual driver through the workload's automatic retries. A
// forwarding server loses one response only AFTER the real Ledger committed it.
// The isolated ledger makes the generated references fresh regardless of the
// random draw; the fault schedule depends only on logical steps, never timing.
func TestReferenceConflictSkipLostResponse(t *testing.T) {
	// NewGRPCConn reads environment variables, so these cases cannot be parallel.
	for _, lostStep := range []string{"fresh", "first", "duplicate", "none"} {
		t.Run(lostStep, func(t *testing.T) {
			ctx, backend := referenceTestServer(t)
			const ledger = "reference-retry"
			_, err := backend.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))
			require.NoError(t, err)
			proxy := &lostResponseServer{backend: backend, lostStep: lostStep, attempts: make(map[string][]applyAttempt)}
			client := retryingClient(t, proxy)
			run(ctx, client, ledger)
			attempts := proxy.snapshot()
			for step, calls := range attempts {
				for _, call := range calls {
					require.NoError(t, call.err, step)
				}
			}
			require.Len(t, attempts, 3, "driver must complete all three logical operations")
			assertTransactions(t, ctx, backend, ledger, attempts)

			for _, step := range []string{"first", "duplicate", "fresh"} {
				calls := attempts[step]
				wantAttempts := 1
				if step == lostStep {
					wantAttempts = 2
				}
				require.Len(t, calls, wantAttempts, step)
				for _, call := range calls {
					require.True(t, proto.Equal(calls[0].request, call.request), "retry must preserve the entire %s request", step)
				}
				require.Equal(t, step == lostStep, calls[0].lostResponse, "exactly one response must be lost at the selected step")
				if len(calls) == 2 {
					require.False(t, calls[1].lostResponse, "the retry must deliver its result")
				}
				original, delivered := calls[0].response, calls[len(calls)-1].response
				t.Logf("step=%s attempts=%d originalSequence=%d deliveredSequence=%d original=%T delivered=%T", step, len(calls), original.GetLogs()[0].GetSequence(), delivered.GetLogs()[0].GetSequence(), applyPayload(t, original).GetPayload(), applyPayload(t, delivered).GetPayload())
				if step == "duplicate" {
					skipped := applyPayload(t, delivered).GetOrderSkipped()
					require.NotNil(t, skipped)
					require.Equal(t, commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT, skipped.GetReason())
					firstID := applyPayload(t, attempts["first"][0].response).GetCreatedTransaction().GetTransaction().GetId()
					require.Equal(t, strconv.FormatUint(firstID, 10), skipped.GetContext()["existingTransactionId"])
					require.Equal(t, ledger, skipped.GetContext()["ledger"])
					require.Equal(t, calls[0].request.GetUnsigned().GetRequests()[0].GetApply().GetAction().GetCreateTransaction().GetReference(), skipped.GetContext()["reference"])
				} else {
					require.NotNil(t, applyPayload(t, original).GetCreatedTransaction(), "fault must follow a committed creation")
					if skipped := applyPayload(t, delivered).GetOrderSkipped(); skipped != nil {
						created := applyPayload(t, original).GetCreatedTransaction().GetTransaction()
						require.Equal(t, commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT, skipped.GetReason())
						require.Equal(t, created.GetReference(), skipped.GetContext()["reference"])
						require.Equal(t, ledger, skipped.GetContext()["ledger"])
						require.Equal(t, strconv.FormatUint(created.GetId(), 10), skipped.GetContext()["existingTransactionId"], "retry skipped its own committed transaction")
					}
					require.NotNil(t, applyPayload(t, delivered).GetCreatedTransaction(), "skip-tolerant first-claim on a fresh reference must NOT fire the skip: step=%s, response=%v", step, delivered)
				}
				require.True(t, proto.Equal(original, delivered), "%s must replay the original outcome, including transaction, postings, and log sequence", step)
			}
			keys := make(map[string]bool)
			for _, step := range []string{"first", "duplicate", "fresh"} {
				calls := attempts[step]
				key := calls[0].request.GetUnsigned().GetIdempotencyKey()
				require.NotEmpty(t, key, step)
				require.False(t, keys[key], "each logical step needs a different key")
				keys[key] = true
			}
			assertLogCount(t, ctx, backend, ledger, 3)
		})
	}
}

// Clearing just the fresh step's key preserves the old wire behavior. The
// resulting skip is legitimate: it points to the successful response we lost,
// and no second transaction exists. This control must survive the driver fix.
func TestReferenceConflictSkipUnkeyedControl(t *testing.T) {
	// SDK output is configured when the process initializes. A child process
	// captures the real assertion as well as checking the transport/state below.
	if os.Getenv("LEDGER_TEST_UNKEYED_REFERENCE_CHILD") != "1" {
		outputPath := filepath.Join(t.TempDir(), "assertions.jsonl")
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReferenceConflictSkipUnkeyedControl$", "-test.v")
		cmd.Env = append(os.Environ(), "LEDGER_TEST_UNKEYED_REFERENCE_CHILD=1", "ANTITHESIS_SDK_LOCAL_OUTPUT="+outputPath)
		output, err := cmd.CombinedOutput()
		t.Logf("%s", output)
		require.NoError(t, err)
		data, err := os.ReadFile(outputPath)
		require.NoError(t, err)
		hits := 0
		for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
			var event struct {
				Assertion struct {
					Message   string         `json:"message"`
					Hit       bool           `json:"hit"`
					Condition bool           `json:"condition"`
					Details   map[string]any `json:"details"`
				} `json:"antithesis_assert"`
			}
			require.NoError(t, json.Unmarshal(line, &event))
			if event.Assertion.Hit && event.Assertion.Message == "skip-tolerant first-claim on a fresh reference must NOT fire the skip" {
				hits++
				require.False(t, event.Assertion.Condition)
				require.Equal(t, "ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT", event.Assertion.Details["skipReason"])
				require.NotEmpty(t, event.Assertion.Details["idempotencyKey"])
				require.NotEmpty(t, event.Assertion.Details["skipContext"])
				require.NotEmpty(t, event.Assertion.Details["logSequence"])
			}
		}
		require.Equal(t, 1, hits, "driver must still report an unexpected fresh-reference skip")
		return
	}

	ctx, backend := referenceTestServer(t)
	const ledger = "reference-unkeyed"
	_, err := backend.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))
	require.NoError(t, err)
	proxy := &lostResponseServer{backend: backend, lostStep: "fresh", unkeyedFresh: true, attempts: make(map[string][]applyAttempt)}
	run(ctx, retryingClient(t, proxy), ledger)
	attempts := proxy.snapshot()
	require.Len(t, attempts, 3)
	calls := attempts["fresh"]
	require.Len(t, calls, 2)
	require.True(t, calls[0].lostResponse)
	require.False(t, calls[1].lostResponse)
	for _, call := range calls {
		require.NoError(t, call.err)
		require.Empty(t, call.request.GetUnsigned().GetIdempotencyKey())
	}
	require.True(t, proto.Equal(calls[0].request, calls[1].request))
	created := applyPayload(t, calls[0].response).GetCreatedTransaction().GetTransaction()
	require.NotNil(t, created)
	skipped := applyPayload(t, calls[1].response).GetOrderSkipped()
	require.NotNil(t, skipped)
	require.Equal(t, commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT, skipped.GetReason())
	require.Equal(t, created.GetReference(), skipped.GetContext()["reference"])
	require.Equal(t, ledger, skipped.GetContext()["ledger"])
	require.Equal(t, strconv.FormatUint(created.GetId(), 10), skipped.GetContext()["existingTransactionId"])
	require.Greater(t, calls[1].response.GetLogs()[0].GetSequence(), calls[0].response.GetLogs()[0].GetSequence())
	assertTransactions(t, ctx, backend, ledger, attempts)
	assertLogCount(t, ctx, backend, ledger, 4)
	t.Log("lost one committed response as Unavailable; unkeyed retry legally skipped its own transaction")
}

type applyAttempt struct {
	request      *servicepb.ApplyRequest
	response     *servicepb.ApplyResponse
	err          error
	lostResponse bool
}

// This is a transport fault injector, not a mock of Ledger business semantics.
type lostResponseServer struct {
	servicepb.UnimplementedBucketServiceServer
	backend        servicepb.BucketServiceClient
	lostStep       string
	unkeyedFresh   bool
	mu             sync.Mutex
	firstReference string
	attempts       map[string][]applyAttempt
}

func (s *lostResponseServer) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	apply := req.GetUnsigned().GetRequests()[0].GetApply()
	ref := apply.GetAction().GetCreateTransaction().GetReference()
	step := "fresh"
	if len(apply.GetSkippableReasons()) == 0 {
		step = "first"
		s.firstReference = ref
	} else if ref == s.firstReference {
		step = "duplicate"
	}
	if s.unkeyedFresh && step == "fresh" {
		req = proto.Clone(req).(*servicepb.ApplyRequest)
		req.GetUnsigned().IdempotencyKey = ""
	}
	resp, err := s.backend.Apply(ctx, req)
	entry := applyAttempt{request: proto.Clone(req).(*servicepb.ApplyRequest), err: err}
	if resp != nil {
		entry.response = proto.Clone(resp).(*servicepb.ApplyResponse)
	}
	entry.lostResponse = err == nil && step == s.lostStep && len(s.attempts[step]) == 0
	s.attempts[step] = append(s.attempts[step], entry)
	if entry.lostResponse {
		return nil, status.Error(codes.Unavailable, "injected response loss after successful commit")
	}
	return resp, err
}

func (s *lostResponseServer) snapshot() map[string][]applyAttempt {
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := make(map[string][]applyAttempt, len(s.attempts))
	for step, attempts := range s.attempts {
		ret[step] = append([]applyAttempt(nil), attempts...)
	}
	return ret
}

func retryingClient(t *testing.T, proxy *lostResponseServer) servicepb.BucketServiceClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	servicepb.RegisterBucketServiceServer(server, proxy)
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-finished)
	})
	t.Setenv("LEDGER_GRPC_ADDR", listener.Addr().String())
	t.Setenv("LEDGER_NO_RETRY", "")
	t.Setenv("LEDGER_RETRY_FOREVER", "")
	conn, err := workloadinternal.NewGRPCConn()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return servicepb.NewBucketServiceClient(conn)
}

func referenceTestServer(t *testing.T) (context.Context, servicepb.BucketServiceClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	lease := testserver.AllocateNodeLease()
	instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{NodeID: 1, ClusterID: "reference-retry", Ports: lease.Ports(), WalDir: t.TempDir(), DataDir: t.TempDir(), Output: io.Discard})
	instruments = append(instruments, testserver.WithBootstrap())
	server := lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
	require.NoError(t, server.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		require.NoError(t, server.Stop(stopCtx))
	})
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", lease.Ports().GRPC()), grpcprotocol.ClientOption(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	cluster := clusterpb.NewClusterServiceClient(conn)
	require.Eventually(t, func() bool {
		state, err := cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		return err == nil && state.GetLeader() != 0
	}, 5*time.Second, 10*time.Millisecond)
	return ctx, servicepb.NewBucketServiceClient(conn)
}

func applyPayload(t *testing.T, resp *servicepb.ApplyResponse) *commonpb.LedgerLogPayload {
	t.Helper()
	require.Len(t, resp.GetLogs(), 1, "Apply must return exactly one log")
	return resp.GetLogs()[0].GetPayload().GetApply().GetLog().GetData()
}

func assertTransactions(t *testing.T, ctx context.Context, client servicepb.BucketServiceClient, ledger string, attempts map[string][]applyAttempt) {
	t.Helper()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{Ledger: ledger})
	require.NoError(t, err)
	byReference := make(map[string][]*commonpb.Transaction)
	for {
		tx, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		byReference[tx.GetReference()] = append(byReference[tx.GetReference()], tx)
	}
	require.Empty(t, stream.Trailer().Get("x-next-cursor"), "private ledger must fit on one page")
	require.Len(t, byReference, 2)
	for _, step := range []string{"first", "fresh"} {
		created := applyPayload(t, attempts[step][0].response).GetCreatedTransaction().GetTransaction()
		require.NotNil(t, created)
		rows := byReference[created.GetReference()]
		require.Len(t, rows, 1, "one committed transaction per reference")
		require.Equal(t, created.GetId(), rows[0].GetId())
		t.Logf("reference=%s committedTransactions=%d transactionID=%d", created.GetReference(), len(rows), rows[0].GetId())
	}
}

func assertLogCount(t *testing.T, ctx context.Context, client servicepb.BucketServiceClient, ledger string, want int) {
	t.Helper()
	stream, err := client.ListLogs(metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable"), &servicepb.ListLogsRequest{Ledger: ledger})
	require.NoError(t, err)
	count := 0
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		count++
	}
	require.Empty(t, stream.Trailer().Get("x-next-cursor"))
	require.Equal(t, want, count, "replays must not append another business log")
}
