package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/publicauditpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAuditDriverStream(t *testing.T) {
	t.Parallel()

	// The real server sanitizes raft: stopped to this Unknown status. Keep
	// the correlation ID so a receive failure remains diagnosable in the SDK output.
	raftStopped := status.Error(codes.Unknown, "unknown server error (correlation ID: 335f07114da38bec)")
	for _, tc := range []struct {
		name       string
		entries    int
		terminal   error
		unexpected bool
		cancelMode string
	}{
		{name: "unknown_before_entries", terminal: raftStopped, unexpected: true},
		{name: "unknown_after_entry", entries: 1, terminal: raftStopped, unexpected: true},
		{name: "internal_before_entries", terminal: status.Error(codes.Internal, "read failed"), unexpected: true},
		{name: "invalid_argument_after_entry", entries: 1, terminal: status.Error(codes.InvalidArgument, "bad filter"), unexpected: true},
		{name: "canceled_after_entry", entries: 1, terminal: status.Error(codes.Canceled, "peer connection closed"), unexpected: true},
		{name: "local_cancel_before_entries", cancelMode: "before_receive"},
		{name: "local_cancel_after_entry", entries: 1, cancelMode: "before_receive"},
		{name: "unknown_with_canceled_context_before_entries", terminal: raftStopped, unexpected: true, cancelMode: "after_error"},
		{name: "unknown_with_canceled_context_after_entry", entries: 1, terminal: raftStopped, unexpected: true, cancelMode: "after_error"},
		{name: "unavailable_before_entries", terminal: status.Error(codes.Unavailable, "leader unavailable")},
		{name: "unavailable_after_entry", entries: 1, terminal: status.Error(codes.Unavailable, "leader unavailable")},
		{name: "deadline_after_entry", entries: 1, terminal: status.Error(codes.DeadlineExceeded, "read deadline")},
		{name: "empty_clean_eof"},
		{name: "one_entry_clean_eof", entries: 1},
		{name: "two_entries_clean_eof", entries: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := &auditStreamServer{entries: tc.entries, terminal: tc.terminal, waitForCancel: tc.cancelMode == "before_receive"}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			servicepb.RegisterBucketServiceServer(server, fixture)
			serverDone := make(chan error, 1)
			go func() { serverDone <- server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				require.NoError(t, <-serverDone)
			})

			// SDK output is initialized at process startup and assertions are
			// deduplicated globally, so each case runs in a fresh process. Cancellation
			// cases call main's cycle with a controllable context; other cases run main.
			outputPath := filepath.Join(t.TempDir(), "assertions.jsonl")
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAuditDriverChild$")
			for _, env := range os.Environ() {
				if strings.HasPrefix(env, "LEDGER_") || strings.HasPrefix(env, "ANTITHESIS_") {
					continue
				}
				cmd.Env = append(cmd.Env, env)
			}
			cmd.Env = append(cmd.Env,
				"LEDGER_TEST_AUDIT_CHILD=1",
				"LEDGER_GRPC_ADDR="+listener.Addr().String(),
				"LEDGER_NO_RETRY=1",
				"ANTITHESIS_SDK_LOCAL_OUTPUT="+outputPath,
				"LEDGER_TEST_AUDIT_CANCEL="+tc.cancelMode,
				"LEDGER_TEST_AUDIT_ENTRIES="+strconv.Itoa(tc.entries),
			)
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "driver process: %s", output)
			require.EqualValues(t, 1, fixture.applyCalls.Load(), "must acknowledge the transaction")
			require.EqualValues(t, 1, fixture.auditCalls.Load(), "must exercise the audit stream exactly once")
			require.EqualValues(t, tc.entries, fixture.sentEntries.Load(), "must send the requested prefix")

			events := readAuditAssertions(t, outputPath)
			content := events["audit trail should contain entries"]
			unexpected := events["ListAuditEntries stream returned unexpected error"]
			if tc.terminal != nil || tc.cancelMode != "" {
				if len(content) != 0 {
					t.Errorf("interrupted stream (%v) emitted content assertions: %+v", tc.terminal, content)
				}
				if strings.Contains(string(output), "audit cycle completed:") {
					t.Errorf("interrupted stream logged a completed audit cycle: %s", output)
				}
			} else {
				require.Len(t, content, 1, "clean EOF must evaluate the content invariant")
				require.Equal(t, tc.entries > 0, content[0].Condition, "a truly empty audit must still fail")
				require.Equal(t, float64(tc.entries), content[0].Details["count"])
			}
			if tc.unexpected {
				require.Len(t, unexpected, 1, "must preserve receive error %v; assertions: %+v", tc.terminal, events)
				require.False(t, unexpected[0].Condition)
				require.Equal(t, tc.terminal.Error(), unexpected[0].Details["error"])
				require.Equal(t, status.Code(tc.terminal).String(), unexpected[0].Details["code"])
				require.Equal(t, "default", unexpected[0].Details["ledger"])
				require.Equal(t, float64(tc.entries), unexpected[0].Details["count"])
			} else {
				require.Empty(t, unexpected, "clean EOF, known transients and local cancellation must not emit unexpected errors")
			}
		})
	}
}

func TestAuditDriverChild(t *testing.T) {
	if os.Getenv("LEDGER_TEST_AUDIT_CHILD") != "1" {
		return
	}
	if mode := os.Getenv("LEDGER_TEST_AUDIT_CANCEL"); mode != "" {
		runAuditCancellation(t, mode)
		return
	}
	main()
}

func runAuditCancellation(t *testing.T, mode string) {
	t.Helper()
	entries, err := strconv.Atoi(os.Getenv("LEDGER_TEST_AUDIT_ENTRIES"))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var receiver *cancelingAuditStream
	conn, err := grpc.NewClient(os.Getenv("LEDGER_GRPC_ADDR"),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, conn *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			stream, err := streamer(ctx, desc, conn, method, opts...)
			if err != nil || method != servicepb.BucketService_ListAuditEntries_FullMethodName {
				return stream, err
			}
			receiver = &cancelingAuditStream{ClientStream: stream, cancel: cancel, mode: mode, prefix: entries}
			return receiver, nil
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	runAuditCycle(ctx, servicepb.NewBucketServiceClient(conn), "default")
	require.NotNil(t, receiver, "must enter the audit receive loop")
	require.Equal(t, entries, receiver.received, "must receive the prefix before canceling")
	require.ErrorIs(t, ctx.Err(), context.Canceled, "must cancel the actual caller context")
	if mode == "before_receive" {
		require.Equal(t, codes.Canceled, status.Code(receiver.terminal), "cancellation must come from the real gRPC receive")
	} else {
		require.Equal(t, codes.Unknown, status.Code(receiver.terminal), "must preserve the real server error before cancellation")
	}
}

// This wrapper cancels the caller context at a deterministic receive boundary;
// it always delegates RecvMsg and never substitutes a synthetic receive error.
type cancelingAuditStream struct {
	grpc.ClientStream
	cancel   context.CancelFunc
	mode     string
	prefix   int
	received int
	terminal error
}

func (s *cancelingAuditStream) RecvMsg(message any) error {
	if s.mode == "before_receive" && s.received == s.prefix {
		// Waiting for real headers ensures even the zero-entry case reaches
		// the server before cancellation; stream creation alone is lazy.
		if _, err := s.ClientStream.Header(); err != nil {
			return err
		}
		s.cancel()
	}
	err := s.ClientStream.RecvMsg(message)
	if err == nil {
		s.received++
	} else {
		s.terminal = err
		if s.mode == "after_error" {
			// Make the caller context canceled when the driver observes Unknown.
			s.cancel()
		}
	}
	return err
}

type auditAssertion struct {
	Message   string         `json:"message"`
	Condition bool           `json:"condition"`
	Hit       bool           `json:"hit"`
	Details   map[string]any `json:"details"`
}

func readAuditAssertions(t *testing.T, path string) map[string][]auditAssertion {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	events := make(map[string][]auditAssertion)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event struct {
			Assertion *auditAssertion `json:"antithesis_assert"`
		}
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &event))
		if event.Assertion != nil && event.Assertion.Hit {
			events[event.Assertion.Message] = append(events[event.Assertion.Message], *event.Assertion)
		}
	}
	require.NoError(t, scanner.Err())
	return events
}

type auditStreamServer struct {
	servicepb.UnimplementedBucketServiceServer
	entries       int
	terminal      error
	waitForCancel bool
	applyCalls    atomic.Int32
	auditCalls    atomic.Int32
	sentEntries   atomic.Int32
}

func (s *auditStreamServer) ListLedgers(_ *servicepb.ListLedgersRequest, stream grpc.ServerStreamingServer[commonpb.LedgerInfo]) error {
	return stream.Send(&commonpb.LedgerInfo{Name: "default"})
}

func (s *auditStreamServer) Apply(_ context.Context, request *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	requests := request.GetUnsigned().GetRequests()
	if len(requests) != 1 || requests[0].GetApply().GetLedger() != "default" || requests[0].GetApply().GetAction().GetCreateTransaction() == nil {
		return nil, status.Error(codes.InvalidArgument, "fixture expected one audit setup transaction")
	}
	s.applyCalls.Add(1)
	return &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Sequence: 1}}}, nil
}

func (s *auditStreamServer) ListAuditEntries(request *servicepb.ListAuditEntriesRequest, stream grpc.ServerStreamingServer[publicauditpb.AuditEntry]) error {
	if request.GetOptions().GetPageSize() != 10 || s.applyCalls.Load() != 1 {
		return status.Error(codes.InvalidArgument, "fixture expected audit page after the confirmed transaction")
	}
	s.auditCalls.Add(1)
	// Commit stream headers even for a zero-entry failure, making Recv the
	// failure boundary rather than the stream-creation interceptor.
	if err := stream.SendHeader(metadata.Pairs("audit-fixture", "ready")); err != nil {
		return err
	}
	for i := 0; i < s.entries; i++ {
		if err := stream.Send(&publicauditpb.AuditEntry{Sequence: uint64(i + 1)}); err != nil {
			return err
		}
		s.sentEntries.Add(1)
	}
	if s.waitForCancel {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	return s.terminal
}
