// Package backupdrivertest exercises backup drivers against real SDK output.
package backupdrivertest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// driverConn runs the generated gRPC client against a deterministic transport
// boundary. No server or retry interceptor can hide the driver's classification.
type driverConn func(context.Context, string, any, any, ...grpc.CallOption) error

func (c driverConn) Invoke(ctx context.Context, method string, req, reply any, opts ...grpc.CallOption) error {
	return c(ctx, method, req, reply, opts...)
}

func (driverConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("unexpected streaming RPC")
}

// Stage describes one RPC in a driver's ordered backup workflow.
type Stage struct {
	Operation              string
	GRPCMethod             string
	NoCheckpointUnexpected bool
}

type driverCase struct {
	Stage        int    `json:"stage"`
	Fault        string `json:"fault"`
	CancelCaller bool   `json:"cancelCaller"`
}

// Run checks each stage's error classification in isolated SDK subprocesses.
// runDriver must execute the same post-connection path used by the real driver.
func Run(t *testing.T, runDriver func(context.Context, clusterpb.ClusterServiceClient), stages ...Stage) {
	t.Helper()
	require.NotEmpty(t, stages)
	// The SDK selects its output file at package initialization. A fresh process
	// also prevents assertion deduplication from hiding sibling error cases.
	if raw := os.Getenv("LEDGER_BACKUP_DRIVER_TEST_CASE"); raw != "" {
		var tc driverCase
		require.NoError(t, json.Unmarshal([]byte(raw), &tc))
		require.Positive(t, tc.Stage)
		require.LessOrEqual(t, tc.Stage, len(stages))
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		calls := 0
		client := clusterpb.NewClusterServiceClient(driverConn(func(_ context.Context, method string, _, reply any, _ ...grpc.CallOption) error {
			calls++
			require.LessOrEqual(t, calls, len(stages), "unexpected RPC after the final driver stage")
			require.Equal(t, stages[calls-1].GRPCMethod, method)
			if calls == tc.Stage {
				if tc.CancelCaller {
					cancel()
				}
				switch tc.Fault {
				case "internal":
					return status.Error(codes.Internal, "injected server invariant")
				case "unknown":
					return status.Error(codes.Unknown, "injected unclassified failure")
				case "canceled":
					return status.Error(codes.Canceled, "injected RPC cancellation")
				case "local-canceled":
					return fmt.Errorf("request ended: %w", context.Canceled)
				case "external-service":
					st, err := status.New(codes.FailedPrecondition, "object store unavailable").WithDetails(&errdetails.ErrorInfo{Reason: "EXTERNAL_SERVICE_ERROR"})
					require.NoError(t, err)
					return st.Err()
				case "no-checkpoint":
					return status.Error(codes.FailedPrecondition, "full checkpoint required")
				default:
					t.Fatalf("unknown fault %q", tc.Fault)
				}
			}
			switch response := reply.(type) {
			case *clusterpb.BackupResponse:
				response.TotalFiles = 1
			case *clusterpb.IncrementalBackupResponse:
				response.LastLogSequence, response.LastAuditSequence = 1, 1
			default:
				t.Fatalf("unexpected response type %T", reply)
			}
			return nil
		}))
		runDriver(ctx, client)
		require.Equal(t, tc.Stage, calls, "the driver must stop after the failing stage")
		return
	}

	t.Parallel()
	testName := t.Name()
	for stageIndex, stage := range stages {
		for _, tc := range []struct {
			name         string
			fault        string
			cancelCaller bool
			unexpected   bool
		}{
			{"internal-with-caller-cancellation", "internal", true, true},
			{"unknown-with-caller-cancellation", "unknown", true, true},
			{"remote-canceled-with-live-caller", "canceled", false, true},
			{"grpc-canceled-with-caller-cancellation", "canceled", true, false},
			{"wrapped-local-cancellation", "local-canceled", true, false},
			{"external-service", "external-service", false, false},
			{"no-checkpoint", "no-checkpoint", false, stage.NoCheckpointUnexpected},
		} {
			t.Run(stage.Operation+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				outputPath := filepath.Join(t.TempDir(), "assertions.jsonl")
				raw, err := json.Marshal(driverCase{Stage: stageIndex + 1, Fault: tc.fault, CancelCaller: tc.cancelCaller})
				require.NoError(t, err)
				executable, err := os.Executable()
				require.NoError(t, err)
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, executable, "-test.run=^"+regexp.QuoteMeta(testName)+"$")
				cmd.Env = append(os.Environ(), "LEDGER_BACKUP_DRIVER_TEST_CASE="+string(raw), "ANTITHESIS_SDK_LOCAL_OUTPUT="+outputPath)
				output, err := cmd.CombinedOutput()
				require.NoError(t, err, "driver subprocess: %s", output)
				data, err := os.ReadFile(outputPath)
				require.NoError(t, err)
				var unexpected []string
				decoder := json.NewDecoder(bytes.NewReader(data))
				for {
					var event struct {
						Assertion *struct {
							Message     string         `json:"message"`
							DisplayType string         `json:"display_type"`
							Hit         bool           `json:"hit"`
							Condition   bool           `json:"condition"`
							Details     map[string]any `json:"details"`
						} `json:"antithesis_assert"`
					}
					err := decoder.Decode(&event)
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(t, err)
					if assertion := event.Assertion; assertion != nil && assertion.Hit && assertion.DisplayType == "Unreachable" {
						require.False(t, assertion.Condition)
						require.NotEmpty(t, assertion.Details["error"])
						unexpected = append(unexpected, assertion.Message)
						t.Logf("SDK assertion JSON: %s", data)
					}
				}
				if tc.unexpected {
					require.Equal(t, []string{stage.Operation + " returned unexpected error"}, unexpected, "server errors must remain visible even if the caller was concurrently canceled; SDK output: %s", data)
				} else {
					require.Empty(t, unexpected, "expected lifecycle or stage precondition outcome; SDK output: %s", data)
				}
			})
		}
	}
}
