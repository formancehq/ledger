// Package sdktest captures real SDK output in an isolated test subprocess.
package sdktest

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type Event struct {
	Message     string         `json:"message"`
	ID          string         `json:"id"`
	DisplayType string         `json:"display_type"`
	MustHit     bool           `json:"must_hit"`
	Hit         bool           `json:"hit"`
	Condition   bool           `json:"condition"`
	Details     map[string]any `json:"details"`
}

// Capture returns nil in the child, after running scenario. The SDK opens its
// output file during init and deduplicates by message, so setting an environment
// variable inside a test or sharing one process between cases is insufficient.
func Capture(t *testing.T, scenario func()) []Event {
	t.Helper()
	const childEnv = "LEDGER_SDK_TEST_CHILD"
	if os.Getenv(childEnv) == t.Name() {
		scenario()
		return nil
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	outputPath := filepath.Join(t.TempDir(), "sdk.jsonl")
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.v")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, childEnv+"=") && !strings.HasPrefix(entry, "ANTITHESIS_SDK_LOCAL_OUTPUT=") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, childEnv+"="+t.Name(), "ANTITHESIS_SDK_LOCAL_OUTPUT="+outputPath)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "SDK scenario failed: %s", output)
	data, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	t.Logf("SDK output:\n%s", data)

	var events []Event
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		var record struct {
			Assertion *Event `json:"antithesis_assert"`
		}
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &record))
		if record.Assertion != nil {
			events = append(events, *record.Assertion)
		}
	}
	require.NoError(t, scanner.Err())
	require.NotEmpty(t, events, "SDK emission must be enabled for these regressions")
	return events
}

func Find(t *testing.T, events []Event, message string) Event {
	t.Helper()
	var found []Event
	for _, event := range events {
		if event.Message == message {
			found = append(found, event)
		}
	}
	require.Len(t, found, 1, "expected one SDK event for %q", message)
	require.Equal(t, message, found[0].ID)
	require.True(t, found[0].Hit)
	return found[0]
}

func Absent(t *testing.T, events []Event, message string) {
	t.Helper()
	for _, event := range events {
		require.NotEqual(t, message, event.Message, "unexpected SDK observation: %+v", event)
	}
}

// Client uses the generated gRPC server/client contract without a live cluster.
func Client(t *testing.T, implementation servicepb.BucketServiceServer, options ...grpc.DialOption) servicepb.BucketServiceClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	servicepb.RegisterBucketServiceServer(server, implementation)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-serveDone)
	})
	options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}))
	conn, err := grpc.NewClient("passthrough:///sdk-fixture", options...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return servicepb.NewBucketServiceClient(conn)
}
