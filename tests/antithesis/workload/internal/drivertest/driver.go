// Package drivertest checks workload assertions against a real local server.
package drivertest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"
)

// CheckDriver executes the unchanged command entry point and requires each
// original business assertion to be evaluated successfully. Registration alone
// is not a hit. A separate process configures the SDK before package init.
func CheckDriver(t *testing.T, main func(), messages ...string) {
	t.Helper()
	CheckEmissions(t, func() {
		ctx, client := StartServer(t)
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("seed", actions.CreateLedgerAction("default", nil)))
		require.NoError(t, err)
		main()
	}, func(records []Assertion) {
		for _, record := range records {
			if record.Hit && (record.Type == "always" || record.Type == "reachability") {
				require.True(t, record.Condition, "unexpected invariant failure: %+v", record)
			}
		}
		for _, message := range messages {
			found := false
			for _, record := range records {
				if record.Hit && record.Message == message {
					found = true
					require.True(t, record.Condition, "oracle failed: %+v", record)
				}
			}
			if !found {
				t.Errorf("oracle was never evaluated: %s", message)
			} else {
				t.Logf("evaluated: %s", message)
			}
		}
	})
}

type Assertion struct {
	Type      string         `json:"assert_type"`
	Message   string         `json:"message"`
	Hit       bool           `json:"hit"`
	Condition bool           `json:"condition"`
	Details   map[string]any `json:"details"`
}

// CheckEmissions captures the real SDK output, including deliberately failing
// assertions in oracle-sensitivity tests, without replacing assertion calls.
func CheckEmissions(t *testing.T, scenario func(), check func([]Assertion)) {
	t.Helper()
	if os.Getenv("LEDGER_ORACLE_TEST_CHILD") == t.Name() {
		scenario()
		return
	}
	t.Parallel()
	outputPath := filepath.Join(t.TempDir(), "assertions.jsonl")
	executable, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.v")
	cmd.Env = append(os.Environ(), "LEDGER_ORACLE_TEST_CHILD="+t.Name(), "ANTITHESIS_SDK_LOCAL_OUTPUT="+outputPath)
	output, err := cmd.CombinedOutput()
	for _, line := range strings.Split(string(output), "\n") {
		if strings.Contains(line, "index not found:") {
			t.Log(line)
		}
	}
	require.NoError(t, err, "driver subprocess: %s", output)
	data, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	var records []Assertion
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		var row struct {
			Assertion *Assertion `json:"antithesis_assert"`
		}
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &row))
		if row.Assertion != nil {
			records = append(records, *row.Assertion)
		}
	}
	require.NoError(t, scanner.Err())
	check(records)
}

// StartServer uses leased ports and the same server bootstrap as the nested
// model integration tests. It also points command entry points at this server.
func StartServer(t *testing.T) (context.Context, servicepb.BucketServiceClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	t.Cleanup(cancel)
	lease := testserver.AllocateNodeLease()
	instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
		NodeID: 1, ClusterID: "query-oracle", Ports: lease.Ports(),
		WalDir: t.TempDir(), DataDir: t.TempDir(), Output: os.Stdout,
	})
	instruments = append(instruments, testserver.WithBootstrap())
	server := lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
	require.NoError(t, server.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		require.NoError(t, server.Stop(stopCtx))
	})
	address := fmt.Sprintf("localhost:%d", lease.Ports().GRPC())
	t.Setenv("LEDGER_GRPC_ADDR", address)
	t.Setenv("LEDGER_NO_RETRY", "1")
	conn, err := grpc.NewClient(address, grpcprotocol.ClientOption(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	cluster := clusterpb.NewClusterServiceClient(conn)
	require.Eventually(t, func() bool {
		state, err := cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		return err == nil && state.GetLeader() != 0
	}, 5*time.Second, 10*time.Millisecond)
	return ctx, servicepb.NewBucketServiceClient(conn)
}
