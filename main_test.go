package main

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/service"

	"github.com/formancehq/ledger/v3/cmd/server"
)

const malformedEnvironmentHelperProcess = "MALFORMED_ENVIRONMENT_HELPER_PROCESS"

// TestMalformedEnvironmentFailsBeforeStartup verifies that the production
// command rejects malformed environment configuration before service startup.
func TestMalformedEnvironmentFailsBeforeStartup(t *testing.T) {
	if os.Getenv(malformedEnvironmentHelperProcess) == "1" {
		cmd := server.NewRootCommand()
		cmd.SetArgs([]string{"run"})
		service.Execute(cmd)

		return
	}

	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMalformedEnvironmentFailsBeforeStartup$")
	cmd.Env = []string{
		malformedEnvironmentHelperProcess + "=1",
		"GRPC_PORT=not-an-integer",
	}
	output, err := cmd.CombinedOutput()

	require.Error(t, err)
	require.Equal(t, 1, cmd.ProcessState.ExitCode())
	require.Contains(t, string(output), "binding environment variable GRPC_PORT to flag --grpc-port")
	require.Contains(t, string(output), "not-an-integer")
	require.NotContains(t, string(output), "Starting application")
	require.NotContains(t, string(output), "Service gRPC server started successfully")
}
