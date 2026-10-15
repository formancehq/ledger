package server

import (
	"context"
	"os"
	"os/exec"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/service"
)

// Exercise the same command and environment binder as main, isolating
// service.Execute's os.Exit in a helper process.
func TestHealthCheckIntervalStartup(t *testing.T) {
	const helperEnv = "LEDGER_HEALTH_INTERVAL_HELPER"
	if os.Getenv(helperEnv) == "1" {
		separator := slices.Index(os.Args, "--")
		require.NotEqual(t, -1, separator)
		command := NewRootCommand()
		command.SetArgs(os.Args[separator+1:])
		service.Execute(command)

		return
	}
	t.Parallel()

	const intervalError = "Error: validating config: --health-check-interval must be greater than zero"
	// A later validation error keeps valid-interval cases from starting any
	// listeners or workers. Reaching it proves the interval guard accepted
	// the selected value. It also bounds the pre-fix invalid-interval cases.
	const laterError = "Error: validating config: wal: health resume threshold 0.000 must satisfy 0 < resume < block"
	for _, tt := range []struct {
		name    string
		flags   []string
		env     string
		wantErr string
	}{
		{name: "CLI zero", flags: []string{"--health-check-interval=0"}, wantErr: intervalError},
		{name: "CLI negative", flags: []string{"--health-check-interval=-1s"}, wantErr: intervalError},
		{name: "environment zero", env: "0", wantErr: intervalError},
		{name: "environment negative", env: "-1s", wantErr: intervalError},
		{name: "malformed CLI", flags: []string{"--health-check-interval=bad"}, wantErr: "invalid argument \"bad\" for \"--health-check-interval\" flag"},
		{name: "malformed environment", env: "bad", wantErr: "binding environment variable HEALTH_CHECK_INTERVAL to flag --health-check-interval"},
		{name: "default", wantErr: laterError},
		{name: "positive CLI", flags: []string{"--health-check-interval=1ns"}, wantErr: laterError},
		{name: "positive environment", env: "10s", wantErr: laterError},
		{name: "invalid CLI overrides positive environment", flags: []string{"--health-check-interval=0"}, env: "10s", wantErr: intervalError},
		{name: "positive CLI overrides zero environment", flags: []string{"--health-check-interval=10s"}, env: "0", wantErr: laterError},
		{name: "positive CLI overrides malformed environment", flags: []string{"--health-check-interval=10s"}, env: "bad", wantErr: laterError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			args := []string{"-test.run=^TestHealthCheckIntervalStartup$", "--", "run",
				"--node-id=1", "--cluster-id=health-interval-test", "--health-wal-resume-threshold=0"}
			args = append(args, tt.flags...)
			command := exec.CommandContext(ctx, os.Args[0], args...)
			// Exclude ambient server configuration so only this case's selected
			// interval and explicit CLI flags can influence validation.
			command.Env = []string{helperEnv + "=1", "HEALTH_CHECK_INTERVAL=" + tt.env}
			output, err := command.CombinedOutput()
			require.NoError(t, ctx.Err(), "helper timed out: %s", output)
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr, "%s", output)
			require.Equal(t, 1, exitErr.ExitCode(), "%s", output)
			require.Contains(t, string(output), tt.wantErr)
			require.NotContains(t, string(output), "panic:")
		})
	}
}
