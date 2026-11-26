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

// Exercise main's environment binder and exit handling in a helper process.
func TestRaftTickIntervalStartup(t *testing.T) {
	const helperEnv = "LEDGER_RAFT_TICK_INTERVAL_HELPER"
	if os.Getenv(helperEnv) == "1" {
		separator := slices.Index(os.Args, "--")
		require.NotEqual(t, -1, separator)
		command := NewRootCommand()
		command.SetArgs(os.Args[separator+1:])
		service.Execute(command)

		return
	}
	t.Parallel()

	const intervalError = "Error: validating config: --raft-tick-interval must be at least 10ns when --raft-processing-tick-interval is zero"
	// A later health validation failure proves the selected Raft intervals
	// passed their guard without starting listeners, workers, or storage.
	const laterError = "Error: validating config: wal: health resume threshold 0.000 must satisfy 0 < resume < block"
	for _, tt := range []struct {
		name          string
		flags         []string
		tickEnv       string
		processingEnv string
		wantErr       string
	}{
		{name: "CLI 1ns", flags: []string{"--raft-tick-interval=1ns"}, wantErr: intervalError},
		{name: "CLI 9ns", flags: []string{"--raft-tick-interval=9ns"}, wantErr: intervalError},
		{name: "environment 1ns", tickEnv: "1ns", wantErr: intervalError},
		{name: "environment 9ns", tickEnv: "9ns", wantErr: intervalError},
		{name: "default", wantErr: laterError},
		{name: "CLI zero", flags: []string{"--raft-tick-interval=0"}, wantErr: laterError},
		{name: "environment zero", tickEnv: "0", wantErr: laterError},
		{name: "CLI fractional zero", flags: []string{"--raft-tick-interval=0.1ns"}, wantErr: laterError},
		{name: "environment fractional zero", tickEnv: "0.1ns", wantErr: laterError},
		{name: "CLI minimum", flags: []string{"--raft-tick-interval=10ns"}, wantErr: laterError},
		{name: "environment minimum", tickEnv: "10ns", wantErr: laterError},
		{name: "CLI positive processing", flags: []string{"--raft-tick-interval=1ns", "--raft-processing-tick-interval=1ns"}, wantErr: laterError},
		{name: "environment positive processing", tickEnv: "9ns", processingEnv: "1ns", wantErr: laterError},
		{name: "CLI tiny tick overrides valid environment", flags: []string{"--raft-tick-interval=1ns"}, tickEnv: "10ns", wantErr: intervalError},
		{name: "CLI valid tick overrides tiny environment", flags: []string{"--raft-tick-interval=10ns"}, tickEnv: "1ns", wantErr: laterError},
		{name: "CLI valid tick overrides malformed environment", flags: []string{"--raft-tick-interval=10ns"}, tickEnv: "bad", wantErr: laterError},
		{name: "CLI zero processing overrides positive environment", flags: []string{"--raft-processing-tick-interval=0"}, tickEnv: "1ns", processingEnv: "1ns", wantErr: intervalError},
		{name: "CLI positive processing overrides zero environment", flags: []string{"--raft-processing-tick-interval=1ns"}, tickEnv: "1ns", processingEnv: "0", wantErr: laterError},
		{name: "CLI positive processing overrides malformed environment", flags: []string{"--raft-processing-tick-interval=1ns"}, tickEnv: "1ns", processingEnv: "bad", wantErr: laterError},
		{name: "CLI fractional zero processing", flags: []string{"--raft-tick-interval=1ns", "--raft-processing-tick-interval=0.1ns"}, wantErr: intervalError},
		{name: "environment fractional zero processing", tickEnv: "1ns", processingEnv: "0.1ns", wantErr: intervalError},
		{name: "malformed CLI tick", flags: []string{"--raft-tick-interval=bad"}, wantErr: "invalid argument \"bad\" for \"--raft-tick-interval\" flag"},
		{name: "malformed environment tick", tickEnv: "bad", wantErr: "binding environment variable RAFT_TICK_INTERVAL to flag --raft-tick-interval"},
		{name: "malformed CLI processing", flags: []string{"--raft-processing-tick-interval=bad"}, wantErr: "invalid argument \"bad\" for \"--raft-processing-tick-interval\" flag"},
		{name: "malformed environment processing", processingEnv: "bad", wantErr: "binding environment variable RAFT_PROCESSING_TICK_INTERVAL to flag --raft-processing-tick-interval"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			args := []string{"-test.run=^TestRaftTickIntervalStartup$", "--", "run",
				"--node-id=1", "--cluster-id=raft-tick-interval-test", "--health-wal-resume-threshold=0"}
			args = append(args, tt.flags...)
			command := exec.CommandContext(ctx, os.Args[0], args...)
			// Isolate the selected flags from ambient server configuration.
			command.Env = []string{helperEnv + "=1", "RAFT_TICK_INTERVAL=" + tt.tickEnv,
				"RAFT_PROCESSING_TICK_INTERVAL=" + tt.processingEnv}
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
