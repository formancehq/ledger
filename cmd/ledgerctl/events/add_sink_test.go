package events

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAddSinkValidation exercises the early validation branches of
// 'events add-sink' without spinning up a gRPC client. Every test case here
// expects the command to fail before GetClient is called, so the error is
// produced purely by the flag-parsing logic in runAddSink.
//
// Tests use the real cobra command (NewAddSinkCommand) because the validation
// uses both Flag().Changed (intent detection) and GetString (value access),
// which cannot be reproduced by hand-rolling a flag set.
func TestAddSinkValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		args        []string
		errContains string
	}{
		{
			name:        "missing --name",
			args:        []string{"--nats-url", "nats://localhost:4222", "--nats-topic", "evt"},
			errContains: "--name is required",
		},
		{
			name:        "no sink type at all",
			args:        []string{"--name", "x"},
			errContains: "must specify a sink type",
		},
		{
			name: "multiple sink types",
			args: []string{
				"--name", "x",
				"--nats-url", "nats://localhost:4222", "--nats-topic", "evt",
				"--http-endpoint", "https://example.com",
			},
			errContains: "cannot specify multiple sink types",
		},
		{
			name: "databricks: OAuth-only flags without host trigger databricks branch",
			args: []string{
				"--name", "x",
				"--databricks-client-id", "id",
				"--databricks-client-secret", "secret",
			},
			// Previously this fell through to "must specify a sink type" because
			// hasDatabricks only checked --databricks-host. With Flag.Changed
			// detection, intent is recognized and the user gets a precise list
			// of missing required fields.
			errContains: "--databricks-host",
		},
		{
			name: "databricks: missing flag list itemizes only what is actually missing",
			args: []string{
				"--name", "x",
				"--databricks-host", "adb-1.azuredatabricks.net",
				"--databricks-token", "dapi",
				// http-path / catalog / schema missing
			},
			errContains: "--databricks-http-path, --databricks-catalog, --databricks-schema",
		},
		{
			name: "databricks: both auth methods rejected",
			args: []string{
				"--name", "x",
				"--databricks-host", "adb-1.azuredatabricks.net",
				"--databricks-http-path", "/sql/1.0/warehouses/abc",
				"--databricks-catalog", "main",
				"--databricks-schema", "default",
				"--databricks-token", "dapi",
				"--databricks-client-id", "id",
				"--databricks-client-secret", "secret",
			},
			errContains: "mutually exclusive",
		},
		{
			name: "databricks: no auth method rejected",
			args: []string{
				"--name", "x",
				"--databricks-host", "adb-1.azuredatabricks.net",
				"--databricks-http-path", "/sql/1.0/warehouses/abc",
				"--databricks-catalog", "main",
				"--databricks-schema", "default",
			},
			errContains: "PAT",
		},
		{
			name: "databricks: OAuth client_id without client_secret",
			args: []string{
				"--name", "x",
				"--databricks-host", "adb-1.azuredatabricks.net",
				"--databricks-http-path", "/sql/1.0/warehouses/abc",
				"--databricks-catalog", "main",
				"--databricks-schema", "default",
				"--databricks-client-id", "id",
			},
			errContains: "both",
		},
		{
			name: "databricks-only: --databricks-port triggers branch on its own",
			args: []string{
				"--name", "x",
				"--databricks-port", "443",
			},
			// Without Flag.Changed, setting only --databricks-port (even to its
			// default) would not trigger hasDatabricks. With Flag.Changed it
			// does, and the user gets the proper missing-flags error.
			errContains: "databricks sink is missing",
		},
		{
			name: "databricks-table default does not trigger branch",
			args: []string{
				"--name", "x",
				"--nats-url", "nats://localhost:4222", "--nats-topic", "evt",
			},
			// 'ledger_events' is the default value for --databricks-table. The
			// presence of that default must NOT make hasDatabricks true and
			// trip the "multiple sink types" error against the NATS config.
			errContains: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cmd := NewAddSinkCommand()
			cmd.SetArgs(tt.args)
			cmd.SilenceUsage = true
			cmd.SilenceErrors = true

			err := cmd.Execute()
			if tt.errContains == "" {
				// Validation passed; runAddSink will fail later trying to dial
				// the gRPC client. We only care that it gets that far.
				if err != nil {
					assert.NotContains(t, err.Error(), "must specify a sink type")
					assert.NotContains(t, err.Error(), "cannot specify multiple sink types")
				}

				return
			}

			require.Error(t, err)
			assert.True(t,
				strings.Contains(err.Error(), tt.errContains),
				"error %q does not contain expected substring %q", err.Error(), tt.errContains,
			)
		})
	}
}
