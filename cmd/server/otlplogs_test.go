package server

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	otlp "github.com/formancehq/go-libs/v5/pkg/observe"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/service"

	"github.com/formancehq/ledger/v3/internal/pkg/version"
)

// newCmdWithLogFlags returns a fresh cobra.Command with the --debug and
// --log-level flags registered, mirroring the binary's flag set.
func newCmdWithLogFlags(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{}
	cmd.Flags().Bool(service.DebugFlag, false, "Debug mode")
	cmd.Flags().String(LogLevelFlag, "", "Log level")

	return cmd
}

func TestResolveLogLevel(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		logLevel  string
		debug     bool
		wantLevel logging.Level
		wantErr   bool
	}{
		{
			name:      "default is info",
			wantLevel: logging.InfoLevel,
		},
		{
			name:      "--debug=true maps to debug",
			debug:     true,
			wantLevel: logging.DebugLevel,
		},
		{
			name:      "--log-level=trace wins over default",
			logLevel:  "trace",
			wantLevel: logging.TraceLevel,
		},
		{
			name:      "--log-level=debug",
			logLevel:  "debug",
			wantLevel: logging.DebugLevel,
		},
		{
			name:      "--log-level=info",
			logLevel:  "info",
			wantLevel: logging.InfoLevel,
		},
		{
			name:      "--log-level=error",
			logLevel:  "error",
			wantLevel: logging.ErrorLevel,
		},
		{
			name:      "--log-level wins over --debug",
			logLevel:  "info",
			debug:     true,
			wantLevel: logging.InfoLevel,
		},
		{
			name:     "invalid --log-level errors",
			logLevel: "verbose",
			wantErr:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cmd := newCmdWithLogFlags(t)
			if tc.debug {
				require.NoError(t, cmd.Flags().Set(service.DebugFlag, "true"))
			}
			if tc.logLevel != "" {
				require.NoError(t, cmd.Flags().Set(LogLevelFlag, tc.logLevel))
			}

			got, err := resolveLogLevel(cmd)
			if tc.wantErr {
				assert.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantLevel, got)
		})
	}
}

func TestResourceFromFlags(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		serviceName string
		attributes  string
		wantName    string
		wantVersion string
	}{
		{name: "node default and build metadata", wantName: "ledger-node-42", wantVersion: "v3.0.0-abc123"},
		{name: "explicit service", serviceName: "ledger-production", wantName: "ledger-production", wantVersion: "v3.0.0-abc123"},
		{name: "resource overrides", serviceName: "ledger-production", attributes: "service.name=override,service.version=override-build,deployment.environment=regression", wantName: "override", wantVersion: "override-build"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cmd := &cobra.Command{}
			otlp.AddFlags(cmd.Flags())
			require.NoError(t, cmd.Flags().Set(otlp.OtelServiceNameFlag, test.serviceName))
			if test.attributes != "" {
				require.NoError(t, cmd.Flags().Set(otlp.OtelResourceAttributesFlag, test.attributes))
			}
			res, err := resourceFromFlags(cmd, 42, version.Info{Version: "v3.0.0", Commit: "abc123"})
			require.NoError(t, err)
			attributes := res.Set()
			name, ok := attributes.Value("service.name")
			require.True(t, ok)
			require.Equal(t, test.wantName, name.AsString())
			build, ok := attributes.Value("service.version")
			require.True(t, ok)
			require.Equal(t, test.wantVersion, build.AsString())
			if test.attributes != "" {
				environment, ok := attributes.Value("deployment.environment")
				require.True(t, ok)
				require.Equal(t, "regression", environment.AsString())
			}
		})
	}
}

func TestResourceFromFlagsRejectsMalformedAttributes(t *testing.T) {
	t.Parallel()
	cmd := &cobra.Command{}
	otlp.AddFlags(cmd.Flags())
	require.NoError(t, cmd.Flags().Set(otlp.OtelResourceAttributesFlag, "missing-value"))
	_, err := resourceFromFlags(cmd, 42, version.Info{})
	require.ErrorContains(t, err, "malformed otlp attribute: missing-value")
}
