package server

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/service"
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
