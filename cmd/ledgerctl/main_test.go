package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

// TestLedgerFlagCompletionRegistered asserts that every command exposing a
// --ledger flag (locally or via the inherited persistent flag) has the
// ledger-name shell completion wired, so pressing TAB suggests ledgers instead
// of falling back to file completion.
func TestLedgerFlagCompletionRegistered(t *testing.T) {
	t.Parallel()

	root := newRootCommand()

	var withLedgerFlag int

	var walk func(cmd *cobra.Command)
	walk = func(cmd *cobra.Command) {
		if cmd.Flag("ledger") != nil {
			withLedgerFlag++

			_, ok := cmd.GetFlagCompletionFunc("ledger")
			require.Truef(t, ok, "command %q exposes --ledger without completion", cmd.CommandPath())
		}

		for _, sub := range cmd.Commands() {
			walk(sub)
		}
	}
	walk(root)

	// Guard against the walk silently matching nothing (e.g. the flag being
	// renamed): the suite must actually exercise the registration path.
	require.NotZero(t, withLedgerFlag, "expected at least one command with a --ledger flag")
}

// TestServerFlagEnvResolution exercises the connection-flag env precedence for
// the representative --server flag: explicit CLI flag > LEDGERCTL_SERVER env >
// cobra default. The bare SERVER name must never be honored for the root
// --server flag, for two reasons: bindSubcommandEnv never binds the root command
// itself, and at bind time cobra has not yet merged the root's inherited
// persistent flags into the subcommand flag sets (that merge happens during
// Execute), so --server is absent from every flag set the binder visits.
//
// These cases mutate process-wide environment (env vars + config dir), so they
// cannot run in parallel.
func TestServerFlagEnvResolution(t *testing.T) {
	const defaultServer = "localhost:8888"

	tests := []struct {
		name       string
		envName    string
		envValue   string
		cliServer  string // when non-empty, passed as --server on the command line
		wantServer string
	}{
		{
			name:       "LEDGERCTL_SERVER env, no CLI flag, resolves to env value",
			envName:    "LEDGERCTL_SERVER",
			envValue:   "env.example.com:443",
			wantServer: "env.example.com:443",
		},
		{
			name:       "bare SERVER env, no CLI flag, is ignored and falls back to default",
			envName:    "SERVER",
			envValue:   "bare.example.com:443",
			wantServer: defaultServer,
		},
		{
			name:       "CLI --server wins over LEDGERCTL_SERVER env",
			envName:    "LEDGERCTL_SERVER",
			envValue:   "env.example.com:443",
			cliServer:  "cli.example.com:443",
			wantServer: "cli.example.com:443",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Hermetic config: point os.UserConfigDir() at an empty temp dir so
			// LoadConfig() returns a zero-value Config on every platform
			// (HOME drives macOS, XDG_CONFIG_HOME drives Linux, APPDATA/
			// LOCALAPPDATA drive Windows).
			tmp := t.TempDir()
			t.Setenv("HOME", tmp)
			t.Setenv("XDG_CONFIG_HOME", tmp)
			t.Setenv("APPDATA", tmp)
			t.Setenv("LOCALAPPDATA", tmp)

			// Ensure neither name leaks in from the ambient environment.
			t.Setenv("LEDGERCTL_SERVER", "")
			t.Setenv("SERVER", "")
			t.Setenv(tc.envName, tc.envValue)

			root := newRootCommand()
			root.SilenceErrors = true

			// Wire env binding through the same helper main() uses, so this
			// test guards the production wiring instead of re-implementing it.
			bindSubcommandEnv(root)

			// "version" is a hermetic leaf: it triggers the root
			// PersistentPreRunE (where resolveFlag runs) without opening a
			// network connection.
			args := []string{"version"}
			if tc.cliServer != "" {
				args = append(args, "--server", tc.cliServer)
			}

			root.SetArgs(args)
			require.NoError(t, root.Execute())

			got, err := root.Flags().GetString("server")
			require.NoError(t, err)
			require.Equal(t, tc.wantServer, got)
		})
	}
}

// TestSubcommandLocalFlagsIgnoreBareEnv guards the connection/security flags
// that some subcommands redeclare locally (profile create, auth generate-token)
// against bare-name env leakage. Before bindSubcommandEnv skipped the
// ledgerctl-owned names, service.BindEnvToCommand bound e.g. SERVER/SIGNING_KEY
// into these local flag sets, so a stray bare env var silently overrode the
// LEDGERCTL_ prefixed lookup (and, for profile create, landed in the stored
// profile). The bare names must never populate the local flags.
//
// These cases mutate process-wide environment, so they cannot run in parallel.
func TestSubcommandLocalFlagsIgnoreBareEnv(t *testing.T) {
	tests := []struct {
		name    string
		path    []string // command path under root
		flag    string
		bareEnv string
	}{
		{
			name:    "profile create --server ignores bare SERVER",
			path:    []string{"profile", "create"},
			flag:    "server",
			bareEnv: "SERVER",
		},
		{
			name:    "profile create --signing-key ignores bare SIGNING_KEY",
			path:    []string{"profile", "create"},
			flag:    "signing-key",
			bareEnv: "SIGNING_KEY",
		},
		{
			name:    "auth generate-token --signing-key ignores bare SIGNING_KEY",
			path:    []string{"auth", "generate-token"},
			flag:    "signing-key",
			bareEnv: "SIGNING_KEY",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.bareEnv, "bare-leak")

			root := newRootCommand()
			bindSubcommandEnv(root)

			cmd, _, err := root.Find(tc.path)
			require.NoError(t, err)
			require.Equal(t, tc.path[len(tc.path)-1], cmd.Name(),
				"expected to resolve %v to its leaf command", tc.path)

			got, err := cmd.Flags().GetString(tc.flag)
			require.NoError(t, err)
			require.Empty(t, got, "bare %s must not populate local --%s", tc.bareEnv, tc.flag)
			require.False(t, cmd.Flags().Changed(tc.flag),
				"local --%s must not be marked changed by bare %s", tc.flag, tc.bareEnv)
		})
	}
}

// TestBindEnvSkipsOwnedProfile guards that "profile" is in ledgerctlOwnedFlagNames.
// ledgerctl owns --profile and resolves it exclusively via LEDGERCTL_PROFILE in
// PersistentPreRunE; the bare go-libs PROFILE name must never feed a --profile
// flag. cobra's lazy persistent-flag merge keeps the inherited --profile out of
// subcommand flag sets at bind time today, so this exercises the binder directly
// on a flag set that does expose --profile — the membership is the only thing
// stopping the leak, and removing "profile" from the owned set fails this test.
//
// Mutates process-wide environment, so it cannot run in parallel.
func TestBindEnvSkipsOwnedProfile(t *testing.T) {
	t.Setenv("PROFILE", "bare-leak")
	t.Setenv("PROBE_ONLY", "bound-value")

	set := pflag.NewFlagSet("probe", pflag.ContinueOnError)
	set.String("profile", "", "")    // owned -> must be skipped
	set.String("probe-only", "", "") // not owned -> control, must be bound

	bindFlagSetSkippingOwned(set)

	profile, err := set.GetString("profile")
	require.NoError(t, err)
	require.Empty(t, profile, "bare PROFILE must not populate the owned --profile flag")
	require.False(t, set.Changed("profile"), "owned --profile must not be marked changed by bare PROFILE")

	// Control: a non-owned flag IS bound from its bare env name, proving the
	// binder actually ran and the skip above is specific to the owned set.
	probe, err := set.GetString("probe-only")
	require.NoError(t, err)
	require.Equal(t, "bound-value", probe)
	require.True(t, set.Changed("probe-only"))
}

// TestRescaleFlagValidation guards the --rescale scale bound: it is a uint8, so
// pflag rejects values above an asset's max precision (255) at parse time, while
// bare --rescale and explicit values in range parse cleanly. "version" is a
// hermetic leaf that runs without opening a connection.
func TestRescaleFlagValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{"absent is fine", []string{"version"}, false},
		{"bare --rescale means scale 0", []string{"version", "--rescale"}, false},
		{"explicit zero", []string{"version", "--rescale=0"}, false},
		{"max in range", []string{"version", "--rescale=255"}, false},
		{"out of range", []string{"version", "--rescale=256"}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Hermetic config: empty temp dir so LoadConfig() returns a zero
			// Config on every platform (see TestServerFlagEnvResolution).
			tmp := t.TempDir()
			t.Setenv("HOME", tmp)
			t.Setenv("XDG_CONFIG_HOME", tmp)
			t.Setenv("APPDATA", tmp)
			t.Setenv("LOCALAPPDATA", tmp)

			root := newRootCommand()
			root.SilenceErrors = true
			root.SetArgs(tc.args)

			err := root.Execute()
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
