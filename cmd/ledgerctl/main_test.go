package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
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
// The wantChanged column also asserts that resolveFlag does NOT light the
// flag's Changed bit for env-derived values: auth login's syncProfile treats
// Changed("server") as "the user typed --server on the CLI", and a spurious
// Changed=true would silently rewrite the active profile's server whenever
// LEDGERCTL_SERVER is set.
//
// These cases mutate process-wide environment (env vars + config dir), so they
// cannot run in parallel.
func TestServerFlagEnvResolution(t *testing.T) {
	const defaultServer = "localhost:8888"

	tests := []struct {
		name        string
		envName     string
		envValue    string
		cliServer   string // when non-empty, passed as --server on the command line
		wantServer  string
		wantChanged bool
	}{
		{
			name:        "LEDGERCTL_SERVER env, no CLI flag, resolves to env value with Changed=false",
			envName:     "LEDGERCTL_SERVER",
			envValue:    "env.example.com:443",
			wantServer:  "env.example.com:443",
			wantChanged: false,
		},
		{
			name:       "bare SERVER env, no CLI flag, is ignored and falls back to default",
			envName:    "SERVER",
			envValue:   "bare.example.com:443",
			wantServer: defaultServer,
		},
		{
			name:        "CLI --server wins over LEDGERCTL_SERVER env with Changed=true",
			envName:     "LEDGERCTL_SERVER",
			envValue:    "env.example.com:443",
			cliServer:   "cli.example.com:443",
			wantServer:  "cli.example.com:443",
			wantChanged: true,
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

			require.Equal(t, tc.wantChanged, root.Flags().Changed("server"),
				"Changed(\"server\") must reflect CLI-passed intent, not env resolution")
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
		{
			// key-id is owned so auth's resolveKeyID can trust Changed("key-id")
			// as a strict CLI-typed signal; a stray KEY_ID must not let env
			// impersonate a CLI flag.
			name:    "auth generate-token --key-id ignores bare KEY_ID",
			path:    []string{"auth", "generate-token"},
			flag:    "key-id",
			bareEnv: "KEY_ID",
		},
		{
			name:    "auth login --key-id ignores bare KEY_ID",
			path:    []string{"auth", "login"},
			flag:    "key-id",
			bareEnv: "KEY_ID",
		},
		{
			name:    "signing revoke-key --key-id ignores bare KEY_ID",
			path:    []string{"signing", "revoke-key"},
			flag:    "key-id",
			bareEnv: "KEY_ID",
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

// TestProfileSigningKeyIDReachesAuthCommandsViaSigningKeyID asserts the
// profile.signingKeyId fallback chain that lets `auth login --profile <name>`
// succeed without an explicit --key-id: PersistentPreRunE populates the
// persistent --signing-key-id flag from the profile, and auth's resolveKeyID
// reads it as the JWT key ID. --key-id itself is deliberately NOT
// pre-populated here — doing so would clobber a bare KEY_ID env value that
// bindSubcommandEnv applied first (both are Changed=false, so the two
// sources cannot be told apart at this layer).
//
// Mutates process-wide environment, so it cannot run in parallel.
func TestProfileSigningKeyIDReachesAuthCommandsViaSigningKeyID(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("XDG_CONFIG_HOME", tmp)
	t.Setenv("APPDATA", tmp)
	t.Setenv("LOCALAPPDATA", tmp)
	t.Setenv("LEDGERCTL_PROFILE", "")
	t.Setenv("KEY_ID", "")

	require.NoError(t, cmdutil.SaveConfig(cmdutil.Config{
		ActiveProfile: "prod",
		Profiles: map[string]cmdutil.Profile{
			"prod": {
				Server:       "prod.example.com:8888",
				SigningKeyID: "prod-key-id",
			},
		},
	}))

	root := newRootCommand()
	root.SilenceErrors = true

	bindSubcommandEnv(root)

	// Stub RunE on `auth login` so we can trigger PersistentPreRunE
	// without hitting the real login flow (which would try to sign a JWT).
	loginCmd, _, err := root.Find([]string{"auth", "login"})
	require.NoError(t, err)
	loginCmd.RunE = func(_ *cobra.Command, _ []string) error { return nil }

	root.SetArgs([]string{"auth", "login"})
	require.NoError(t, root.Execute())

	// --signing-key-id must carry the profile value — that's what
	// resolveKeyID reads as the fallback JWT key ID.
	sk, err := loginCmd.Flags().GetString("signing-key-id")
	require.NoError(t, err)
	require.Equal(t, "prod-key-id", sk,
		"profile.signingKeyId must populate --signing-key-id when neither CLI --signing-key-id nor LEDGERCTL_SIGNING_KEY_ID env is set")
	require.False(t, loginCmd.Flags().Changed("signing-key-id"),
		"profile-derived --signing-key-id must leave Changed=false")

	// --key-id must NOT be pre-populated from the profile: that write
	// would clobber a bindSubcommandEnv-derived bare KEY_ID env value.
	keyID, err := loginCmd.Flags().GetString("key-id")
	require.NoError(t, err)
	require.Empty(t, keyID,
		"--key-id must NOT be pre-populated from the profile — resolveKeyID handles the fallback via --signing-key-id")
}

// TestProfileSigningKeyIDReachesGenerateTokenViaSigningKeyID mirrors the
// login case: `auth generate-token --profile <name>` must inherit the
// profile.signingKeyId via --signing-key-id, and resolveKeyID picks it up.
// generate-token isn't tested via the full RunE (which needs a signing key
// on disk) — asserting the flag value is enough because the whole
// resolveKeyID / signToken chain is covered by resolveLoginParams tests.
//
// Mutates process-wide environment, so it cannot run in parallel.
func TestProfileSigningKeyIDReachesGenerateTokenViaSigningKeyID(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("XDG_CONFIG_HOME", tmp)
	t.Setenv("APPDATA", tmp)
	t.Setenv("LOCALAPPDATA", tmp)
	t.Setenv("LEDGERCTL_PROFILE", "")
	t.Setenv("KEY_ID", "")

	require.NoError(t, cmdutil.SaveConfig(cmdutil.Config{
		ActiveProfile: "prod",
		Profiles: map[string]cmdutil.Profile{
			"prod": {Server: "prod.example.com:8888", SigningKeyID: "prod-key-id"},
		},
	}))

	root := newRootCommand()
	root.SilenceErrors = true

	bindSubcommandEnv(root)

	genCmd, _, err := root.Find([]string{"auth", "generate-token"})
	require.NoError(t, err)
	genCmd.RunE = func(_ *cobra.Command, _ []string) error { return nil }

	root.SetArgs([]string{"auth", "generate-token"})
	require.NoError(t, root.Execute())

	sk, err := genCmd.Flags().GetString("signing-key-id")
	require.NoError(t, err)
	require.Equal(t, "prod-key-id", sk,
		"profile.signingKeyId must populate --signing-key-id on `auth generate-token`")
}

// TestProfileSigningKeyIDDoesNotFeedSigningKeyIDToSigningCommands guards the
// scoping of the profile.signingKeyId -> --key-id fallback: `signing
// revoke-key` and `signing register-key` share the --key-id flag name but
// operate on the key store — silently defaulting them to the active
// profile's signingKeyId would let `ledgerctl signing revoke-key` (no args)
// revoke the current signing key.
//
// Mutates process-wide environment, so it cannot run in parallel.
func TestProfileSigningKeyIDDoesNotFeedSigningCommands(t *testing.T) {
	for _, path := range [][]string{
		{"signing", "revoke-key"},
		{"signing", "register-key"},
	} {
		t.Run(path[1], func(t *testing.T) {
			tmp := t.TempDir()
			t.Setenv("HOME", tmp)
			t.Setenv("XDG_CONFIG_HOME", tmp)
			t.Setenv("APPDATA", tmp)
			t.Setenv("LOCALAPPDATA", tmp)
			t.Setenv("LEDGERCTL_PROFILE", "")
			t.Setenv("KEY_ID", "")

			require.NoError(t, cmdutil.SaveConfig(cmdutil.Config{
				ActiveProfile: "prod",
				Profiles: map[string]cmdutil.Profile{
					"prod": {Server: "prod.example.com:8888", SigningKeyID: "prod-key-id"},
				},
			}))

			root := newRootCommand()
			root.SilenceErrors = true

			bindSubcommandEnv(root)

			cmd, _, err := root.Find(path)
			require.NoError(t, err)
			cmd.RunE = func(_ *cobra.Command, _ []string) error { return nil }

			root.SetArgs(path)
			require.NoError(t, root.Execute())

			got, err := cmd.Flags().GetString("key-id")
			require.NoError(t, err)
			require.Empty(t, got,
				"signing/%s must not inherit --key-id from the active profile", path[1])
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
