package auth

import (
	"context"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
)

// newTestCmd assembles a bare login-like cobra command carrying the
// persistent flags PersistentPreRunE would normally hand down from root
// (--profile, --server, --tls-ca-cert, --insecure, --signing-key,
// --signing-key-id, --response-verify-key) plus the local --key-id auth
// login declares. syncProfile reads them directly off cmd.Flags(), so we
// skip the full root wiring and drive it in isolation.
func newTestCmd(t *testing.T) *cobra.Command {
	t.Helper()

	cmd := &cobra.Command{Use: "login"}
	cmd.Flags().String("profile", "", "")
	cmd.Flags().String("server", "localhost:8888", "")
	cmd.Flags().Bool("insecure", false, "")
	cmd.Flags().String("tls-ca-cert", "", "")
	cmd.Flags().String("signing-key", "", "")
	cmd.Flags().String("signing-key-id", "", "")
	cmd.Flags().String("response-verify-key", "", "")
	cmd.Flags().String("key-id", "", "")
	cmd.SetContext(context.Background())

	return cmd
}

// pinConfig points cmdutil.LoadConfig() / SaveConfig() at a hermetic temp
// directory so syncProfile round-trips through a real config.json without
// touching the user's environment.
func pinConfig(t *testing.T) {
	t.Helper()

	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("XDG_CONFIG_HOME", tmp)
	t.Setenv("APPDATA", tmp)
	t.Setenv("LOCALAPPDATA", tmp)
	t.Setenv("LEDGERCTL_PROFILE", "")
}

func TestSyncProfile_BootstrapsMissingProfile(t *testing.T) {
	pinConfig(t)

	cmd := newTestCmd(t)
	require.NoError(t, cmd.ParseFlags([]string{
		"--profile", "prod",
		"--server", "prod.example.com:8888",
		"--signing-key", "/keys/prod.hex",
		"--signing-key-id", "prod-key",
		"--tls-ca-cert", "/tls/ca.pem",
	}))

	require.NoError(t, syncProfile(cmd, "prod.example.com:8888"))

	cfg, err := cmdutil.LoadConfig()
	require.NoError(t, err)

	profile, ok := cfg.Profiles["prod"]
	require.True(t, ok, "auth login --profile <new> must persist the profile")
	require.Equal(t, "prod.example.com:8888", profile.Server)
	require.Equal(t, "/keys/prod.hex", profile.SigningKey)
	require.Equal(t, "prod-key", profile.SigningKeyID)
	require.Equal(t, "/tls/ca.pem", profile.TLSCaCert)
	require.Equal(t, "prod", cfg.ActiveProfile,
		"first profile must be activated")
}

func TestSyncProfile_UpdatesServerOnExplicitFlag(t *testing.T) {
	pinConfig(t)

	// Seed an existing profile.
	seed := cmdutil.Config{
		ActiveProfile: "prod",
		Profiles: map[string]cmdutil.Profile{
			"prod": {Server: "old.example.com:8888"},
		},
	}
	require.NoError(t, cmdutil.SaveConfig(seed))

	cmd := newTestCmd(t)
	require.NoError(t, cmd.ParseFlags([]string{
		"--profile", "prod",
		"--server", "new.example.com:8888",
	}))

	require.NoError(t, syncProfile(cmd, "new.example.com:8888"))

	cfg, err := cmdutil.LoadConfig()
	require.NoError(t, err)
	require.Equal(t, "new.example.com:8888", cfg.Profiles["prod"].Server,
		"explicit --server must rewrite the profile so the keychain lookup finds the just-stored token")
}

func TestSyncProfile_DoesNotRewriteOnEnvOnlyServer(t *testing.T) {
	pinConfig(t)

	seed := cmdutil.Config{
		ActiveProfile: "prod",
		Profiles: map[string]cmdutil.Profile{
			"prod": {Server: "prod.example.com:8888"},
		},
	}
	require.NoError(t, cmdutil.SaveConfig(seed))

	// Simulate PersistentPreRunE having resolved --server from
	// LEDGERCTL_SERVER: value present, Changed=false (that's what
	// resolveFlag does now).
	cmd := newTestCmd(t)
	require.NoError(t, cmd.Flags().Lookup("server").Value.Set("env.example.com:1234"))
	require.NoError(t, cmd.Flags().Lookup("profile").Value.Set("prod"))

	require.False(t, cmd.Flags().Changed("server"),
		"guard: env-derived server must leave Changed=false")

	require.NoError(t, syncProfile(cmd, "env.example.com:1234"))

	cfg, err := cmdutil.LoadConfig()
	require.NoError(t, err)
	require.Equal(t, "prod.example.com:8888", cfg.Profiles["prod"].Server,
		"env-only server divergence must not rewrite the profile")
}

func TestSyncProfile_NoProfileFlagIsNoOp(t *testing.T) {
	pinConfig(t)

	cmd := newTestCmd(t)
	require.NoError(t, cmd.ParseFlags([]string{
		"--server", "some.example.com:8888",
	}))

	require.NoError(t, syncProfile(cmd, "some.example.com:8888"))

	cfg, err := cmdutil.LoadConfig()
	require.NoError(t, err)
	require.Empty(t, cfg.Profiles,
		"auth login without --profile must not touch profiles")
}

func TestSyncProfile_UpdatesActiveProfileWhenFlagOmitted(t *testing.T) {
	pinConfig(t)

	// Existing active profile.
	seed := cmdutil.Config{
		ActiveProfile: "prod",
		Profiles: map[string]cmdutil.Profile{
			"prod": {Server: "old.example.com:8888"},
		},
	}
	require.NoError(t, cmdutil.SaveConfig(seed))

	// User re-logs into a new address without --profile: syncProfile must
	// still update the active profile, otherwise the just-stored token sits
	// under new.example.com:9999 but every command with the active profile
	// resolves old.example.com:8888 and cannot find the token.
	cmd := newTestCmd(t)
	require.NoError(t, cmd.ParseFlags([]string{
		"--server", "new.example.com:9999",
	}))

	require.NoError(t, syncProfile(cmd, "new.example.com:9999"))

	cfg, err := cmdutil.LoadConfig()
	require.NoError(t, err)
	require.Equal(t, "new.example.com:9999", cfg.Profiles["prod"].Server,
		"active profile must track the server the token was just stored under")
}

func TestSyncProfile_BootstrapUsesKeyIDForSigningKeyID(t *testing.T) {
	pinConfig(t)

	// User bootstraps with --key-id (the local auth login flag) but does
	// not set the persistent --signing-key-id: the profile must still
	// persist a signingKeyId so a later `auth login --profile prod` can
	// use the profile-derived --key-id fallback.
	cmd := newTestCmd(t)
	require.NoError(t, cmd.ParseFlags([]string{
		"--profile", "prod",
		"--server", "prod.example.com:8888",
		"--signing-key", "/keys/prod.hex",
		"--key-id", "prod-key",
	}))

	require.NoError(t, syncProfile(cmd, "prod.example.com:8888"))

	cfg, err := cmdutil.LoadConfig()
	require.NoError(t, err)
	require.Equal(t, "prod-key", cfg.Profiles["prod"].SigningKeyID,
		"--key-id must be persisted as signingKeyId when --signing-key-id is not set")
}

func TestSyncProfile_BootstrapPrefersSigningKeyIDOverKeyID(t *testing.T) {
	pinConfig(t)

	// If both flags are set they should typically match; if they don't, the
	// explicit --signing-key-id takes precedence — that's the flag whose
	// name matches the persisted field.
	cmd := newTestCmd(t)
	require.NoError(t, cmd.ParseFlags([]string{
		"--profile", "prod",
		"--server", "prod.example.com:8888",
		"--signing-key", "/keys/prod.hex",
		"--signing-key-id", "explicit-signing-id",
		"--key-id", "jwt-only-id",
	}))

	require.NoError(t, syncProfile(cmd, "prod.example.com:8888"))

	cfg, err := cmdutil.LoadConfig()
	require.NoError(t, err)
	require.Equal(t, "explicit-signing-id", cfg.Profiles["prod"].SigningKeyID)
}

func TestSyncProfile_ActiveButDeletedIsNotResurrected(t *testing.T) {
	pinConfig(t)

	// A stale activeProfile pointer with no matching entry: this is a broken
	// state we should not silently repair on the next login.
	seed := cmdutil.Config{ActiveProfile: "ghost"}
	require.NoError(t, cmdutil.SaveConfig(seed))

	cmd := newTestCmd(t)
	require.NoError(t, cmd.ParseFlags([]string{
		"--server", "some.example.com:8888",
	}))

	require.NoError(t, syncProfile(cmd, "some.example.com:8888"))

	cfg, err := cmdutil.LoadConfig()
	require.NoError(t, err)
	require.Empty(t, cfg.Profiles,
		"a dangling activeProfile must not be resurrected by auth login")
}
