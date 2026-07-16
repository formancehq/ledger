package cmdutil

import (
	"context"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// newConnCmd builds a leaf command carrying the same connection/security flags
// ledgerctl declares as root persistent flags, so ResolveConnectionFlags can be
// exercised in isolation without standing up the whole command tree.
func newConnCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "list"}
	cmd.Flags().String("profile", "", "")
	cmd.Flags().String("server", "localhost:8888", "")
	cmd.Flags().Bool("insecure", false, "")
	cmd.Flags().String("tls-ca-cert", "", "")
	cmd.Flags().String("tls-server-name", "", "")
	cmd.Flags().String("consistency", "", "")
	cmd.Flags().String("auth-token", "", "")
	cmd.Flags().String("signing-key", "", "")
	cmd.Flags().String("signing-key-id", "", "")
	cmd.Flags().String("response-verify-key", "", "")
	cmd.Flags().String("result-file", "", "")

	return cmd
}

// emptyKeyring is a Keyring that never has a token, so GetContext's auth-token
// fallback resolves to "" without touching the OS keychain.
type emptyKeyring struct{}

func (emptyKeyring) Get(string) (string, error) { return "", ErrTokenNotFound }
func (emptyKeyring) Set(string, string) error   { return nil }
func (emptyKeyring) Delete(string) error        { return nil }

// TestResolveConnectionFlagsAppliesActiveProfile asserts the shared resolver
// pulls the connection settings from the profile named by --profile. This is
// the behavior the root PersistentPreRunE and every shell-completion function
// rely on.
func TestResolveConnectionFlagsAppliesActiveProfile(t *testing.T) {
	isolateConfigDir(t)
	t.Setenv("LEDGERCTL_PROFILE", "")
	t.Setenv("LEDGERCTL_SERVER", "")

	require.NoError(t, SaveConfig(Config{
		Profiles: map[string]Profile{
			"dev": {Server: "dev.example.com:443", Insecure: true},
		},
	}))

	cmd := newConnCmd()
	require.NoError(t, cmd.Flags().Set("profile", "dev"))

	require.NoError(t, ResolveConnectionFlags(cmd))

	server, _ := cmd.Flags().GetString("server")
	require.Equal(t, "dev.example.com:443", server, "server must come from the active profile")

	insecure, _ := cmd.Flags().GetBool("insecure")
	require.True(t, insecure, "insecure must come from the active profile")
}

// TestResolveConnectionFlagsAppliesServerName asserts the profile's
// tlsServerName reaches the --tls-server-name flag through the shared resolver,
// so a profile can pin the verification hostname without repeating it on every
// invocation.
func TestResolveConnectionFlagsAppliesServerName(t *testing.T) {
	isolateConfigDir(t)
	t.Setenv("LEDGERCTL_PROFILE", "")
	t.Setenv("LEDGERCTL_TLS_SERVER_NAME", "")

	require.NoError(t, SaveConfig(Config{
		Profiles: map[string]Profile{
			"prod": {Server: "10.0.0.5:8888", TLSServerName: "ledger.svc.cluster.local"},
		},
	}))

	cmd := newConnCmd()
	require.NoError(t, cmd.Flags().Set("profile", "prod"))

	require.NoError(t, ResolveConnectionFlags(cmd))

	serverName, _ := cmd.Flags().GetString("tls-server-name")
	require.Equal(t, "ledger.svc.cluster.local", serverName, "tls-server-name must come from the active profile")
}

// TestResolveConnectionFlagsExplicitFlagWins guards the precedence: an explicit
// --server on the command line is never overwritten by the profile value.
func TestResolveConnectionFlagsExplicitFlagWins(t *testing.T) {
	isolateConfigDir(t)
	t.Setenv("LEDGERCTL_PROFILE", "")
	t.Setenv("LEDGERCTL_SERVER", "")

	require.NoError(t, SaveConfig(Config{
		Profiles: map[string]Profile{
			"dev": {Server: "dev.example.com:443"},
		},
	}))

	cmd := newConnCmd()
	require.NoError(t, cmd.Flags().Set("profile", "dev"))
	require.NoError(t, cmd.Flags().Set("server", "cli.example.com:443"))

	require.NoError(t, ResolveConnectionFlags(cmd))

	server, _ := cmd.Flags().GetString("server")
	require.Equal(t, "cli.example.com:443", server, "explicit --server must win over the profile")
}

// TestResolveConnectionFlagsUnknownProfile asserts an explicitly named profile
// that does not exist is a hard error (matching command execution).
func TestResolveConnectionFlagsUnknownProfile(t *testing.T) {
	isolateConfigDir(t)
	t.Setenv("LEDGERCTL_PROFILE", "")

	require.NoError(t, SaveConfig(Config{
		Profiles: map[string]Profile{
			"dev": {Server: "dev.example.com:443"},
		},
	}))

	cmd := newConnCmd()
	require.NoError(t, cmd.Flags().Set("profile", "ghost"))

	err := ResolveConnectionFlags(cmd)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ghost")
}

// TestResolveConnectionFlagsSkipsCompletionCommand asserts the resolver is a
// no-op when invoked for the cobra completion helper command. cobra runs the
// root PersistentPreRunE with that command during `__complete`; resolving there
// would apply the active profile to the shared persistent flags and block the
// real per-completion resolution from honoring the explicit --profile.
func TestResolveConnectionFlagsSkipsCompletionCommand(t *testing.T) {
	isolateConfigDir(t)
	t.Setenv("LEDGERCTL_PROFILE", "")
	t.Setenv("LEDGERCTL_SERVER", "")

	require.NoError(t, SaveConfig(Config{
		ActiveProfile: "acme",
		Profiles: map[string]Profile{
			"acme": {Server: "active.example.com:443"},
		},
	}))

	cmd := newConnCmd()
	cmd.Use = cobra.ShellCompRequestCmd // simulate the __complete helper command

	require.NoError(t, ResolveConnectionFlags(cmd))

	server, _ := cmd.Flags().GetString("server")
	require.Equal(t, "localhost:8888", server, "completion helper command must not resolve the active profile")
	require.False(t, cmd.Flags().Changed("server"), "server must stay unchanged so per-completion resolution can set it")
}

// TestCompleteLedgerNamesResolvesProfileServer is the regression guard for the
// reported bug: `ledgerctl __complete --profile dev ... --ledger=` listed
// ledgers from the default server because cobra skips PersistentPreRunE during
// completion. CompleteLedgerNames must run the profile resolution itself.
//
// We point the profile at an unreachable address with a tiny timeout so the
// connection fails fast, and assert the --server flag was resolved from the
// profile (the resolution happens before the dial, so its effect is observable
// regardless of the connection outcome). Before the fix, --server stayed at the
// default localhost:8888.
func TestCompleteLedgerNamesResolvesProfileServer(t *testing.T) {
	isolateConfigDir(t)
	t.Setenv("LEDGERCTL_PROFILE", "")
	t.Setenv("LEDGERCTL_SERVER", "")

	require.NoError(t, SaveConfig(Config{
		Profiles: map[string]Profile{
			"dev": {Server: "127.0.0.1:1", Insecure: true},
		},
	}))

	cmd := newConnCmd()
	cmd.Flags().Duration("timeout", 100*time.Millisecond, "")
	require.NoError(t, cmd.Flags().Set("profile", "dev"))

	// Inject an empty keyring so the auth-token fallback in GetContext never
	// touches the OS keychain.
	cmd.SetContext(context.WithValue(context.Background(), contextKeyKeyring{}, emptyKeyring{}))

	// Connection will fail (nothing listens on 127.0.0.1:1); we only care that
	// the resolution ran. The directive on failure is NoFileComp.
	_, directive := CompleteLedgerNames(cmd, nil, "")
	require.Equal(t, cobra.ShellCompDirectiveNoFileComp, directive)

	server, _ := cmd.Flags().GetString("server")
	require.Equal(t, "127.0.0.1:1", server, "completion must resolve --server from the active profile")
}
