package cmdutil

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// isolateConfigDir redirects os.UserConfigDir to a temp directory so the test
// never touches the developer's real ledgerctl config. It sets both HOME
// (used on macOS) and XDG_CONFIG_HOME (used on Linux) to keep the test
// cross-platform. Env mutation precludes t.Parallel.
func isolateConfigDir(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("XDG_CONFIG_HOME", dir)
}

func TestCompleteProfileNames(t *testing.T) {
	t.Run("returns sorted profile names", func(t *testing.T) {
		isolateConfigDir(t)

		require.NoError(t, SaveConfig(Config{
			ActiveProfile: "prod",
			Profiles: map[string]Profile{
				"prod":    {Server: "prod:8888"},
				"staging": {Server: "stg:8888"},
				"local":   {Server: "localhost:8888"},
			},
		}))

		names, directive := CompleteProfileNames(&cobra.Command{}, nil, "")

		require.Equal(t, []string{"local", "prod", "staging"}, names)
		require.Equal(t, cobra.ShellCompDirectiveNoFileComp, directive)
	})

	t.Run("missing config yields no suggestions", func(t *testing.T) {
		isolateConfigDir(t)

		names, directive := CompleteProfileNames(&cobra.Command{}, nil, "")

		require.Empty(t, names)
		require.Equal(t, cobra.ShellCompDirectiveNoFileComp, directive)
	})
}
