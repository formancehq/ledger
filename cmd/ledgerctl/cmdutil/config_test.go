package cmdutil

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestConfigDirUsesFormanceNamespace(t *testing.T) {
	isolateConfigDir(t)

	base, err := os.UserConfigDir()
	require.NoError(t, err)

	dir, err := ConfigDir()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(base, "formance", "ledgerctl"), dir)
}

func TestLoadConfigIgnoresLegacyPath(t *testing.T) {
	isolateConfigDir(t)

	base, err := os.UserConfigDir()
	require.NoError(t, err)

	legacyDir := filepath.Join(base, "ledgerctl")
	require.NoError(t, os.MkdirAll(legacyDir, 0o700))
	require.NoError(t, os.WriteFile(
		filepath.Join(legacyDir, "config.json"),
		[]byte(`{"activeProfile":"legacy","profiles":{"legacy":{"server":"legacy:8888"}}}`),
		0o600,
	))

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Empty(t, cfg.ActiveProfile)
	require.Empty(t, cfg.Profiles)
}

func TestSaveConfigPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not available on Windows")
	}

	isolateConfigDir(t)

	require.NoError(t, SaveConfig(Config{
		ActiveProfile: "prod",
		Profiles: map[string]Profile{
			"prod": {Server: "prod:8888"},
		},
	}))

	dir, err := ConfigDir()
	require.NoError(t, err)
	path, err := ConfigPath()
	require.NoError(t, err)

	dirInfo, err := os.Stat(dir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())

	fileInfo, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fileInfo.Mode().Perm())
}

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
