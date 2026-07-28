package upgrade

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchiveAssetName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		goos   string
		goarch string
		want   string
	}{
		{
			name:   "linux amd64",
			goos:   "linux",
			goarch: "amd64",
			want:   "ledger_linux-amd64.tar.gz",
		},
		{
			name:   "darwin arm64",
			goos:   "darwin",
			goarch: "arm64",
			want:   "ledger_darwin-arm64.tar.gz",
		},
		{
			name:   "windows amd64",
			goos:   "windows",
			goarch: "amd64",
			want:   "ledger_windows-amd64.zip",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, test.want, archiveAssetName(test.goos, test.goarch))
		})
	}
}

func TestExecutableName(t *testing.T) {
	t.Parallel()

	require.Equal(t, "ledgerctl", executableName("linux"))
	require.Equal(t, "ledgerctl", executableName("darwin"))
	require.Equal(t, "ledgerctl.exe", executableName("windows"))
}

func TestFindAssetForPlatform(t *testing.T) {
	t.Parallel()

	release := &releaseInfo{
		Assets: []assetInfo{
			{Name: "checksums.txt"},
			{Name: archiveAssetName("linux", "amd64")},
			{Name: archiveAssetName("windows", "amd64")},
		},
	}

	asset, err := findAssetForPlatform(release, "windows", "amd64")
	require.NoError(t, err)
	require.Equal(t, archiveAssetName("windows", "amd64"), asset.Name)
}
