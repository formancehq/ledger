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
			want:   "ledger-v3_linux-amd64.tar.gz",
		},
		{
			name:   "darwin arm64",
			goos:   "darwin",
			goarch: "arm64",
			want:   "ledger-v3_darwin-arm64.tar.gz",
		},
		{
			name:   "windows amd64",
			goos:   "windows",
			goarch: "amd64",
			want:   "ledger-v3_windows-amd64.zip",
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
			{Name: "ledger-v3_linux-amd64.tar.gz"},
			{Name: "ledger-v3_windows-amd64.zip"},
		},
	}

	asset, err := findAssetForPlatform(release, "windows", "amd64")
	require.NoError(t, err)
	require.Equal(t, "ledger-v3_windows-amd64.zip", asset.Name)
}
