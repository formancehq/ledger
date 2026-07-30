package upgrade

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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

func TestFindStableRelease(t *testing.T) {
	t.Parallel()

	releases := []releaseInfo{
		{TagName: "v2.4.12"},
		{TagName: "v3.0.0-alpha.13", Prerelease: true},
		{TagName: "v3.0.0-rc.1"},
		{TagName: "v3.0.0", Draft: true},
		{TagName: "v3.0.0"},
	}

	release := findStableRelease(releases, 3)
	require.NotNil(t, release)
	require.Equal(t, "v3.0.0", release.TagName)
}

func TestFindStableReleaseRejectsOtherMajorsAndPrereleases(t *testing.T) {
	t.Parallel()

	releases := []releaseInfo{
		{TagName: "v2.4.12"},
		{TagName: "v3.0.0-alpha.13", Prerelease: true},
	}

	require.Nil(t, findStableRelease(releases, 3))
}

func TestFetchStableReleaseFromURLPaginates(t *testing.T) {
	t.Parallel()

	firstPage := make([]releaseInfo, 100)
	for i := range firstPage {
		firstPage[i] = releaseInfo{TagName: fmt.Sprintf("v2.4.%d", i)}
	}

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)

		var releases []releaseInfo
		switch r.URL.Query().Get("page") {
		case "1":
			releases = firstPage
		case "2":
			releases = []releaseInfo{{TagName: "v3.0.0"}}
		default:
			require.Fail(t, "unexpected releases page", r.URL.String())
		}

		require.NoError(t, json.NewEncoder(w).Encode(releases))
	}))
	t.Cleanup(server.Close)

	release, err := fetchStableReleaseFromURL(
		"nightly-deadbeef",
		"github.com/formancehq/ledger/v3",
		server.URL,
	)
	require.NoError(t, err)
	require.Equal(t, "v3.0.0", release.TagName)
	require.EqualValues(t, 2, requests.Load())
}

func TestVersionMajor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		currentVersion string
		modulePath     string
		want           uint64
		wantErr        string
	}{
		{
			name:           "semantic prerelease",
			currentVersion: "v4.0.0-alpha.1",
			modulePath:     "github.com/formancehq/ledger/v3",
			want:           4,
		},
		{
			name:           "nightly module fallback",
			currentVersion: "nightly-deadbeef",
			modulePath:     "github.com/formancehq/ledger/v3",
			want:           3,
		},
		{
			name:           "missing module major",
			currentVersion: "nightly-deadbeef",
			modulePath:     "github.com/formancehq/ledger",
			wantErr:        "has no major suffix",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			major, err := versionMajor(test.currentVersion, test.modulePath)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)

				return
			}

			require.NoError(t, err)
			require.Equal(t, test.want, major)
		})
	}
}
