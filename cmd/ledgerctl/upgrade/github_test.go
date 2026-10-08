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
		name    string
		version string
		goos    string
		goarch  string
		want    string
	}{
		{
			name:    "linux amd64",
			version: "v3.0.0",
			goos:    "linux",
			goarch:  "amd64",
			want:    "ledger_v3.0.0_linux-amd64.tar.gz",
		},
		{
			name:    "darwin arm64 prerelease",
			version: "v3.0.0-beta.0",
			goos:    "darwin",
			goarch:  "arm64",
			want:    "ledger_v3.0.0-beta.0_darwin-arm64.tar.gz",
		},
		{
			name:    "windows amd64 nightly",
			version: "nightly-deadbeef",
			goos:    "windows",
			goarch:  "amd64",
			want:    "ledger_nightly-deadbeef_windows-amd64.zip",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, test.want, archiveAssetName(test.version, test.goos, test.goarch))
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

	tests := []struct {
		name    string
		version string
	}{
		{name: "stable", version: "v3.0.0"},
		{name: "prerelease", version: "v3.0.0-beta.0"},
		{name: "nightly", version: "nightly-deadbeef"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			release := &releaseInfo{
				Assets: []assetInfo{
					{Name: "checksums.txt"},
					{Name: archiveAssetName(test.version, "linux", "amd64")},
					{Name: archiveAssetName(test.version, "windows", "amd64")},
					{Name: archiveAssetName(test.version, "windows", "arm64")},
				},
			}

			for _, platform := range [][2]string{{"linux", "amd64"}, {"windows", "amd64"}} {
				asset, err := findAssetForPlatform(release, platform[0], platform[1])
				require.NoError(t, err)
				require.Equal(t, archiveAssetName(test.version, platform[0], platform[1]), asset.Name)
			}
		})
	}
}

func TestFindAssetForPlatformRejectsUnversionedOrForeignAssets(t *testing.T) {
	t.Parallel()

	release := &releaseInfo{
		Assets: []assetInfo{
			{Name: "ledger_linux-amd64.tar.gz"},
			{Name: "ledger_benchmarks_v3.0.0_linux-amd64.tar.gz"},
			{Name: archiveAssetName("v3.0.0", "linux", "arm64")},
		},
	}

	_, err := findAssetForPlatform(release, "linux", "amd64")
	require.EqualError(t, err, `no binary available for linux/amd64 (expected asset "ledger_<version>_linux-amd64.tar.gz")`)
}

func TestFindAssetForPlatformRejectsAmbiguousAssets(t *testing.T) {
	t.Parallel()

	release := &releaseInfo{
		TagName: "v3.0.0",
		Assets: []assetInfo{
			{Name: archiveAssetName("v3.0.0", "linux", "amd64")},
			{Name: archiveAssetName("v3.0.1", "linux", "amd64")},
		},
	}

	_, err := findAssetForPlatform(release, "linux", "amd64")
	require.ErrorContains(t, err, "multiple binaries available for linux/amd64")
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
