package upgrade

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"

	"github.com/Masterminds/semver/v3"
	"golang.org/x/mod/module"
)

const (
	githubRepo  = "formancehq/ledger"
	projectName = "ledger"
)

type releaseInfo struct {
	TagName         string      `json:"tag_name"`
	Name            string      `json:"name"`
	TargetCommitish string      `json:"target_commitish"`
	Draft           bool        `json:"draft"`
	Prerelease      bool        `json:"prerelease"`
	Assets          []assetInfo `json:"assets"`
}

type assetInfo struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

// fetchRelease fetches the release info for the given channel.
// For "nightly", it fetches the release tagged "nightly".
// For "stable", it fetches the most recent final release matching the running
// binary's major version.
func fetchRelease(channel, currentVersion string) (*releaseInfo, error) {
	switch channel {
	case "nightly":
		return fetchNightlyRelease()
	case "stable":
		return fetchStableRelease(currentVersion)
	default:
		return nil, fmt.Errorf("unknown channel %q; use \"nightly\" or \"stable\"", channel)
	}
}

func fetchNightlyRelease() (*releaseInfo, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/releases/tags/nightly", githubRepo)

	var release releaseInfo

	err := githubGet(url, &release)
	if err != nil {
		return nil, err
	}

	return &release, nil
}

func fetchStableRelease(currentVersion string) (*releaseInfo, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/releases", githubRepo)
	modulePath := ""
	if buildInfo, ok := debug.ReadBuildInfo(); ok {
		modulePath = buildInfo.Main.Path
	}

	return fetchStableReleaseFromURL(currentVersion, modulePath, url)
}

func fetchStableReleaseFromURL(currentVersion, modulePath, releasesURL string) (*releaseInfo, error) {
	currentMajor, err := versionMajor(currentVersion, modulePath)
	if err != nil {
		return nil, fmt.Errorf(
			"cannot select a stable release for current version %q: %w; use --channel nightly",
			currentVersion,
			err,
		)
	}

	const releasesPerPage = 100

	for page := 1; ; page++ {
		url := fmt.Sprintf("%s?per_page=%d&page=%d", releasesURL, releasesPerPage, page)

		var releases []releaseInfo
		if err := githubGet(url, &releases); err != nil {
			return nil, err
		}

		if release := findStableRelease(releases, currentMajor); release != nil {
			return release, nil
		}

		if len(releases) < releasesPerPage {
			break
		}
	}

	return nil, fmt.Errorf(
		"no final release found for major v%d; use --channel nightly",
		currentMajor,
	)
}

func versionMajor(currentVersion, modulePath string) (uint64, error) {
	current, versionErr := semver.NewVersion(currentVersion)
	if versionErr == nil {
		return current.Major(), nil
	}

	_, pathMajor, ok := module.SplitPathVersion(modulePath)
	if !ok || pathMajor == "" {
		return 0, fmt.Errorf(
			"version is not semantic and module path %q has no major suffix: %w",
			modulePath,
			versionErr,
		)
	}

	major, err := strconv.ParseUint(strings.TrimPrefix(pathMajor, "/v"), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing module major %q: %w", pathMajor, err)
	}

	return major, nil
}

func findStableRelease(releases []releaseInfo, currentMajor uint64) *releaseInfo {
	for i := range releases {
		release := &releases[i]
		if release.Draft || release.Prerelease {
			continue
		}

		releaseVersion, err := semver.StrictNewVersion(strings.TrimPrefix(release.TagName, "v"))
		if err != nil || releaseVersion.Prerelease() != "" {
			continue
		}

		if releaseVersion.Major() == currentMajor {
			return release
		}
	}

	return nil
}

var (
	githubToken     string
	githubTokenOnce sync.Once
)

// resolveGitHubToken returns a GitHub token from GITHUB_TOKEN env var,
// falling back to `gh auth token` if the CLI is installed.
func resolveGitHubToken() string {
	githubTokenOnce.Do(func() {
		if t := os.Getenv("GITHUB_TOKEN"); t != "" {
			githubToken = t

			return
		}

		out, err := exec.Command("gh", "auth", "token").Output()
		if err == nil {
			githubToken = strings.TrimSpace(string(out))
		}
	})

	return githubToken
}

// setGitHubAuth adds the Authorization header if a token is available.
func setGitHubAuth(req *http.Request) {
	if token := resolveGitHubToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
}

func githubGet(url string, target any) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Accept", "application/vnd.github+json")
	setGitHubAuth(req)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetching %s: %w", url, err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusForbidden {
		return errors.New("GitHub API rate limit exceeded; try again later or set GITHUB_TOKEN")
	}

	if resp.StatusCode == http.StatusNotFound {
		if resolveGitHubToken() == "" {
			return fmt.Errorf("GitHub API returned 404 for %s (the repo may be private; set GITHUB_TOKEN or run `gh auth login`)", url)
		}

		return fmt.Errorf("GitHub API returned 404 for %s (check that the token has access to the repo)", url)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GitHub API returned %s for %s", resp.Status, url)
	}

	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}

	return nil
}

// githubDownload performs an HTTP GET with GitHub authentication (if available)
// and returns the response. The caller must close the response body.
func githubDownload(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Accept", "application/octet-stream")
	setGitHubAuth(req)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching %s: %w", url, err)
	}

	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()

		return nil, fmt.Errorf("HTTP %s for %s", resp.Status, url)
	}

	return resp, nil
}

// archiveAssetName returns the expected archive filename for an OS/arch pair.
func archiveAssetName(goos, goarch string) string {
	extension := ".tar.gz"
	if goos == "windows" {
		extension = ".zip"
	}

	return fmt.Sprintf("%s_%s-%s%s", projectName, goos, goarch, extension)
}

func executableName(goos string) string {
	if goos == "windows" {
		return "ledgerctl.exe"
	}

	return "ledgerctl"
}

// findAsset finds the archive asset matching the current OS/arch in the release.
func findAsset(release *releaseInfo) (*assetInfo, error) {
	return findAssetForPlatform(release, runtime.GOOS, runtime.GOARCH)
}

func findAssetForPlatform(release *releaseInfo, goos, goarch string) (*assetInfo, error) {
	want := archiveAssetName(goos, goarch)
	for i := range release.Assets {
		if release.Assets[i].Name == want {
			return &release.Assets[i], nil
		}
	}

	return nil, fmt.Errorf("no binary available for %s/%s (expected asset %q)", goos, goarch, want)
}

// findChecksumsAsset finds the checksums.txt asset in the release.
func findChecksumsAsset(release *releaseInfo) (*assetInfo, error) {
	for i := range release.Assets {
		if release.Assets[i].Name == "checksums.txt" {
			return &release.Assets[i], nil
		}
	}

	return nil, fmt.Errorf("no checksums.txt asset found in release %s", release.TagName)
}

// releaseVersion returns the display version for a release.
// For nightly: "nightly-<shortcommit>" from the release name.
// For stable: the tag name.
func releaseVersion(release *releaseInfo) string {
	return release.Name
}

// isUpToDate checks if the current version matches the release version.
func isUpToDate(currentVersion string, release *releaseInfo) bool {
	rv := releaseVersion(release)
	// Normalize: strip leading "v" for comparison.
	return strings.TrimPrefix(currentVersion, "v") == strings.TrimPrefix(rv, "v")
}
