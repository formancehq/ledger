package upgrade

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"runtime"
	"strings"
)

const (
	githubRepo = "formancehq/ledger-v3-poc"
	projectName = "ledger-v3"
)

type releaseInfo struct {
	TagName         string      `json:"tag_name"`
	Name            string      `json:"name"`
	TargetCommitish string      `json:"target_commitish"`
	Assets          []assetInfo `json:"assets"`
}

type assetInfo struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

// fetchRelease fetches the release info for the given channel.
// For "nightly", it fetches the release tagged "nightly".
// For "stable", it fetches the most recent release matching a semver tag.
func fetchRelease(channel string) (*releaseInfo, error) {
	switch channel {
	case "nightly":
		return fetchNightlyRelease()
	case "stable":
		return fetchStableRelease()
	default:
		return nil, fmt.Errorf("unknown channel %q; use \"nightly\" or \"stable\"", channel)
	}
}

func fetchNightlyRelease() (*releaseInfo, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/releases/tags/nightly", githubRepo)

	var release releaseInfo
	if err := githubGet(url, &release); err != nil {
		return nil, err
	}

	return &release, nil
}

var semverTagRe = regexp.MustCompile(`^v\d+\.\d+\.\d+`)

func fetchStableRelease() (*releaseInfo, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/releases?per_page=20", githubRepo)

	var releases []releaseInfo
	if err := githubGet(url, &releases); err != nil {
		return nil, err
	}

	for i := range releases {
		if semverTagRe.MatchString(releases[i].TagName) {
			return &releases[i], nil
		}
	}

	return nil, fmt.Errorf("no stable release found; use --channel nightly")
}

func githubGet(url string, target any) error {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetching %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("GitHub API rate limit exceeded; try again later")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GitHub API returned %s for %s", resp.Status, url)
	}

	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}

	return nil
}

// archiveAssetName returns the expected archive filename for the current OS/arch.
func archiveAssetName() string {
	return fmt.Sprintf("%s_%s-%s.tar.gz", projectName, runtime.GOOS, runtime.GOARCH)
}

// findAsset finds the archive asset matching the current OS/arch in the release.
func findAsset(release *releaseInfo) (*assetInfo, error) {
	want := archiveAssetName()
	for i := range release.Assets {
		if release.Assets[i].Name == want {
			return &release.Assets[i], nil
		}
	}

	return nil, fmt.Errorf("no binary available for %s/%s (expected asset %q)", runtime.GOOS, runtime.GOARCH, want)
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
