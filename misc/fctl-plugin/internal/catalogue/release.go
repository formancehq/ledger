// Package catalogue prepares product release entries for the fctl catalogue.
// Its wire fields follow catalogue schemaVersion 1 without importing the core.
package catalogue

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/formancehq/fctl/pkg/pluginsdk"
)

type Platform struct {
	OS   string `json:"os"`
	Arch string `json:"arch"`
}

type Artifact struct {
	Registry   string `json:"registry"`
	Repository string `json:"repository"`
	Digest     string `json:"digest"`
}

type Release struct {
	Service        string             `json:"service"`
	ServiceVersion string             `json:"serviceVersion"`
	Revision       int                `json:"revision"`
	Platform       Platform           `json:"platform"`
	Artifact       Artifact           `json:"artifact"`
	SHA256         string             `json:"sha256"`
	Manifest       pluginsdk.Manifest `json:"manifest"`
}

type Catalogue struct {
	SchemaVersion int       `json:"schemaVersion"`
	Releases      []Release `json:"releases"`
}

const (
	maxManifestBytes = 1 << 20
	maxBinaryBytes   = 128 << 20
)

var repositoryPattern = regexp.MustCompile(`^[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*$`)

// ReadManifest reads the SDK metadata exported by the native executable.
func ReadManifest(path string) (pluginsdk.Manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return pluginsdk.Manifest{}, err
	}
	defer func() { _ = file.Close() }() // Read-only resource; read failures are returned below.
	data, err := io.ReadAll(io.LimitReader(file, maxManifestBytes+1))
	if err != nil {
		return pluginsdk.Manifest{}, err
	}
	if len(data) > maxManifestBytes {
		return pluginsdk.Manifest{}, errors.New("plugin manifest exceeds 1 MiB")
	}
	var manifest pluginsdk.Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return pluginsdk.Manifest{}, fmt.Errorf("read SDK manifest: %w", err)
	}

	return manifest, nil
}

// NewRelease binds the actual raw binary checksum to an immutable OCI digest.
// Binary is an explicit operator-selected file, not a path from a manifest.
func NewRelease(manifest pluginsdk.Manifest, binary string, artifact Artifact, platform Platform, revision int) (Release, error) {
	if manifest.Name != "ledger" || manifest.Service != "ledger" || pluginsdk.CommandName(manifest.Root) != "ledger" ||
		manifest.ProtocolVersion != pluginsdk.ProtocolVersion {
		return Release{}, errors.New("manifest must describe the Ledger plugin and current SDK protocol")
	}
	if manifest.Version == "" || len(manifest.Version) > 128 || strings.TrimSpace(manifest.Version) != manifest.Version ||
		strings.ContainsAny(manifest.Version, "/\\\x00\r\n\t ") || revision < 1 {
		return Release{}, errors.New("exact service version and positive plugin revision are required")
	}
	if platform.OS != "linux" && platform.OS != "darwin" && platform.OS != "windows" ||
		platform.Arch != "amd64" && platform.Arch != "arm64" {
		return Release{}, errors.New("unsupported plugin platform")
	}
	if err := validateArtifact(artifact); err != nil {
		return Release{}, err
	}
	checksum, err := hashBinary(binary)
	if err != nil {
		return Release{}, err
	}

	return Release{Service: manifest.Service, ServiceVersion: manifest.Version, Revision: revision,
		Platform: platform, Artifact: artifact, SHA256: checksum, Manifest: manifest}, nil
}

func validateArtifact(artifact Artifact) error {
	origin, err := url.Parse(artifact.Registry)
	if err != nil || origin.Host == "" || origin.User != nil || origin.Path != "" || origin.RawQuery != "" || origin.Fragment != "" {
		return errors.New("registry must be an HTTPS origin without credentials")
	}
	ip := net.ParseIP(origin.Hostname())
	loopback := origin.Hostname() == "localhost" || ip != nil && ip.IsLoopback()
	if origin.Scheme != "https" && (origin.Scheme != "http" || !loopback) {
		return errors.New("registry requires HTTPS, except loopback development registries")
	}
	if len(artifact.Repository) > 255 || !repositoryPattern.MatchString(artifact.Repository) {
		return errors.New("invalid OCI repository")
	}
	checksum, ok := strings.CutPrefix(artifact.Digest, "sha256:")
	if !ok || len(checksum) != 64 || strings.ToLower(checksum) != checksum {
		return errors.New("artifact requires an immutable sha256 digest")
	}
	if _, err := hex.DecodeString(checksum); err != nil {
		return errors.New("artifact requires an immutable sha256 digest")
	}

	return nil
}

func hashBinary(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }() // Read-only resource; all read failures are returned.
	checksum := sha256.New()
	size, err := io.Copy(checksum, io.LimitReader(file, maxBinaryBytes+1))
	if err != nil {
		return "", err
	}
	if size == 0 || size > maxBinaryBytes {
		return "", errors.New("raw plugin binary must contain 1 byte to 128 MiB")
	}

	return hex.EncodeToString(checksum.Sum(nil)), nil
}
