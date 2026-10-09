//go:build it

package catalogue

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/formancehq/fctl/pkg/pluginsdk"

	ledger "github.com/formancehq/ledger/misc/fctl-plugin"
)

func TestPublisherPreflightAndFailureMatrix(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("oras"); err != nil {
		t.Fatal("run publisher integration tests in the repository-pinned Nix environment: ", err)
	}
	buildDirectory := t.TempDir()
	artifacts := buildPlatforms(t, buildDirectory)
	manifest, err := ledger.NewVersion(nil, "3.0.0-beta.10").GetManifest(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(buildDirectory, "manifest.json")
	writeFailureFixtureJSON(t, manifestPath, manifest)
	artifactsPath := filepath.Join(buildDirectory, "artifacts.json")
	writeFailureFixtureJSON(t, artifactsPath, artifacts)
	layout := filepath.Join(t.TempDir(), "oci")
	options := PublishOptions{ServiceVersion: manifest.Version, ManifestPath: manifestPath, ArtifactsPath: artifactsPath, SourceRoot: buildDirectory,
		Registry: "http://127.0.0.1:5000", Repository: "formancehq/fctl-plugin-ledger", Revision: 2, Layout: layout}
	for _, failure := range []string{"missing service version", "release tag not stripped", "stale manifest", "wrong manifest product", "wrong manifest service", "wrong manifest command", "wrong platform", "wrong revision", "mixed service versions", "mixed plugin revisions", "wrong entry point", "missing platform", "duplicate platform"} {
		t.Run(failure, func(t *testing.T) {
			invalid, expectedError := invalidPublishOptions(t, failure, options, manifest, artifacts, buildDirectory)
			assertPreflightRejected(t, invalid, expectedError)
		})
	}
	t.Run("ORAS invocation failure emits no catalogue", func(t *testing.T) {
		assertORASFailure(t, options)
	})
	var output bytes.Buffer
	if err := Publish(t.Context(), options, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	var catalogue Catalogue
	if err := json.Unmarshal(output.Bytes(), &catalogue); err != nil || catalogue.SchemaVersion != 1 || len(catalogue.Releases) != 6 {
		t.Fatalf("catalogue=%s, err=%v", output.Bytes(), err)
	}
	verifyLayout(t, layout, buildDirectory, options.ServiceVersion, catalogue)
}

func writeFailureFixtureJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func buildPlatforms(t *testing.T, buildDirectory string) []buildArtifact {
	t.Helper()
	var artifacts []buildArtifact
	for _, goos := range []string{"linux", "darwin", "windows"} {
		for _, arch := range []string{"amd64", "arm64"} {
			path := filepath.Join(buildDirectory, goos+"_"+arch)
			buildPlugin(t, path, goos, arch, "-X main.serviceVersion=3.0.0-beta.10 -X main.revision=2", "./cmd/fctl-plugin-ledger")
			artifact := buildArtifact{Path: path, OS: goos, Arch: arch, Type: "Binary"}
			artifact.Extra.ID = "fctl-plugin-ledger"
			artifacts = append(artifacts, artifact)
		}
	}

	return artifacts
}

func invalidPublishOptions(t *testing.T, failure string, options PublishOptions, manifest pluginsdk.Manifest, artifacts []buildArtifact, buildDirectory string) (PublishOptions, string) {
	t.Helper()
	invalid := options
	invalid.Layout = filepath.Join(t.TempDir(), "must-not-publish")
	expectedError := "service version or plugin revision"
	switch failure {
	case "missing service version":
		invalid.ServiceVersion = ""
		expectedError = "service version"
	case "release tag not stripped":
		invalid.ServiceVersion = "v" + manifest.Version
		expectedError = "service version"
	case "stale manifest":
		stale := manifest
		stale.Version = "3.0.999"
		invalid.ManifestPath = filepath.Join(t.TempDir(), "stale.json")
		writeFailureFixtureJSON(t, invalid.ManifestPath, stale)
		expectedError = "service version"
	case "wrong manifest product", "wrong manifest service", "wrong manifest command":
		wrong := manifest
		switch failure {
		case "wrong manifest product":
			wrong.Name = "auth"
		case "wrong manifest service":
			wrong.Service = "auth"
		case "wrong manifest command":
			wrong.Root.Use = "auth"
		}
		invalid.ManifestPath = filepath.Join(t.TempDir(), "wrong-product.json")
		writeFailureFixtureJSON(t, invalid.ManifestPath, wrong)
		expectedError = "manifest must describe"
	case "wrong platform":
		mixed := append([]buildArtifact{}, artifacts...)
		// The last binary claims windows/arm64 but contains linux/amd64.
		mixed[len(mixed)-1].Path = artifacts[0].Path
		invalid.ArtifactsPath = filepath.Join(t.TempDir(), "wrong-platform.json")
		writeFailureFixtureJSON(t, invalid.ArtifactsPath, mixed)
		expectedError = "platform or product entry point"
	case "missing platform":
		invalid.ArtifactsPath = filepath.Join(t.TempDir(), "missing-platform.json")
		writeFailureFixtureJSON(t, invalid.ArtifactsPath, artifacts[:len(artifacts)-1])
		expectedError = "expected six"
	case "duplicate platform":
		mixed := append([]buildArtifact{}, artifacts...)
		mixed = append(mixed, artifacts[0])
		invalid.ArtifactsPath = filepath.Join(t.TempDir(), "duplicate-platform.json")
		writeFailureFixtureJSON(t, invalid.ArtifactsPath, mixed)
		expectedError = "duplicate binary"
	case "wrong revision":
		invalid.Revision = 99
	case "mixed service versions", "mixed plugin revisions":
		path := filepath.Join(buildDirectory, strings.ReplaceAll(failure, " ", "-")+"-windows-arm64")
		linkFlags := "-X main.serviceVersion=3.0.1 -X main.revision=2"
		if failure == "mixed plugin revisions" {
			linkFlags = "-X main.serviceVersion=3.0.0-beta.10 -X main.revision=99"
		}
		buildPlugin(t, path, "windows", "arm64", linkFlags, "./cmd/fctl-plugin-ledger")
		mixed := append([]buildArtifact{}, artifacts...)
		mixed[len(mixed)-1].Path = path
		invalid.ArtifactsPath = filepath.Join(t.TempDir(), "mixed.json")
		writeFailureFixtureJSON(t, invalid.ArtifactsPath, mixed)
	case "wrong entry point":
		path := filepath.Join(buildDirectory, "wrong-entry-point")
		buildPlugin(t, path, "windows", "arm64", "-X main.serviceVersion=3.0.0-beta.10 -X main.revision=2", "./cmd/fctl-plugin-publish")
		mixed := append([]buildArtifact{}, artifacts...)
		mixed[len(mixed)-1].Path = path
		invalid.ArtifactsPath = filepath.Join(t.TempDir(), "wrong-entry-point.json")
		writeFailureFixtureJSON(t, invalid.ArtifactsPath, mixed)
		expectedError = "product entry point"
	}

	return invalid, expectedError
}

func assertORASFailure(t *testing.T, options PublishOptions) {
	t.Helper()
	invalid := options
	// A file cannot contain an OCI layout. Real ORAS must fail locally.
	invalid.Layout = filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(invalid.Layout, []byte("block layout creation"), 0o600); err != nil {
		t.Fatal(err)
	}
	var rejected, diagnostics bytes.Buffer
	err := Publish(t.Context(), invalid, &rejected, &diagnostics)
	if err == nil || !strings.Contains(err.Error(), "publish linux/amd64") {
		t.Fatalf("expected ORAS invocation failure, got %v (diagnostics: %s)", err, &diagnostics)
	}
	if rejected.Len() != 0 {
		t.Fatalf("failed ORAS advertised a catalogue: %s", &rejected)
	}
}

func verifyLayout(t *testing.T, layout, buildDirectory, version string, catalogue Catalogue) {
	t.Helper()
	root, err := os.OpenRoot(layout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	source := openTestRoot(t, buildDirectory)
	for _, release := range catalogue.Releases {
		verifyRelease(t, root, source, version, release)
	}
}

func verifyRelease(t *testing.T, root, source *os.Root, version string, release Release) {
	t.Helper()
	digest := strings.TrimPrefix(release.Artifact.Digest, "sha256:")
	data, err := root.ReadFile(filepath.Join("blobs", "sha256", digest))
	if err != nil {
		t.Fatal(err)
	}
	checksum := sha256.Sum256(data)
	if hex.EncodeToString(checksum[:]) != digest || release.ServiceVersion != "3.0.0-beta.10" || release.Revision != 2 {
		t.Fatalf("invalid immutable release identity: %#v", release)
	}
	verifyOCIManifest(t, data, release.SHA256)
	binary, err := root.ReadFile(filepath.Join("blobs", "sha256", release.SHA256))
	if err != nil {
		t.Fatal(err)
	}
	original, err := source.ReadFile(release.Platform.OS + "_" + release.Platform.Arch)
	if err != nil {
		t.Fatal(err)
	}
	checksum = sha256.Sum256(binary)
	if !bytes.Equal(binary, original) || hex.EncodeToString(checksum[:]) != release.SHA256 ||
		release.Service != "ledger" || release.Manifest.Name != "ledger" || release.Manifest.Version != version {
		t.Fatalf("OCI executable or Ledger manifest differs from release input: %#v", release)
	}
}

func verifyOCIManifest(t *testing.T, data []byte, checksum string) {
	t.Helper()
	var image struct {
		MediaType    string `json:"mediaType"`
		ArtifactType string `json:"artifactType"`
		Config       struct {
			MediaType string `json:"mediaType"`
		} `json:"config"`
		Layers []struct {
			MediaType string `json:"mediaType"`
			Digest    string `json:"digest"`
		} `json:"layers"`
	}
	if err := json.Unmarshal(data, &image); err != nil {
		t.Fatal(err)
	}
	if image.MediaType != "application/vnd.oci.image.manifest.v1+json" || image.ArtifactType != "application/vnd.formance.fctl.plugin.v1" ||
		image.Config.MediaType != "application/vnd.oci.empty.v1+json" || len(image.Layers) != 1 ||
		image.Layers[0].MediaType != "application/vnd.formance.fctl.plugin.executable.v1" || image.Layers[0].Digest != "sha256:"+checksum {
		t.Fatalf("unexpected OCI manifest: %s", data)
	}
}

func buildPlugin(t *testing.T, path, goos, arch, linkFlags, entryPoint string) {
	t.Helper()
	//nolint:gosec // Arguments are controlled test fixtures; go runs directly without a shell.
	command := exec.CommandContext(t.Context(), "go", "build", "-o", path, "-ldflags", linkFlags, entryPoint)
	command.Dir = "../.."
	command.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+arch, "CGO_ENABLED=0")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %s/%s: %s, %v", goos, arch, output, err)
	}
}

func openTestRoot(t *testing.T, path string) *os.Root {
	t.Helper()
	root, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})

	return root
}

func assertPreflightRejected(t *testing.T, invalid PublishOptions, expectedError string) {
	t.Helper()
	var rejected bytes.Buffer
	if err := Publish(t.Context(), invalid, &rejected, io.Discard); err == nil || !strings.Contains(err.Error(), expectedError) {
		t.Fatalf("wrong release identity accepted or wrong rejection: %v", err)
	}
	if rejected.Len() != 0 {
		t.Fatalf("failed preflight advertised a catalogue: %s", rejected.Bytes())
	}
	if _, err := os.Stat(invalid.Layout); !os.IsNotExist(err) {
		t.Fatalf("failed preflight reached ORAS publication: %v", err)
	}
}
