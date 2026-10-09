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

	ledger "github.com/formancehq/ledger/misc/fctl-plugin"
)

func TestPublisherCreatesSixRealOCIArtifacts(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("oras"); err != nil {
		t.Fatal("run publisher integration tests in the repository-pinned Nix environment: ", err)
	}
	buildDirectory := t.TempDir()
	var artifacts []buildArtifact
	for _, goos := range []string{"linux", "darwin", "windows"} {
		for _, arch := range []string{"amd64", "arm64"} {
			path := filepath.Join(buildDirectory, goos+"_"+arch)
			command := exec.CommandContext(t.Context(), "go", "build", "-o", path,
				"-ldflags", "-X main.serviceVersion=3.0.0-beta.10 -X main.revision=2", "./cmd/fctl-plugin-ledger")
			command.Dir = "../.."
			command.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+arch, "CGO_ENABLED=0")
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("build %s/%s: %s, %v", goos, arch, output, err)
			}
			artifact := buildArtifact{Path: path, OS: goos, Arch: arch, Type: "Binary"}
			artifact.Extra.ID = "fctl-plugin-ledger"
			artifacts = append(artifacts, artifact)
		}
	}
	manifest, err := ledger.NewVersion(nil, "3.0.0-beta.10").GetManifest(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(buildDirectory, "manifest.json")
	writeJSON(t, manifestPath, manifest)
	artifactsPath := filepath.Join(buildDirectory, "artifacts.json")
	writeJSON(t, artifactsPath, artifacts)
	layout := filepath.Join(t.TempDir(), "oci")
	options := PublishOptions{ManifestPath: manifestPath, ArtifactsPath: artifactsPath, SourceRoot: buildDirectory,
		Registry: "http://127.0.0.1:5000", Repository: "formancehq/fctl-plugin-ledger", Revision: 2, Layout: layout}
	for _, failure := range []string{"stale manifest", "wrong revision", "mixed service versions", "wrong entry point"} {
		t.Run(failure, func(t *testing.T) {
			invalid := options
			invalid.Layout = filepath.Join(t.TempDir(), "must-not-publish")
			expectedError := "service version or plugin revision"
			switch failure {
			case "stale manifest":
				stale := manifest
				stale.Version = "3.0.999"
				invalid.ManifestPath = filepath.Join(t.TempDir(), "stale.json")
				writeJSON(t, invalid.ManifestPath, stale)
			case "wrong revision":
				invalid.Revision = 99
			case "mixed service versions":
				path := filepath.Join(buildDirectory, "wrong-windows-arm64")
				command := exec.CommandContext(t.Context(), "go", "build", "-o", path,
					"-ldflags", "-X main.serviceVersion=3.0.1 -X main.revision=2", "./cmd/fctl-plugin-ledger")
				command.Dir = "../.."
				command.Env = append(os.Environ(), "GOOS=windows", "GOARCH=arm64", "CGO_ENABLED=0")
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("build mismatched foreign executable: %s, %v", output, err)
				}
				mixed := append([]buildArtifact{}, artifacts...)
				mixed[len(mixed)-1].Path = path
				invalid.ArtifactsPath = filepath.Join(t.TempDir(), "mixed.json")
				writeJSON(t, invalid.ArtifactsPath, mixed)
			case "wrong entry point":
				path := filepath.Join(buildDirectory, "wrong-entry-point")
				command := exec.CommandContext(t.Context(), "go", "build", "-o", path,
					"-ldflags", "-X main.serviceVersion=3.0.0-beta.10 -X main.revision=2", "./cmd/fctl-plugin-catalogue")
				command.Dir = "../.."
				command.Env = append(os.Environ(), "GOOS=windows", "GOARCH=arm64", "CGO_ENABLED=0")
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("build wrong product entry point: %s, %v", output, err)
				}
				mixed := append([]buildArtifact{}, artifacts...)
				mixed[len(mixed)-1].Path = path
				invalid.ArtifactsPath = filepath.Join(t.TempDir(), "wrong-entry-point.json")
				writeJSON(t, invalid.ArtifactsPath, mixed)
				expectedError = "product entry point"
			}
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
		})
	}
	var output bytes.Buffer
	if err := Publish(t.Context(), options, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	var catalogue Catalogue
	if err := json.Unmarshal(output.Bytes(), &catalogue); err != nil || catalogue.SchemaVersion != 1 || len(catalogue.Releases) != 6 {
		t.Fatalf("catalogue=%s, err=%v", output.Bytes(), err)
	}
	root, err := os.OpenRoot(layout)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	for _, release := range catalogue.Releases {
		digest := strings.TrimPrefix(release.Artifact.Digest, "sha256:")
		data, err := root.ReadFile(filepath.Join("blobs", "sha256", digest))
		if err != nil {
			t.Fatal(err)
		}
		checksum := sha256.Sum256(data)
		if hex.EncodeToString(checksum[:]) != digest || release.ServiceVersion != "3.0.0-beta.10" || release.Revision != 2 {
			t.Fatalf("invalid immutable release identity: %#v", release)
		}
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
			image.Layers[0].MediaType != "application/vnd.formance.fctl.plugin.executable.v1" || image.Layers[0].Digest != "sha256:"+release.SHA256 {
			t.Fatalf("unexpected OCI manifest: %s", data)
		}
	}
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
