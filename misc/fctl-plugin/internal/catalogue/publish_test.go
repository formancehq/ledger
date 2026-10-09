package catalogue

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ledger "github.com/formancehq/ledger/misc/fctl-plugin"
)

func TestArtifactPathsStayInsideGoReleaserBuildRoot(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	buildPath := filepath.Join(parent, "build")
	if err := os.Mkdir(buildPath, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(parent, "outside")
	if err := os.WriteFile(outside, []byte("must not publish this file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(buildPath, "escape")); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(buildPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	stagePath := t.TempDir()
	stage, err := os.OpenRoot(stagePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stage.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, path := range []string{outside, "../outside", "escape"} {
		artifact := buildArtifact{Path: path, OS: "linux", Arch: "amd64"}
		_, err := stageBinary(root, buildPath, stage, artifact, "3.0.0-beta.10", 2)
		if err == nil || strings.Contains(err.Error(), "build identity") {
			t.Fatalf("path %q reached binary inspection outside root: %v", path, err)
		}
	}
	entries, err := os.ReadDir(stagePath)
	if err != nil || len(entries) != 0 {
		t.Fatalf("rejected artifact produced staging files: %v, %v", entries, err)
	}
}

func TestPublishRequiresExactServiceVersionBeforeReadingArtifacts(t *testing.T) {
	t.Parallel()
	manifest, err := ledger.NewVersion(nil, "3.0.0-beta.10").GetManifest(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"", "3.0.1", "v3.0.0-beta.10"} {
		t.Run("version="+version, func(t *testing.T) {
			var output bytes.Buffer
			options := PublishOptions{ManifestPath: path, ServiceVersion: version, ArtifactsPath: "does-not-exist"}
			if err := Publish(t.Context(), options, &output, io.Discard); err == nil || !strings.Contains(err.Error(), "service version") {
				t.Fatalf("expected exact service version rejection before reading artifacts, got %v", err)
			}
			if output.Len() != 0 {
				t.Fatalf("rejected release advertised catalogue: %s", &output)
			}
		})
	}
}
