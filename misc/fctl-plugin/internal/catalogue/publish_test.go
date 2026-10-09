package catalogue

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	defer func() { _ = root.Close() }()
	stagePath := t.TempDir()
	stage, err := os.OpenRoot(stagePath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stage.Close() }()
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
