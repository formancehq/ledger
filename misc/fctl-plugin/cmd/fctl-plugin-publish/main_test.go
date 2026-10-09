package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ledger "github.com/formancehq/ledger/misc/fctl-plugin"
)

func TestRunHelpDocumentsPublisherFlags(t *testing.T) {
	t.Parallel()
	var output, diagnostics bytes.Buffer
	if err := run([]string{"--help"}, &output, &diagnostics); err != nil {
		t.Fatal(err)
	}
	for _, flag := range []string{"service-version", "manifest", "artifacts", "source-root", "registry", "repository", "layout", "revision"} {
		if !strings.Contains(diagnostics.String(), "-"+flag) {
			t.Errorf("help omits --%s: %s", flag, &diagnostics)
		}
	}
	if !strings.Contains(diagnostics.String(), "formancehq/fctl-plugin-ledger") || output.Len() != 0 {
		t.Fatalf("help must document Ledger repository on diagnostics only: output=%s diagnostics=%s", &output, &diagnostics)
	}
}

func TestRunRejectsInvalidArgumentsWithoutCatalogue(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{{"--unknown"}, {"--revision", "invalid"}, {"unexpected"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var output, diagnostics bytes.Buffer
			if err := run(args, &output, &diagnostics); err == nil {
				t.Fatal("invalid arguments accepted")
			}
			if output.Len() != 0 {
				t.Fatalf("invalid arguments advertised catalogue: %s", &output)
			}
		})
	}
}

func TestRunRequiresServiceVersionAndPassesExactVersionToPublisher(t *testing.T) {
	t.Parallel()
	manifest, err := ledger.NewVersion(nil, "3.0.0-beta.10").GetManifest(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	manifestPath := filepath.Join(directory, "manifest.json")
	if err := os.WriteFile(manifestPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	artifactsPath := filepath.Join(directory, "artifacts.json")
	if err := os.WriteFile(artifactsPath, []byte("[]"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"", "3.0.1", "v3.0.0-beta.10", manifest.Version} {
		t.Run("version="+version, func(t *testing.T) {
			assertPublisherVersion(t, directory, manifestPath, artifactsPath, version, manifest.Version)
		})
	}
}

func assertPublisherVersion(t *testing.T, directory, manifestPath, artifactsPath, version, manifestVersion string) {
	t.Helper()
	layout := filepath.Join(t.TempDir(), "must-not-publish")
	args := []string{"--manifest", manifestPath, "--artifacts", artifactsPath, "--source-root", directory,
		"--registry", "https://ghcr.io", "--repository", "formancehq/fctl-plugin-ledger", "--revision", "2", "--layout", layout}
	if version != "" {
		args = append(args, "--service-version", version)
	}
	var output, diagnostics bytes.Buffer
	err := run(args, &output, &diagnostics)
	want := "service version"
	if version == manifestVersion {
		// Matching version must reach artifact preflight, proving flag wiring.
		want = "expected six"
	}
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("expected %q error, got %v", want, err)
	}
	if output.Len() != 0 {
		t.Fatalf("failed preflight advertised catalogue: %s", &output)
	}
	if _, err := os.Stat(layout); !os.IsNotExist(err) {
		t.Fatalf("failed preflight reached publication: %v", err)
	}
}
