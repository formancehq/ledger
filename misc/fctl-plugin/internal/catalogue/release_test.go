package catalogue

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ledger "github.com/formancehq/ledger/misc/fctl-plugin"
)

func TestReleaseUsesSDKManifestAndActualRawBinaryChecksum(t *testing.T) {
	t.Parallel()
	manifest, err := ledger.NewVersion(nil, "3.0.0-beta.5").GetManifest(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	binary := filepath.Join(directory, "binary")
	const data = "synthetic native executable bytes"
	if err := os.WriteFile(binary, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	checksum := sha256.Sum256([]byte(data))
	artifact := Artifact{Registry: "https://ghcr.io", Repository: "formancehq/fctl-plugin-ledger", Digest: "sha256:" + strings.Repeat("a", 64)}
	release, err := NewRelease(manifest, binary, artifact, Platform{OS: "windows", Arch: "arm64"}, 2)
	if err != nil || release.ServiceVersion != "3.0.0-beta.5" || release.Revision != 2 || release.SHA256 != hex.EncodeToString(checksum[:]) {
		t.Fatalf("release=%#v, err=%v", release, err)
	}
	encoded, err := json.Marshal(Catalogue{SchemaVersion: 1, Releases: []Release{release}})
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &wire); err != nil || string(wire["schemaVersion"]) != "1" || wire["releases"] == nil {
		t.Fatalf("catalogue wire=%s, err=%v", encoded, err)
	}
	manifestPath := filepath.Join(directory, "manifest.json")
	encoded, err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	read, err := ReadManifest(manifestPath)
	if err != nil || read.Version != manifest.Version || len(read.Root.Subcommands) != len(manifest.Root.Subcommands) {
		t.Fatalf("read manifest=%#v, err=%v", read, err)
	}
}

func TestImmutableArtifactValidation(t *testing.T) {
	t.Parallel()
	valid := Artifact{Registry: "https://ghcr.io", Repository: "formancehq/fctl-plugin-ledger", Digest: "sha256:" + strings.Repeat("a", 64)}
	for _, registry := range []string{"https://ghcr.io", "http://localhost:5000", "http://127.0.0.1:5000", "http://[::1]:5000"} {
		artifact := valid
		artifact.Registry = registry
		if err := validateArtifact(artifact); err != nil {
			t.Errorf("valid registry %s: %v", registry, err)
		}
	}
	for _, mutate := range []func(*Artifact){
		func(a *Artifact) { a.Digest = "latest" },
		func(a *Artifact) { a.Digest = "sha256:" + strings.Repeat("A", 64) },
		func(a *Artifact) { a.Digest = "sha256:" + strings.Repeat("x", 64) },
		func(a *Artifact) { a.Registry = "http://registry.example" },
		func(a *Artifact) { a.Registry = "https://token@ghcr.io" },
		func(a *Artifact) { a.Registry = "https://ghcr.io/api" },
		func(a *Artifact) { a.Repository = "../secret" },
	} {
		artifact := valid
		mutate(&artifact)
		if err := validateArtifact(artifact); err == nil {
			t.Fatalf("accepted invalid artifact: %#v", artifact)
		}
	}
}
