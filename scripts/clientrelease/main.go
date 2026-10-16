package main

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/mod/module"
	"golang.org/x/mod/sumdb/dirhash"
	modzip "golang.org/x/mod/zip"
)

func verify(repoRoot, clientCommit, version, downloadedDir, moduleChecksum string) error {
	moduleVersion := module.Version{
		Path:    "github.com/formancehq/ledger/pkg/client/v3",
		Version: version,
	}
	expectedZip, err := os.CreateTemp("", "ledger-client-tag-*.zip")
	if err != nil {
		return err
	}
	defer func() {
		// Cleanup errors must not replace the verification result.
		_ = expectedZip.Close()
		_ = os.Remove(expectedZip.Name())
	}()

	// CreateFromVCS applies Go module ZIP exclusions, including vendor and
	// nested modules, to the exact client tag commit rather than the worktree.
	if err := modzip.CreateFromVCS(expectedZip, moduleVersion, repoRoot, clientCommit, "pkg/client/v3"); err != nil {
		return fmt.Errorf("create module archive from client tag: %w", err)
	}
	if err := expectedZip.Close(); err != nil {
		return err
	}
	expectedHash, err := dirhash.HashZip(expectedZip.Name(), dirhash.Hash1)
	if err != nil {
		return err
	}
	actualHash, err := dirhash.HashDir(downloadedDir, moduleVersion.Path+"@"+version, dirhash.Hash1)
	if err != nil {
		return err
	}
	if actualHash != expectedHash || moduleChecksum != expectedHash {
		return fmt.Errorf("downloaded module source differs from client tag: tag=%s directory=%s checksum=%s", expectedHash, actualHash, moduleChecksum)
	}

	return nil
}

func main() {
	if len(os.Args) != 6 {
		fmt.Fprintln(os.Stderr, "usage: clientrelease <repo-root> <client-commit> <client-version> <downloaded-dir> <module-checksum>")
		os.Exit(2)
	}
	if err := verify(filepath.Clean(os.Args[1]), os.Args[2], os.Args[3], filepath.Clean(os.Args[4]), os.Args[5]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
