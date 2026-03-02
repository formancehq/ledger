package upgrade

import (
	"fmt"
	"os"
	"path/filepath"
)

// replaceBinary atomically replaces the current binary with the new one at srcPath.
// It resolves symlinks to find the actual binary path, preserves permissions,
// and uses rename for atomicity (requires same filesystem).
func replaceBinary(srcPath string) (string, error) {
	currentPath, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("finding current binary: %w", err)
	}

	currentPath, err = filepath.EvalSymlinks(currentPath)
	if err != nil {
		return "", fmt.Errorf("resolving symlinks: %w", err)
	}

	// Preserve original permissions.
	info, err := os.Stat(currentPath)
	if err != nil {
		return "", fmt.Errorf("reading binary permissions: %w", err)
	}

	// Create temp file in the same directory to ensure same filesystem for atomic rename.
	dir := filepath.Dir(currentPath)
	tmpFile, err := os.CreateTemp(dir, ".ledgerctl-upgrade-*")
	if err != nil {
		return "", fmt.Errorf("creating temp file for replacement: %w (you may need elevated permissions)", err)
	}
	tmpPath := tmpFile.Name()
	_ = tmpFile.Close()

	// Copy new binary to the temp file.
	src, err := os.Open(srcPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("opening new binary: %w", err)
	}
	defer func() { _ = src.Close() }()

	dst, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_TRUNC, info.Mode())
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("opening temp file: %w", err)
	}

	if _, err := dst.ReadFrom(src); err != nil {
		_ = dst.Close()
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("writing new binary: %w", err)
	}

	if err := dst.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("closing temp file: %w", err)
	}

	// Atomic rename.
	if err := os.Rename(tmpPath, currentPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("replacing binary: %w (you may need elevated permissions)", err)
	}

	return currentPath, nil
}
