package upgrade

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// replaceBinary replaces the current binary with the new one at srcPath.
// It resolves symlinks to find the actual binary path and preserves permissions.
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

	if err := copyReplacement(srcPath, tmpPath, info.Mode()); err != nil {
		_ = os.Remove(tmpPath)

		return "", err
	}

	if err := replaceExecutable(currentPath, tmpPath, runtime.GOOS); err != nil {
		_ = os.Remove(tmpPath)

		return "", fmt.Errorf("replacing binary: %w (you may need elevated permissions)", err)
	}

	return currentPath, nil
}

func copyReplacement(srcPath, dstPath string, mode os.FileMode) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("opening new binary: %w", err)
	}

	defer func() { _ = src.Close() }()

	dst, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return fmt.Errorf("opening temp file: %w", err)
	}

	if _, err := dst.ReadFrom(src); err != nil {
		_ = dst.Close()

		return fmt.Errorf("writing new binary: %w", err)
	}

	if err := dst.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}

	if err := os.Chmod(dstPath, mode); err != nil {
		return fmt.Errorf("preserving binary permissions: %w", err)
	}

	return nil
}

func replaceExecutable(currentPath, replacementPath, goos string) error {
	if goos != "windows" {
		return os.Rename(replacementPath, currentPath)
	}

	backupPath := currentPath + ".old"
	if err := os.Remove(backupPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing previous backup: %w", err)
	}

	// Windows cannot replace a running executable in place. Renaming it keeps the
	// current process alive while freeing the original path for the new binary.
	if err := os.Rename(currentPath, backupPath); err != nil {
		return fmt.Errorf("backing up current executable: %w", err)
	}

	if err := os.Rename(replacementPath, currentPath); err != nil {
		if rollbackErr := os.Rename(backupPath, currentPath); rollbackErr != nil {
			return fmt.Errorf(
				"installing replacement: %w; restoring current executable from backup %q: %w; recover manually by renaming %q to %q",
				err,
				backupPath,
				rollbackErr,
				backupPath,
				currentPath,
			)
		}

		return fmt.Errorf("installing replacement: %w", err)
	}

	return nil
}
