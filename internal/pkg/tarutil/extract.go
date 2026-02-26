package tarutil

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ExtractTar reads a tar stream from r and extracts all entries into targetDir.
// It creates directories and regular files, preserving permissions from the archive headers.
func ExtractTar(r io.Reader, targetDir string) error {
	tr := tar.NewReader(r)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading tar header: %w", err)
		}

		targetPath := filepath.Join(targetDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("creating directory %s: %w", targetPath, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
				return fmt.Errorf("creating parent directory for %s: %w", targetPath, err)
			}

			f, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("creating file %s: %w", targetPath, err)
			}

			if _, err := io.Copy(f, tr); err != nil {
				_ = f.Close()
				return fmt.Errorf("writing file %s: %w", targetPath, err)
			}

			if err := f.Close(); err != nil {
				return fmt.Errorf("closing file %s: %w", targetPath, err)
			}
		}
	}

	return nil
}
