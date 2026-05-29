package grpc

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

const (
	// defaultChunkSize is the size of each chunk sent in streaming RPCs (64KB).
	defaultChunkSize = 64 * 1024
)

// buildManifest walks dirPath and returns a manifest listing every regular file
// with its relative path, size, and SHA256 hash.
func buildManifest(dirPath string) (*snapshotpb.SnapshotManifest, error) {
	var files []*snapshotpb.FileEntry

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(dirPath, path)
		if err != nil {
			return fmt.Errorf("computing relative path for %s: %w", path, err)
		}

		hash, err := hashFile(path)
		if err != nil {
			return fmt.Errorf("hashing %s: %w", relPath, err)
		}

		files = append(files, &snapshotpb.FileEntry{
			Path:   relPath,
			Size:   uint64(info.Size()),
			Sha256: hash,
		})

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walking checkpoint directory: %w", err)
	}

	return &snapshotpb.SnapshotManifest{Files: files}, nil
}

// streamOneFile reads a single file in chunks and sends them via send.
func streamOneFile(
	dirPath string,
	relPath string,
	buf []byte,
	send func(*snapshotpb.FetchFileResponse) error,
) error {
	f, err := os.Open(filepath.Join(dirPath, relPath))
	if err != nil {
		return err
	}

	defer func() {
		_ = f.Close()
	}()

	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			resp := &snapshotpb.FetchFileResponse{
				Data: buf[:n],
			}

			// If we also hit EOF on this read, mark it as the last chunk.
			if readErr == io.EOF {
				resp.Eof = true
			}

			if err := send(resp); err != nil {
				return err
			}

			if readErr == io.EOF {
				return nil
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				// Empty file: send a single EOF chunk.
				return send(&snapshotpb.FetchFileResponse{Eof: true})
			}

			return readErr
		}
	}
}

// hashFile computes the SHA256 hex digest of a file.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}

	defer func() {
		_ = f.Close()
	}()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
