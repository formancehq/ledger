package grpc

import (
	"context"
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
// with its relative path and size. File contents are deliberately not read:
// their SHA-256 digest is computed once while FetchFile streams them.
func buildManifest(ctx context.Context, dirPath string) (*snapshotpb.SnapshotManifest, error) {
	var files []*snapshotpb.FileEntry

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err := ctx.Err(); err != nil {
			return err
		}

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

		files = append(files, &snapshotpb.FileEntry{
			Path: relPath,
			Size: uint64(info.Size()),
		})

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walking checkpoint directory: %w", err)
	}

	return &snapshotpb.SnapshotManifest{Files: files}, nil
}

// streamOneFile reads a single file beneath dirPath in chunks and sends it via
// send. The rooted open is load-bearing: a network-supplied path or symlink
// must not escape the prepared snapshot checkpoint.
func streamOneFile(
	dirPath string,
	relPath string,
	buf []byte,
	send func(*snapshotpb.FetchFileResponse) error,
) error {
	root, err := os.OpenRoot(dirPath)
	if err != nil {
		return err
	}

	defer func() {
		_ = root.Close()
	}()

	f, err := root.Open(relPath)
	if err != nil {
		return err
	}

	defer func() {
		_ = f.Close()
	}()

	hash := sha256.New()

	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			if _, err := hash.Write(buf[:n]); err != nil {
				return fmt.Errorf("hashing %s: %w", relPath, err)
			}

			resp := &snapshotpb.FetchFileResponse{
				Data: buf[:n],
			}

			// If we also hit EOF on this read, mark it as the last chunk.
			if readErr == io.EOF {
				resp.Eof = true
				resp.Sha256 = hex.EncodeToString(hash.Sum(nil))
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
				return send(&snapshotpb.FetchFileResponse{
					Eof:    true,
					Sha256: hex.EncodeToString(hash.Sum(nil)),
				})
			}

			return readErr
		}
	}
}
