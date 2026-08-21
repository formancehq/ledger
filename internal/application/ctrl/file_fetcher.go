package ctrl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

const (
	fileInitialBackoff = 200 * time.Millisecond
	fileMaxBackoff     = 5 * time.Second
)

// fileFetcher fetches a single file from a snapshot session via gRPC.
type fileFetcher struct {
	client     snapshotpb.SnapshotServiceClient
	sessionID  string
	maxRetries int
}

// fetchFile streams a single file, writes it atomically (.tmp → rename), and
// verifies the SHA256. Retries up to maxRetries on transient errors.
func (f *fileFetcher) fetchFile(ctx context.Context, entry *snapshotpb.FileEntry, targetDir string, progress *state.SyncProgress) error {
	for attempt := range f.maxRetries {
		err := f.fetchFileOnce(ctx, entry, targetDir, progress)
		if err == nil {
			return nil
		}

		if !isRetryableError(err) || attempt == f.maxRetries-1 {
			return err
		}

		delay := fileInitialBackoff
		for range attempt {
			delay *= 2
			if delay > fileMaxBackoff {
				delay = fileMaxBackoff

				break
			}
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("file fetch failed after %d attempts: %s", f.maxRetries, entry.GetPath())
}

func (f *fileFetcher) fetchFileOnce(ctx context.Context, entry *snapshotpb.FileEntry, targetDir string, progress *state.SyncProgress) error {
	if !filepath.IsLocal(entry.GetPath()) {
		return fmt.Errorf("invalid snapshot path: %q", entry.GetPath())
	}

	root, err := os.OpenRoot(targetDir)
	if err != nil {
		return fmt.Errorf("opening snapshot target directory: %w", err)
	}

	defer func() {
		_ = root.Close()
	}()

	stream, err := f.client.FetchFile(ctx, &snapshotpb.FetchFileRequest{
		SessionId: f.sessionID,
		Path:      entry.GetPath(),
	})
	if err != nil {
		return fmt.Errorf("opening stream for %s: %w", entry.GetPath(), err)
	}

	tmpPath := entry.GetPath() + ".tmp"
	if err := root.MkdirAll(filepath.Dir(tmpPath), 0755); err != nil {
		return fmt.Errorf("creating parent directory for %s: %w", entry.GetPath(), err)
	}

	tmpFile, err := root.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("creating temp file for %s: %w", entry.GetPath(), err)
	}

	hash := sha256.New()
	var received uint64
	var expectedHash string

	defer func() {
		_ = tmpFile.Close()
		_ = root.Remove(tmpPath)
	}()

	for {
		resp, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("receiving chunk for %s: %w", entry.GetPath(), err)
		}

		if len(resp.GetData()) > 0 {
			received += uint64(len(resp.GetData()))
			if received > entry.GetSize() {
				return fmt.Errorf("size mismatch for %s: expected %d bytes, got at least %d", entry.GetPath(), entry.GetSize(), received)
			}

			if _, err := tmpFile.Write(resp.GetData()); err != nil {
				return fmt.Errorf("writing chunk for %s: %w", entry.GetPath(), err)
			}

			if _, err := hash.Write(resp.GetData()); err != nil {
				return fmt.Errorf("hashing chunk for %s: %w", entry.GetPath(), err)
			}

			if progress != nil {
				progress.AddReceived(uint64(len(resp.GetData())))
			}
		}

		if resp.GetEof() {
			expectedHash = resp.GetSha256()

			break
		}
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("closing temp file for %s: %w", entry.GetPath(), err)
	}

	if received != entry.GetSize() {
		return fmt.Errorf("size mismatch for %s: expected %d bytes, got %d", entry.GetPath(), entry.GetSize(), received)
	}

	expectedDigest, err := hex.DecodeString(expectedHash)
	if err != nil || len(expectedDigest) != sha256.Size {
		return fmt.Errorf("invalid or missing SHA-256 digest for %s", entry.GetPath())
	}

	gotHash := hex.EncodeToString(hash.Sum(nil))
	if expectedHash != gotHash {
		return fmt.Errorf("hash mismatch for %s: expected %s, got %s", entry.GetPath(), expectedHash, gotHash)
	}

	if err := root.Rename(tmpPath, entry.GetPath()); err != nil {
		return fmt.Errorf("renaming %s: %w", entry.GetPath(), err)
	}

	if progress != nil {
		progress.AddFileCompleted()
	}

	return nil
}
