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
	stream, err := f.client.FetchFile(ctx, &snapshotpb.FetchFileRequest{
		SessionId: f.sessionID,
		Path:      entry.GetPath(),
	})
	if err != nil {
		return fmt.Errorf("opening stream for %s: %w", entry.GetPath(), err)
	}

	tmpPath := filepath.Join(targetDir, entry.GetPath()+".tmp")
	if err := os.MkdirAll(filepath.Dir(tmpPath), 0755); err != nil {
		return fmt.Errorf("creating parent directory for %s: %w", entry.GetPath(), err)
	}

	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("creating temp file for %s: %w", entry.GetPath(), err)
	}

	hash := sha256.New()

	defer func() {
		_ = tmpFile.Close()
	}()

	for {
		resp, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("receiving chunk for %s: %w", entry.GetPath(), err)
		}

		if len(resp.GetData()) > 0 {
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
			break
		}
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("closing temp file for %s: %w", entry.GetPath(), err)
	}

	gotHash := hex.EncodeToString(hash.Sum(nil))
	if entry.GetSha256() != gotHash {
		return fmt.Errorf("hash mismatch for %s: expected %s, got %s", entry.GetPath(), entry.GetSha256(), gotHash)
	}

	finalPath := filepath.Join(targetDir, entry.GetPath())
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("renaming %s: %w", entry.GetPath(), err)
	}

	if progress != nil {
		progress.AddFileCompleted()
	}

	return nil
}
