package actions

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/restorepb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// CheckStoreResult holds the errors and progress events from a CheckStore RPC call.
type CheckStoreResult struct {
	Errors   []*servicepb.CheckStoreError
	Progress []*servicepb.CheckStoreProgress
}

// CollectCheckStoreEvents runs the CheckStore RPC and returns all errors and progress events.
func CollectCheckStoreEvents(ctx context.Context, client servicepb.BucketServiceClient) (*CheckStoreResult, error) {
	stream, err := client.CheckStore(ctx, &servicepb.CheckStoreRequest{})
	if err != nil {
		return nil, err
	}

	result := &CheckStoreResult{}
	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		switch t := event.GetType().(type) {
		case *servicepb.CheckStoreEvent_Error:
			result.Errors = append(result.Errors, t.Error)
		case *servicepb.CheckStoreEvent_Progress:
			result.Progress = append(result.Progress, t.Progress)
		}
	}

	return result, nil
}

// StreamBackup runs the Backup RPC and writes the tar archive to the provided writer.
// Returns the total bytes written and the content SHA-256 reported by the server.
func StreamBackup(ctx context.Context, client clusterpb.ClusterServiceClient, w io.Writer) (uint64, string, error) {
	stream, err := client.Backup(ctx, &clusterpb.BackupRequest{})
	if err != nil {
		return 0, "", err
	}

	var (
		totalBytes    uint64
		contentSha256 string
	)
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, "", err
		}

		if len(resp.GetData()) > 0 {
			if _, err := w.Write(resp.GetData()); err != nil {
				return 0, "", err
			}
			totalBytes += uint64(len(resp.GetData()))
		}

		if resp.GetEof() {
			contentSha256 = resp.GetContentSha256()

			break
		}
	}

	return totalBytes, contentSha256, nil
}

// BackupData holds the raw backup archive and its SHA-256 hash.
type BackupData struct {
	Data []byte
	Hash string
}

// BackupToBuffer runs the Backup RPC and captures the full tar archive in memory.
// It verifies the SHA-256 hash matches the server-reported value.
func BackupToBuffer(ctx context.Context, client clusterpb.ClusterServiceClient) (*BackupData, error) {
	stream, err := client.Backup(ctx, &clusterpb.BackupRequest{})
	if err != nil {
		return nil, fmt.Errorf("backup RPC: %w", err)
	}

	var buf bytes.Buffer
	hash := sha256.New()

	var contentSha256 string
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("backup recv: %w", err)
		}

		if len(resp.GetData()) > 0 {
			buf.Write(resp.GetData())
			_, _ = hash.Write(resp.GetData())
		}

		if resp.GetEof() {
			contentSha256 = resp.GetContentSha256()

			break
		}
	}

	actualHash := hex.EncodeToString(hash.Sum(nil))
	if actualHash != contentSha256 {
		return nil, fmt.Errorf("backup hash mismatch: got %s, server reported %s", actualHash, contentSha256)
	}

	return &BackupData{Data: buf.Bytes(), Hash: contentSha256}, nil
}

// UploadAndFinalizeRestore uploads a backup to a restore-mode server, validates it,
// and finalizes the restore. The caller must start the server with --restore before
// calling this, and restart it normally after.
func UploadAndFinalizeRestore(ctx context.Context, restoreClient restorepb.RestoreServiceClient, backup *BackupData) error {
	// Upload in 64KB chunks.
	stream, err := restoreClient.UploadBackup(ctx)
	if err != nil {
		return fmt.Errorf("upload backup: %w", err)
	}

	const chunkSize = 64 * 1024
	for offset := 0; offset < len(backup.Data); offset += chunkSize {
		end := min(offset+chunkSize, len(backup.Data))
		if err := stream.Send(&restorepb.UploadBackupRequest{
			Data: backup.Data[offset:end],
		}); err != nil {
			return fmt.Errorf("upload send chunk: %w", err)
		}
	}

	if err := stream.Send(&restorepb.UploadBackupRequest{
		Eof:           true,
		ContentSha256: backup.Hash,
	}); err != nil {
		return fmt.Errorf("upload send EOF: %w", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("upload close: %w", err)
	}
	if resp.GetSha256() != backup.Hash {
		return fmt.Errorf("upload hash mismatch: got %s, expected %s", resp.GetSha256(), backup.Hash)
	}

	// Validate.
	valStream, err := restoreClient.ValidateRestore(ctx, &restorepb.ValidateRestoreRequest{})
	if err != nil {
		return fmt.Errorf("validate restore: %w", err)
	}

	var validationErrors []string
	for {
		event, err := valStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("validate recv: %w", err)
		}
		if e := event.GetError(); e != nil {
			validationErrors = append(validationErrors, e.GetMessage())
		}
	}
	if len(validationErrors) > 0 {
		return fmt.Errorf("validation errors: %v", validationErrors)
	}

	// Finalize.
	if _, err := restoreClient.FinalizeRestore(ctx, &restorepb.FinalizeRestoreRequest{}); err != nil {
		return fmt.Errorf("finalize restore: %w", err)
	}

	return nil
}
