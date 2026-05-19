package ctrl

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/proto/snapshotpb"
)

const (
	sessionBackoff      = 1 * time.Second
	sessionMaxBackoff   = 10 * time.Second
	closeSessionTimeout = 5 * time.Second
)

// grpcSnapshotFetcher implements state.SnapshotFetcher using the session-based gRPC protocol.
type grpcSnapshotFetcher struct {
	client         snapshotpb.SnapshotServiceClient
	parallelism    int
	retryCount     int
	fileRetryCount int
}

// isUnavailableError checks if the error is a gRPC Unavailable error (connection refused, etc.)
func isUnavailableError(err error) bool {
	if s, ok := status.FromError(err); ok {
		return s.Code() == codes.Unavailable
	}

	return false
}

// isRetryableError returns true for transient gRPC errors that may resolve on retry.
func isRetryableError(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}

	switch s.Code() {
	case codes.Unavailable, codes.Aborted, codes.DeadlineExceeded, codes.Internal:
		return true
	default:
		return false
	}
}

// isSessionExpired returns true if the error indicates the session is no longer valid.
func isSessionExpired(err error) bool {
	if s, ok := status.FromError(err); ok {
		return s.Code() == codes.NotFound
	}

	return false
}

func (f *grpcSnapshotFetcher) FetchSnapshot(ctx context.Context, targetDir string, progress *state.SyncProgress, minAppliedIndex uint64) (uint64, error) {
	for attempt := range f.retryCount {
		size, err := f.fetchWithSession(ctx, targetDir, progress, minAppliedIndex)
		if err == nil {
			return size, nil
		}

		if isUnavailableError(err) {
			return 0, fmt.Errorf("snapshot fetch: %w", state.ErrNotAvailable)
		}

		if !isRetryableError(err) && !isSessionExpired(err) {
			return 0, err
		}

		if attempt < f.retryCount-1 {
			if waitErr := sessionBackoffWait(ctx, attempt); waitErr != nil {
				return 0, waitErr
			}
		}
	}

	return 0, fmt.Errorf("snapshot fetch failed after %d attempts", f.retryCount)
}

func (f *grpcSnapshotFetcher) fetchWithSession(ctx context.Context, targetDir string, progress *state.SyncProgress, minAppliedIndex uint64) (uint64, error) {
	// 1. Prepare session (create checkpoint + get manifest).
	resp, err := f.client.PrepareSnapshot(ctx, &snapshotpb.PrepareSnapshotRequest{
		MinAppliedIndex: minAppliedIndex,
	})
	if err != nil {
		return 0, fmt.Errorf("preparing snapshot: %w", err)
	}

	sessionID := resp.GetSessionId()
	manifest := resp.GetManifest()

	// Always try to close the session on exit.
	defer f.closeSession(sessionID)

	// 2. Determine which files still need fetching (resume support).
	completedFiles, err := scanCompletedFiles(targetDir, manifest)
	if err != nil {
		return 0, fmt.Errorf("scanning completed files: %w", err)
	}

	completedSet := make(map[string]struct{}, len(completedFiles))
	for _, p := range completedFiles {
		completedSet[p] = struct{}{}
	}

	var pending []*snapshotpb.FileEntry
	for _, entry := range manifest.GetFiles() {
		if _, ok := completedSet[entry.GetPath()]; !ok {
			pending = append(pending, entry)
		}
	}

	// 3. Set progress totals.
	if progress != nil {
		progress.SetTotal(manifestTotalSize(manifest))
		progress.SetFilesTotal(uint64(len(manifest.GetFiles())))
	}

	// 4. Fetch pending files in parallel.
	ff := &fileFetcher{
		client:     f.client,
		sessionID:  sessionID,
		maxRetries: f.fileRetryCount,
	}

	logger := logging.FromContext(ctx)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(f.parallelism)

	for _, entry := range pending {
		g.Go(func() error {
			logger.WithFields(map[string]any{
				"path": entry.GetPath(),
				"size": entry.GetSize(),
			}).Infof("Downloading snapshot file")

			if err := ff.fetchFile(gCtx, entry, targetDir, progress); err != nil {
				return err
			}

			fields := map[string]any{
				"path": entry.GetPath(),
				"size": entry.GetSize(),
			}
			if progress != nil {
				fields["filesCompleted"] = progress.FilesCompleted()
				fields["filesTotal"] = progress.FilesTotal()
			}
			logger.WithFields(fields).Infof("Snapshot file downloaded")

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return 0, err
	}

	return manifestTotalSize(manifest), nil
}

func (f *grpcSnapshotFetcher) closeSession(sessionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), closeSessionTimeout)
	defer cancel()

	_, _ = f.client.CloseSession(ctx, &snapshotpb.CloseSessionRequest{SessionId: sessionID})
}

// sessionBackoffWait sleeps with exponential backoff, respecting context cancellation.
func sessionBackoffWait(ctx context.Context, attempt int) error {
	delay := sessionBackoff
	for range attempt {
		delay *= 2
		if delay > sessionMaxBackoff {
			delay = sessionMaxBackoff

			break
		}
	}

	select {
	case <-time.After(delay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// grpcSnapshotFetcherProvider provides snapshot fetchers for peers.
type grpcSnapshotFetcherProvider struct {
	transport      *node.DefaultTransport
	parallelism    int
	retryCount     int
	fileRetryCount int
}

func (p *grpcSnapshotFetcherProvider) GetForPeer(id uint64) (state.SnapshotFetcher, error) {
	conn := p.transport.GetPeerConnection(id)
	if conn == nil {
		return nil, fmt.Errorf("no connection to peer %d", id)
	}

	return &grpcSnapshotFetcher{
		client:         snapshotpb.NewSnapshotServiceClient(conn),
		parallelism:    p.parallelism,
		retryCount:     p.retryCount,
		fileRetryCount: p.fileRetryCount,
	}, nil
}

// GRPCSnapshotFetcherProvider creates a new snapshot fetcher provider using gRPC.
func GRPCSnapshotFetcherProvider(transport *node.DefaultTransport, parallelism, retryCount, fileRetryCount int) state.SnapshotFetcherProvider {
	return &grpcSnapshotFetcherProvider{
		transport:      transport,
		parallelism:    parallelism,
		retryCount:     retryCount,
		fileRetryCount: fileRetryCount,
	}
}
