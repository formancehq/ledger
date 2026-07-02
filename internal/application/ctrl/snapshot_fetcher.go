package ctrl

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

const (
	sessionBackoff      = 1 * time.Second
	sessionMaxBackoff   = 10 * time.Second
	closeSessionTimeout = 5 * time.Second
	// prepareSnapshotTimeout caps PrepareSnapshot — the metadata call that
	// creates a Pebble checkpoint and builds a manifest on the leader.
	// Without a cap the client goroutine can wait forever if the server
	// hangs in the checkpoint or manifest step, and the outer sync task
	// blocks along with it (node.Run's WaitForApplied → /readyz → pod stuck
	// at 0/1 for the whole probe budget).
	prepareSnapshotTimeout = 60 * time.Second
	// prepareSnapshotHeartbeat drives the periodic "still waiting" log
	// during PrepareSnapshot. It's short enough to be visible during a
	// human operator's live look, long enough not to flood.
	prepareSnapshotHeartbeat = 10 * time.Second
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
	logger := logging.FromContext(ctx)

	for attempt := range f.retryCount {
		if attempt > 0 {
			logger.WithFields(map[string]any{
				"attempt":         attempt + 1,
				"maxAttempts":     f.retryCount,
				"minAppliedIndex": minAppliedIndex,
			}).Infof("Retrying snapshot fetch")
		}

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

		logger.WithFields(map[string]any{
			"attempt": attempt + 1,
			"error":   err.Error(),
		}).Errorf("Snapshot fetch attempt failed, will retry")

		if attempt < f.retryCount-1 {
			if waitErr := sessionBackoffWait(ctx, attempt); waitErr != nil {
				return 0, waitErr
			}
		}
	}

	return 0, fmt.Errorf("snapshot fetch failed after %d attempts", f.retryCount)
}

// prepareSnapshotWithHeartbeat issues PrepareSnapshot with a bounded timeout
// and a periodic "still waiting" heartbeat log. Without the timeout, a hung
// server-side step (long checkpoint creation, deadlocked waitForApplied)
// blocks the client goroutine indefinitely; without the heartbeat, an
// operator watching the follower's logs sees only "Fetching fresh checkpoint
// from leader" and nothing else for the duration.
func (f *grpcSnapshotFetcher) prepareSnapshotWithHeartbeat(ctx context.Context, minAppliedIndex uint64) (*snapshotpb.PrepareSnapshotResponse, error) {
	logger := logging.FromContext(ctx)

	callCtx, cancel := context.WithTimeout(ctx, prepareSnapshotTimeout)
	defer cancel()

	type result struct {
		resp *snapshotpb.PrepareSnapshotResponse
		err  error
	}

	done := make(chan result, 1)

	go func() {
		resp, err := f.client.PrepareSnapshot(callCtx, &snapshotpb.PrepareSnapshotRequest{
			MinAppliedIndex: minAppliedIndex,
		})
		done <- result{resp: resp, err: err}
	}()

	started := time.Now()
	ticker := time.NewTicker(prepareSnapshotHeartbeat)
	defer ticker.Stop()

	for {
		select {
		case r := <-done:
			return r.resp, r.err
		case <-ticker.C:
			logger.WithFields(map[string]any{
				"minAppliedIndex": minAppliedIndex,
				"elapsed":         time.Since(started).String(),
				"timeout":         prepareSnapshotTimeout.String(),
			}).Errorf("Still waiting for PrepareSnapshot response from leader")
		}
	}
}

func (f *grpcSnapshotFetcher) fetchWithSession(ctx context.Context, targetDir string, progress *state.SyncProgress, minAppliedIndex uint64) (uint64, error) {
	logger := logging.FromContext(ctx)

	logger.WithFields(map[string]any{
		"minAppliedIndex": minAppliedIndex,
	}).Infof("Requesting snapshot session from leader")

	prepareStart := time.Now()

	// 1. Prepare session (create checkpoint + get manifest).
	resp, err := f.prepareSnapshotWithHeartbeat(ctx, minAppliedIndex)
	if err != nil {
		return 0, fmt.Errorf("preparing snapshot: %w", err)
	}

	sessionID := resp.GetSessionId()
	manifest := resp.GetManifest()
	totalSize := manifestTotalSize(manifest)

	logger.WithFields(map[string]any{
		"sessionId":  sessionID,
		"filesTotal": len(manifest.GetFiles()),
		"totalSize":  totalSize,
		"duration":   time.Since(prepareStart).String(),
	}).Infof("Snapshot session prepared")

	// Always try to close the session on exit.
	defer f.closeSession(sessionID)

	// 2. Determine which files still need fetching (resume support).
	scanStart := time.Now()
	completedFiles, err := scanCompletedFiles(targetDir, manifest)
	if err != nil {
		return 0, fmt.Errorf("scanning completed files: %w", err)
	}

	if len(completedFiles) > 0 {
		logger.WithFields(map[string]any{
			"filesResumed": len(completedFiles),
			"filesTotal":   len(manifest.GetFiles()),
			"duration":     time.Since(scanStart).String(),
		}).Infof("Resuming snapshot fetch from partial staging dir")
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
