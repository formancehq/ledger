package ctrl

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dustin/go-humanize"
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

// errSnapshotDonorIntegrity marks failures that are specific to a donor's
// advertised checkpoint or streamed bytes. They are safe reasons to discard
// the current donor session and try another candidate; local staging errors
// deliberately do not use this sentinel.
var errSnapshotDonorIntegrity = errors.New("snapshot donor integrity")

// snapshotClientSource resolves the client for the peer being synced from.
// It is called before every attempt so a fetch survives the Raft transport
// restarting the shared peer connection: calls on the closed connection fail
// with Canceled, and the next attempt gets the re-dialed one.
type snapshotClientSource func() (snapshotpb.SnapshotServiceClient, error)

// grpcSnapshotFetcher implements state.SnapshotFetcher using the session-based gRPC protocol.
type grpcSnapshotFetcher struct {
	clientSource   snapshotClientSource
	logger         logging.Logger
	parallelism    int
	retryCount     int
	fileRetryCount int
}

type snapshotDonorCandidate struct {
	id       uint64
	newFetch func() (state.SnapshotFetcher, error)
}

// fallbackSnapshotFetcher tries one complete checkpoint session per donor.
// Sessions are never mixed: a session ID and its manifest belong to exactly
// one donor, so changing source is only safe after the previous attempt has
// been discarded and the staging directory has been reset.
type fallbackSnapshotFetcher struct {
	candidates []snapshotDonorCandidate
	logger     logging.Logger
}

func (f *fallbackSnapshotFetcher) FetchSnapshot(ctx context.Context, targetDir string, progress *state.SyncProgress, minAppliedIndex uint64) (uint64, error) {
	if len(f.candidates) == 0 {
		return 0, fmt.Errorf("snapshot fetch: %w", state.ErrNotAvailable)
	}

	errs := make([]error, 0, len(f.candidates))
	for i, candidate := range f.candidates {
		if i > 0 {
			if err := resetSnapshotTargetDir(targetDir); err != nil {
				return 0, fmt.Errorf("resetting snapshot staging directory before donor %d: %w", candidate.id, err)
			}
			if progress != nil {
				progress.Reset()
			}
		}

		fetcher, err := candidate.newFetch()
		if err != nil {
			errs = append(errs, fmt.Errorf("donor %d: %w", candidate.id, err))
			continue
		}

		size, err := fetcher.FetchSnapshot(ctx, targetDir, progress, minAppliedIndex)
		if err == nil {
			return size, nil
		}

		errs = append(errs, fmt.Errorf("donor %d: %w", candidate.id, err))
		if !isDonorFallbackError(ctx, err) {
			return 0, err
		}

		f.logger.WithFields(map[string]any{
			"donor":           candidate.id,
			"minAppliedIndex": minAppliedIndex,
			"error":           err.Error(),
		}).Errorf("Snapshot donor failed, trying next candidate")
	}

	return 0, fmt.Errorf("snapshot fetch failed from all donors: %w", errors.Join(errs...))
}

func isDonorFallbackError(ctx context.Context, err error) bool {
	if errors.Is(err, state.ErrNotAvailable) || errors.Is(err, errSnapshotDonorIntegrity) {
		return true
	}

	for current := err; current != nil; current = errors.Unwrap(current) {
		if isRetryableError(ctx, current) || isSessionExpired(current) {
			return true
		}
	}

	return false
}

// resetSnapshotTargetDir removes only the contents of the restore staging
// directory. IncomingRestoreFactory owns this directory, so clearing it
// between donor sessions cannot affect the live Pebble store.
func resetSnapshotTargetDir(targetDir string) error {
	entries, err := os.ReadDir(targetDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}

	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(targetDir, entry.Name())); err != nil {
			return err
		}
	}

	return nil
}

// isUnavailableError checks if the error is a gRPC Unavailable error (connection refused, etc.)
func isUnavailableError(err error) bool {
	if s, ok := status.FromError(err); ok {
		return s.Code() == codes.Unavailable
	}

	return false
}

// isRetryableError returns true for transient gRPC errors that may resolve on
// retry. Canceled counts only while ctx is live: it then comes from the peer
// connection being closed under the call, not from the caller giving up.
func isRetryableError(ctx context.Context, err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}

	switch s.Code() {
	case codes.Unavailable, codes.Aborted, codes.DeadlineExceeded, codes.Internal:
		return true
	case codes.Canceled:
		return ctx.Err() == nil
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
	logger := f.logger.WithContext(ctx)

	for attempt := range f.retryCount {
		if attempt > 0 {
			logger.WithFields(map[string]any{
				"attempt":         attempt + 1,
				"maxAttempts":     f.retryCount,
				"minAppliedIndex": minAppliedIndex,
			}).Infof("Retrying snapshot fetch")
		}

		client, err := f.clientSource()
		if err != nil {
			return 0, fmt.Errorf("snapshot fetch: %w: %w", state.ErrNotAvailable, err)
		}

		size, err := f.fetchWithSession(ctx, client, targetDir, progress, minAppliedIndex)
		if err == nil {
			return size, nil
		}

		if isUnavailableError(err) {
			return 0, fmt.Errorf("snapshot fetch: %w", state.ErrNotAvailable)
		}

		if !isRetryableError(ctx, err) && !isSessionExpired(err) {
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
//
// The select waits on both `done` and `callCtx.Done()` so the wrapper
// returns as soon as the deadline expires — even if the underlying gRPC
// stub does not observe context cancellation (a defence-in-depth against
// grpc-go bugs). When we exit via ctx cancellation the inner goroutine may
// briefly outlive the call while it drains gRPC internals; that is a
// bounded leak, and the caller is unblocked to retry a fresh session.
func (f *grpcSnapshotFetcher) prepareSnapshotWithHeartbeat(ctx context.Context, client snapshotpb.SnapshotServiceClient, minAppliedIndex uint64) (*snapshotpb.PrepareSnapshotResponse, error) {
	logger := f.logger.WithContext(ctx)

	callCtx, cancel := context.WithTimeout(ctx, prepareSnapshotTimeout)
	defer cancel()

	type result struct {
		resp *snapshotpb.PrepareSnapshotResponse
		err  error
	}

	done := make(chan result, 1)

	go func() {
		resp, err := client.PrepareSnapshot(callCtx, &snapshotpb.PrepareSnapshotRequest{
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
		case <-callCtx.Done():
			return nil, fmt.Errorf("PrepareSnapshot did not return within %s: %w", prepareSnapshotTimeout, callCtx.Err())
		case <-ticker.C:
			logger.WithFields(map[string]any{
				"minAppliedIndex": minAppliedIndex,
				"elapsed":         time.Since(started).String(),
				"timeout":         prepareSnapshotTimeout.String(),
			}).Errorf("Still waiting for PrepareSnapshot response from leader")
		}
	}
}

func (f *grpcSnapshotFetcher) fetchWithSession(ctx context.Context, client snapshotpb.SnapshotServiceClient, targetDir string, progress *state.SyncProgress, minAppliedIndex uint64) (uint64, error) {
	logger := f.logger.WithContext(ctx)

	logger.WithFields(map[string]any{
		"minAppliedIndex": minAppliedIndex,
	}).Infof("Requesting snapshot session from leader")

	prepareStart := time.Now()

	// 1. Prepare session (create checkpoint + get manifest).
	resp, err := f.prepareSnapshotWithHeartbeat(ctx, client, minAppliedIndex)
	if err != nil {
		return 0, fmt.Errorf("preparing snapshot: %w", err)
	}

	sessionID := resp.GetSessionId()
	// The server created a temporary checkpoint with this session. Close it on
	// every later failure, including a rejected manifest.
	defer f.closeSession(client, sessionID)

	manifest := resp.GetManifest()
	if err := validateSnapshotManifest(manifest); err != nil {
		return 0, fmt.Errorf("%w: validating snapshot manifest: %v", errSnapshotDonorIntegrity, err)
	}

	totalSize := manifestTotalSize(manifest)

	logger.WithFields(map[string]any{
		"sessionId":      sessionID,
		"filesTotal":     len(manifest.GetFiles()),
		"totalSize":      totalSize,
		"totalSizeHuman": humanize.IBytes(totalSize),
		"duration":       time.Since(prepareStart).String(),
	}).Infof("Snapshot session prepared")

	// 2. Fetch every file in this session. The manifest intentionally carries
	// no content digest, so a file from an earlier session cannot be trusted
	// based on path and size alone.
	pending := manifest.GetFiles()

	// 3. Set progress totals.
	if progress != nil {
		progress.SetTotal(manifestTotalSize(manifest))
		progress.SetFilesTotal(uint64(len(manifest.GetFiles())))
	}

	// 4. Fetch pending files in parallel.
	ff := &fileFetcher{
		clientSource: f.clientSource,
		sessionID:    sessionID,
		maxRetries:   f.fileRetryCount,
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(f.parallelism)

	for _, entry := range pending {
		g.Go(func() error {
			logger.WithFields(map[string]any{
				"path":      entry.GetPath(),
				"size":      entry.GetSize(),
				"sizeHuman": humanize.IBytes(entry.GetSize()),
			}).Infof("Downloading snapshot file")

			if err := ff.fetchFile(gCtx, entry, targetDir, progress); err != nil {
				return err
			}

			fields := map[string]any{
				"path":      entry.GetPath(),
				"size":      entry.GetSize(),
				"sizeHuman": humanize.IBytes(entry.GetSize()),
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

func (f *grpcSnapshotFetcher) closeSession(client snapshotpb.SnapshotServiceClient, sessionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), closeSessionTimeout)
	defer cancel()

	_, _ = client.CloseSession(ctx, &snapshotpb.CloseSessionRequest{SessionId: sessionID})
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
	logger         logging.Logger
	parallelism    int
	retryCount     int
	fileRetryCount int
}

func (p *grpcSnapshotFetcherProvider) GetForPeer(id uint64) (state.SnapshotFetcher, error) {
	clientSource := func() (snapshotpb.SnapshotServiceClient, error) {
		conn := p.transport.GetPeerConnection(id)
		if conn == nil {
			return nil, fmt.Errorf("no connection to peer %d", id)
		}

		return snapshotpb.NewSnapshotServiceClient(conn), nil
	}

	if _, err := clientSource(); err != nil {
		return nil, err
	}

	return &grpcSnapshotFetcher{
		clientSource:   clientSource,
		logger:         p.logger,
		parallelism:    p.parallelism,
		retryCount:     p.retryCount,
		fileRetryCount: p.fileRetryCount,
	}, nil
}

// GetForPeers builds a lazy, ordered donor fallback. Connections are resolved
// only when their candidate is attempted, so one unavailable peer cannot make
// the whole provider construction fail.
func (p *grpcSnapshotFetcherProvider) GetForPeers(ids []uint64) (state.SnapshotFetcher, error) {
	candidates := make([]snapshotDonorCandidate, 0, len(ids))
	seen := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}

		candidateID := id
		candidates = append(candidates, snapshotDonorCandidate{
			id: candidateID,
			newFetch: func() (state.SnapshotFetcher, error) {
				return p.GetForPeer(candidateID)
			},
		})
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no snapshot donors available")
	}

	return &fallbackSnapshotFetcher{
		candidates: candidates,
		logger:     p.logger,
	}, nil
}

// GRPCSnapshotFetcherProvider creates a new snapshot fetcher provider using gRPC.
//
// The logger is injected explicitly rather than resolved through
// logging.FromContext: the sync context originates in the applier and never
// carries a logger, so FromContext would degrade to go-libs' bare logrus
// stderr fallback and emit lines in a different format than the rest of the
// process.
func GRPCSnapshotFetcherProvider(transport *node.DefaultTransport, logger logging.Logger, parallelism, retryCount, fileRetryCount int) state.SnapshotFetcherProvider {
	return &grpcSnapshotFetcherProvider{
		transport:      transport,
		logger:         logger.WithFields(map[string]any{"cmp": "snapshot-fetcher"}),
		parallelism:    parallelism,
		retryCount:     retryCount,
		fileRetryCount: fileRetryCount,
	}
}
