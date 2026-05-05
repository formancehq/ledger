package state

import (
	"context"
	"errors"
)

// ErrNotAvailable is returned when the peer is not available (e.g., connection refused).
// This error should be used to signal transient unavailability that may resolve on retry
// or during graceful shutdown scenarios.
var ErrNotAvailable = errors.New("peer not available")

// SnapshotFetcher fetches a fresh checkpoint from a peer.
type SnapshotFetcher interface {
	// FetchSnapshot requests a fresh checkpoint from the leader and writes it
	// to targetDir. Returns the total size in bytes and SHA256 hash.
	// May return ErrNotAvailable if the peer is not reachable.
	// If progress is non-nil, the fetcher reports transfer progress to it.
	FetchSnapshot(ctx context.Context, targetDir string, progress *SyncProgress) (size uint64, hash string, err error)
}

// SnapshotFetcherProvider provides snapshot fetchers for peers.
type SnapshotFetcherProvider interface {
	GetForPeer(id uint64) (SnapshotFetcher, error)
}
