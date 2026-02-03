package state

import (
	"context"
	"errors"
)

// ErrNotAvailable is returned when the peer is not available (e.g., connection refused).
// This error should be used to signal transient unavailability that may resolve on retry
// or during graceful shutdown scenarios.
var ErrNotAvailable = errors.New("peer not available")

// SnapshotFetcher fetches a snapshot from a peer.
type SnapshotFetcher interface {
	// FetchSnapshot fetches a snapshot by ID and writes it to the given directory.
	// Returns the total size in bytes and SHA256 hash of the content.
	// May return ErrNotAvailable if the peer is not reachable.
	FetchSnapshot(ctx context.Context, snapshotID uint64, targetDir string) (size uint64, hash string, err error)
}

// SnapshotFetcherProvider provides snapshot fetchers for peers.
type SnapshotFetcherProvider interface {
	GetForPeer(id uint64) (SnapshotFetcher, error)
}
