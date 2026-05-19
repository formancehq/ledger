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
	// to targetDir. Returns the total size in bytes.
	// minAppliedIndex is the minimum Raft index the checkpoint must include —
	// the leader waits until its FSM has applied at least this index before
	// creating the Pebble checkpoint.
	// May return ErrNotAvailable if the peer is not reachable.
	// If progress is non-nil, the fetcher reports transfer progress to it.
	FetchSnapshot(ctx context.Context, targetDir string, progress *SyncProgress, minAppliedIndex uint64) (size uint64, err error)
}

// SnapshotFetcherProvider provides snapshot fetchers for peers.
type SnapshotFetcherProvider interface {
	GetForPeer(id uint64) (SnapshotFetcher, error)
}
