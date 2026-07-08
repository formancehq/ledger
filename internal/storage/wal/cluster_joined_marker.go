package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// MarkClusterJoined satisfies the WAL interface by delegating to the
// package-level helper with the WAL's data directory. The bootstrap path
// holds the WAL instance so it can call this method; tryAddLearner (which
// runs in the fx lifecycle before the WAL is wired) uses the package-level
// helper directly.
func (s *DefaultWAL) MarkClusterJoined() error {
	return MarkClusterJoined(s.dataDir)
}

// MarkClusterJoined creates the CLUSTER_JOINED marker in dataDir. The marker
// signals that this node has been successfully registered with the cluster
// and is safe to restart with no --bootstrap/--join flag. Writing the marker
// before the registration succeeds defeats the purpose, because a restart
// would skip the learner-registration step on a node the cluster does not
// know about and leave it orphaned.
//
// Idempotent: calling MarkClusterJoined on an already-joined node is a
// no-op. The file content is irrelevant — only its presence is checked.
func MarkClusterJoined(dataDir string) error {
	if dataDir == "" {
		return errors.New("MarkClusterJoined: empty dataDir")
	}

	markerPath := filepath.Join(dataDir, ClusterJoinedMarkerFile)
	f, err := os.Create(markerPath)
	if err != nil {
		return fmt.Errorf("creating cluster-joined marker: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()

		return fmt.Errorf("syncing cluster-joined marker: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("closing cluster-joined marker: %w", err)
	}

	return nil
}

// IsClusterJoined reports whether the CLUSTER_JOINED marker exists in
// dataDir. Used by tests and diagnostics; the operator entrypoint performs
// the equivalent check directly in shell because the marker check must run
// before the server binary starts.
func IsClusterJoined(dataDir string) bool {
	if dataDir == "" {
		return false
	}

	_, err := os.Stat(filepath.Join(dataDir, ClusterJoinedMarkerFile))

	return err == nil
}
