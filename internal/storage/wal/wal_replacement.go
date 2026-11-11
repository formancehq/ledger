package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
)

const replacementWALDir = "etcd.next"
const previousWALDir = "etcd.previous"

func pathExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}

	return false, err
}

// recoverWALReplacement resolves the only two intermediate directory states
// left by the two-rename switch. The active path remains the legacy etcd/
// path, so an earlier Ledger binary can read every completed replacement.
func recoverWALReplacement(dataDir string) error {
	active := filepath.Join(dataDir, etcdWalDir)
	next := filepath.Join(dataDir, replacementWALDir)
	previous := filepath.Join(dataDir, previousWALDir)
	activeExists, err := pathExists(active)
	if err != nil {
		return fmt.Errorf("checking active WAL: %w", err)
	}
	if activeExists {
		return nil
	}
	previousExists, err := pathExists(previous)
	if err != nil {
		return fmt.Errorf("checking previous WAL: %w", err)
	}
	if !previousExists {
		nextExists, checkErr := pathExists(next)
		if checkErr != nil {
			return fmt.Errorf("checking replacement WAL: %w", checkErr)
		}
		if nextExists {
			return errors.New("replacement WAL exists without an active or previous WAL")
		}

		return nil
	}
	nextExists, err := pathExists(next)
	if err != nil {
		return fmt.Errorf("checking replacement WAL: %w", err)
	}
	source := previous
	if nextExists {
		// The switch had started only after the complete candidate was synced.
		source = next
	}
	if err := os.Rename(source, active); err != nil {
		return fmt.Errorf("recovering active WAL directory: %w", err)
	}
	if err := syncDirectory(dataDir); err != nil {
		return fmt.Errorf("syncing recovered WAL directory: %w", err)
	}

	return nil
}

// cleanupReplacementWALs runs only after the selected WAL and its matching
// snapshot have replayed successfully. It never touches the active etcd/ path.
func cleanupReplacementWALs(dataDir string) error {
	for _, name := range []string{replacementWALDir, previousWALDir} {
		if err := os.RemoveAll(filepath.Join(dataDir, name)); err != nil {
			return fmt.Errorf("removing inactive WAL directory %s: %w", name, err)
		}
	}

	return syncDirectory(dataDir)
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}

	return closeErr
}

// prepareReplacementWAL writes a complete candidate while etcd/ remains the
// recovery source. The candidate contains all state needed by an older binary.
func (s *DefaultWAL) prepareReplacementWAL(snap *raftpb.Snapshot, state *raftpb.HardState, suffix []*raftpb.Entry) error {
	dir := filepath.Join(s.dataDir, replacementWALDir)
	w, err := wal.Create(s.zapLogger, dir, nil)
	if err != nil {
		return fmt.Errorf("creating replacement WAL: %w", err)
	}
	cleanup := func(err error) error {
		_ = w.Close()
		_ = os.RemoveAll(dir)

		return err
	}
	walsnap := &walpb.Snapshot{
		Index: new(snap.GetMetadata().GetIndex()), Term: new(snap.GetMetadata().GetTerm()),
		ConfState: snap.GetMetadata().GetConfState(),
	}
	if err := w.SaveSnapshot(walsnap); err != nil {
		return cleanup(fmt.Errorf("saving replacement WAL snapshot: %w", err))
	}
	if err := w.Save(state, suffix); err != nil {
		return cleanup(fmt.Errorf("saving replacement WAL state and suffix: %w", err))
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("closing replacement WAL: %w", err)
	}
	if err := syncDirectory(s.dataDir); err != nil {
		return fmt.Errorf("syncing replacement WAL directory: %w", err)
	}

	return nil
}

// switchWALDirectory uses two atomic renames and syncs the parent after each.
// Startup recovers either interrupted state from the previous/next directories.
func switchWALDirectory(dataDir string) error {
	active := filepath.Join(dataDir, etcdWalDir)
	next := filepath.Join(dataDir, replacementWALDir)
	previous := filepath.Join(dataDir, previousWALDir)
	if err := os.Rename(active, previous); err != nil {
		return fmt.Errorf("retiring active WAL: %w", err)
	}
	if err := syncDirectory(dataDir); err != nil {
		return fmt.Errorf("syncing retired WAL: %w", err)
	}
	if err := os.Rename(next, active); err != nil {
		return fmt.Errorf("publishing replacement WAL: %w", err)
	}
	if err := syncDirectory(dataDir); err != nil {
		return fmt.Errorf("syncing published WAL: %w", err)
	}

	return nil
}
