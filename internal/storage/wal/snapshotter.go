package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// Snapshotter manages snapshot files on disk.
// Each snapshot is stored as a separate file named <term>-<index>.snap
// containing the marshaled raftpb.Snapshot proto.
type Snapshotter struct {
	dir    string
	logger logging.Logger
}

// NewSnapshotter creates a Snapshotter that stores files in dir.
func NewSnapshotter(dir string, logger logging.Logger) (*Snapshotter, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("creating snapshot directory: %w", err)
	}

	return &Snapshotter{dir: dir, logger: logger}, nil
}

// Save writes the snapshot to a file named <term>-<index>.snap.
// The write is crash-safe: data is written to a temporary file, fsynced,
// then atomically renamed to the final path, and the directory is fsynced.
// Old snap files are NOT removed here — call CleanupOlderThan after
// the WAL snapshot record is persisted to avoid losing the only valid
// snap file on a crash between Save and WAL write.
func (s *Snapshotter) Save(snap raftpb.Snapshot) error {
	data, err := snap.Marshal()
	if err != nil {
		return fmt.Errorf("marshaling snapshot: %w", err)
	}

	name := snapFileName(snap.Metadata.Term, snap.Metadata.Index)
	path := filepath.Join(s.dir, name)
	tmpPath := path + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("creating temp snap file: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)

		return fmt.Errorf("writing temp snap file: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)

		return fmt.Errorf("syncing temp snap file: %w", err)
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)

		return fmt.Errorf("closing temp snap file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)

		return fmt.Errorf("renaming temp snap file: %w", err)
	}

	// Fsync the directory to make the rename durable.
	if err := syncDir(s.dir); err != nil {
		return fmt.Errorf("syncing snap directory: %w", err)
	}

	return nil
}

// syncDir fsyncs a directory to ensure file creates/renames are durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}

	err = d.Sync()
	_ = d.Close()

	return err
}

// CleanupOlderThan removes snap files with index strictly less than keepIndex.
// Must be called only after the corresponding WAL snapshot record is persisted.
func (s *Snapshotter) CleanupOlderThan(keepIndex uint64) {
	s.cleanupOlder(keepIndex)
}

// Load scans the directory for the most recent .snap file and returns it.
// Returns nil if no snapshot is found.
func (s *Snapshotter) Load() (*raftpb.Snapshot, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("reading snap directory: %w", err)
	}

	var bestName string
	var bestIndex uint64
	found := false

	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		_, index, ok := parseSnapFileName(e.Name())
		if !ok {
			continue
		}

		if !found || index > bestIndex {
			bestIndex = index
			bestName = e.Name()
			found = true
		}
	}

	if !found {
		return nil, nil
	}

	data, err := os.ReadFile(filepath.Join(s.dir, bestName))
	if err != nil {
		return nil, fmt.Errorf("reading snap file %s: %w", bestName, err)
	}

	var snap raftpb.Snapshot
	if err := snap.Unmarshal(data); err != nil {
		return nil, fmt.Errorf("unmarshaling snap file %s: %w", bestName, err)
	}

	return &snap, nil
}

// LoadNewestAvailable loads the newest snap file that matches one of the
// given WAL snapshot records. This filters out orphaned snap files that
// were written before a crash but have no corresponding WAL record.
// Returns nil if no matching snap file is found.
func (s *Snapshotter) LoadNewestAvailable(walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error) {
	names, err := s.snapNames()
	if err != nil {
		s.logger.WithFields(map[string]any{
			"error": err,
		}).Errorf("Failed to read snap directory, treating as empty")

		return nil, nil
	}

	for _, name := range names {
		data, readErr := os.ReadFile(filepath.Join(s.dir, name))
		if readErr != nil {
			s.logger.WithFields(map[string]any{
				"file":  name,
				"error": readErr,
			}).Errorf("Failed to read snap file, skipping")

			continue
		}

		var snap raftpb.Snapshot
		if unmarshalErr := snap.Unmarshal(data); unmarshalErr != nil {
			s.logger.WithFields(map[string]any{
				"file":  name,
				"size":  len(data),
				"error": unmarshalErr,
			}).Errorf("Corrupt snap file, skipping")

			continue
		}

		// Check if this snap file matches any WAL snapshot record.
		for i := len(walSnaps) - 1; i >= 0; i-- {
			if snap.Metadata.Term == walSnaps[i].Term && snap.Metadata.Index == walSnaps[i].Index {
				return &snap, nil
			}
		}

		s.logger.WithFields(map[string]any{
			"file":  name,
			"term":  snap.Metadata.Term,
			"index": snap.Metadata.Index,
		}).Infof("Snap file does not match any WAL snapshot record, skipping")
	}

	return nil, nil
}

// snapNames returns snap file names sorted from newest to oldest.
func (s *Snapshotter) snapNames() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}

	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		if _, _, ok := parseSnapFileName(e.Name()); ok {
			names = append(names, e.Name())
		}
	}

	sort.Sort(sort.Reverse(sort.StringSlice(names)))

	return names, nil
}

// LoadForIndex loads the snap file matching the given term and index.
// Returns nil if no matching file is found.
func (s *Snapshotter) LoadForIndex(term, index uint64) (*raftpb.Snapshot, error) {
	name := snapFileName(term, index)
	path := filepath.Join(s.dir, name)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("reading snap file %s: %w", name, err)
	}

	var snap raftpb.Snapshot
	if err := snap.Unmarshal(data); err != nil {
		return nil, fmt.Errorf("unmarshaling snap file %s: %w", name, err)
	}

	return &snap, nil
}

func snapFileName(term, index uint64) string {
	return fmt.Sprintf("%016x-%016x.snap", term, index)
}

func parseSnapFileName(name string) (term, index uint64, ok bool) {
	n, err := fmt.Sscanf(name, "%016x-%016x.snap", &term, &index)

	return term, index, err == nil && n == 2
}

func (s *Snapshotter) cleanupOlder(keepIndex uint64) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		_, index, ok := parseSnapFileName(e.Name())
		if !ok {
			continue
		}

		if index < keepIndex {
			_ = os.Remove(filepath.Join(s.dir, e.Name()))
		}
	}
}
