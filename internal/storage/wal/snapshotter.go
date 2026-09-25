package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// ErrWALDirectoryMissing reports that the directory holding the WAL — the etcd
// segments, the creation marker and the instance id — is gone underneath a
// running node. Everything that node has acknowledged as persisted since the
// removal is unrecoverable, so callers must stop rather than continue.
var ErrWALDirectoryMissing = errors.New("WAL directory is missing")

// Snapshotter manages snapshot files on disk.
// Each snapshot is stored as a separate file named <term>-<index>.snap
// containing the marshaled raftpb.Snapshot proto.
//
// Every file operation goes through root, a handle on the WAL directory opened
// at startup. That handle refers to the directory itself, not to its pathname:
// once the WAL directory is unlinked, operations through it fail even while
// another directory occupies the path.
type Snapshotter struct {
	root   *os.Root
	pinned os.FileInfo
	walDir string
	name   string
	dir    string
	logger logging.Logger
}

// NewSnapshotter creates a Snapshotter that stores files in dir.
// Close releases the handle it holds on the parent of dir.
func NewSnapshotter(dir string, logger logging.Logger) (*Snapshotter, error) {
	dir = filepath.Clean(dir)

	if err := mkdirAllSynced(dir); err != nil {
		return nil, fmt.Errorf("creating snapshot directory: %w", err)
	}

	walDir, err := filepath.Abs(filepath.Dir(dir))
	if err != nil {
		return nil, fmt.Errorf("resolving WAL directory: %w", err)
	}

	root, err := os.OpenRoot(walDir)
	if err != nil {
		return nil, fmt.Errorf("opening WAL directory: %w", err)
	}

	pinned, err := root.Stat(".")
	if err != nil {
		_ = root.Close()

		return nil, fmt.Errorf("identifying WAL directory: %w", err)
	}

	return &Snapshotter{
		root:   root,
		pinned: pinned,
		walDir: walDir,
		name:   filepath.Base(dir),
		dir:    dir,
		logger: logger,
	}, nil
}

// Close releases the handle on the WAL directory.
func (s *Snapshotter) Close() error {
	return s.root.Close()
}

// Save writes the snapshot to a file named <term>-<index>.snap.
// The write is crash-safe: data is written to a temporary file, fsynced,
// then atomically renamed to the final path, and the directory is fsynced.
// Old snap files are NOT removed here — call CleanupOlderThan after
// the WAL snapshot record is persisted to avoid losing the only valid
// snap file on a crash between Save and WAL write.
//
// A WAL directory that went away, or moved away from the path the node was
// configured with, is reported as ErrWALDirectoryMissing — including when it
// happens partway through the write.
func (s *Snapshotter) Save(snap *raftpb.Snapshot) error {
	data, err := proto.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshaling snapshot: %w", err)
	}

	if err := s.ensureDir(); err != nil {
		return err
	}

	if err := s.writeSnapFile(snap, data); err != nil {
		return s.classify(err)
	}

	// The snapshot is durable, but only inside the directory the handle holds.
	// Reporting success is what lets the caller publish it, so the configured path
	// is confirmed once the write is complete: a move landing mid-write must not
	// be published as persisted.
	return s.checkWALDir()
}

// checkWALDir reports whether the WAL directory still answers to the path the
// node was configured with. The handle keeps writes inside the directory etcd
// opened even after that directory is moved, and a moved directory is as
// unrecoverable as a deleted one: a restart reads the configured path, finds no
// WAL and no identity there, and rejoins as a new member.
func (s *Snapshotter) checkWALDir() error {
	current, err := os.Stat(s.walDir)
	switch {
	case err == nil && os.SameFile(s.pinned, current):
		return nil
	case err != nil && !errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("checking WAL directory: %w", err)
	}

	return s.walDirLost(err)
}

// writeSnapFile performs the crash-safe write. Its errors are classified by the
// caller, which is what distinguishes a retryable failure from a lost WAL.
func (s *Snapshotter) writeSnapFile(snap *raftpb.Snapshot, data []byte) error {
	name := s.path(snapFileName(snap.GetMetadata().GetTerm(), snap.GetMetadata().GetIndex()))
	tmpName := name + ".tmp"

	f, err := s.root.Create(tmpName)
	if err != nil {
		return fmt.Errorf("creating temp snap file: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = s.root.Remove(tmpName)

		return fmt.Errorf("writing temp snap file: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = s.root.Remove(tmpName)

		return fmt.Errorf("syncing temp snap file: %w", err)
	}

	if err := f.Close(); err != nil {
		_ = s.root.Remove(tmpName)

		return fmt.Errorf("closing temp snap file: %w", err)
	}

	if err := s.root.Rename(tmpName, name); err != nil {
		_ = s.root.Remove(tmpName)

		return fmt.Errorf("renaming temp snap file: %w", err)
	}

	// Fsync the directory to make the rename durable.
	if err := s.syncDir(s.name); err != nil {
		return fmt.Errorf("syncing snap directory: %w", err)
	}

	return nil
}

// classify turns a write failure into the terminal ErrWALDirectoryMissing when
// the WAL directory is what went away. A missing component can appear at any
// point of the write, not only at the check that precedes it, so the decision is
// taken from the failure itself.
func (s *Snapshotter) classify(err error) error {
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// Recreating the snapshot directory is both the probe and the recovery: it
	// reaches the WAL directory through the pinned handle, so it fails only when
	// that directory is gone.
	if recreateErr := s.recreateDir(); recreateErr != nil {
		return recreateErr
	}

	return err
}

// ensureDir makes the snapshot directory usable again, or reports that it cannot
// be. NewSnapshotter creates it, so its absence means it went away underneath a
// running node. Losing that directory alone is recoverable and is recreated;
// losing the WAL directory holding it is not, and returns ErrWALDirectoryMissing.
func (s *Snapshotter) ensureDir() error {
	info, err := s.root.Stat(s.name)
	if err == nil {
		if info.IsDir() {
			return nil
		}

		return fmt.Errorf("snapshot path %s is not a directory", s.dir)
	}

	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("checking snapshot directory: %w", err)
	}

	return s.recreateDir()
}

// recreateDir recreates the snapshot directory inside the WAL directory the node
// opened at startup.
func (s *Snapshotter) recreateDir() error {
	err := s.root.Mkdir(s.name, 0755)
	if err == nil {
		if syncErr := s.syncDir("."); syncErr != nil {
			return fmt.Errorf("syncing WAL directory: %w", syncErr)
		}

		details := map[string]any{"dir": s.dir}

		assert.Unreachable("snapshot directory disappeared underneath a running node", details)

		s.logger.WithFields(details).Errorf("Snapshot directory was missing, recreated it")

		return nil
	}

	// The create resolves from the handle on the WAL directory, so the only
	// component that can be missing is that directory. It holds the etcd WAL, the
	// creation marker and the instance id. Losing it means etcd is fsyncing
	// unlinked inodes, so the terms, votes and entries this node acknowledges as
	// persisted are already gone: the next restart finds no marker, rebuilds an
	// empty WAL and rejoins as a new member. Recreating on top of that would keep
	// acknowledging unrecoverable writes.
	if errors.Is(err, os.ErrNotExist) {
		return s.walDirLost(err)
	}

	// A directory that appeared since the check is usable; anything else
	// occupying the name is not.
	if info, statErr := s.root.Stat(s.name); statErr == nil && info.IsDir() {
		return nil
	}

	return fmt.Errorf("recreating snapshot directory: %w", err)
}

// walDirLost reports the WAL directory as unrecoverable. cause is the failure
// that revealed it, and is nil when the loss was found by the identity check.
func (s *Snapshotter) walDirLost(cause error) error {
	details := map[string]any{"dir": s.dir, "walDir": s.walDir}

	assert.Unreachable("WAL directory disappeared underneath a running node", details)

	s.logger.WithFields(details).Errorf("WAL directory is missing, consensus state cannot be recovered")

	if cause == nil {
		return fmt.Errorf("%w: %s no longer holds the directory this node opened, so consensus state cannot be recovered", ErrWALDirectoryMissing, s.walDir)
	}

	return fmt.Errorf("%w: %s is gone along with %s, so consensus state cannot be recovered: %w", ErrWALDirectoryMissing, s.dir, s.walDir, cause)
}

// path returns name inside the snapshot directory, relative to the WAL directory.
func (s *Snapshotter) path(name string) string {
	return filepath.Join(s.name, name)
}

// syncDir fsyncs a directory inside the WAL directory. "." is the WAL directory
// itself.
func (s *Snapshotter) syncDir(name string) error {
	d, err := s.root.Open(name)
	if err != nil {
		return err
	}

	err = d.Sync()
	_ = d.Close()

	return err
}

func (s *Snapshotter) readDir() ([]os.DirEntry, error) {
	d, err := s.root.Open(s.name)
	if err != nil {
		return nil, err
	}

	entries, err := d.ReadDir(-1)
	_ = d.Close()

	return entries, err
}

// mkdirSynced creates dir inside an existing parent and fsyncs that parent: a
// directory entry is only durable once the directory holding it has been
// fsynced.
func mkdirSynced(dir string) error {
	dir = filepath.Clean(dir)

	if err := os.Mkdir(dir, 0755); err != nil {
		// An existing directory is usable; anything else occupying the path is not.
		info, statErr := os.Stat(dir)
		if statErr != nil || !info.IsDir() {
			return fmt.Errorf("creating %s: %w", dir, err)
		}
	}

	parent := filepath.Dir(dir)
	if err := fsyncDir(parent); err != nil {
		return fmt.Errorf("syncing %s: %w", parent, err)
	}

	return nil
}

// mkdirAllSynced creates dir and any missing ancestor, outermost first so every
// new entry is fsynced into a directory that already exists.
func mkdirAllSynced(dir string) error {
	missing, err := missingAncestors(dir)
	if err != nil {
		return err
	}

	for _, path := range slices.Backward(missing) {
		if err := mkdirSynced(path); err != nil {
			return err
		}
	}

	return nil
}

// missingAncestors returns dir and each of its ancestors that does not exist,
// deepest first, stopping at the first one that does.
func missingAncestors(dir string) ([]string, error) {
	var missing []string

	for path := filepath.Clean(dir); path != filepath.Dir(path); path = filepath.Dir(path) {
		info, err := os.Stat(path)
		if err == nil {
			if !info.IsDir() {
				return nil, fmt.Errorf("%s is not a directory", path)
			}

			return missing, nil
		}

		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("checking %s: %w", path, err)
		}

		missing = append(missing, path)
	}

	return missing, nil
}

// fsyncDir fsyncs a directory to ensure file creates/renames are durable.
func fsyncDir(dir string) error {
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
	entries, err := s.readDir()
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

	data, err := s.root.ReadFile(s.path(bestName))
	if err != nil {
		return nil, fmt.Errorf("reading snap file %s: %w", bestName, err)
	}

	var snap raftpb.Snapshot
	if err := proto.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("unmarshaling snap file %s: %w", bestName, err)
	}

	return &snap, nil
}

// LoadNewestAvailable loads the newest snap file that matches one of the
// given WAL snapshot records. This filters out orphaned snap files that
// were written before a crash but have no corresponding WAL record.
// Returns nil if no matching snap file is found.
func (s *Snapshotter) LoadNewestAvailable(walSnaps []*walpb.Snapshot) (*raftpb.Snapshot, error) {
	names, err := s.snapNames()
	if err != nil {
		s.logger.WithFields(map[string]any{
			"error": err,
		}).Errorf("Failed to read snap directory, treating as empty")

		return nil, nil
	}

	for _, name := range names {
		data, readErr := s.root.ReadFile(s.path(name))
		if readErr != nil {
			s.logger.WithFields(map[string]any{
				"file":  name,
				"error": readErr,
			}).Errorf("Failed to read snap file, skipping")

			continue
		}

		var snap raftpb.Snapshot
		if unmarshalErr := proto.Unmarshal(data, &snap); unmarshalErr != nil {
			s.logger.WithFields(map[string]any{
				"file":  name,
				"size":  len(data),
				"error": unmarshalErr,
			}).Errorf("Corrupt snap file, skipping")

			continue
		}

		// Check if this snap file matches any WAL snapshot record.
		for _, v := range slices.Backward(walSnaps) {
			if snap.GetMetadata().GetTerm() == v.GetTerm() && snap.GetMetadata().GetIndex() == v.GetIndex() {
				return &snap, nil
			}
		}

		s.logger.WithFields(map[string]any{
			"file":  name,
			"term":  snap.GetMetadata().GetTerm(),
			"index": snap.GetMetadata().GetIndex(),
		}).Infof("Snap file does not match any WAL snapshot record, skipping")
	}

	return nil, nil
}

// snapNames returns snap file names sorted from newest to oldest.
func (s *Snapshotter) snapNames() ([]string, error) {
	entries, err := s.readDir()
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

	data, err := s.root.ReadFile(s.path(name))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("reading snap file %s: %w", name, err)
	}

	var snap raftpb.Snapshot
	if err := proto.Unmarshal(data, &snap); err != nil {
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
	entries, err := s.readDir()
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
			_ = s.root.Remove(s.path(e.Name()))
		}
	}
}
