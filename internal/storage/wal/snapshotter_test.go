package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func newTestSnapshotter(t *testing.T) *Snapshotter {
	t.Helper()

	return newSnapshotterAt(t, t.TempDir())
}

func newSnapshotterAt(t *testing.T, dir string) *Snapshotter {
	t.Helper()

	s, err := NewSnapshotter(dir, logging.Testing())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

// snapshotterOver binds a Snapshotter to dir without creating it, so Save can be
// driven against a snapshot path that is missing or occupied.
func snapshotterOver(t *testing.T, dir string) *Snapshotter {
	t.Helper()

	root, err := os.OpenRoot(filepath.Dir(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })

	return &Snapshotter{root: root, name: filepath.Base(dir), dir: dir, logger: logging.Testing()}
}

func TestSnapshotter_SaveAndLoad(t *testing.T) {
	t.Parallel()

	s := newTestSnapshotter(t)

	snap := &raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{
			Index:     proto.Uint64(42),
			Term:      proto.Uint64(3),
			ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		},
		Data: []byte("fsm-state"),
	}

	require.NoError(t, s.Save(snap))

	loaded, err := s.Load()
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, uint64(42), loaded.GetMetadata().GetIndex())
	require.Equal(t, uint64(3), loaded.GetMetadata().GetTerm())
	require.Equal(t, []byte("fsm-state"), loaded.GetData())
	require.Equal(t, []uint64{1, 2, 3}, loaded.GetMetadata().GetConfState().GetVoters())
}

func TestSnapshotter_SaveRecreatesMissingDir(t *testing.T) {
	t.Parallel()

	s := newTestSnapshotter(t)
	require.NoError(t, os.RemoveAll(s.dir))

	require.NoError(t, s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(9000), Term: proto.Uint64(7)},
	}))

	loaded, err := s.Load()
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, uint64(9000), loaded.GetMetadata().GetIndex())
	require.Equal(t, uint64(7), loaded.GetMetadata().GetTerm())
}

func TestSnapshotter_SaveRefusesWhenTheParentIsMissing(t *testing.T) {
	t.Parallel()

	walDir := filepath.Join(t.TempDir(), "waldir")
	s := newSnapshotterAt(t, filepath.Join(walDir, snapDir))

	// The whole WAL directory, not just the snapshot directory: the etcd WAL and
	// the node identity went with it, so there is nothing left to recover.
	require.NoError(t, os.RemoveAll(walDir))

	err := s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(11), Term: proto.Uint64(2)},
	})
	require.ErrorIs(t, err, ErrWALDirectoryMissing)
	require.NoDirExists(t, walDir)
}

func TestSnapshotter_SaveFailsWhenTheDirCannotBeChecked(t *testing.T) {
	t.Parallel()

	// An absolute symlink leaves the WAL directory, which the handle refuses to
	// follow: neither a usable directory nor a missing one.
	walDir := t.TempDir()
	dir := filepath.Join(walDir, snapDir)
	require.NoError(t, os.Symlink(t.TempDir(), dir))

	s := snapshotterOver(t, dir)

	err := s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(1), Term: proto.Uint64(1)},
	})
	require.ErrorContains(t, err, "checking snapshot directory")
}

func TestSnapshotter_SaveFailsWhenTheDirCannotBeRecreated(t *testing.T) {
	t.Parallel()

	walDir := t.TempDir()

	// A dangling symlink reads as missing but cannot be replaced by a directory.
	dir := filepath.Join(walDir, snapDir)
	require.NoError(t, os.Symlink("nowhere", dir))

	s := snapshotterOver(t, dir)

	err := s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(1), Term: proto.Uint64(1)},
	})
	require.ErrorContains(t, err, "recreating snapshot directory")
}

// TestSnapshotter_SaveClassifiesAWALDirectoryLostDuringTheWrite drives the
// interleaving Save cannot reach on its own: the directory check passes, then the
// WAL directory goes away before the file is created. The failure must still be
// classified as terminal, not absorbed as a retryable write error.
func TestSnapshotter_SaveClassifiesAWALDirectoryLostDuringTheWrite(t *testing.T) {
	t.Parallel()

	walDir := filepath.Join(t.TempDir(), "waldir")
	s := newSnapshotterAt(t, filepath.Join(walDir, snapDir))

	require.NoError(t, s.ensureDir())
	require.NoError(t, os.RemoveAll(walDir))

	err := s.writeSnapFile(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(5), Term: proto.Uint64(1)},
	}, nil)
	require.ErrorContains(t, err, "creating temp snap file")
	require.NotErrorIs(t, err, ErrWALDirectoryMissing)

	require.ErrorIs(t, s.classify(err), ErrWALDirectoryMissing)
}

// TestSnapshotter_SaveRefusesAWALDirectoryReplacedByADirectory removes the WAL
// directory and puts a complete one back at the same path. etcd still holds the
// unlinked tree, so writing into the replacement would acknowledge state the node
// cannot recover.
func TestSnapshotter_SaveRefusesAWALDirectoryReplacedByADirectory(t *testing.T) {
	t.Parallel()

	walDir := filepath.Join(t.TempDir(), "waldir")
	s := newSnapshotterAt(t, filepath.Join(walDir, snapDir))

	require.NoError(t, os.RemoveAll(walDir))
	require.NoError(t, os.MkdirAll(filepath.Join(walDir, snapDir), 0755))

	err := s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(11), Term: proto.Uint64(2)},
	})
	require.ErrorIs(t, err, ErrWALDirectoryMissing)

	entries, err := os.ReadDir(filepath.Join(walDir, snapDir))
	require.NoError(t, err)
	require.Empty(t, entries, "the replacement must not receive snapshot files")
}

// TestSnapshotter_SaveRefusesAWALDirectoryReplacedByAFile covers the same
// substitution with a regular file, which resolves to ENOTDIR rather than ENOENT
// when taken from the pathname.
func TestSnapshotter_SaveRefusesAWALDirectoryReplacedByAFile(t *testing.T) {
	t.Parallel()

	walDir := filepath.Join(t.TempDir(), "waldir")
	s := newSnapshotterAt(t, filepath.Join(walDir, snapDir))

	require.NoError(t, os.RemoveAll(walDir))
	require.NoError(t, os.WriteFile(walDir, nil, 0600))

	err := s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(12), Term: proto.Uint64(2)},
	})
	require.ErrorIs(t, err, ErrWALDirectoryMissing)
}

// TestSnapshotter_ReadsDoNotFollowAWALDirectoryReplacement pins the read side to
// the same directory: a replacement's snapshot files must not be loaded as this
// node's own.
func TestSnapshotter_ReadsDoNotFollowAWALDirectoryReplacement(t *testing.T) {
	t.Parallel()

	walDir := filepath.Join(t.TempDir(), "waldir")
	s := newSnapshotterAt(t, filepath.Join(walDir, snapDir))

	require.NoError(t, os.RemoveAll(walDir))

	// A restart binds to whatever is on disk, so the replacement is usable — for
	// the reopened snapshotter only.
	reopened := newSnapshotterAt(t, filepath.Join(walDir, snapDir))
	require.NoError(t, reopened.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(77), Term: proto.Uint64(3)},
	}))

	loaded, err := reopened.Load()
	require.NoError(t, err)
	require.Equal(t, uint64(77), loaded.GetMetadata().GetIndex())

	_, err = s.Load()
	require.ErrorContains(t, err, "reading snap directory")

	require.ErrorIs(t, s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(78), Term: proto.Uint64(3)},
	}), ErrWALDirectoryMissing)
}

func TestSnapshotter_NewCreatesMissingAncestors(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "waldir", "nested", snapDir)
	s, err := NewSnapshotter(dir, logging.Testing())
	require.NoError(t, err)

	require.NoError(t, s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(4), Term: proto.Uint64(1)},
	}))

	loaded, err := s.Load()
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, uint64(4), loaded.GetMetadata().GetIndex())
}

func TestSnapshotter_NewFailsWhenThePathIsADanglingSymlink(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dir := filepath.Join(root, snapDir)
	require.NoError(t, os.Symlink(filepath.Join(root, "nowhere"), dir))

	_, err := NewSnapshotter(dir, logging.Testing())
	require.ErrorContains(t, err, "creating snapshot directory")
}

func TestSnapshotter_NewFailsWhenAnAncestorIsNotADirectory(t *testing.T) {
	t.Parallel()

	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blocker, nil, 0600))

	_, err := NewSnapshotter(filepath.Join(blocker, snapDir), logging.Testing())
	require.ErrorContains(t, err, "creating snapshot directory")
}

func TestSnapshotter_NewFailsWhenThePathIsAFile(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), snapDir)
	require.NoError(t, os.WriteFile(dir, nil, 0600))

	_, err := NewSnapshotter(dir, logging.Testing())
	require.ErrorContains(t, err, "is not a directory")
}

func TestSnapshotter_SaveFailsWhenThePathIsAFile(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), snapDir)
	require.NoError(t, os.WriteFile(dir, nil, 0600))

	s := snapshotterOver(t, dir)

	err := s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(1), Term: proto.Uint64(1)},
	})
	require.ErrorContains(t, err, "is not a directory")
}

func TestMkdirSynced_ToleratesAnExistingDirectory(t *testing.T) {
	t.Parallel()

	// A directory that appeared since the stat is usable.
	dir := filepath.Join(t.TempDir(), snapDir)
	require.NoError(t, os.Mkdir(dir, 0755))

	require.NoError(t, mkdirSynced(dir))
	require.DirExists(t, dir)
}

func TestMkdirSynced_NormalizesThePath(t *testing.T) {
	t.Parallel()

	// --wal-dir is operator-supplied, so a trailing separator reaches the walk. An
	// unnormalized path lists the same directory twice.
	root := t.TempDir()
	dir := filepath.Join(root, "waldir", snapDir) + "/"

	missing, err := missingAncestors(dir)
	require.NoError(t, err)
	require.Equal(t, []string{filepath.Join(root, "waldir", snapDir), filepath.Join(root, "waldir")}, missing)

	require.NoError(t, mkdirAllSynced(dir))
	require.DirExists(t, filepath.Join(root, "waldir", snapDir))
}

func TestSnapshotter_LoadEmpty(t *testing.T) {
	t.Parallel()

	s := newTestSnapshotter(t)

	loaded, err := s.Load()
	require.NoError(t, err)
	require.Nil(t, loaded)
}

func TestSnapshotter_LoadLatest(t *testing.T) {
	t.Parallel()

	s := newTestSnapshotter(t)

	// Save two snapshots
	require.NoError(t, s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(10), Term: proto.Uint64(1)},
		Data:     []byte("old"),
	}))
	require.NoError(t, s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(20), Term: proto.Uint64(2)},
		Data:     []byte("new"),
	}))

	loaded, err := s.Load()
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, uint64(20), loaded.GetMetadata().GetIndex())
	require.Equal(t, []byte("new"), loaded.GetData())
}

func TestSnapshotter_CleansUpOldFiles(t *testing.T) {
	t.Parallel()

	s := newTestSnapshotter(t)

	require.NoError(t, s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(10), Term: proto.Uint64(1)},
	}))
	require.NoError(t, s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(20), Term: proto.Uint64(2)},
	}))

	// Before cleanup, both snap files exist
	snapFiles := listSnapFiles(t, s.dir)
	require.Len(t, snapFiles, 2)

	// After explicit cleanup, only the latest snap file remains
	s.CleanupOlderThan(20)

	snapFiles = listSnapFiles(t, s.dir)
	require.Len(t, snapFiles, 1)
	require.Equal(t, snapFileName(2, 20), snapFiles[0])
}

func listSnapFiles(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	var names []string
	for _, e := range entries {
		if _, _, ok := parseSnapFileName(e.Name()); ok {
			names = append(names, e.Name())
		}
	}

	return names
}

func TestSnapshotter_IgnoresNonSnapFiles(t *testing.T) {
	t.Parallel()

	s := newTestSnapshotter(t)

	// Create a non-snap file
	require.NoError(t, os.WriteFile(filepath.Join(s.dir, "random.txt"), []byte("hi"), 0644))

	loaded, err := s.Load()
	require.NoError(t, err)
	require.Nil(t, loaded)
}

func TestSnapFileName_RoundTrip(t *testing.T) {
	t.Parallel()

	name := snapFileName(3, 42)
	term, index, ok := parseSnapFileName(name)
	require.True(t, ok)
	require.Equal(t, uint64(3), term)
	require.Equal(t, uint64(42), index)
}

func TestParseSnapFileName_Invalid(t *testing.T) {
	t.Parallel()

	_, _, ok := parseSnapFileName("not-a-snap.txt")
	require.False(t, ok)

	_, _, ok = parseSnapFileName("0000000000000001.snap")
	require.False(t, ok)
}
