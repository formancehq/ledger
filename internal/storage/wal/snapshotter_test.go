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

	dir := t.TempDir()
	s, err := NewSnapshotter(dir, logging.Testing())
	require.NoError(t, err)

	return s
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

func TestSnapshotter_SaveRecreatesMissingAncestors(t *testing.T) {
	t.Parallel()

	walDir := filepath.Join(t.TempDir(), "waldir")
	s, err := NewSnapshotter(filepath.Join(walDir, snapDir), logging.Testing())
	require.NoError(t, err)

	// The whole WAL directory, not just the snapshot directory: Save has to
	// recreate every level it needs.
	require.NoError(t, os.RemoveAll(walDir))

	require.NoError(t, s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(11), Term: proto.Uint64(2)},
	}))

	loaded, err := s.Load()
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, uint64(11), loaded.GetMetadata().GetIndex())

	info, err := os.Stat(walDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestSnapshotter_SaveFailsWhenTheDirCannotBeChecked(t *testing.T) {
	t.Parallel()

	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blocker, nil, 0600))

	s := &Snapshotter{dir: filepath.Join(blocker, snapDir), logger: logging.Testing()}

	err := s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(1), Term: proto.Uint64(1)},
	})
	require.ErrorContains(t, err, "checking snapshot directory")
}

func TestSnapshotter_SaveFailsWhenTheDirCannotBeRecreated(t *testing.T) {
	t.Parallel()

	root := t.TempDir()

	// A dangling symlink reads as missing but cannot be replaced by a directory,
	// so the recreation fails on the level below it.
	dangling := filepath.Join(root, "dangling")
	require.NoError(t, os.Symlink(filepath.Join(root, "nowhere"), dangling))

	s := &Snapshotter{dir: filepath.Join(dangling, snapDir), logger: logging.Testing()}

	err := s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(1), Term: proto.Uint64(1)},
	})
	require.ErrorContains(t, err, "recreating snapshot directory")
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
