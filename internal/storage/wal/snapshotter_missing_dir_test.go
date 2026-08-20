package wal

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// A follower catching up on a leader snapshot persists it through Save, and the
// failure propagates as a task-pool panic that takes the node down. Losing the
// directory underneath a running node is not a reason to lose the node.
func TestSnapshotterSaveRecreatesAMissingDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir() + "/snap"
	s, err := NewSnapshotter(dir, logging.Testing())
	require.NoError(t, err)

	require.NoError(t, os.RemoveAll(dir))

	require.NoError(t, s.Save(&raftpb.Snapshot{
		Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(9000), Term: proto.Uint64(7)},
	}))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "the snapshot is on disk")
}
