package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
)

func countWALFiles(t *testing.T, dir string) int {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	count := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".wal" {
			count++
		}
	}

	return count
}

func newTestWAL(t *testing.T, opts ...Option) *DefaultWAL {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	w, err := New(t.TempDir(), logger, meter, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	return w
}

func TestPurgeOldWALSegments(t *testing.T) {
	t.Parallel()

	w := newTestWAL(t, WithPurgeInterval(100*time.Millisecond))

	// Write enough data to force WAL segment rotation.
	// etcd WAL segments are 64MB, so we need ~140 entries of 1MB to create at least 3 segments.
	// ReleaseLockTo requires at least 3 segments to actually release the oldest lock,
	// because it always keeps the segment just before the target index locked.
	const numEntries = 140
	entryData := make([]byte, 1024*1024)
	for i := uint64(1); i <= numEntries; i++ {
		err := w.Append(
			raftpb.HardState{Term: 1, Vote: 1, Commit: i},
			[]raftpb.Entry{{Index: i, Term: 1, Data: entryData}},
		)
		require.NoError(t, err)
	}

	segmentsAfterWrite := countWALFiles(t, w.etcdWalDir)
	require.GreaterOrEqual(t, segmentsAfterWrite, 3, "writing ~140MB should create at least 3 WAL segments")

	// Create a snapshot at a high index and compact to release locks on old segments.
	cs := &raftpb.ConfState{Voters: []uint64{1}}
	require.NoError(t, w.CreateSnapshot(numEntries, cs, nil))
	require.NoError(t, w.Compact(numEntries))

	// The background purger should eventually delete old unlocked segments.
	require.Eventually(t, func() bool {
		return countWALFiles(t, w.etcdWalDir) < segmentsAfterWrite
	}, 10*time.Second, 200*time.Millisecond, "old WAL segments should be purged")
}
