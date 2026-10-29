package waltest_test

import (
	"flag"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	etcdwal "go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/wal"
	"github.com/formancehq/ledger/v3/internal/storage/wal/waltest"
)

// etcdProductionSegmentSizeBytes is etcd's default segment size, which
// production nodes run with because nothing outside tests assigns it.
const etcdProductionSegmentSizeBytes int64 = 64 * 1000 * 1000

// This binary never calls UseSmallSegments from TestMain, so it is where the
// production segment size stays covered. The tests below are not parallel:
// TestUseSmallSegments changes the process-wide value. The DefaultWAL test
// cannot close the snapshotters it opens, because Close leaves them open and
// they are unexported; each holds one directory handle until GC.

func TestDefaultWALRunsAtProductionSegmentSize(t *testing.T) {
	require.Equal(t, etcdProductionSegmentSizeBytes, etcdwal.SegmentSizeBytes,
		"importing waltest must not change the segment size")

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")
	dir := t.TempDir()

	w, err := wal.New(dir, logger, meter)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	segments, err := filepath.Glob(filepath.Join(dir, "etcd", "*.wal"))
	require.NoError(t, err)
	require.Len(t, segments, 1)
	info, err := os.Stat(segments[0])
	require.NoError(t, err)
	require.Equal(t, etcdProductionSegmentSizeBytes, info.Size(),
		"etcd preallocates the first segment at the production size")

	require.NoError(t, w.Append(&raftpb.HardState{Term: new(uint64(1)), Commit: new(uint64(2))}, []*raftpb.Entry{
		{Index: new(uint64(1)), Term: new(uint64(1)), Data: []byte("first")},
		{Index: new(uint64(2)), Term: new(uint64(1)), Data: []byte("second")},
	}))
	require.NoError(t, w.Close())

	reopened, err := wal.New(dir, logger, meter)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	entries, err := reopened.Entries(1, 3, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, []byte("first"), entries[0].GetData())
	require.Equal(t, []byte("second"), entries[1].GetData())
}

// Each case pins -test.bench itself, so the result does not depend on whether
// this binary was started with -bench.
func TestUseSmallSegments(t *testing.T) {
	bench := flag.Lookup("test.bench")
	require.NotNil(t, bench)

	for _, tc := range []struct {
		name  string
		bench string
		want  int64
	}{
		{name: "test run installs the small size", bench: "", want: waltest.SegmentSizeBytes},
		{name: "benchmark run keeps the production size", bench: ".", want: etcdProductionSegmentSizeBytes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			originalSize, originalBench := etcdwal.SegmentSizeBytes, bench.Value.String()
			t.Cleanup(func() {
				etcdwal.SegmentSizeBytes = originalSize
				require.NoError(t, bench.Value.Set(originalBench))
			})
			etcdwal.SegmentSizeBytes = etcdProductionSegmentSizeBytes
			require.NoError(t, bench.Value.Set(tc.bench))

			waltest.UseSmallSegments()

			require.Equal(t, tc.want, etcdwal.SegmentSizeBytes)
		})
	}
}
