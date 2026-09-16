package dal

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// benchSink keeps the benchmarked results live so the compiler cannot elide
// the copy being measured.
var (
	benchSink    []byte
	benchSinkErr error
)

// newBenchStoreWithValue returns a store holding one SST-backed key of the
// given value size. The flush matters: a memtable-backed lookup takes no file
// cache reference and would not exercise the path under measurement.
func newBenchStoreWithValue(b *testing.B, key []byte, size int) *Store {
	b.Helper()

	ctx := logging.TestingContext()

	s, err := NewStore(b.TempDir(), logging.FromContext(ctx), noop.NewMeterProvider().Meter("bench"), DefaultConfig())
	require.NoError(b, err)
	b.Cleanup(func() { _ = s.Close() })

	val := make([]byte, size)
	for i := range val {
		val[i] = byte(i)
	}

	batch := s.OpenWriteSession()
	require.NoError(b, batch.SetBytes(key, val))
	require.NoError(b, batch.Commit())
	require.NoError(b, s.Flush())

	// Warm the block cache so the benchmark measures steady-state reads
	// rather than first-touch I/O.
	_, closer, err := s.Get(key)
	require.NoError(b, err)
	require.NoError(b, closer.Close())

	return s
}

// BenchmarkStoreGet measures (*Store).Get, which copies the value and releases
// Pebble's resource under dbMu.RLock (EN-2072).
func BenchmarkStoreGet(b *testing.B) {
	key := []byte("bench-key")

	for _, size := range []int{64, 512, 4096} {
		b.Run(fmt.Sprintf("value=%dB", size), func(b *testing.B) {
			s := newBenchStoreWithValue(b, key, size)

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				val, closer, err := s.Get(key)
				benchSink, benchSinkErr = val, err

				if err == nil {
					_ = closer.Close()
				}
			}
		})
	}
}

// BenchmarkStoreGetValue measures the GetValue path, where (*Store).Get's copy
// is followed by GetValue's own copy.
func BenchmarkStoreGetValue(b *testing.B) {
	key := []byte("bench-key")

	for _, size := range []int{64, 512, 4096} {
		b.Run(fmt.Sprintf("value=%dB", size), func(b *testing.B) {
			s := newBenchStoreWithValue(b, key, size)

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				benchSink, benchSinkErr = GetValue(s, key)
			}
		})
	}
}
