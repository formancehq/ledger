package enginetest

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/engine"
)

// Ledger-shaped workload parameters. Keys mirror the main store: a zone
// byte, a sub-prefix byte, then a 32-byte canonical hash (attributes) or a
// big-endian sequence (history). Values are protobuf-sized.
const (
	benchZoneAttributes = 0x01
	benchZoneHistory    = 0x04
	benchBatchOps       = 200
	benchAttrValueSize  = 96
	benchLogValueSize   = 320
	benchHotKeys        = 100_000
	benchScanLen        = 1000
	benchSyncEvery      = 16 // batches between SyncWAL calls, like FSM maintenance
)

// RunBenchmarks runs the engine comparison suite against open.
func RunBenchmarks(b *testing.B, open Opener) {
	b.Helper()

	opts := engine.Options{CacheBytes: 256 << 20, MemTableBytes: 64 << 20, BloomBitsPerKey: 10, MaxConcurrentCompactions: 2}

	b.Run("ApplyBatches", func(b *testing.B) {
		db := mustOpenB(b, open, b.TempDir(), opts)
		w := newWorkload(1)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			w.applyBatch(b, db)
			if i%benchSyncEvery == benchSyncEvery-1 {
				require.NoError(b, db.SyncWAL())
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(b.N*benchBatchOps)/b.Elapsed().Seconds(), "ops/s")
		reportRSS(b)
	})

	b.Run("PointGetHot", func(b *testing.B) {
		db := mustOpenB(b, open, b.TempDir(), opts)
		w := newWorkload(2)
		w.load(b, db, benchHotKeys/benchBatchOps)
		require.NoError(b, db.Flush())
		keys := w.sampleAttrKeys(4096)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			v, closer, err := db.Get(keys[i%len(keys)])
			if err != nil {
				b.Fatal(err)
			}
			if len(v) == 0 {
				b.Fatal("empty value")
			}
			_ = closer.Close()
		}
	})

	b.Run("PointGetMissing", func(b *testing.B) {
		db := mustOpenB(b, open, b.TempDir(), opts)
		w := newWorkload(3)
		w.load(b, db, benchHotKeys/benchBatchOps)
		require.NoError(b, db.Flush())
		missing := make([][]byte, 4096)
		for i := range missing {
			missing[i] = attrKey(0x7f, uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			_, _, err := db.Get(missing[i%len(missing)])
			if !errors.Is(err, engine.ErrNotFound) {
				b.Fatalf("expected not found, got %v", err)
			}
		}
	})

	b.Run("ScanHistory1000", func(b *testing.B) {
		db := mustOpenB(b, open, b.TempDir(), opts)
		w := newWorkload(4)
		w.load(b, db, benchHotKeys/benchBatchOps)
		require.NoError(b, db.Flush())
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			start := 1 + uint64(rand.IntN(int(w.seq)-benchScanLen)) // sequences start at 1
			it, err := db.NewIter(&engine.IterOptions{LowerBound: logKey(start), UpperBound: logKey(start + benchScanLen)})
			if err != nil {
				b.Fatal(err)
			}
			n := 0
			for ok := it.First(); ok; ok = it.Next() {
				if _, err := it.ValueAndErr(); err != nil {
					b.Fatal(err)
				}
				n++
			}
			_ = it.Close()
			if n != benchScanLen {
				b.Fatalf("scanned %d, want %d", n, benchScanLen)
			}
		}
		b.ReportMetric(float64(b.N*benchScanLen)/b.Elapsed().Seconds(), "keys/s")
	})

	b.Run("SnapshotIterAttributes", func(b *testing.B) {
		db := mustOpenB(b, open, b.TempDir(), opts)
		w := newWorkload(5)
		w.load(b, db, benchHotKeys/benchBatchOps)
		require.NoError(b, db.Flush())
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			snap := db.NewSnapshot()
			it, err := snap.NewIter(&engine.IterOptions{LowerBound: []byte{benchZoneAttributes, 0x01}, UpperBound: []byte{benchZoneAttributes, 0x02}})
			if err != nil {
				b.Fatal(err)
			}
			n := 0
			for ok := it.First(); ok && n < benchScanLen; ok = it.Next() {
				n++
			}
			_ = it.Close()
			_ = snap.Close()
		}
	})

	b.Run("CheckpointAndReopen", func(b *testing.B) {
		root := b.TempDir()
		db := mustOpenB(b, open, filepath.Join(root, "live"), opts)
		w := newWorkload(6)
		w.load(b, db, 4*benchHotKeys/benchBatchOps)
		require.NoError(b, db.Flush())
		w.load(b, db, 50) // leave some WAL-only data
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			cp := filepath.Join(root, fmt.Sprintf("cp-%d", i))
			require.NoError(b, db.Checkpoint(cp))
			re, err := open(cp, opts)
			require.NoError(b, err)
			require.NoError(b, re.Close())
			require.NoError(b, os.RemoveAll(cp))
		}
		b.StopTimer()
		b.ReportMetric(float64(dirSize(b, filepath.Join(root, "live")))/1e6, "db-MB")
	})
}

type workload struct {
	rng  *rand.Rand
	seq  uint64
	attr [][]byte
	buf  []byte
}

func newWorkload(seed uint64) *workload {
	return &workload{rng: rand.New(rand.NewPCG(seed, seed)), buf: make([]byte, benchLogValueSize)}
}

func (w *workload) load(b *testing.B, db engine.DB, batches int) {
	b.Helper()
	for range batches {
		w.applyBatch(b, db)
	}
}

// applyBatch mimics one FSM apply: a burst of attribute upserts (volumes,
// metadata), a few history rows, committed without sync.
func (w *workload) applyBatch(b *testing.B, db engine.DB) {
	b.Helper()
	batch := db.NewBatch()
	for i := range benchBatchOps {
		if i%10 == 0 {
			w.seq++
			w.fill(benchLogValueSize)
			if err := batch.Set(logKey(w.seq), w.buf[:benchLogValueSize]); err != nil {
				b.Fatal(err)
			}

			continue
		}
		key := attrKey(byte(1+i%4), uint64(w.rng.IntN(benchHotKeys)))
		w.fill(benchAttrValueSize)
		if err := batch.Set(key, w.buf[:benchAttrValueSize]); err != nil {
			b.Fatal(err)
		}
		if len(w.attr) < benchHotKeys {
			w.attr = append(w.attr, key)
		}
	}
	if err := batch.Commit(false); err != nil {
		b.Fatal(err)
	}
	_ = batch.Close()
}

// fill writes n pseudo-random bytes into w.buf (compressible like protobuf
// payloads are not: random bytes keep compression honest across engines).
func (w *workload) fill(n int) {
	for i := 0; i < n; i += 8 {
		binary.LittleEndian.PutUint64(w.buf[i:], w.rng.Uint64())
	}
}

func (w *workload) sampleAttrKeys(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = w.attr[w.rng.IntN(len(w.attr))]
	}

	return out
}

func attrKey(sub byte, id uint64) []byte {
	var idb [8]byte
	binary.BigEndian.PutUint64(idb[:], id)
	h := sha256.Sum256(idb[:])
	key := make([]byte, 0, 2+32)
	key = append(key, benchZoneAttributes, sub)

	return append(key, h[:]...)
}

func logKey(seq uint64) []byte {
	key := make([]byte, 10)
	key[0] = benchZoneHistory
	key[1] = 0x01
	binary.BigEndian.PutUint64(key[2:], seq)

	return key
}

func mustOpenB(b *testing.B, open Opener, dir string, o engine.Options) engine.DB {
	b.Helper()
	db, err := open(dir, o)
	require.NoError(b, err)
	b.Cleanup(func() { _ = db.Close() })

	return db
}

func dirSize(b *testing.B, dir string) int64 {
	b.Helper()
	var total int64
	require.NoError(b, filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}

		return err
	}))

	return total
}

// reportRSS reports the process max RSS: RocksDB memory lives outside the Go
// heap, so Go allocation metrics alone would hide it.
func reportRSS(b *testing.B) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err == nil {
		b.ReportMetric(float64(ru.Maxrss)/rssDivisor, "maxrss-MB")
	}
}
