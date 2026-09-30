package enginetest

import (
	"testing"

	"github.com/formancehq/ledger/v3/internal/storage/engine"
)

// RunIterSetupBenchmark isolates iterator creation + one seek + close, the
// fixed cost every short scan pays.
func RunIterSetupBenchmark(b *testing.B, open Opener) {
	b.Helper()
	db := mustOpenB(b, open, b.TempDir(), engine.Options{CacheBytes: 256 << 20, BloomBitsPerKey: 10})
	w := newWorkload(9)
	w.load(b, db, benchHotKeys/benchBatchOps)
	if err := db.Flush(); err != nil {
		b.Fatal(err)
	}
	lower, upper := []byte{benchZoneAttributes, 0x01}, []byte{benchZoneAttributes, 0x02}
	b.Run("NewIterFirstClose", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			it, err := db.NewIter(&engine.IterOptions{LowerBound: lower, UpperBound: upper})
			if err != nil {
				b.Fatal(err)
			}
			it.First()
			_ = it.Close()
		}
	})
	b.Run("SnapshotNewIterFirstClose", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			s := db.NewSnapshot()
			it, err := s.NewIter(&engine.IterOptions{LowerBound: lower, UpperBound: upper})
			if err != nil {
				b.Fatal(err)
			}
			it.First()
			_ = it.Close()
			_ = s.Close()
		}
	})
}
