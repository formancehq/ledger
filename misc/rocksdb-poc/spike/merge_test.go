package spike

import (
	"math/big"
	"testing"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/require"
)

func TestVolumeMergeAccumulates(t *testing.T) {
	mo := &countingMergeOperator{}
	opts := openOptions(t, nil, mo)
	db := openDB(t, opts)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	key := []byte{ReplayPrefixVolume, 'a'}
	for i := range 1000 {
		require.NoError(t, db.Merge(wo, key, EncodeVolume(big.NewInt(1), big.NewInt(2))))
		if i%100 == 99 {
			flush(t, db) // spread operands across files
		}
	}

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	v, err := db.Get(ro, key)
	require.NoError(t, err)
	in, out, err := DecodeVolume(v.Data())
	v.Free()
	require.NoError(t, err)
	require.Equal(t, int64(1000), in.Int64())
	require.Equal(t, int64(2000), out.Int64())
	require.Positive(t, mo.full.Load())
}

func TestTxMergeResolvesOrderedOps(t *testing.T) {
	opts := openOptions(t, nil, ReplayMergeOperator{})
	db := openDB(t, opts)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	key := []byte{ReplayPrefixTransaction, 'x'}
	require.NoError(t, db.Merge(wo, key, append([]byte{TxOpSet}, "base"...)))
	flush(t, db)
	require.NoError(t, db.Merge(wo, key, append([]byte{TxOpAdd}, "+1"...)))
	flush(t, db)
	require.NoError(t, db.Merge(wo, key, append([]byte{TxOpAdd}, "+2"...)))

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	v, err := db.Get(ro, key)
	require.NoError(t, err)
	require.Equal(t, "base+1+2", string(v.Data()))
	v.Free()

	// Iteration resolves merges too.
	it := db.NewIterator(ro)
	defer it.Close()
	it.Seek(key)
	require.True(t, it.Valid())
	require.Equal(t, "base+1+2", string(it.Value().Data()))
}

// TestPartialMergeDefersTxOps checks the Pebble includesBase=false branch:
// when a compaction folds operands without reaching the base value, the tx
// ops are re-emitted as an ordered batch and resolved later by FullMerge.
//
// The C API cannot place a file at a chosen level, and a level-style
// CompactRange on a fresh DB leaves the base in L1 where every later
// L0 compaction picks it up (FullMerge). Universal compaction with a
// two-run merge width compacts the two small operand runs together and
// leaves the much larger base run alone, which is the shape we need.
func TestPartialMergeDefersTxOps(t *testing.T) {
	mo := &countingMergeOperator{}
	opts := openOptions(t, nil, mo)
	opts.SetDisableAutoCompactions(false)
	opts.SetCompactionStyle(grocksdb.UniversalCompactionStyle)
	opts.SetLevel0FileNumCompactionTrigger(3)
	uni := grocksdb.NewDefaultUniversalCompactionOptions()
	// SetUniversalCompactionOptions consumes the object; destroying it after
	// the call segfaults (same ownership pattern as the merge operator).
	uni.SetMinMergeWidth(2)
	uni.SetMaxMergeWidth(2)
	uni.SetSizeRatio(1)
	uni.SetMaxSizeAmplificationPercent(100000)
	opts.SetUniversalCompactionOptions(uni)
	db := openDB(t, opts)

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	key := []byte{ReplayPrefixTransaction, 'x'}

	// Base run, padded with filler so it is far larger than the operand runs.
	require.NoError(t, db.Put(wo, key, []byte("base")))
	filler := make([]byte, 1<<20)
	for i := range 8 {
		require.NoError(t, db.Put(wo, []byte{'z', byte(i)}, filler))
	}
	flush(t, db)

	// Two tiny operand-only runs: their compaction cannot see the base.
	require.NoError(t, db.Merge(wo, key, append([]byte{TxOpAdd}, "+1"...)))
	flush(t, db)
	require.NoError(t, db.Merge(wo, key, append([]byte{TxOpAdd}, "+2"...)))
	flush(t, db)

	require.Eventually(t, func() bool { return mo.partial.Load() > 0 }, 10*time.Second, 20*time.Millisecond,
		"expected the operand-run compaction to call PartialMerge (levelstats: %s)", db.GetProperty("rocksdb.levelstats"))

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	v, err := db.Get(ro, key)
	require.NoError(t, err)
	require.Equal(t, "base+1+2", string(v.Data()))
	v.Free()
}

func TestPartialMergeVolumesFold(t *testing.T) {
	var mo ReplayMergeOperator
	key := []byte{ReplayPrefixVolume}
	folded, ok := mo.PartialMerge(key, EncodeVolume(big.NewInt(1), big.NewInt(10)), EncodeVolume(big.NewInt(2), big.NewInt(20)))
	require.True(t, ok)
	full, ok := mo.FullMerge(key, EncodeVolume(big.NewInt(100), big.NewInt(1000)), [][]byte{folded})
	require.True(t, ok)
	in, out, err := DecodeVolume(full)
	require.NoError(t, err)
	require.Equal(t, int64(103), in.Int64())
	require.Equal(t, int64(1030), out.Int64())
}

func TestMergeRejectsUnknownPrefix(t *testing.T) {
	var mo ReplayMergeOperator
	_, ok := mo.FullMerge([]byte{'?'}, nil, [][]byte{{1}})
	require.False(t, ok)
	_, ok = mo.FullMerge(nil, nil, nil)
	require.False(t, ok)
}
