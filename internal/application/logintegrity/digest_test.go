package logintegrity

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func newLogDB(t *testing.T) *pebble.DB {
	t.Helper()
	db, err := pebble.Open("logs", &pebble.Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	return db
}

func putLog(t *testing.T, db *pebble.DB, keySeq uint64, log *commonpb.Log) {
	t.Helper()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(keySeq).Build()
	value, err := log.MarshalVT()
	require.NoError(t, err)
	require.NoError(t, db.Set(key, value, pebble.Sync))
}

func digestDB(t *testing.T, db *pebble.DB, first, last uint64) (RangeDigest, error) {
	t.Helper()
	snap := db.NewSnapshot()
	defer func() { require.NoError(t, snap.Close()) }()

	return DigestRange(t.Context(), snap, first, last)
}

func ledgerLog(seq uint64, names ...string) *commonpb.Log {
	metadata := make(map[string]*commonpb.MetadataValue, len(names))
	for _, name := range names {
		metadata[name] = commonpb.NewStringValue(name)
	}

	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger", Metadata: metadata},
		}},
	}
}

func TestDigestRangeCanonicalAndResultSensitive(t *testing.T) {
	t.Parallel()
	left, right := newLogDB(t), newLogDB(t)
	first := ledgerLog(1, "a", "z")
	otherFirst := ledgerLog(1, "z", "a")
	putLog(t, left, 1, first)
	putLog(t, right, 1, otherFirst)
	putLog(t, left, 2, ledgerLog(2, "result"))
	putLog(t, right, 2, ledgerLog(2, "result"))

	leftDigest, err := digestDB(t, left, 1, 2)
	require.NoError(t, err)
	rightDigest, err := digestDB(t, right, 1, 2)
	require.NoError(t, err)
	require.Equal(t, leftDigest, rightDigest, "map iteration must not change the digest")

	otherFirst.ResponseSignature = &signaturepb.SignedLog{KeyId: "unexpected-persisted-signature"}
	putLog(t, right, 1, otherFirst)
	signed, err := digestDB(t, right, 1, 2)
	require.NoError(t, err)
	require.NotEqual(t, leftDigest.Hash, signed.Hash, "every persisted field must be covered")
	otherFirst.ResponseSignature = nil
	putLog(t, right, 1, otherFirst)

	putLog(t, right, 2, ledgerLog(2, "different-result"))
	changed, err := digestDB(t, right, 1, 2)
	require.NoError(t, err)
	require.NotEqual(t, leftDigest.Hash, changed.Hash, "a different committed result must be detected")

	// A range digest binds the window, not just the sequence of present rows.
	single, err := digestDB(t, left, 1, 1)
	require.NoError(t, err)
	require.NotEqual(t, leftDigest.Hash, single.Hash)
}

func TestDigestRangeRejectsHolesAndCorruptSequence(t *testing.T) {
	t.Parallel()
	db := newLogDB(t)
	putLog(t, db, 1, ledgerLog(1))
	putLog(t, db, 3, ledgerLog(3))
	_, err := digestDB(t, db, 1, 3)
	var missing *MissingLogError
	require.ErrorAs(t, err, &missing)
	require.Equal(t, uint64(2), missing.Sequence)

	putLog(t, db, 2, ledgerLog(99))
	_, err = digestDB(t, db, 1, 3)
	require.ErrorContains(t, err, "key 2 carries sequence 99")
}

func TestDigestRangeBoundsAndCancellation(t *testing.T) {
	t.Parallel()
	db := newLogDB(t)
	for _, tc := range [][2]uint64{{0, 1}, {2, 1}, {1, MaxRangeEntries + 1}} {
		_, err := digestDB(t, db, tc[0], tc[1])
		require.ErrorContains(t, err, "invalid log digest range")
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := DigestRange(ctx, db, 1, 1)
	require.True(t, errors.Is(err, context.Canceled))
}

func TestDigestRangeGolden(t *testing.T) {
	t.Parallel()
	db := newLogDB(t)
	putLog(t, db, 1, ledgerLog(1, "a", "z"))
	digest, err := digestDB(t, db, 1, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(DigestVersion), digest.Version)
	require.Equal(t, "b8be8e3c42dd95012d52bb14c4a1a6b3385d87609cfaf785bd0081316bbbd185", hex.EncodeToString(digest.Hash[:]))
}

func TestLastLogSequenceUsesKey(t *testing.T) {
	t.Parallel()
	db := newLogDB(t)
	last, err := LastLogSequence(db)
	require.NoError(t, err)
	require.Zero(t, last)
	putLog(t, db, 7, ledgerLog(99))
	last, err = LastLogSequence(db)
	require.NoError(t, err)
	require.Equal(t, uint64(7), last)
}

func BenchmarkDigestRange(b *testing.B) {
	for _, count := range []uint64{256, MaxRangeEntries} {
		b.Run(fmt.Sprintf("logs_%d", count), func(b *testing.B) {
			db, err := pebble.Open("logs", &pebble.Options{FS: vfs.NewMem()})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = db.Close() }() // Benchmark cleanup.
			batch := db.NewBatch()
			defer func() { _ = batch.Close() }() // Benchmark cleanup.
			for seq := uint64(1); seq <= count; seq++ {
				key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(seq).Build()
				value, err := ledgerLog(seq, "result").MarshalVT()
				if err != nil {
					b.Fatal(err)
				}
				if err := batch.Set(key, value, nil); err != nil {
					b.Fatal(err)
				}
			}
			if err := batch.Commit(pebble.NoSync); err != nil {
				b.Fatal(err)
			}
			snap := db.NewSnapshot()
			defer func() { _ = snap.Close() }() // Benchmark cleanup.
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := DigestRange(context.Background(), snap, 1, count); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
