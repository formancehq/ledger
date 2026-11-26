package dal

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestPrefixUpperBound(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		prefix []byte
		want   []byte
	}{
		{"ordinary prefix increments the last byte", []byte{0x04, 0x01}, []byte{0x04, 0x02}},
		{"single byte", []byte{0x04}, []byte{0x05}},
		{"trailing 0xFF carries and truncates", []byte{0x04, 0xFF}, []byte{0x05}},
		{"a run of 0xFF carries through all of it", []byte{0x04, 0xFF, 0xFF, 0xFF}, []byte{0x05}},
		{"an interior 0xFF is untouched", []byte{0xFF, 0x01}, []byte{0xFF, 0x02}},
		{"all 0xFF has no successor", []byte{0xFF, 0xFF}, nil},
		{"empty prefix has no successor", []byte{}, nil},
		{"nil prefix has no successor", nil, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.want, PrefixUpperBound(tc.prefix))
		})
	}
}

// TestPrefixUpperBound_DoesNotAliasInput pins that the bound is a fresh
// allocation. Callers build the prefix with a KeyBuilder and keep using it as
// the lower bound; mutating it in place would move both ends of the range.
func TestPrefixUpperBound_DoesNotAliasInput(t *testing.T) {
	t.Parallel()

	prefix := []byte{0x04, 0x01}
	bound := PrefixUpperBound(prefix)

	require.Equal(t, []byte{0x04, 0x02}, bound)
	require.Equal(t, []byte{0x04, 0x01}, prefix, "the prefix must not be mutated")
}

func TestZonePrefixUpperBound(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		zone byte
		sub  byte
		want []byte
	}{
		{"history log", ZoneHistory, SubHistoryLog, []byte{ZoneHistory, SubHistoryAudit}},
		{"history audit", ZoneHistory, SubHistoryAudit, []byte{ZoneHistory, SubHistoryAuditItem}},
		{"sub 0xFF carries into the zone", 0x04, 0xFF, []byte{0x05}},
		{"both 0xFF has no successor", 0xFF, 0xFF, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.want, ZonePrefixUpperBound(tc.zone, tc.sub))
		})
	}
}

// TestZonePrefixUpperBound_IncludesMaxUint64Suffix is the regression this helper
// exists for. Pebble's iterator upper bound is EXCLUSIVE, so the bound this
// codebase used to build — the two-byte prefix followed by an eight-byte 0xFF
// run — is byte-identical to the key whose sequence suffix is math.MaxUint64 and
// silently drops exactly that row. Every sequence-keyed scan in the store was
// blind to it.
func TestZonePrefixUpperBound_IncludesMaxUint64Suffix(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	key := func(sequence uint64) []byte {
		out := make([]byte, 10)
		out[0], out[1] = ZoneHistory, SubHistoryLog
		binary.BigEndian.PutUint64(out[2:], sequence)

		return out
	}

	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetProto(key(1), &commonpb.Log{Sequence: 1}))
	require.NoError(t, batch.SetProto(key(math.MaxUint64), &commonpb.Log{Sequence: math.MaxUint64}))
	require.NoError(t, batch.Commit())

	handle, err := s.NewReadHandle()
	require.NoError(t, err)

	// Not t.Parallel() below: the subtests share this handle, and a parallel
	// subtest would run after this function returns and its deferred Close has
	// already fired.
	defer func() { _ = handle.Close() }()

	t.Run("the old 0xFF-run bound drops the row", func(t *testing.T) {
		legacy := append(append([]byte{}, ZoneHistory, SubHistoryLog),
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

		iter, err := NewBoundedIter(handle, []byte{ZoneHistory, SubHistoryLog}, legacy)
		require.NoError(t, err)

		defer func() { _ = iter.Close() }()

		require.True(t, iter.Last())
		require.EqualValues(t, 1, binary.BigEndian.Uint64(iter.Key()[2:10]),
			"this is the bug: the highest row reads as 1, not MaxUint64")
	})

	t.Run("ReadLastEntry sees it", func(t *testing.T) {
		last, err := ReadLastEntry[*commonpb.Log](handle, ZoneHistory, SubHistoryLog)
		require.NoError(t, err)
		require.NotNil(t, last)
		require.EqualValues(t, uint64(math.MaxUint64), last.GetSequence())
	})

	t.Run("ScanZone sees it", func(t *testing.T) {
		logs, err := CollectZone[*commonpb.Log](handle, ZoneHistory, SubHistoryLog)
		require.NoError(t, err)
		require.Len(t, logs, 2)
		require.EqualValues(t, uint64(math.MaxUint64), logs[1].GetSequence())
	})
}
