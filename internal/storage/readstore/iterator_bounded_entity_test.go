package readstore

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Exercise both production constructors against their real key layouts. In
// particular, a transaction attribute has one canonical key, without a byLog
// suffix: updating its value must not create another entity in the scan.
func TestBoundedEntityIterator_ProductionConstructors(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"logs", "transactions"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			s := newTestStore(t)
			const ledger = "ledger"
			key := func(name string, id uint64) []byte {
				if kind == "logs" {
					return LedgerLogKey(dal.NewKeyBuilder(), name, id)
				}

				return (domain.TransactionKey{LedgerName: name, ID: id}).AppendBytes(
					[]byte{dal.ZoneAttributes, dal.SubAttrTransaction},
				)
			}
			for _, id := range []uint64{0, 3, 5, 9, math.MaxUint64 - 1, math.MaxUint64} {
				require.NoError(t, s.DB().Set(key(ledger, id), nil, pebble.NoSync))
			}
			// Neighboring ledger prefixes and an overwritten entity must neither
			// leak into this ledger nor produce duplicate IDs.
			for _, name := range []string{"ledgeq", "ledgers"} {
				require.NoError(t, s.DB().Set(key(name, 4), nil, pebble.NoSync))
			}
			require.NoError(t, s.DB().Set(key(ledger, 5), []byte("updated"), pebble.NoSync))
			otherKindKey := LedgerLogKey(dal.NewKeyBuilder(), ledger, 4)
			if kind == "logs" {
				otherKindKey = (domain.TransactionKey{LedgerName: ledger, ID: 4}).AppendBytes(
					[]byte{dal.ZoneAttributes, dal.SubAttrTransaction},
				)
			}
			require.NoError(t, s.DB().Set(otherKindKey, nil, pebble.NoSync))

			newIterator := func(t *testing.T, lower, upper []byte) EntityIterator {
				t.Helper()

				var it EntityIterator
				var err error
				if kind == "logs" {
					it, err = NewLedgerLogRangeIterator(s.DB(), dal.NewKeyBuilder(), ledger, lower, upper)
				} else {
					it, err = NewPebbleTxRangeIterator(s.DB(), ledger, lower, upper)
				}
				require.NoError(t, err)
				t.Cleanup(it.Close)

				return it
			}
			for _, tc := range []struct {
				name         string
				lower, upper []byte
				want         []uint64
			}{
				{"unbounded", nil, nil, []uint64{0, 3, 5, 9, math.MaxUint64 - 1, math.MaxUint64}},
				{"half open", txIDBytes(3), txIDBytes(9), []uint64{3, 5}},
				{"sparse bounds", txIDBytes(4), txIDBytes(8), []uint64{5}},
				{"empty gap", txIDBytes(6), txIDBytes(8), nil},
				{"exclusive maximum", nil, txIDBytes(math.MaxUint64), []uint64{0, 3, 5, 9, math.MaxUint64 - 1}},
				{"singleton maximum", txIDBytes(math.MaxUint64), nil, []uint64{math.MaxUint64}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					it := newIterator(t, tc.lower, tc.upper)
					var got []uint64
					// Bound the drain so a regression wrapping MaxUint64 fails
					// immediately instead of hanging the test process.
					for it.Next() {
						got = append(got, binary.BigEndian.Uint64(it.Current()))
						require.LessOrEqual(t, len(got), len(tc.want), "scan emitted extra or wrapped IDs")
					}
					require.Equal(t, tc.want, got)
					require.False(t, it.Next())
					require.NoError(t, it.Err())
				})
			}

			t.Run("absolute seek and exhaustion floor", func(t *testing.T) {
				it := newIterator(t, txIDBytes(3), txIDBytes(9))
				seek := func(target, want uint64) {
					t.Helper()
					require.True(t, it.SeekGE(txIDBytes(target)))
					require.Equal(t, want, binary.BigEndian.Uint64(it.Current()))
				}
				seek(0, 3) // Pebble clamps a seek below the lower bound.
				seek(4, 5)
				seek(4, 5) // Absolute seeks do not consume the row.
				require.False(t, it.SeekGE(txIDBytes(6)))
				require.False(t, it.Next(), "failed seek leaves iterator unpositioned")
				seek(3, 3)
				require.False(t, it.SeekGE(txIDBytes(7)), "covered by cached exhaustion floor")
				require.False(t, it.Next(), "cached failed seek must invalidate earlier position")
				seek(4, 5)
				require.False(t, it.Next())
				seek(0, 3) // Reposition after Next exhaustion.
				require.True(t, it.Next())
				require.Equal(t, uint64(5), binary.BigEndian.Uint64(it.Current()))
				require.NoError(t, it.Err())
			})

			t.Run("seek after maximum exhaustion", func(t *testing.T) {
				it := newIterator(t, nil, nil)
				require.True(t, it.SeekGE(txIDBytes(math.MaxUint64-1)))
				require.True(t, it.Next())
				require.Equal(t, uint64(math.MaxUint64), binary.BigEndian.Uint64(it.Current()))
				require.False(t, it.Next(), "maximum entity must not wrap to zero")
				require.False(t, it.Next())
				require.True(t, it.SeekGE(txIDBytes(4)))
				require.Equal(t, uint64(5), binary.BigEndian.Uint64(it.Current()))
				require.NoError(t, it.Err())
			})
		})
	}
}

func TestBoundedEntityIterator_FixedWidthSuffixAndOwnedPrefix(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	prefix := []byte{0x70, 0x20}
	for _, suffix := range [][]byte{{0, 1}, {0, 3}, {0, 5}, {0xff, 0xff}} {
		key := append(append([]byte(nil), prefix...), suffix...)
		require.NoError(t, s.DB().Set(key, nil, pebble.NoSync))
	}
	it, err := NewBoundedEntityIterator(s.DB(), prefix, []byte{0, 2}, []byte{0, 5}, 2)
	require.NoError(t, err)
	t.Cleanup(it.Close)

	// Constructors must own the prefix, including the bytes used by future
	// seeks: callers may reuse their key-builder buffer immediately.
	prefix[0] = 0x71
	require.True(t, it.Next())
	require.Equal(t, []byte{0, 3}, it.Current())
	require.False(t, it.Next(), "upper bound remains exclusive")
	require.True(t, it.SeekGE([]byte{0, 0}), "seek clamps to the stored lower bound")
	require.Equal(t, []byte{0, 3}, it.Current())
	require.False(t, it.SeekGE([]byte{0, 5}))
	require.False(t, it.Next())
	require.True(t, it.SeekGE([]byte{0, 3}), "seek uses the original prefix after exhaustion")
	require.Equal(t, []byte{0, 3}, it.Current())
	require.NoError(t, it.Err())
}
