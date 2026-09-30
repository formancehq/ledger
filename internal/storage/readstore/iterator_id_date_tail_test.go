package readstore

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestIDDateRangeIterator_NarrowWindowStopsAtLastMatch(t *testing.T) {
	t.Parallel()
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "ascending", true: "descending"}[reverse], func(t *testing.T) {
			t.Parallel()
			store, err := New(t.TempDir(), logging.FromContext(logging.TestingContext()), DefaultConfig())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			kb := dal.NewKeyBuilder()
			batch := store.NewBatch()
			matchID := uint64(1)
			if reverse {
				matchID = 1_000
			}
			for id := uint64(1); id <= 1_000; id++ {
				date := uint64(100)
				if id == matchID {
					date = 5
				}
				require.NoError(t, batch.SetBytes(TransactionTimestampKey(kb, "ledger", date, id), nil))
				var value [8]byte
				binary.BigEndian.PutUint64(value[:], date)
				require.NoError(t, batch.SetBytes(IDDateKey(kb, PrefixTransactionTimestampByID, "ledger", id), value[:]))
			}
			require.NoError(t, batch.Commit())
			prefix := TransactionTimestampRangePrefix(kb, "ledger")
			lower := append(append([]byte(nil), prefix...), EncodeTxID(nil, 5)...)
			upper := append(append([]byte(nil), prefix...), EncodeTxID(nil, 6)...)
			idPrefix := IDDatePrefix(kb, PrefixTransactionTimestampByID, "ledger")
			for _, cursor := range []bool{false, true} {
				if reverse {
					it, err := NewIDDateRangeIterator[Desc](store.DB(), idPrefix, lower, upper, len(prefix)+8, 5, 6, true, true, false, 0)
					require.NoError(t, err)
					var before []byte
					if cursor {
						before = EncodeTxID(nil, matchID)
					}
					items, more, err := PaginateReverse(it, 1, before)
					require.NoError(t, err)
					require.False(t, more)
					if cursor {
						require.Empty(t, items)
					} else {
						require.Equal(t, [][]byte{EncodeTxID(nil, matchID)}, items)
					}
					require.Less(t, it.iter.Stats().ReverseStepCount[pebble.InterfaceCall], 100)
					it.Close()
				} else {
					it, err := NewIDDateRangeIterator[Asc](store.DB(), idPrefix, lower, upper, len(prefix)+8, 5, 6, true, true, false, 0)
					require.NoError(t, err)
					var after []byte
					if cursor {
						after = EncodeTxID(nil, matchID)
					}
					items, more, err := PaginateForward(it, 1, after)
					require.NoError(t, err)
					require.False(t, more)
					if cursor {
						require.Empty(t, items)
					} else {
						require.Equal(t, [][]byte{EncodeTxID(nil, matchID)}, items)
					}
					require.Less(t, it.iter.Stats().ForwardStepCount[pebble.InterfaceCall], 100)
					it.Close()
				}
			}
		})
	}
}

func TestIDDateRangeIterator_SparseCursorGap(t *testing.T) {
	t.Parallel()
	store, err := New(t.TempDir(), logging.FromContext(logging.TestingContext()), DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()
	for id := uint64(1); id <= 1_000; id++ {
		date := uint64(100)
		if id == 1 || id == 1_000 {
			date = 5
		}
		require.NoError(t, batch.SetBytes(TransactionTimestampKey(kb, "ledger", date, id), nil))
		var value [8]byte
		binary.BigEndian.PutUint64(value[:], date)
		require.NoError(t, batch.SetBytes(IDDateKey(kb, PrefixTransactionTimestampByID, "ledger", id), value[:]))
	}
	require.NoError(t, batch.Commit())
	prefix := TransactionTimestampRangePrefix(kb, "ledger")
	lower := append(append([]byte(nil), prefix...), EncodeTxID(nil, 5)...)
	upper := append(append([]byte(nil), prefix...), EncodeTxID(nil, 6)...)
	idPrefix := IDDatePrefix(kb, PrefixTransactionTimestampByID, "ledger")
	ascFirst, err := NewIDDateRangeIterator[Asc](store.DB(), idPrefix, lower, upper, len(prefix)+8, 5, 6, true, true, false, 0)
	require.NoError(t, err)
	items, more, err := PaginateForward(ascFirst, 1, nil)
	require.NoError(t, err)
	require.True(t, more)
	require.Equal(t, [][]byte{EncodeTxID(nil, 1)}, items)
	require.Less(t, ascFirst.iter.Stats().ForwardStepCount[pebble.InterfaceCall], 100)
	ascFirst.Close()

	asc, err := NewIDDateRangeIterator[Asc](store.DB(), idPrefix, lower, upper, len(prefix)+8, 5, 6, true, true, false, 0)
	require.NoError(t, err)
	items, more, err = PaginateForward(asc, 1, EncodeTxID(nil, 1))
	require.NoError(t, err)
	require.False(t, more)
	require.Equal(t, [][]byte{EncodeTxID(nil, 1_000)}, items)
	require.Less(t, asc.iter.Stats().ForwardStepCount[pebble.InterfaceCall], 100)
	asc.Close()

	descFirst, err := NewIDDateRangeIterator[Desc](store.DB(), idPrefix, lower, upper, len(prefix)+8, 5, 6, true, true, false, 0)
	require.NoError(t, err)
	items, more, err = PaginateReverse(descFirst, 1, nil)
	require.NoError(t, err)
	require.True(t, more)
	require.Equal(t, [][]byte{EncodeTxID(nil, 1_000)}, items)
	require.Less(t, descFirst.iter.Stats().ReverseStepCount[pebble.InterfaceCall], 100)
	descFirst.Close()

	desc, err := NewIDDateRangeIterator[Desc](store.DB(), idPrefix, lower, upper, len(prefix)+8, 5, 6, true, true, false, 0)
	require.NoError(t, err)
	items, more, err = PaginateReverse(desc, 1, EncodeTxID(nil, 1_000))
	require.NoError(t, err)
	require.False(t, more)
	require.Equal(t, [][]byte{EncodeTxID(nil, 1)}, items)
	require.Less(t, desc.iter.Stats().ReverseStepCount[pebble.InterfaceCall], 100)
	desc.Close()
}

func BenchmarkIDDateRangeNarrowTail(b *testing.B) {
	for _, rows := range []uint64{10_000, 100_000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			for _, reverse := range []bool{false, true} {
				name := "ascending"
				if reverse {
					name = "descending"
				}
				b.Run(name, func(b *testing.B) {
					store, err := New(b.TempDir(), logging.FromContext(logging.TestingContext()), DefaultConfig())
					require.NoError(b, err)
					b.Cleanup(func() { require.NoError(b, store.Close()) })
					kb := dal.NewKeyBuilder()
					batch := store.NewBatch()
					matchID := uint64(1)
					if reverse {
						matchID = rows
					}
					for id := uint64(1); id <= rows; id++ {
						date := uint64(100)
						if id == matchID {
							date = 5
						}
						require.NoError(b, batch.SetBytes(TransactionTimestampKey(kb, "ledger", date, id), nil))
						var value [8]byte
						binary.BigEndian.PutUint64(value[:], date)
						require.NoError(b, batch.SetBytes(IDDateKey(kb, PrefixTransactionTimestampByID, "ledger", id), value[:]))
						if id%10_000 == 0 && id != rows {
							require.NoError(b, batch.Commit())
							batch = store.NewBatch()
						}
					}
					require.NoError(b, batch.Commit())
					prefix := TransactionTimestampRangePrefix(kb, "ledger")
					lower := append(append([]byte(nil), prefix...), EncodeTxID(nil, 5)...)
					upper := append(append([]byte(nil), prefix...), EncodeTxID(nil, 6)...)
					idPrefix := IDDatePrefix(kb, PrefixTransactionTimestampByID, "ledger")
					for _, cursor := range []bool{false, true} {
						page := "first"
						if cursor {
							page = "after-last"
						}
						b.Run(page, func(b *testing.B) {
							b.ReportAllocs()
							for b.Loop() {
								var after []byte
								if cursor {
									after = EncodeTxID(nil, matchID)
								}
								if reverse {
									it, err := NewIDDateRangeIterator[Desc](store.DB(), idPrefix, lower, upper, len(prefix)+8, 5, 6, true, true, false, 0)
									require.NoError(b, err)
									items, _, err := PaginateReverse(it, 1, after)
									require.NoError(b, err)
									it.Close()
									if cursor {
										require.Empty(b, items)
									} else {
										require.Len(b, items, 1)
									}
								} else {
									it, err := NewIDDateRangeIterator[Asc](store.DB(), idPrefix, lower, upper, len(prefix)+8, 5, 6, true, true, false, 0)
									require.NoError(b, err)
									items, _, err := PaginateForward(it, 1, after)
									require.NoError(b, err)
									it.Close()
									if cursor {
										require.Empty(b, items)
									} else {
										require.Len(b, items, 1)
									}
								}
							}
						})
					}
				})
			}
		})
	}
}
