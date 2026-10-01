package query_test

import (
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// Independent ID/date truth: 1 has a later date than 2, 3 and 4, so
// streaming the date index directly would violate the public ID order.
func TestTimestampRange_IDCursorPagesAndSnapshot(t *testing.T) {
	t.Parallel()
	store, err := readstore.New(t.TempDir(), logging.FromContext(logging.TestingContext()), readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()
	for id, date := range map[uint64]uint64{1: 300, 2: 100, 3: 200, 4: 150, 5: 500, 6: 200, 7: 250, 8: 1_000} {
		require.NoError(t, batch.SetBytes(readstore.TransactionTimestampKey(kb, parityLedger, date, id), nil))
		var value [8]byte
		binary.BigEndian.PutUint64(value[:], date)
		require.NoError(t, batch.SetBytes(readstore.IDDateKey(kb, readstore.PrefixTransactionTimestampByID, parityLedger, id), value[:]))
	}
	require.NoError(t, batch.Commit())
	snap := store.NewSnapshot()
	defer func() { require.NoError(t, snap.Close()) }()
	// The aligned request keeps this snapshot. Later index writes must not
	// enter either its date scan or its ID-ordered membership check.
	require.NoError(t, store.DB().Set(readstore.TransactionTimestampKey(kb, parityLedger, 200, 9), nil, pebble.NoSync))
	var newDate [8]byte
	binary.BigEndian.PutUint64(newDate[:], 200)
	require.NoError(t, store.DB().Set(readstore.IDDateKey(kb, readstore.PrefixTransactionTimestampByID, parityLedger, 9), newDate[:], pebble.NoSync))

	for _, pageSize := range []uint32{1, 2, 3, 10} {
		for _, reverse := range []bool{false, true} {
			name := "forward"
			want := []uint64{1, 3, 4, 6, 7}
			if reverse {
				name = "reverse"
				slices.Reverse(want)
			}
			t.Run(fmt.Sprintf("%s/size=%d", name, pageSize), func(t *testing.T) {
				var cursor []byte
				var got []uint64
				for range 10 {
					profile := &query.QueryProfile{}
					var items [][]byte
					if reverse {
						it, cErr := query.CompileReverse(snap, dal.NewKeyBuilder(), txTimestampRangeFilter(150, 300), commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, parityLedger, nil, nil, parityInfo(), parityRegistry(), parityResolver(), profile, snap, parityPin)
						require.NoError(t, cErr)
						items, _, cErr = readstore.PaginateReverse(it, pageSize, cursor)
						it.Close()
						require.NoError(t, cErr)
					} else {
						it, cErr := query.Compile(snap, dal.NewKeyBuilder(), txTimestampRangeFilter(150, 300), commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, parityLedger, nil, nil, parityInfo(), parityRegistry(), parityResolver(), profile, snap, parityPin)
						require.NoError(t, cErr)
						items, _, cErr = readstore.PaginateForward(it, pageSize, cursor)
						it.Close()
						require.NoError(t, cErr)
					}
					require.Zero(t, profile.MaterializedItems)
					if len(items) == 0 {
						break
					}
					for _, item := range items {
						got = append(got, binary.BigEndian.Uint64(item))
					}
					cursor = items[len(items)-1]
				}
				require.Equal(t, want, got)
			})
		}
	}
}
