package query_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// logHistoryStore seeds ledger-local log ids 1..n in the per-ledger log index.
func logHistoryStore(t *testing.T, n uint64) *readstore.Store {
	t.Helper()

	store, err := readstore.New(t.TempDir(), logging.FromContext(logging.TestingContext()), readstore.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()

	for id := uint64(1); id <= n; id++ {
		require.NoError(t, batch.SetBytes(readstore.LedgerLogKey(kb, parityLedger, id), binary.BigEndian.AppendUint64(nil, id)))
	}

	require.NoError(t, batch.Commit())

	return store
}

func logIDBelow(id uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogId{
		LogId: &commonpb.LogIdCondition{Cond: &commonpb.UintCondition{Max: &id, MaxExclusive: true}},
	}}
}

// TestReverseLogPageIsBoundedByThePage pins that a descending log page reads
// about one page whatever the history size, both when the position is a
// paginator seek and when it is a log_id range: the leaf streams rather than
// materializing every id below the position.
func TestReverseLogPageIsBoundedByThePage(t *testing.T) {
	t.Parallel()

	const pageSize = 20

	for _, history := range []uint64{100, 5000} {
		store := logHistoryStore(t, history)
		position := history - 10

		for _, tc := range []struct {
			name   string
			filter *commonpb.QueryFilter
			before []byte
		}{
			{name: "seek", before: binary.BigEndian.AppendUint64(nil, position)},
			{name: "log_id range", filter: logIDBelow(position)},
		} {
			profile := &query.QueryProfile{}

			iter, err := query.CompileReverse(
				store.DB(), dal.NewKeyBuilder(), tc.filter,
				commonpb.QueryTarget_QUERY_TARGET_LOGS, parityLedger,
				nil, nil, parityInfo(), parityRegistry(), parityResolver(), profile, store.DB(), parityPin)
			require.NoError(t, err, tc.name)

			items, hasMore, err := readstore.PaginateReverse(iter, pageSize, tc.before)
			iter.Close()
			require.NoError(t, err, tc.name)

			require.Len(t, items, pageSize, "%s, history %d", tc.name, history)
			require.True(t, hasMore, "%s, history %d", tc.name, history)
			require.Equal(t, position-1, binary.BigEndian.Uint64(items[0]), "%s: the page starts just below the position", tc.name)

			require.NotNil(t, profile.Root, tc.name)
			require.Zero(t, profile.Root.MaterializedItems, "%s, history %d: the leaf must stream", tc.name, history)
			require.LessOrEqual(t, profile.Root.ItemsEmitted, int64(pageSize+1),
				"%s, history %d: work must track the page, not the history", tc.name, history)
		}
	}
}
