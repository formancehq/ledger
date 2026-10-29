package query

import (
	"encoding/binary"
	"math"
	"strconv"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// EN-2601: the public filter must choose the target-specific point lookup,
// including IDs whose inclusive upper bound cannot be incremented.
func TestExactIDFilterPlansAndPaginates(t *testing.T) {
	t.Parallel()
	const ledger = "ledger1"
	store, err := readstore.New(t.TempDir(), logging.FromContext(logging.TestingContext()), readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	kb := dal.NewKeyBuilder()
	for _, id := range []uint64{0, 7, math.MaxUint64} {
		require.NoError(t, store.DB().Set(readstore.LedgerLogKey(kb, ledger, id), nil, pebble.NoSync))
		require.NoError(t, store.DB().Set((domain.TransactionKey{LedgerName: ledger, ID: id}).AppendBytes([]byte{dal.ZoneAttributes, dal.SubAttrTransaction}), nil, pebble.NoSync))
	}

	for _, target := range []commonpb.QueryTarget{commonpb.QueryTarget_QUERY_TARGET_LOGS, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS} {
		for _, id := range []uint64{0, 7, 8, math.MaxUint64} {
			idText := strconv.FormatUint(id, 10)
			for _, raw := range []string{`{"$match":{"id":"` + idText + `"}}`, `id == ` + idText} {
				filter, err := filterexpr.DecodeDualFormat([]byte(raw), target)
				require.NoError(t, err)
				profile := &QueryProfile{}
				ctx := &compileCtx{target: target, ledgerName: ledger, kb: kb, pebbleReader: store.DB(), indexReader: store.DB(), profile: profile}
				iter, err := compile(ctx, filter)
				require.NoError(t, err)
				items, hasMore, err := readstore.PaginateForward(iter, 1, nil)
				require.NoError(t, err)
				iter.Close()
				require.False(t, hasMore)
				if id == 8 {
					require.Empty(t, items)
				} else {
					require.Len(t, items, 1)
					require.Equal(t, id, binary.BigEndian.Uint64(items[0]))
				}
				if id != 8 {
					require.Equal(t, "Slice", profile.Root.Kind, "exact id should use a point lookup")
				}
			}
		}
		filter, err := filterexpr.DecodeDualFormat([]byte(`{"$or":[{"$match":{"id":"0"}},{"$match":{"id":"7"}}]}`), target)
		require.NoError(t, err)
		ctx := &compileCtx{target: target, ledgerName: ledger, kb: kb, pebbleReader: store.DB(), indexReader: store.DB()}
		for page, want := range []uint64{0, 7} {
			iter, err := compile(ctx, filter)
			require.NoError(t, err)
			var after []byte
			if page > 0 {
				after = u64Bytes(0)
			}
			items, hasMore, err := readstore.PaginateForward(iter, 1, after)
			require.NoError(t, err)
			iter.Close()
			require.Len(t, items, 1)
			require.Equal(t, want, binary.BigEndian.Uint64(items[0]))
			require.Equal(t, page == 0, hasMore)
		}
	}
}
