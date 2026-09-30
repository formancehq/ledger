package query

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// BenchmarkTimestampRangePage keeps the public ID cursor and measures both
// first and second pages of the same date window, including compilation.
func BenchmarkTimestampRangePage(b *testing.B) {
	for _, rows := range []int{1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			store, err := readstore.New(b.TempDir(), logging.FromContext(logging.TestingContext()), readstore.DefaultConfig())
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, store.Close()) })
			kb := dal.NewKeyBuilder()
			batch := store.NewBatch()
			for id := range rows {
				require.NoError(b, batch.SetBytes(readstore.TransactionTimestampKey(kb, "bench", uint64(id), uint64(id)), nil))
				var date [8]byte
				binary.BigEndian.PutUint64(date[:], uint64(id))
				require.NoError(b, batch.SetBytes(readstore.IDDateKey(kb, readstore.PrefixTransactionTimestampByID, "bench", uint64(id)), date[:]))
				if id%10_000 == 9_999 && id != rows-1 {
					require.NoError(b, batch.Commit())
					batch = store.NewBatch()
				}
			}
			require.NoError(b, batch.Commit())
			for _, page := range []struct {
				name  string
				after []byte
			}{{name: "first"}, {name: "second", after: func() []byte {
				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, 14)

				return v
			}()}} {
				b.Run(page.name, func(b *testing.B) {
					b.ReportAllocs()
					var itemsRead int
					b.ResetTimer()
					for b.Loop() {
						profile := &QueryProfile{}
						ctx := &compileCtx{kb: dal.NewKeyBuilder(), indexReader: store.DB(), ledgerName: "bench", profile: profile}
						minV, maxV := uint64(0), uint64(rows-1)
						iter, err := compileTimestampRangeCondition(ctx, &commonpb.UintCondition{Min: &minV, Max: &maxV}, readstore.TransactionTimestampRangePrefix(ctx.kb, "bench"), "tstmp", 0)
						require.NoError(b, err)
						items, _, err := readstore.PaginateForward(iter, 15, page.after)
						require.NoError(b, err)
						iter.Close()
						require.Len(b, items, 15)
						itemsRead = profile.MaterializedItems
					}
					b.ReportMetric(float64(itemsRead), "materialized_items/op")
				})
			}
		})
	}
}
