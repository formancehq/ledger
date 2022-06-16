package ledger

import (
	"context"
	"testing"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestVolumeAggregator(t *testing.T) {
	withContainer(fx.Invoke(func(lc fx.Lifecycle, driver storage.Driver) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				name := uuid.New()

				store, _, err := driver.GetStore(context.Background(), name, true)
				require.NoError(t, err)

				_, err = store.Initialize(context.Background())
				require.NoError(t, err)

				var bobToto int64 = 100
				firstTxLog := core.NewTransactionLog(nil, core.Transaction{
					ID: 0,
					TransactionData: core.TransactionData{
						Postings: []core.Posting{
							{
								Source:      "bob",
								Destination: "toto",
								Amount:      bobToto,
								Asset:       "USD",
							},
						},
					},
				})

				var totoAlice int64 = 100
				secondTxLog := core.NewTransactionLog(&firstTxLog, core.Transaction{
					ID: 1,
					TransactionData: core.TransactionData{
						Postings: []core.Posting{
							{
								Source:      "toto",
								Destination: "alice",
								Amount:      totoAlice,
								Asset:       "USD",
							},
						},
					},
				})
				require.NoError(t, store.AppendLog(context.Background(), firstTxLog, secondTxLog))

				vAggr := newVolumeAggregator(store)
				firstTx := vAggr.nextTx()

				var bobAlice int64 = 100
				assert.NoError(t, firstTx.transfer(context.Background(), "bob", "alice", "USD", uint64(bobAlice)))
				var bobZoro int64 = 50
				assert.NoError(t, firstTx.transfer(context.Background(), "bob", "zoro", "USD", uint64(bobZoro)))

				assert.Equal(t, core.AccountsVolumes{
					"bob": core.AssetsVolumes{
						"USD": {
							Output: bobToto,
						},
					},
					"alice": core.AssetsVolumes{
						"USD": {
							Input: totoAlice,
						},
					},
					"zoro": core.AssetsVolumes{
						"USD": {},
					},
				}, firstTx.PreCommitVolumes)

				assert.Equal(t, core.AccountsVolumes{
					"bob": core.AssetsVolumes{
						"USD": {
							Output: bobToto + bobAlice + bobZoro,
						},
					},
					"alice": core.AssetsVolumes{
						"USD": {
							Input: totoAlice + bobAlice,
						},
					},
					"zoro": {
						"USD": {
							Input: bobZoro,
						},
					},
				}, firstTx.PostCommitVolumes)

				secondTx := vAggr.nextTx()
				var aliceFred int64 = 50
				assert.NoError(t, secondTx.transfer(context.Background(), "alice", "fred", "USD", uint64(aliceFred)))
				var bobFred int64 = 25
				assert.NoError(t, secondTx.transfer(context.Background(), "bob", "fred", "USD", uint64(bobFred)))

				assert.Equal(t, core.AccountsVolumes{
					"bob":   firstTx.PostCommitVolumes["bob"],
					"alice": firstTx.PostCommitVolumes["alice"],
					"fred": core.AssetsVolumes{
						"USD": {},
					},
				}, secondTx.PreCommitVolumes)

				assert.Equal(t, core.AccountsVolumes{
					"bob": core.AssetsVolumes{
						"USD": {
							Output: bobToto + bobAlice + bobZoro + bobFred,
						},
					},
					"alice": core.AssetsVolumes{
						"USD": {
							Input:  200,
							Output: 50,
						},
					},
					"fred": core.AssetsVolumes{
						"USD": {
							Input: 75,
						},
					},
				}, secondTx.PostCommitVolumes)

				aggregatedPostVolumes := vAggr.aggregatedPostCommitVolumes()
				assert.Equal(t, core.AccountsVolumes{
					"bob": core.AssetsVolumes{
						"USD": {
							Output: 275,
						},
					},
					"alice": core.AssetsVolumes{
						"USD": {
							Input:  200,
							Output: 50,
						},
					},
					"fred": core.AssetsVolumes{
						"USD": {
							Input: 75,
						},
					},
					"zoro": core.AssetsVolumes{
						"USD": {
							Input: 50,
						},
					},
				}, aggregatedPostVolumes)

				aggregatedPreVolumes := vAggr.aggregatedPreCommitVolumes()
				assert.Equal(t, core.AccountsVolumes{
					"bob": core.AssetsVolumes{
						"USD": {
							Output: 100,
						},
					},
					"alice": core.AssetsVolumes{
						"USD": {
							Input: 100,
						},
					},
					"fred": core.AssetsVolumes{
						"USD": {},
					},
					"zoro": core.AssetsVolumes{
						"USD": {},
					},
				}, aggregatedPreVolumes)

				return nil
			},
		})
	}))
}
