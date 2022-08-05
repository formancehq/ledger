package ledger

import (
	"context"
	"testing"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestVolumeAggregator(t *testing.T) {
	withContainer(fx.Invoke(func(lc fx.Lifecycle, storageDriver storage.Driver) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				name := uuid.New()

				store, _, err := storageDriver.GetStore(context.Background(), name, true)
				if err != nil {
					return err
				}

				_, err = store.Initialize(context.Background())
				if err != nil {
					return err
				}

				tx1 := core.ExpandedTransaction{
					Transaction: core.Transaction{
						ID: 0,
						TransactionData: core.TransactionData{
							Postings: []core.Posting{
								{
									Source:      "bob",
									Destination: "zozo",
									Amount:      core.NewMonetaryInt(100),
									Asset:       "USD",
								},
							},
						},
					},
					PreCommitVolumes: map[string]core.AssetsVolumes{
						"bob": {
							"USD": {},
						},
						"zozo": {
							"USD": {},
						},
					},
					PostCommitVolumes: map[string]core.AssetsVolumes{
						"bob": {
							"USD": {
								Output: core.NewMonetaryInt(100),
							},
						},
						"zozo": {
							"USD": {
								Input: core.NewMonetaryInt(100),
							},
						},
					},
				}

				tx2 := core.ExpandedTransaction{
					Transaction: core.Transaction{
						ID: 1,
						TransactionData: core.TransactionData{
							Postings: []core.Posting{
								{
									Source:      "zozo",
									Destination: "alice",
									Amount:      core.NewMonetaryInt(100),
									Asset:       "USD",
								},
							},
						},
					},
					PostCommitVolumes: map[string]core.AssetsVolumes{
						"alice": {
							"USD": {
								Input: core.NewMonetaryInt(100),
							},
						},
						"zozo": {
							"USD": {
								Input:  core.NewMonetaryInt(100),
								Output: core.NewMonetaryInt(100),
							},
						},
					},
					PreCommitVolumes: map[string]core.AssetsVolumes{
						"alice": {
							"USD": {},
						},
						"zozo": {
							"USD": {
								Input: core.NewMonetaryInt(100),
							},
						},
					},
				}
				require.NoError(t, store.Commit(context.Background(), tx1, tx2))

				volumeAggregator := newVolumeAggregator(store)
				firstTx := volumeAggregator.nextTx()
				require.NoError(t, firstTx.transfer(context.Background(), "bob", "alice", "USD", core.NewMonetaryInt(100)))
				require.NoError(t, firstTx.transfer(context.Background(), "bob", "zoro", "USD", core.NewMonetaryInt(50)))

				require.Equal(t, core.AccountsAssetsVolumes{
					"bob": core.AssetsVolumes{
						"USD": {
							Output: core.NewMonetaryInt(250),
						},
					},
					"alice": core.AssetsVolumes{
						"USD": {
							Input: core.NewMonetaryInt(200),
						},
					},
					"zoro": {
						"USD": {
							Input: core.NewMonetaryInt(50),
						},
					},
				}, firstTx.postCommitVolumes())
				require.Equal(t, core.AccountsAssetsVolumes{
					"bob": core.AssetsVolumes{
						"USD": {
							Output: core.NewMonetaryInt(100),
						},
					},
					"alice": core.AssetsVolumes{
						"USD": {
							Input: core.NewMonetaryInt(100),
						},
					},
					"zoro": core.AssetsVolumes{
						"USD": {
							Input: core.NewMonetaryInt(0),
						},
					},
				}, firstTx.preCommitVolumes())

				secondTx := volumeAggregator.nextTx()
				require.NoError(t, secondTx.transfer(context.Background(), "alice", "fred", "USD", core.NewMonetaryInt(50)))
				require.NoError(t, secondTx.transfer(context.Background(), "bob", "fred", "USD", core.NewMonetaryInt(25)))
				require.Equal(t, core.AccountsAssetsVolumes{
					"bob": core.AssetsVolumes{
						"USD": {
							Output: core.NewMonetaryInt(275),
						},
					},
					"alice": core.AssetsVolumes{
						"USD": {
							Input:  core.NewMonetaryInt(200),
							Output: core.NewMonetaryInt(50),
						},
					},
					"fred": core.AssetsVolumes{
						"USD": {
							Input: core.NewMonetaryInt(75),
						},
					},
				}, secondTx.postCommitVolumes())
				require.Equal(t, core.AccountsAssetsVolumes{
					"bob": core.AssetsVolumes{
						"USD": {
							Output: core.NewMonetaryInt(250),
						},
					},
					"alice": core.AssetsVolumes{
						"USD": {
							Input: core.NewMonetaryInt(200),
						},
					},
					"fred": core.AssetsVolumes{
						"USD": {},
					},
				}, secondTx.preCommitVolumes())

				aggregatedPostVolumes := volumeAggregator.aggregatedPostCommitVolumes()
				require.Equal(t, core.AccountsAssetsVolumes{
					"bob": core.AssetsVolumes{
						"USD": {
							Output: core.NewMonetaryInt(275),
						},
					},
					"alice": core.AssetsVolumes{
						"USD": {
							Input:  core.NewMonetaryInt(200),
							Output: core.NewMonetaryInt(50),
						},
					},
					"fred": core.AssetsVolumes{
						"USD": {
							Input: core.NewMonetaryInt(75),
						},
					},
					"zoro": core.AssetsVolumes{
						"USD": {
							Input:  core.NewMonetaryInt(50),
							Output: core.NewMonetaryInt(0),
						},
					},
				}, aggregatedPostVolumes)

				aggregatedPreVolumes := volumeAggregator.aggregatedPreCommitVolumes()
				require.Equal(t, core.AccountsAssetsVolumes{
					"bob": core.AssetsVolumes{
						"USD": {
							Output: core.NewMonetaryInt(100),
						},
					},
					"alice": core.AssetsVolumes{
						"USD": {
							Input: core.NewMonetaryInt(100),
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
