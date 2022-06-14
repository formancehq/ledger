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

				firstTxLog := core.NewTransactionLog(nil, core.Transaction{
					ID: 0,
					TransactionData: core.TransactionData{
						Postings: []core.Posting{
							{
								Source:      "bob",
								Destination: "zozo",
								Amount:      100,
								Asset:       "USD",
							},
						},
					},
				})
				secondTxLog := core.NewTransactionLog(&firstTxLog, core.Transaction{
					ID: 1,
					TransactionData: core.TransactionData{
						Postings: []core.Posting{
							{
								Source:      "zozo",
								Destination: "alice",
								Amount:      100,
								Asset:       "USD",
							},
						},
					},
				})
				require.NoError(t, store.AppendLog(context.Background(), firstTxLog, secondTxLog))

				volumeAggregator := newVolumeAggregator(store)
				firstTx := volumeAggregator.nextTx()
				require.NoError(t, firstTx.transfer(context.Background(), "bob", "alice", "USD", 100))
				require.NoError(t, firstTx.transfer(context.Background(), "bob", "zoro", "USD", 50))

				require.Equal(t, core.AggregatedVolumes{
					"bob": core.Volumes{
						"USD": {
							Output: 250,
						},
					},
					"alice": core.Volumes{
						"USD": {
							Input: 200,
						},
					},
					"zoro": {
						"USD": {
							Input: 50,
						},
					},
				}, firstTx.postCommitVolumes())
				require.Equal(t, core.AggregatedVolumes{
					"bob": core.Volumes{
						"USD": {
							Output: 100,
						},
					},
					"alice": core.Volumes{
						"USD": {
							Input: 100,
						},
					},
					"zoro": core.Volumes{
						"USD": {
							Input: 0,
						},
					},
				}, firstTx.preCommitVolumes())

				secondTx := volumeAggregator.nextTx()
				require.NoError(t, secondTx.transfer(context.Background(), "alice", "fred", "USD", 50))
				require.NoError(t, secondTx.transfer(context.Background(), "bob", "fred", "USD", 25))
				require.Equal(t, core.AggregatedVolumes{
					"bob": core.Volumes{
						"USD": {
							Output: 275,
						},
					},
					"alice": core.Volumes{
						"USD": {
							Input:  200,
							Output: 50,
						},
					},
					"fred": core.Volumes{
						"USD": {
							Input: 75,
						},
					},
				}, secondTx.postCommitVolumes())
				require.Equal(t, core.AggregatedVolumes{
					"bob": core.Volumes{
						"USD": {
							Output: 250,
						},
					},
					"alice": core.Volumes{
						"USD": {
							Input: 200,
						},
					},
					"fred": core.Volumes{
						"USD": {},
					},
				}, secondTx.preCommitVolumes())

				aggregatedPostVolumes := volumeAggregator.aggregatedPostCommitVolumes()
				require.Equal(t, core.AggregatedVolumes{
					"bob": core.Volumes{
						"USD": {
							Output: 275,
						},
					},
					"alice": core.Volumes{
						"USD": {
							Input:  200,
							Output: 50,
						},
					},
					"fred": core.Volumes{
						"USD": {
							Input: 75,
						},
					},
					"zoro": core.Volumes{
						"USD": {
							Input:  50,
							Output: 0,
						},
					},
				}, aggregatedPostVolumes)

				aggregatedPreVolumes := volumeAggregator.aggregatedPreCommitVolumes()
				require.Equal(t, core.AggregatedVolumes{
					"bob": core.Volumes{
						"USD": {
							Output: 100,
						},
					},
					"alice": core.Volumes{
						"USD": {
							Input: 100,
						},
					},
					"fred": core.Volumes{
						"USD": {},
					},
					"zoro": core.Volumes{
						"USD": {},
					},
				}, aggregatedPreVolumes)

				return nil
			},
		})
	}))
}
