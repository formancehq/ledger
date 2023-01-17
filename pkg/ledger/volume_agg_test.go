package ledger_test

import (
	"context"
	"testing"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/stretchr/testify/require"
)

func TestVolumeAggregator(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		defer func(l *ledger.Ledger, ctx context.Context) {
			require.NoError(t, l.Close(ctx))
		}(l, context.Background())

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
					"USD": {
						Input:  core.NewMonetaryInt(0),
						Output: core.NewMonetaryInt(0),
					},
				},
				"zozo": {
					"USD": {
						Input:  core.NewMonetaryInt(0),
						Output: core.NewMonetaryInt(0),
					},
				},
			},
			PostCommitVolumes: map[string]core.AssetsVolumes{
				"bob": {
					"USD": {
						Input:  core.NewMonetaryInt(0),
						Output: core.NewMonetaryInt(100),
					},
				},
				"zozo": {
					"USD": {
						Input:  core.NewMonetaryInt(100),
						Output: core.NewMonetaryInt(0),
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
						Input:  core.NewMonetaryInt(100),
						Output: core.NewMonetaryInt(0),
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
					"USD": {
						Input:  core.NewMonetaryInt(0),
						Output: core.NewMonetaryInt(0),
					},
				},
				"zozo": {
					"USD": {
						Input:  core.NewMonetaryInt(100),
						Output: core.NewMonetaryInt(0),
					},
				},
			},
		}
		err := l.GetLedgerStore().Commit(context.Background(), tx1, tx2)
		require.NoError(t, err)

		volumeAggregator := ledger.NewVolumeAggregator(l)
		firstTx := volumeAggregator.NextTx()
		accs := map[string]*core.AccountWithVolumes{}
		require.NoError(t, firstTx.Transfer(context.Background(), "bob", "alice", "USD", core.NewMonetaryInt(100), accs))
		require.NoError(t, firstTx.Transfer(context.Background(), "bob", "zoro", "USD", core.NewMonetaryInt(50), accs))

		require.Equal(t, core.AccountsAssetsVolumes{
			"bob": core.AssetsVolumes{
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(250),
				},
			},
			"alice": core.AssetsVolumes{
				"USD": {
					Input:  core.NewMonetaryInt(200),
					Output: core.NewMonetaryInt(0),
				},
			},
			"zoro": {
				"USD": {
					Input:  core.NewMonetaryInt(50),
					Output: core.NewMonetaryInt(0),
				},
			},
		}, firstTx.PostCommitVolumes)
		require.Equal(t, core.AccountsAssetsVolumes{
			"bob": core.AssetsVolumes{
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(100),
				},
			},
			"alice": core.AssetsVolumes{
				"USD": {
					Input:  core.NewMonetaryInt(100),
					Output: core.NewMonetaryInt(0),
				},
			},
			"zoro": core.AssetsVolumes{
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(0),
				},
			},
		}, firstTx.PreCommitVolumes)

		secondTx := volumeAggregator.NextTx()
		require.NoError(t, secondTx.Transfer(context.Background(), "alice", "fred", "USD", core.NewMonetaryInt(50), accs))
		require.NoError(t, secondTx.Transfer(context.Background(), "bob", "fred", "USD", core.NewMonetaryInt(25), accs))
		require.Equal(t, core.AccountsAssetsVolumes{
			"bob": core.AssetsVolumes{
				"USD": {
					Input:  core.NewMonetaryInt(0),
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
					Input:  core.NewMonetaryInt(75),
					Output: core.NewMonetaryInt(0),
				},
			},
		}, secondTx.PostCommitVolumes)
		require.Equal(t, core.AccountsAssetsVolumes{
			"bob": core.AssetsVolumes{
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(250),
				},
			},
			"alice": core.AssetsVolumes{
				"USD": {
					Input:  core.NewMonetaryInt(200),
					Output: core.NewMonetaryInt(0),
				},
			},
			"fred": core.AssetsVolumes{
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(0),
				},
			},
		}, secondTx.PreCommitVolumes)
	})
}
