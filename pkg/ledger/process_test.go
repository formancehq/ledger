package ledger_test

import (
	"context"
	"testing"
	"time"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLedger_processTx(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		t.Run("multi assets", func(t *testing.T) {
			worldTotoUSD := core.NewMonetaryInt(43)
			worldAliceUSD := core.NewMonetaryInt(98)
			aliceTotoUSD := core.NewMonetaryInt(45)
			worldTotoEUR := core.NewMonetaryInt(15)
			worldAliceEUR := core.NewMonetaryInt(10)
			totoAliceEUR := core.NewMonetaryInt(5)

			postings := []core.Posting{
				{
					Source:      "world",
					Destination: "toto",
					Amount:      worldTotoUSD,
					Asset:       "USD",
				},
				{
					Source:      "world",
					Destination: "alice",
					Amount:      worldAliceUSD,
					Asset:       "USD",
				},
				{
					Source:      "alice",
					Destination: "toto",
					Amount:      aliceTotoUSD,
					Asset:       "USD",
				},
				{
					Source:      "world",
					Destination: "toto",
					Amount:      worldTotoEUR,
					Asset:       "EUR",
				},
				{
					Source:      "world",
					Destination: "alice",
					Amount:      worldAliceEUR,
					Asset:       "EUR",
				},
				{
					Source:      "toto",
					Destination: "alice",
					Amount:      totoAliceEUR,
					Asset:       "EUR",
				},
			}

			expectedPreCommitVol := core.AccountsAssetsVolumes{
				"alice": core.AssetsVolumes{
					"USD": {
						Input:  core.NewMonetaryInt(0),
						Output: core.NewMonetaryInt(0),
					},
					"EUR": {
						Input:  core.NewMonetaryInt(0),
						Output: core.NewMonetaryInt(0),
					},
				},
				"toto": core.AssetsVolumes{
					"USD": {
						Input:  core.NewMonetaryInt(0),
						Output: core.NewMonetaryInt(0),
					},
					"EUR": {
						Input:  core.NewMonetaryInt(0),
						Output: core.NewMonetaryInt(0),
					},
				},
				"world": core.AssetsVolumes{
					"USD": {
						Input:  core.NewMonetaryInt(0),
						Output: core.NewMonetaryInt(0),
					},
					"EUR": {
						Input:  core.NewMonetaryInt(0),
						Output: core.NewMonetaryInt(0),
					},
				},
			}

			expectedPostCommitVol := core.AccountsAssetsVolumes{
				"alice": core.AssetsVolumes{
					"USD": {
						Input:  worldAliceUSD,
						Output: aliceTotoUSD,
					},
					"EUR": {
						Input:  worldAliceEUR.Add(totoAliceEUR),
						Output: core.NewMonetaryInt(0),
					},
				},
				"toto": core.AssetsVolumes{
					"USD": {
						Input:  worldTotoUSD.Add(aliceTotoUSD),
						Output: core.NewMonetaryInt(0),
					},
					"EUR": {
						Input:  worldTotoEUR,
						Output: totoAliceEUR,
					},
				},
				"world": core.AssetsVolumes{
					"USD": {
						Input:  core.NewMonetaryInt(0),
						Output: worldTotoUSD.Add(worldAliceUSD),
					},
					"EUR": {
						Input:  core.NewMonetaryInt(0),
						Output: worldTotoEUR.Add(worldAliceEUR),
					},
				},
			}

			t.Run("single transaction multi postings", func(t *testing.T) {
				txsData := []core.TransactionData{
					{
						Postings:  postings,
						Timestamp: time.Now().UTC().Round(time.Second),
						Metadata:  core.Metadata{},
					},
				}

				res, err := l.Execute(context.Background(), true, true,
					core.TxsToScriptsData(txsData...)...)
				assert.NoError(t, err)

				assert.Equal(t, len(txsData), len(res.GeneratedTransactions))
				assert.Equal(t, expectedPreCommitVol, res.PreCommitVolumes)
				assert.Equal(t, expectedPostCommitVol, res.PostCommitVolumes)

				expectedTxs := []core.ExpandedTransaction{{
					Transaction: core.Transaction{
						TransactionData: txsData[0],
						ID:              0,
					},
					PreCommitVolumes:  expectedPreCommitVol,
					PostCommitVolumes: expectedPostCommitVol,
				}}
				assert.Equal(t, expectedTxs, res.GeneratedTransactions)
			})

			t.Run("multi transactions single postings", func(t *testing.T) {
				now := time.Now().Round(time.Second)
				txsData := []core.TransactionData{
					{
						Postings:  []core.Posting{postings[0]},
						Timestamp: now,
					},
					{
						Postings:  []core.Posting{postings[1]},
						Timestamp: now.Add(time.Second),
					},
					{
						Postings:  []core.Posting{postings[2]},
						Timestamp: now.Add(2 * time.Second),
					},
					{
						Postings:  []core.Posting{postings[3]},
						Timestamp: now.Add(3 * time.Second),
					},
					{
						Postings:  []core.Posting{postings[4]},
						Timestamp: now.Add(4 * time.Second),
					},
					{
						Postings:  []core.Posting{postings[5]},
						Timestamp: now.Add(5 * time.Second),
					},
				}

				res, err := l.Execute(context.Background(), true, true,
					core.TxsToScriptsData(txsData...)...)
				require.NoError(t, err)
				require.Equal(t, len(txsData), len(res.GeneratedTransactions))

				expectedTxs := []core.ExpandedTransaction{
					{
						Transaction: core.Transaction{
							TransactionData: core.TransactionData{
								Timestamp: now.UTC(),
								Postings:  core.Postings{postings[0]},
								Metadata:  core.Metadata{},
							},
							ID: 0,
						},
						PreCommitVolumes: core.AccountsAssetsVolumes{
							"toto":  core.AssetsVolumes{"USD": core.Volumes{Input: core.NewMonetaryInt(0), Output: core.NewMonetaryInt(0)}},
							"world": core.AssetsVolumes{"USD": core.Volumes{Input: core.NewMonetaryInt(0), Output: core.NewMonetaryInt(0)}}},
						PostCommitVolumes: core.AccountsAssetsVolumes{
							"toto":  core.AssetsVolumes{"USD": core.Volumes{Input: worldTotoUSD, Output: core.NewMonetaryInt(0)}},
							"world": core.AssetsVolumes{"USD": core.Volumes{Input: core.NewMonetaryInt(0), Output: worldTotoUSD}}},
					},
					{
						Transaction: core.Transaction{
							TransactionData: core.TransactionData{
								Postings:  core.Postings{postings[1]},
								Timestamp: now.UTC().Add(time.Second),
								Metadata:  core.Metadata{},
							},
							ID: 1,
						},
						PreCommitVolumes: core.AccountsAssetsVolumes{
							"world": core.AssetsVolumes{"USD": core.Volumes{Input: core.NewMonetaryInt(0), Output: worldTotoUSD}},
							"alice": core.AssetsVolumes{"USD": core.Volumes{Input: core.NewMonetaryInt(0), Output: core.NewMonetaryInt(0)}},
						},
						PostCommitVolumes: core.AccountsAssetsVolumes{
							"world": core.AssetsVolumes{"USD": core.Volumes{Input: core.NewMonetaryInt(0), Output: worldTotoUSD.Add(worldAliceUSD)}},
							"alice": core.AssetsVolumes{"USD": core.Volumes{Input: worldAliceUSD, Output: core.NewMonetaryInt(0)}},
						},
					},
					{
						Transaction: core.Transaction{
							TransactionData: core.TransactionData{
								Timestamp: now.UTC().Add(2 * time.Second),
								Postings:  core.Postings{postings[2]},
								Metadata:  core.Metadata{},
							},
							ID: 2,
						},
						PreCommitVolumes: core.AccountsAssetsVolumes{
							"alice": core.AssetsVolumes{"USD": core.Volumes{Input: worldAliceUSD, Output: core.NewMonetaryInt(0)}},
							"toto":  core.AssetsVolumes{"USD": core.Volumes{Input: worldTotoUSD, Output: core.NewMonetaryInt(0)}},
						},
						PostCommitVolumes: core.AccountsAssetsVolumes{
							"alice": core.AssetsVolumes{"USD": core.Volumes{Input: worldAliceUSD, Output: aliceTotoUSD}},
							"toto":  core.AssetsVolumes{"USD": core.Volumes{Input: worldTotoUSD.Add(aliceTotoUSD), Output: core.NewMonetaryInt(0)}},
						},
					},
					{
						Transaction: core.Transaction{
							TransactionData: core.TransactionData{
								Timestamp: now.UTC().Add(3 * time.Second),
								Postings:  core.Postings{postings[3]},
								Metadata:  core.Metadata{},
							},
							ID: 3,
						},
						PreCommitVolumes: core.AccountsAssetsVolumes{
							"world": core.AssetsVolumes{"EUR": core.Volumes{Input: core.NewMonetaryInt(0), Output: core.NewMonetaryInt(0)}},
							"toto":  core.AssetsVolumes{"EUR": core.Volumes{Input: core.NewMonetaryInt(0), Output: core.NewMonetaryInt(0)}},
						},
						PostCommitVolumes: core.AccountsAssetsVolumes{
							"world": core.AssetsVolumes{"EUR": core.Volumes{Input: core.NewMonetaryInt(0), Output: worldTotoEUR}},
							"toto":  core.AssetsVolumes{"EUR": core.Volumes{Input: worldTotoEUR, Output: core.NewMonetaryInt(0)}},
						},
					},
					{
						Transaction: core.Transaction{
							TransactionData: core.TransactionData{
								Timestamp: now.UTC().Add(4 * time.Second),
								Postings:  core.Postings{postings[4]},
								Metadata:  core.Metadata{},
							},
							ID: 4,
						},
						PreCommitVolumes: core.AccountsAssetsVolumes{
							"world": core.AssetsVolumes{"EUR": core.Volumes{Input: core.NewMonetaryInt(0), Output: worldTotoEUR}},
							"alice": core.AssetsVolumes{"EUR": core.Volumes{Input: core.NewMonetaryInt(0), Output: core.NewMonetaryInt(0)}},
						},
						PostCommitVolumes: core.AccountsAssetsVolumes{
							"world": core.AssetsVolumes{"EUR": core.Volumes{Input: core.NewMonetaryInt(0), Output: worldTotoEUR.Add(worldAliceEUR)}},
							"alice": core.AssetsVolumes{"EUR": core.Volumes{Input: worldAliceEUR, Output: core.NewMonetaryInt(0)}},
						},
					},
					{
						Transaction: core.Transaction{
							TransactionData: core.TransactionData{
								Timestamp: now.UTC().Add(5 * time.Second),
								Postings:  core.Postings{postings[5]},
								Metadata:  core.Metadata{},
							},
							ID: 5,
						},
						PreCommitVolumes: core.AccountsAssetsVolumes{
							"toto":  core.AssetsVolumes{"EUR": core.Volumes{Input: worldTotoEUR, Output: core.NewMonetaryInt(0)}},
							"alice": core.AssetsVolumes{"EUR": core.Volumes{Input: worldAliceEUR, Output: core.NewMonetaryInt(0)}},
						},
						PostCommitVolumes: core.AccountsAssetsVolumes{
							"toto":  core.AssetsVolumes{"EUR": core.Volumes{Input: worldTotoEUR, Output: totoAliceEUR}},
							"alice": core.AssetsVolumes{"EUR": core.Volumes{Input: worldAliceEUR.Add(totoAliceEUR), Output: core.NewMonetaryInt(0)}},
						},
					},
				}
				assert.Equal(t, expectedTxs, res.GeneratedTransactions)
			})
		})

		t.Run("no script", func(t *testing.T) {
			_, err := l.Execute(context.Background(), true, true, core.ScriptData{})
			assert.Error(t, err)
			assert.ErrorContains(t, err, "no script to execute")
		})
	})

	runOnLedger(func(l *ledger.Ledger) {
		t.Run("date in the past (allowed by policy)", func(t *testing.T) {
			now := time.Now()
			require.NoError(t, l.GetLedgerStore().Commit(context.Background(), core.ExpandedTransaction{
				Transaction: core.Transaction{
					TransactionData: core.TransactionData{
						Timestamp: now.UTC(),
						Postings:  []core.Posting{{}},
					},
					ID: 0,
				},
			}))

			_, err := l.Execute(context.Background(), true, true,
				core.TxsToScriptsData(core.TransactionData{
					Postings: []core.Posting{{
						Source:      "world",
						Destination: "bank",
						Amount:      core.NewMonetaryInt(100),
						Asset:       "USD",
					}},
					Timestamp: now.UTC().Add(-time.Second),
				})...)
			assert.NoError(t, err)
		})
	}, ledger.WithPastTimestamps)
}
