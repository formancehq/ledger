package ledger

import (
	"context"
	"testing"
	"time"

	"github.com/numary/ledger/pkg/core"
	"github.com/stretchr/testify/assert"
)

func TestLedger_processTx(t *testing.T) {
	runOnLedger(func(l *Ledger) {
		t.Run("multi assets", func(t *testing.T) {
			const (
				worldTotoUSD  int64 = 43
				worldAliceUSD int64 = 98
				aliceTotoUSD  int64 = 45
				worldTotoEUR  int64 = 15
				worldAliceEUR int64 = 10
				totoAliceEUR  int64 = 5
			)

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
					"USD": {},
					"EUR": {},
				},
				"toto": core.AssetsVolumes{
					"USD": {},
					"EUR": {},
				},
				"world": core.AssetsVolumes{
					"USD": {},
					"EUR": {},
				},
			}

			expectedPostCommitVol := core.AccountsAssetsVolumes{
				"alice": core.AssetsVolumes{
					"USD": {
						Input:  worldAliceUSD,
						Output: aliceTotoUSD,
					},
					"EUR": {
						Input: worldAliceEUR + totoAliceEUR,
					},
				},
				"toto": core.AssetsVolumes{
					"USD": {
						Input: worldTotoUSD + aliceTotoUSD,
					},
					"EUR": {
						Input:  worldTotoEUR,
						Output: totoAliceEUR,
					},
				},
				"world": core.AssetsVolumes{
					"USD": {
						Output: worldTotoUSD + worldAliceUSD,
					},
					"EUR": {
						Output: worldTotoEUR + worldAliceEUR,
					},
				},
			}

			t.Run("single transaction multi postings", func(t *testing.T) {
				txsData := []core.TransactionData{
					{Postings: postings},
				}

				res, err := l.processTx(context.Background(), txsData)
				assert.NoError(t, err)

				assert.Equal(t, expectedPreCommitVol, res.PreCommitVolumes)
				assert.Equal(t, expectedPostCommitVol, res.PostCommitVolumes)

				expectedTxs := []core.Transaction{{
					TransactionData:   txsData[0],
					ID:                0,
					Timestamp:         time.Now().UTC().Format(time.RFC3339),
					PreCommitVolumes:  expectedPreCommitVol,
					PostCommitVolumes: expectedPostCommitVol,
				}}
				assert.Equal(t, expectedTxs, res.GeneratedTransactions)

				assert.True(t, time.Until(res.GeneratedLogs[0].Date) < time.Millisecond)

				expectedLogs := []core.Log{{
					ID:   0,
					Type: core.NewTransactionType,
					Data: core.LoggedTX(expectedTxs[0]),
					Date: res.GeneratedLogs[0].Date,
				}}
				expectedLogs[0].Hash = core.Hash(nil, expectedLogs[0])

				assert.Equal(t, expectedLogs, res.GeneratedLogs)
			})

			t.Run("multi transactions single postings", func(t *testing.T) {
				txsData := []core.TransactionData{
					{Postings: []core.Posting{postings[0]}},
					{Postings: []core.Posting{postings[1]}},
					{Postings: []core.Posting{postings[2]}},
					{Postings: []core.Posting{postings[3]}},
					{Postings: []core.Posting{postings[4]}},
					{Postings: []core.Posting{postings[5]}},
				}

				res, err := l.processTx(context.Background(), txsData)
				assert.NoError(t, err)

				assert.Equal(t, expectedPreCommitVol, res.PreCommitVolumes)
				assert.Equal(t, expectedPostCommitVol, res.PostCommitVolumes)

				expectedTxs := []core.Transaction{
					{
						TransactionData: core.TransactionData{Postings: core.Postings{postings[0]}},
						ID:              0,
						Timestamp:       time.Now().UTC().Format(time.RFC3339),
						PreCommitVolumes: core.AccountsAssetsVolumes{
							"toto":  core.AssetsVolumes{"USD": core.Volumes{Input: 0, Output: 0}},
							"world": core.AssetsVolumes{"USD": core.Volumes{Input: 0, Output: 0}}},
						PostCommitVolumes: core.AccountsAssetsVolumes{
							"toto":  core.AssetsVolumes{"USD": core.Volumes{Input: worldTotoUSD, Output: 0}},
							"world": core.AssetsVolumes{"USD": core.Volumes{Input: 0, Output: worldTotoUSD}}},
					},
					{
						TransactionData: core.TransactionData{Postings: core.Postings{postings[1]}},
						ID:              1,
						Timestamp:       time.Now().UTC().Format(time.RFC3339),
						PreCommitVolumes: core.AccountsAssetsVolumes{
							"world": core.AssetsVolumes{"USD": core.Volumes{Input: 0, Output: worldTotoUSD}},
							"alice": core.AssetsVolumes{"USD": core.Volumes{Input: 0, Output: 0}},
						},
						PostCommitVolumes: core.AccountsAssetsVolumes{
							"world": core.AssetsVolumes{"USD": core.Volumes{Input: 0, Output: worldTotoUSD + worldAliceUSD}},
							"alice": core.AssetsVolumes{"USD": core.Volumes{Input: worldAliceUSD, Output: 0}},
						},
					},
					{
						TransactionData: core.TransactionData{Postings: core.Postings{postings[2]}},
						ID:              2,
						Timestamp:       time.Now().UTC().Format(time.RFC3339),
						PreCommitVolumes: core.AccountsAssetsVolumes{
							"alice": core.AssetsVolumes{"USD": core.Volumes{Input: worldAliceUSD, Output: 0}},
							"toto":  core.AssetsVolumes{"USD": core.Volumes{Input: worldTotoUSD, Output: 0}},
						},
						PostCommitVolumes: core.AccountsAssetsVolumes{
							"alice": core.AssetsVolumes{"USD": core.Volumes{Input: worldAliceUSD, Output: aliceTotoUSD}},
							"toto":  core.AssetsVolumes{"USD": core.Volumes{Input: worldTotoUSD + aliceTotoUSD, Output: 0}},
						},
					},
					{
						TransactionData: core.TransactionData{Postings: core.Postings{postings[3]}},
						ID:              3,
						Timestamp:       time.Now().UTC().Format(time.RFC3339),
						PreCommitVolumes: core.AccountsAssetsVolumes{
							"world": core.AssetsVolumes{"EUR": core.Volumes{Input: 0, Output: 0}},
							"toto":  core.AssetsVolumes{"EUR": core.Volumes{Input: 0, Output: 0}},
						},
						PostCommitVolumes: core.AccountsAssetsVolumes{
							"world": core.AssetsVolumes{"EUR": core.Volumes{Input: 0, Output: worldTotoEUR}},
							"toto":  core.AssetsVolumes{"EUR": core.Volumes{Input: worldTotoEUR, Output: 0}},
						},
					},
					{
						TransactionData: core.TransactionData{Postings: core.Postings{postings[4]}},
						ID:              4,
						Timestamp:       time.Now().UTC().Format(time.RFC3339),
						PreCommitVolumes: core.AccountsAssetsVolumes{
							"world": core.AssetsVolumes{"EUR": core.Volumes{Input: 0, Output: worldTotoEUR}},
							"alice": core.AssetsVolumes{"EUR": core.Volumes{Input: 0, Output: 0}},
						},
						PostCommitVolumes: core.AccountsAssetsVolumes{
							"world": core.AssetsVolumes{"EUR": core.Volumes{Input: 0, Output: worldTotoEUR + worldAliceEUR}},
							"alice": core.AssetsVolumes{"EUR": core.Volumes{Input: worldAliceEUR, Output: 0}},
						},
					},
					{
						TransactionData: core.TransactionData{Postings: core.Postings{postings[5]}},
						ID:              5,
						Timestamp:       time.Now().UTC().Format(time.RFC3339),
						PreCommitVolumes: core.AccountsAssetsVolumes{
							"toto":  core.AssetsVolumes{"EUR": core.Volumes{Input: worldTotoEUR, Output: 0}},
							"alice": core.AssetsVolumes{"EUR": core.Volumes{Input: worldAliceEUR, Output: 0}},
						},
						PostCommitVolumes: core.AccountsAssetsVolumes{
							"toto":  core.AssetsVolumes{"EUR": core.Volumes{Input: worldTotoEUR, Output: totoAliceEUR}},
							"alice": core.AssetsVolumes{"EUR": core.Volumes{Input: worldAliceEUR + totoAliceEUR, Output: 0}},
						},
					},
				}

				assert.Equal(t, expectedTxs, res.GeneratedTransactions)

				expectedLogs := []core.Log{
					{
						ID:   0,
						Type: core.NewTransactionType,
						Data: core.LoggedTX(expectedTxs[0]),
						Date: res.GeneratedLogs[0].Date,
					},
					{
						ID:   1,
						Type: core.NewTransactionType,
						Data: core.LoggedTX(expectedTxs[1]),
						Date: res.GeneratedLogs[1].Date,
					},
					{
						ID:   2,
						Type: core.NewTransactionType,
						Data: core.LoggedTX(expectedTxs[2]),
						Date: res.GeneratedLogs[2].Date,
					},
					{
						ID:   3,
						Type: core.NewTransactionType,
						Data: core.LoggedTX(expectedTxs[3]),
						Date: res.GeneratedLogs[3].Date,
					},
					{
						ID:   4,
						Type: core.NewTransactionType,
						Data: core.LoggedTX(expectedTxs[4]),
						Date: res.GeneratedLogs[4].Date,
					},
					{
						ID:   5,
						Type: core.NewTransactionType,
						Data: core.LoggedTX(expectedTxs[5]),
						Date: res.GeneratedLogs[5].Date,
					},
				}
				expectedLogs[0].Hash = core.Hash(nil, expectedLogs[0])
				expectedLogs[1].Hash = core.Hash(expectedLogs[0], expectedLogs[1])
				expectedLogs[2].Hash = core.Hash(expectedLogs[1], expectedLogs[2])
				expectedLogs[3].Hash = core.Hash(expectedLogs[2], expectedLogs[3])
				expectedLogs[4].Hash = core.Hash(expectedLogs[3], expectedLogs[4])
				expectedLogs[5].Hash = core.Hash(expectedLogs[4], expectedLogs[5])

				assert.True(t, time.Until(res.GeneratedLogs[0].Date) < time.Millisecond)
				assert.True(t, time.Until(res.GeneratedLogs[1].Date) < time.Millisecond)
				assert.True(t, time.Until(res.GeneratedLogs[2].Date) < time.Millisecond)
				assert.True(t, time.Until(res.GeneratedLogs[3].Date) < time.Millisecond)
				assert.True(t, time.Until(res.GeneratedLogs[4].Date) < time.Millisecond)
				assert.True(t, time.Until(res.GeneratedLogs[5].Date) < time.Millisecond)

				assert.Equal(t, expectedLogs, res.GeneratedLogs)
			})
		})

		t.Run("no transactions", func(t *testing.T) {
			result, err := l.processTx(context.Background(), []core.TransactionData{})
			assert.NoError(t, err)
			assert.Equal(t, &core.CommitResult{
				PreCommitVolumes:      core.AccountsAssetsVolumes{},
				PostCommitVolumes:     core.AccountsAssetsVolumes{},
				GeneratedTransactions: []core.Transaction{},
				GeneratedLogs:         []core.Log{},
			}, result)
		})
	})
}
