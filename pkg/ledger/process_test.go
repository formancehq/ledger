package ledger

import (
	"context"
	"testing"
	"time"

	"github.com/numary/ledger/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
					{
						Postings:  postings,
						Timestamp: time.Now().UTC().Round(time.Second),
					},
				}

				res, err := l.processTx(context.Background(), txsData)
				assert.NoError(t, err)

				assert.Equal(t, expectedPreCommitVol, res.PreCommitVolumes)
				assert.Equal(t, expectedPostCommitVol, res.PostCommitVolumes)

				expectedTxs := []core.Transaction{{
					TransactionData:   txsData[0],
					ID:                0,
					PreCommitVolumes:  expectedPreCommitVol,
					PostCommitVolumes: expectedPostCommitVol,
				}}
				assert.Equal(t, expectedTxs, res.GeneratedTransactions)

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

				res, err := l.processTx(context.Background(), txsData)
				assert.NoError(t, err)

				assert.Equal(t, expectedPreCommitVol, res.PreCommitVolumes)
				assert.Equal(t, expectedPostCommitVol, res.PostCommitVolumes)

				expectedTxs := []core.Transaction{
					{
						TransactionData: core.TransactionData{
							Timestamp: now,
							Postings:  core.Postings{postings[0]},
						},
						ID: 0,
						PreCommitVolumes: core.AccountsAssetsVolumes{
							"toto":  core.AssetsVolumes{"USD": core.Volumes{Input: 0, Output: 0}},
							"world": core.AssetsVolumes{"USD": core.Volumes{Input: 0, Output: 0}}},
						PostCommitVolumes: core.AccountsAssetsVolumes{
							"toto":  core.AssetsVolumes{"USD": core.Volumes{Input: worldTotoUSD, Output: 0}},
							"world": core.AssetsVolumes{"USD": core.Volumes{Input: 0, Output: worldTotoUSD}}},
					},
					{
						TransactionData: core.TransactionData{
							Postings:  core.Postings{postings[1]},
							Timestamp: now.Add(time.Second),
						},
						ID: 1,
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
						TransactionData: core.TransactionData{
							Timestamp: now.Add(2 * time.Second),
							Postings:  core.Postings{postings[2]},
						},
						ID: 2,
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
						TransactionData: core.TransactionData{
							Timestamp: now.Add(3 * time.Second),
							Postings:  core.Postings{postings[3]},
						},
						ID: 3,
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
						TransactionData: core.TransactionData{
							Timestamp: now.Add(4 * time.Second),
							Postings:  core.Postings{postings[4]},
						},
						ID: 4,
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
						TransactionData: core.TransactionData{
							Timestamp: now.Add(5 * time.Second),
							Postings:  core.Postings{postings[5]},
						},
						ID: 5,
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
						Date: now,
					},
					{
						ID:   1,
						Type: core.NewTransactionType,
						Data: core.LoggedTX(expectedTxs[1]),
						Date: now.Add(time.Second),
					},
					{
						ID:   2,
						Type: core.NewTransactionType,
						Data: core.LoggedTX(expectedTxs[2]),
						Date: now.Add(2 * time.Second),
					},
					{
						ID:   3,
						Type: core.NewTransactionType,
						Data: core.LoggedTX(expectedTxs[3]),
						Date: now.Add(3 * time.Second),
					},
					{
						ID:   4,
						Type: core.NewTransactionType,
						Data: core.LoggedTX(expectedTxs[4]),
						Date: now.Add(4 * time.Second),
					},
					{
						ID:   5,
						Type: core.NewTransactionType,
						Data: core.LoggedTX(expectedTxs[5]),
						Date: now.Add(5 * time.Second),
					},
				}
				expectedLogs[0].Hash = core.Hash(nil, expectedLogs[0])
				expectedLogs[1].Hash = core.Hash(expectedLogs[0], expectedLogs[1])
				expectedLogs[2].Hash = core.Hash(expectedLogs[1], expectedLogs[2])
				expectedLogs[3].Hash = core.Hash(expectedLogs[2], expectedLogs[3])
				expectedLogs[4].Hash = core.Hash(expectedLogs[3], expectedLogs[4])
				expectedLogs[5].Hash = core.Hash(expectedLogs[4], expectedLogs[5])

				assert.Equal(t, expectedLogs, res.GeneratedLogs)
			})
		})

		t.Run("no transactions", func(t *testing.T) {
			result, err := l.processTx(context.Background(), []core.TransactionData{})
			assert.NoError(t, err)
			assert.Equal(t, &CommitResult{
				PreCommitVolumes:      core.AccountsAssetsVolumes{},
				PostCommitVolumes:     core.AccountsAssetsVolumes{},
				GeneratedTransactions: []core.Transaction{},
				GeneratedLogs:         []core.Log{},
			}, result)
		})

		t.Run("date in the past", func(t *testing.T) {
			now := time.Now()
			log := core.NewTransactionLogWithDate(nil, core.Transaction{
				TransactionData: core.TransactionData{
					Postings: []core.Posting{{
						Source:      "world",
						Destination: "bank",
						Amount:      100,
						Asset:       "USD",
					}},
					Timestamp: now,
				},
				ID: 0,
			}, now)
			require.NoError(t, l.store.AppendLog(context.Background(), log))

			_, err := l.processTx(context.Background(), []core.TransactionData{
				{
					Postings: []core.Posting{{
						Source:      "world",
						Destination: "bank",
						Amount:      100,
						Asset:       "USD",
					}},
					Timestamp: now.Add(-time.Second),
				},
			})

			assert.Error(t, err)
			assert.True(t, IsValidationError(err))
		})
	})
	runOnLedger(func(l *Ledger) {
		t.Run("date in the past (allowed by policy)", func(t *testing.T) {
			now := time.Now()
			log := core.NewTransactionLogWithDate(nil, core.Transaction{
				TransactionData: core.TransactionData{
					Postings: []core.Posting{{
						Source:      "world",
						Destination: "bank",
						Amount:      100,
						Asset:       "USD",
					}},
					Timestamp: now,
				},
				ID: 0,
			}, now)
			require.NoError(t, l.store.AppendLog(context.Background(), log))

			_, err := l.processTx(context.Background(), []core.TransactionData{
				{
					Postings: []core.Posting{{
						Source:      "world",
						Destination: "bank",
						Amount:      100,
						Asset:       "USD",
					}},
					Timestamp: now.Add(-time.Second),
				},
			})

			assert.NoError(t, err)

		})
	}, WithPastTimestamps)
}
