package ledger_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/stretchr/testify/require"
)

const nbPostings = 1000

func BenchmarkLedger_PostTransactions_Scripts_Single_FixedAccounts(b *testing.B) {
	var execResScript core.ExpandedTransaction

	txData := core.TransactionData{}
	for i := 0; i < nbPostings; i++ {
		txData.Postings = append(txData.Postings, core.Posting{
			Source:      "world",
			Destination: "benchmarks:" + strconv.Itoa(i),
			Asset:       "COIN",
			Amount:      core.NewMonetaryInt(10),
		})
	}
	_, err := txData.Postings.Validate()
	require.NoError(b, err)

	runOnLedger(b, func(l *ledger.Ledger) {
		b.ResetTimer()

		res := core.ExpandedTransaction{}

		for n := 0; n < b.N; n++ {
			b.StopTimer()
			script := txToScriptData(txData)
			b.StartTimer()
			_res, waitAndPostProcess := l.ExecuteScript(context.Background(), true, script)
			require.NoError(b, waitAndPostProcess(context.Background()))
			require.Len(b, res.Postings, nbPostings)
			res = _res
		}

		execResScript = res
		require.Len(b, execResScript.Postings, nbPostings)
	})
}

func BenchmarkLedger_PostTransactions_Postings_Single_FixedAccounts(b *testing.B) {
	var execRes []core.ExpandedTransaction

	runOnLedger(b, func(l *ledger.Ledger) {
		txData := core.TransactionData{}
		for i := 0; i < nbPostings; i++ {
			txData.Postings = append(txData.Postings, core.Posting{
				Source:      "world",
				Destination: "benchmarks:" + strconv.Itoa(i),
				Asset:       "COIN",
				Amount:      core.NewMonetaryInt(10),
			})
		}

		b.ResetTimer()

		res := []core.ExpandedTransaction{}

		for n := 0; n < b.N; n++ {
			_, err := txData.Postings.Validate()
			require.NoError(b, err)

			tx, waitAndPostProcess := l.ExecuteScript(context.Background(), true, core.TxToScriptData(txData))
			require.NoError(b, waitAndPostProcess(context.Background()))
			require.Len(b, res, 1)
			require.Len(b, res[0].Postings, nbPostings)
			res = append(res, tx)
		}

		execRes = res
		require.Len(b, execRes, 1)
		require.Len(b, execRes[0].Postings, nbPostings)
	})
}

func BenchmarkLedger_PostTransactions_Postings_Batch_FixedAccounts(b *testing.B) {
	var execRes []core.ExpandedTransaction

	txsData := newTxsData(1)

	runOnLedger(b, func(l *ledger.Ledger) {
		b.ResetTimer()

		res := []core.ExpandedTransaction{}

		for n := 0; n < b.N; n++ {
			for _, txData := range txsData {
				_, err := txData.Postings.Validate()
				require.NoError(b, err)
			}
			for _, script := range core.TxsToScriptsData(txsData...) {
				tx, waitAndPostProcess := l.ExecuteScript(context.Background(), true, script)
				require.NoError(b, waitAndPostProcess(context.Background()))
				res = append(res, tx)
			}

			require.Len(b, res, 7)
			require.Len(b, res[0].Postings, 1)
			require.Len(b, res[1].Postings, 1)
			require.Len(b, res[2].Postings, 2)
			require.Len(b, res[3].Postings, 4)
			require.Len(b, res[4].Postings, 4)
			require.Len(b, res[5].Postings, 1)
			require.Len(b, res[6].Postings, 1)
		}

		execRes = res
		require.Len(b, execRes, 7)
		require.Len(b, execRes[0].Postings, 1)
		require.Len(b, execRes[1].Postings, 1)
		require.Len(b, execRes[2].Postings, 2)
		require.Len(b, execRes[3].Postings, 4)
		require.Len(b, execRes[4].Postings, 4)
		require.Len(b, execRes[5].Postings, 1)
		require.Len(b, execRes[6].Postings, 1)
	})
}

func BenchmarkLedger_PostTransactions_Postings_Batch_VaryingAccounts(b *testing.B) {
	var execRes []core.ExpandedTransaction

	runOnLedger(b, func(l *ledger.Ledger) {
		b.ResetTimer()

		res := make([]core.ExpandedTransaction, 0)

		for n := 0; n < b.N; n++ {
			b.StopTimer()
			txsData := newTxsData(n)
			b.StartTimer()

			for _, txData := range txsData {
				_, err := txData.Postings.Validate()
				require.NoError(b, err)
			}
			for _, script := range core.TxsToScriptsData(txsData...) {
				tx, waitAndPostProcess := l.ExecuteScript(context.Background(), true, script)
				require.NoError(b, waitAndPostProcess(context.Background()))
				res = append(res, tx)
			}
			require.Len(b, res, 7)
			require.Len(b, res[0].Postings, 1)
			require.Len(b, res[1].Postings, 1)
			require.Len(b, res[2].Postings, 2)
			require.Len(b, res[3].Postings, 4)
			require.Len(b, res[4].Postings, 4)
			require.Len(b, res[5].Postings, 1)
			require.Len(b, res[6].Postings, 1)
		}

		execRes = res
		require.Len(b, execRes, 7)
		require.Len(b, execRes[0].Postings, 1)
		require.Len(b, execRes[1].Postings, 1)
		require.Len(b, execRes[2].Postings, 2)
		require.Len(b, execRes[3].Postings, 4)
		require.Len(b, execRes[4].Postings, 4)
		require.Len(b, execRes[5].Postings, 1)
		require.Len(b, execRes[6].Postings, 1)
	})
}

func newTxsData(i int) []core.TransactionData {
	return []core.TransactionData{
		{
			Postings: core.Postings{
				{
					Source:      "world",
					Destination: fmt.Sprintf("payins:%d", i),
					Amount:      core.NewMonetaryInt(10000),
					Asset:       "EUR/2",
				},
			},
		},
		{
			Postings: core.Postings{
				{
					Source:      fmt.Sprintf("payins:%d", i),
					Destination: fmt.Sprintf("users:%d:wallet", i),
					Amount:      core.NewMonetaryInt(10000),
					Asset:       "EUR/2",
				},
			},
		},
		{
			Postings: core.Postings{
				{
					Source:      "world",
					Destination: fmt.Sprintf("teller:%d", i),
					Amount:      core.NewMonetaryInt(350000),
					Asset:       "RBLX/6",
				},
				{
					Source:      "world",
					Destination: fmt.Sprintf("teller:%d", i),
					Amount:      core.NewMonetaryInt(1840000),
					Asset:       "SNAP/6",
				},
			},
		},
		{
			Postings: core.Postings{
				{
					Source:      fmt.Sprintf("users:%d:wallet", i),
					Destination: fmt.Sprintf("trades:%d", i),
					Amount:      core.NewMonetaryInt(1500),
					Asset:       "EUR/2",
				},
				{
					Source:      fmt.Sprintf("trades:%d", i),
					Destination: fmt.Sprintf("fiat:holdings:%d", i),
					Amount:      core.NewMonetaryInt(1500),
					Asset:       "EUR/2",
				},
				{
					Source:      fmt.Sprintf("teller:%d", i),
					Destination: fmt.Sprintf("trades:%d", i),
					Amount:      core.NewMonetaryInt(350000),
					Asset:       "RBLX/6",
				},
				{
					Source:      fmt.Sprintf("trades:%d", i),
					Destination: fmt.Sprintf("users:%d:wallet", i),
					Amount:      core.NewMonetaryInt(350000),
					Asset:       "RBLX/6",
				},
			},
		},
		{
			Postings: core.Postings{
				{
					Source:      fmt.Sprintf("users:%d:wallet", i),
					Destination: fmt.Sprintf("trades:%d", i),
					Amount:      core.NewMonetaryInt(4230),
					Asset:       "EUR/2",
				},
				{
					Source:      fmt.Sprintf("trades:%d", i),
					Destination: fmt.Sprintf("fiat:holdings:%d", i),
					Amount:      core.NewMonetaryInt(4230),
					Asset:       "EUR/2",
				},
				{
					Source:      fmt.Sprintf("teller:%d", i),
					Destination: fmt.Sprintf("trades:%d", i),
					Amount:      core.NewMonetaryInt(1840000),
					Asset:       "SNAP/6",
				},
				{
					Source:      fmt.Sprintf("trades:%d", i),
					Destination: fmt.Sprintf("users:%d:wallet", i),
					Amount:      core.NewMonetaryInt(1840000),
					Asset:       "SNAP/6",
				},
			},
		},
		{
			Postings: core.Postings{
				{
					Source:      fmt.Sprintf("users:%d:wallet", i),
					Destination: fmt.Sprintf("users:%d:withdrawals", i),
					Amount:      core.NewMonetaryInt(2270),
					Asset:       "EUR/2",
				},
			},
		},
		{
			Postings: core.Postings{
				{
					Source:      fmt.Sprintf("users:%d:withdrawals", i),
					Destination: fmt.Sprintf("payouts:%d", i),
					Amount:      core.NewMonetaryInt(2270),
					Asset:       "EUR/2",
				},
			},
		},
	}
}
