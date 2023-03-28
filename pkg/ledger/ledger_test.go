package ledger

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestAccountMetadata(t *testing.T) {
	runOnLedger(t, func(l *Ledger) {

		err := l.SaveMeta(context.Background(), core.MetaTargetTypeAccount, "users:001", core.Metadata{
			"a random metadata": "old value",
		})
		require.NoError(t, err)

		err = l.SaveMeta(context.Background(), core.MetaTargetTypeAccount, "users:001", core.Metadata{
			"a random metadata": "new value",
		})
		require.NoError(t, err)

		acc, err := l.dbCache.GetAccountWithVolumes(context.Background(), "users:001")
		require.NoError(t, err)

		meta, ok := acc.Metadata["a random metadata"]
		require.True(t, ok)

		require.Equalf(t, meta, "new value",
			"metadata entry did not match in get: expected \"new value\", got %v", meta)

		// We have to create at least one transaction to retrieve an account from GetAccounts store method
		_, err = l.CreateTransaction(context.Background(), false, core.TxToScriptData(core.TransactionData{
			Postings: core.Postings{
				{
					Source:      "world",
					Amount:      big.NewInt(100),
					Asset:       "USD",
					Destination: "users:001",
				},
			},
		}))
		require.NoError(t, err)

		acc, err = l.dbCache.GetAccountWithVolumes(context.Background(), "users:001")
		require.NoError(t, err)
		require.NotNil(t, acc)

		meta, ok = acc.Metadata["a random metadata"]
		require.True(t, ok)
		require.Equalf(t, meta, "new value",
			"metadata entry did not match in find: expected \"new value\", got %v", meta)
	})
}

func TestTransactionMetadata(t *testing.T) {
	runOnLedger(t, func(l *Ledger) {
		err := l.SaveMeta(context.Background(), core.MetaTargetTypeTransaction, uint64(0), core.Metadata{
			"a random metadata": "old value",
		})
		require.NoError(t, err)
	})
}

func TestRevertTransaction(t *testing.T) {
	runOnLedger(t, func(l *Ledger) {
		tx := core.Transaction{
			TransactionData: core.TransactionData{
				Reference: "foo",
				Postings: []core.Posting{
					core.NewPosting("world", "payments:001", "COIN", big.NewInt(100)),
				},
			},
		}
		expandedTx := core.ExpandedTransaction{
			Transaction: tx,
			PreCommitVolumes: map[string]core.AssetsVolumes{
				"world": {
					"COIN": core.NewEmptyVolumes().WithOutput(big.NewInt(10)),
				},
				"payments:001": {
					"COIN": core.NewEmptyVolumes(),
				},
			},
			PostCommitVolumes: map[string]core.AssetsVolumes{
				"world": {
					"COIN": core.NewEmptyVolumes().WithOutput(big.NewInt(110)),
				},
				"payments:001": {
					"COIN": core.NewEmptyVolumes().WithInput(big.NewInt(100)),
				},
			},
		}

		require.NoError(t, l.GetLedgerStore().InsertTransactions(context.Background(), expandedTx))
		require.NoError(t, l.GetLedgerStore().EnsureAccountExists(context.Background(), "payments:001"))
		require.NoError(t, l.GetLedgerStore().UpdateVolumes(context.Background(), core.AccountsAssetsVolumes{
			"payments:001": {
				"COIN": core.NewEmptyVolumes().WithInput(big.NewInt(110)),
			},
			"world": {
				"COIN": core.NewEmptyVolumes().WithOutput(big.NewInt(110)),
			},
		}))

		revertTx, err := l.RevertTransaction(context.Background(), tx.ID)
		require.NoError(t, err)

		require.Equal(t, core.Postings{
			{
				Source:      "payments:001",
				Destination: "world",
				Amount:      big.NewInt(100),
				Asset:       "COIN",
			},
		}, revertTx.TransactionData.Postings)

		require.EqualValues(t, fmt.Sprintf("%d", tx.ID), revertTx.Metadata[core.RevertMetadataSpecKey()])
		require.Equal(t, revertTx.Timestamp, l.runner.GetMoreRecentTransactionDate())

		account, err := l.dbCache.GetAccountWithVolumes(context.Background(), "payments:001")
		require.NoError(t, err)
		require.Equal(t, core.AccountWithVolumes{
			Account: core.Account{
				Address:  "payments:001",
				Metadata: core.Metadata{},
			},
			Volumes: core.AssetsVolumes{
				"COIN": core.NewEmptyVolumes().
					WithInput(big.NewInt(110)).
					WithOutput(tx.Postings[0].Amount),
			},
		}, *account)

		rawLogs, err := l.GetLedgerStore().ReadLogsStartingFromID(context.Background(), 0)
		require.NoError(t, err)
		require.Len(t, rawLogs, 1)
		require.Equal(t, core.NewRevertedTransactionLog(revertTx.Timestamp, tx.ID, revertTx.Transaction).
			WithReference("revert_"+tx.Reference).
			ComputeHash(nil), rawLogs[0])
	})
}

func TestVeryBigTransaction(t *testing.T) {
	runOnLedger(t, func(l *Ledger) {
		amount, ok := new(big.Int).SetString(
			"199999999999999999992919191919192929292939847477171818284637291884661818183647392936472918836161728274766266161728493736383838", 10)
		require.True(t, ok)

		_, err := l.CreateTransaction(context.Background(), false,
			core.TxToScriptData(core.TransactionData{
				Postings: []core.Posting{{
					Source:      "world",
					Destination: "bank",
					Asset:       "ETH/18",
					Amount:      amount,
				}},
			}))
		require.NoError(t, err)
	})
}

func BenchmarkSequentialWrites(b *testing.B) {
	ledgerName := uuid.NewString()
	resolver := newResolver(b)

	ledger, err := resolver.GetLedger(context.TODO(), ledgerName)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ledger.CreateTransaction(context.Background(), false, core.RunScript{
			Script: core.Script{
				Plain: `send [USD/2 100] (
					source = @world
					destination = @bank
				)`,
			},
		})
		require.NoError(b, err)
	}
}

func BenchmarkParallelWrites(b *testing.B) {
	resolver := newResolver(b)

	ledger, err := resolver.GetLedger(context.Background(), uuid.NewString())
	require.NoError(b, err)

	b.ResetTimer()
	wg := sync.WaitGroup{}
	wg.Add(b.N)
	for i := 0; i < b.N; i++ {
		go func() {
			defer wg.Done()

			_, err := ledger.CreateTransaction(context.Background(), false, core.RunScript{
				Script: core.Script{
					Plain: `send [USD/2 100] (
					source = @world
					destination = @bank
				)`,
				},
			})
			require.NoError(b, err)
		}()
	}
	wg.Wait()
}
