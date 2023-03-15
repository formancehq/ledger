package ledger_test

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/ledgertesting"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/mitchellh/mapstructure"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func withContainer(t pgtesting.TestingT, options ...fx.Option) {
	done := make(chan struct{})
	opts := append([]fx.Option{
		fx.NopLogger,
		ledgertesting.ProvideLedgerStorageDriver(t),
	}, options...)
	opts = append(opts, fx.Invoke(func(lc fx.Lifecycle) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				close(done)
				return nil
			},
		})
	}))
	app := fx.New(opts...)
	require.NoError(t, app.Start(context.Background()))
}

func runOnLedger(t pgtesting.TestingT, f func(l *ledger.Ledger), ledgerOptions ...ledger.LedgerOption) {
	var storageDriver storage.Driver
	withContainer(t, fx.Populate(&storageDriver))

	name := uuid.New()
	store, _, err := storageDriver.GetLedgerStore(context.Background(), name, true)
	require.NoError(t, err)

	_, err = store.Initialize(context.Background())
	require.NoError(t, err)

	// 100 000 000 is 100MB
	cache := ledger.NewCache(100000000, 100, true)
	l, err := ledger.NewLedger(store,
		ledger.NewNoOpMonitor(),
		cache,
		ledgerOptions...)
	if err != nil {
		panic(err)
	}
	defer l.Close(context.Background())
	defer cache.Close()

	f(l)
}

func TestTransaction(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		testsize := 1e4
		total := core.NewMonetaryInt(0)
		batch := []core.TransactionData{}

		for i := 1; i <= int(testsize); i++ {
			user := fmt.Sprintf("users:%03d", 1+rand.Intn(100))
			amount := core.NewMonetaryInt(100)
			total = total.Add(amount)

			batch = append(batch, core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "mint",
						Asset:       "GEM",
						Amount:      amount,
					},
					{
						Source:      "mint",
						Destination: user,
						Asset:       "GEM",
						Amount:      amount,
					},
				},
			})

			if i%int(1e3) != 0 {
				continue
			}

			for _, script := range core.TxsToScriptsData(batch...) {
				_, logs, err := l.ProcessScript(context.Background(), true, false, script)
				require.NoError(t, err)
				require.NoError(t, logs.Wait(context.Background()))
			}

			batch = []core.TransactionData{}
		}

		world, err := l.GetAccount(context.Background(), "world")
		require.NoError(t, err)

		expected := total.Neg()
		b := world.Balances["GEM"]
		require.Equalf(t, expected, b,
			"wrong GEM balance for account world, expected: %s got: %s",
			expected, b)

		require.NoError(t, l.Close(context.Background()))
	})
}

func TestTransactionBatchWithConflictingReference(t *testing.T) {
	t.Run("With conflict reference on transaction set", func(t *testing.T) {
		runOnLedger(t, func(l *ledger.Ledger) {
			batch := []core.TransactionData{
				{
					Postings: []core.Posting{
						{
							Source:      "world",
							Destination: "player",
							Asset:       "GEM",
							Amount:      core.NewMonetaryInt(100),
						},
					},
					Reference: "ref1",
				},
				{
					Postings: []core.Posting{
						{
							Source:      "player",
							Destination: "game",
							Asset:       "GEM",
							Amount:      core.NewMonetaryInt(100),
						},
					},
					Reference: "ref2",
				},
				{
					Postings: []core.Posting{
						{
							Source:      "player",
							Destination: "player2",
							Asset:       "GEM",
							Amount:      core.NewMonetaryInt(1000), // Should trigger an insufficient fund error but the conflict error has precedence over it
						},
					},
					Reference: "ref1",
				},
			}

			for i, script := range core.TxsToScriptsData(batch...) {
				_, logs, err := l.ProcessScript(context.Background(), true, false, script)
				if err == nil {
					err = logs.Wait(context.Background())
				} else {
					require.Nil(t, logs)
				}

				if i == 2 {
					require.IsType(t, new(ledger.ConflictError), err)
				} else {
					require.NoError(t, err)
				}
			}
		})
	})
	t.Run("with conflict reference on database", func(t *testing.T) {
		runOnLedger(t, func(l *ledger.Ledger) {
			txData := core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player",
						Asset:       "GEM",
						Amount:      core.NewMonetaryInt(100),
					},
				},
				Reference: "ref1",
			}
			_, logs, err := l.ProcessScript(context.Background(), true, false, core.TxToScriptData(txData))
			require.NoError(t, err)
			require.NoError(t, logs.Wait(context.Background()))

			_, logs, err = l.ProcessScript(context.Background(), true, false, core.TxToScriptData(txData))
			if err == nil {
				err = logs.Wait(context.Background())
			}
			require.Error(t, err)
			require.IsType(t, new(ledger.ConflictError), err)
		})
	})
}

// TODO: Second test is based of first test data. Clean this! It doesn't allow to target a specific test.
func TestTransactionBatchTimestamps(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		timestamp1 := time.Now().UTC().Add(-10 * time.Second)
		timestamp2 := time.Now().UTC().Add(-9 * time.Second)
		timestamp3 := time.Now().UTC().Add(-8 * time.Second)
		timestamp4 := time.Now().UTC().Add(-7 * time.Second)
		t.Run("descending order should fail", func(t *testing.T) {
			batch := []core.TransactionData{
				{
					Postings: []core.Posting{
						{
							Source:      core.WORLD,
							Destination: "player",
							Asset:       "GEM",
							Amount:      core.NewMonetaryInt(1),
						},
					},
					Timestamp: timestamp2,
				},
				{
					Postings: []core.Posting{
						{
							Source:      core.WORLD,
							Destination: "player",
							Asset:       "GEM",
							Amount:      core.NewMonetaryInt(1),
						},
					},
					Timestamp: timestamp1,
				},
			}
			for i, script := range core.TxsToScriptsData(batch...) {
				_, logs, err := l.ProcessScript(context.Background(), true, false, script)
				if err == nil {
					err = logs.Wait(context.Background())
				} else {
					require.Nil(t, logs)
				}

				if i == 1 {
					require.True(t, ledger.IsValidationError(err), err)
					require.ErrorContains(t, err, "cannot pass a timestamp prior to the last transaction")
				} else {
					require.NoError(t, err)
				}
			}
		})
		t.Run("ascending order should succeed", func(t *testing.T) {
			batch := []core.TransactionData{
				{
					Postings: []core.Posting{
						{
							Source:      core.WORLD,
							Destination: "player",
							Asset:       "GEM",
							Amount:      core.NewMonetaryInt(1),
						},
					},
					Timestamp: timestamp2,
				},
				{
					Postings: []core.Posting{
						{
							Source:      core.WORLD,
							Destination: "player",
							Asset:       "GEM",
							Amount:      core.NewMonetaryInt(1),
						},
					},
					Timestamp: timestamp3,
				},
			}
			for _, script := range core.TxsToScriptsData(batch...) {
				_, logs, err := l.ProcessScript(context.Background(), true, false, script)
				require.NoError(t, err)
				require.NoError(t, logs.Wait(context.Background()))
			}
		})
		t.Run("ascending order but before last inserted should fail", func(t *testing.T) {
			batch := []core.TransactionData{
				{
					Postings: []core.Posting{
						{
							Source:      core.WORLD,
							Destination: "player",
							Asset:       "GEM",
							Amount:      core.NewMonetaryInt(1),
						},
					},
					Timestamp: timestamp1,
				},
				{
					Postings: []core.Posting{
						{
							Source:      core.WORLD,
							Destination: "player",
							Asset:       "GEM",
							Amount:      core.NewMonetaryInt(1),
						},
					},
					Timestamp: timestamp4,
				},
			}
			for i, script := range core.TxsToScriptsData(batch...) {
				_, logs, err := l.ProcessScript(context.Background(), true, false, script)
				if err == nil {
					err = logs.Wait(context.Background())
				} else {
					require.Nil(t, logs)
				}

				if i == 0 {
					require.True(t, ledger.IsValidationError(err))
					require.ErrorContains(t, err, "cannot pass a timestamp prior to the last transaction")
				} else {
					require.NoError(t, err)
				}
			}
		})
	})
}

func TestTransactionExpectedVolumes(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		txsData := []core.TransactionData{
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player",
						Asset:       "USD",
						Amount:      core.NewMonetaryInt(100),
					},
				},
			},
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player",
						Asset:       "EUR",
						Amount:      core.NewMonetaryInt(100),
					},
				},
			},
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player2",
						Asset:       "EUR",
						Amount:      core.NewMonetaryInt(100),
					},
				},
			},
			{
				Postings: []core.Posting{
					{
						Source:      "player",
						Destination: "player2",
						Asset:       "EUR",
						Amount:      core.NewMonetaryInt(50),
					},
				},
			},
		}

		res := make([]core.ExpandedTransaction, 0)
		for _, script := range core.TxsToScriptsData(txsData...) {
			tx, logs, err := l.ProcessScript(context.Background(), true, false, script)
			require.NoError(t, err)
			require.NoError(t, logs.Wait(context.Background()))
			res = append(res, tx)
		}

		postCommitVolumes := core.AggregatePostCommitVolumes(res...)
		require.Equal(t, 4, len(res))
		require.EqualValues(t, core.AccountsAssetsVolumes{
			"world": core.AssetsVolumes{
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(100),
				},
				"EUR": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(200),
				},
			},
			"player": core.AssetsVolumes{
				"USD": {
					Input:  core.NewMonetaryInt(100),
					Output: core.NewMonetaryInt(0),
				},
				"EUR": {
					Input:  core.NewMonetaryInt(100),
					Output: core.NewMonetaryInt(50),
				},
			},
			"player2": core.AssetsVolumes{
				"EUR": {
					Input:  core.NewMonetaryInt(150),
					Output: core.NewMonetaryInt(0),
				},
			},
		}, postCommitVolumes)
	})
}

func TestReference(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		tx := core.TransactionData{
			Reference: "payment_processor_id_01",
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "payments:001",
					Amount:      core.NewMonetaryInt(100),
					Asset:       "COIN",
				},
			},
		}

		_, logs, err := l.ProcessScript(context.Background(), true, false, core.TxToScriptData(tx))
		require.NoError(t, err)
		require.NoError(t, logs.Wait(context.Background()))

		_, logs, err = l.ProcessScript(context.Background(), true, false, core.TxToScriptData(tx))
		require.Error(t, err)
		require.Nil(t, logs)
	})
}

func TestAccountMetadata(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {

		logs, err := l.SaveMeta(context.Background(), core.MetaTargetTypeAccount, "users:001", core.Metadata{
			"a random metadata": "old value",
		})
		require.NoError(t, err)
		require.NoError(t, logs.Wait(context.Background()))

		logs, err = l.SaveMeta(context.Background(), core.MetaTargetTypeAccount, "users:001", core.Metadata{
			"a random metadata": "new value",
		})
		require.NoError(t, err)
		require.NoError(t, logs.Wait(context.Background()))

		{
			acc, err := l.GetAccount(context.Background(), "users:001")
			require.NoError(t, err)

			meta, ok := acc.Metadata["a random metadata"]
			require.True(t, ok)

			require.Equalf(t, meta, "new value",
				"metadata entry did not match in get: expected \"new value\", got %v", meta)
		}

		{
			// We have to create at least one transaction to retrieve an account from GetAccounts store method
			_, logs, err := l.ProcessScript(context.Background(), true, false, core.TxToScriptData(core.TransactionData{
				Postings: core.Postings{
					{
						Source:      "world",
						Amount:      core.NewMonetaryInt(100),
						Asset:       "USD",
						Destination: "users:001",
					},
				},
			}))
			require.NoError(t, err)
			require.NoError(t, logs.Wait(context.Background()))

			acc, err := l.GetAccount(context.Background(), "users:001")
			require.NoError(t, err)
			require.True(t, acc.Address == "users:001", "no account returned by get account")

			meta, ok := acc.Metadata["a random metadata"]
			require.True(t, ok)
			require.Equalf(t, meta, "new value",
				"metadata entry did not match in find: expected \"new value\", got %v", meta)
		}
	})
}

func TestTransactionMetadata(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		_, logs, err := l.ProcessScript(context.Background(), true, false,
			core.TxToScriptData(core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "payments:001",
						Amount:      core.NewMonetaryInt(100),
						Asset:       "COIN",
					},
				},
			}))
		require.NoError(t, err)
		require.NoError(t, logs.Wait(context.Background()))

		tx, err := l.GetLedgerStore().GetLastTransaction(context.Background())
		require.NoError(t, err)

		logs, err = l.SaveMeta(context.Background(), core.MetaTargetTypeTransaction, tx.ID, core.Metadata{
			"a random metadata": "old value",
		})
		require.NoError(t, err)
		require.NoError(t, logs.Wait(context.Background()))

		logs, err = l.SaveMeta(context.Background(), core.MetaTargetTypeTransaction, tx.ID, core.Metadata{
			"a random metadata": "new value",
		})
		require.NoError(t, err)
		require.NoError(t, logs.Wait(context.Background()))

		tx, err = l.GetLedgerStore().GetLastTransaction(context.Background())
		require.NoError(t, err)

		meta, ok := tx.Metadata["a random metadata"]
		require.True(t, ok)

		require.Equalf(t, meta, "new value",
			"metadata entry did not match: expected \"new value\", got %v", meta)
	})
}

func TestSaveTransactionMetadata(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		_, logs, err := l.ProcessScript(context.Background(), true, false,
			core.TxToScriptData(core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "payments:001",
						Amount:      core.NewMonetaryInt(100),
						Asset:       "COIN",
					},
				},
				Metadata: core.Metadata{
					"a metadata": "a value",
				},
			}))
		require.NoError(t, err)
		require.NoError(t, logs.Wait(context.Background()))

		tx, err := l.GetLedgerStore().GetLastTransaction(context.Background())
		require.NoError(t, err)

		meta, ok := tx.Metadata["a metadata"]
		require.True(t, ok)

		require.Equalf(t, meta, "a value",
			"metadata entry did not match: expected \"a value\", got %v", meta)
	})
}

func TestGetTransaction(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		_, logs, err := l.ProcessScript(context.Background(), true, false,
			core.TxToScriptData(core.TransactionData{
				Reference: "bar",
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "payments:001",
						Amount:      core.NewMonetaryInt(100),
						Asset:       "COIN",
					},
				},
			}))
		require.NoError(t, err)
		require.NoError(t, logs.Wait(context.Background()))

		last, err := l.GetLedgerStore().GetLastTransaction(context.Background())
		require.NoError(t, err)

		tx, err := l.GetTransaction(context.Background(), last.ID)
		require.NoError(t, err)

		require.True(t, reflect.DeepEqual(tx, last))
	})
}

func TestGetTransactions(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		tx := core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "test_get_transactions",
					Amount:      core.NewMonetaryInt(100),
					Asset:       "COIN",
				},
			},
		}

		_, logs, err := l.ProcessScript(context.Background(), true, false, core.TxToScriptData(tx))
		require.NoError(t, err)
		require.NoError(t, logs.Wait(context.Background()))

		res, err := l.GetTransactions(context.Background(), *storage.NewTransactionsQuery())
		require.NoError(t, err)

		require.Equal(t, "test_get_transactions", res.Data[0].Postings[0].Destination)
	})
}

func TestRevertTransaction(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		revertAmt := core.NewMonetaryInt(100)

		res, logs, err := l.ProcessScript(context.Background(), true, false,
			core.TxToScriptData(core.TransactionData{
				Reference: "foo",
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "payments:001",
						Amount:      revertAmt,
						Asset:       "COIN",
					},
				},
			}))
		require.NoError(t, err)
		require.NoError(t, logs.Wait(context.Background()))

		world, err := l.GetAccount(context.Background(), "world")
		require.NoError(t, err)

		originalBal := world.Balances["COIN"]

		revertTx, logs, err := l.RevertTransaction(context.Background(), res.ID)
		require.NoError(t, err)
		require.NoError(t, logs.Wait(context.Background()))

		require.Equal(t, core.Postings{
			{
				Source:      "payments:001",
				Destination: "world",
				Amount:      core.NewMonetaryInt(100),
				Asset:       "COIN",
			},
		}, revertTx.TransactionData.Postings)

		require.EqualValues(t, fmt.Sprintf("%d", res.ID),
			revertTx.Metadata[core.RevertMetadataSpecKey()])

		tx, err := l.GetTransaction(context.Background(), res.ID)
		require.NoError(t, err)

		v := core.RevertedMetadataSpecValue{}
		require.NoError(t, mapstructure.Decode(tx.Metadata[core.RevertedMetadataSpecKey()], &v))
		require.Equal(t, core.RevertedMetadataSpecValue{
			By: fmt.Sprint(revertTx.ID),
		}, v)

		world, err = l.GetAccount(context.Background(), "world")
		require.NoError(t, err)

		newBal := world.Balances["COIN"]
		expectedBal := originalBal.Add(revertAmt)
		require.Equalf(t, expectedBal, newBal,
			"COIN world balances expected %d, got %d", expectedBal, newBal)
	})
}

func TestVeryBigTransaction(t *testing.T) {
	runOnLedger(t, func(l *ledger.Ledger) {
		amount, err := core.ParseMonetaryInt(
			"199999999999999999992919191919192929292939847477171818284637291884661818183647392936472918836161728274766266161728493736383838")
		require.NoError(t, err)

		res, logs, err := l.ProcessScript(context.Background(), true, false,
			core.TxToScriptData(core.TransactionData{
				Postings: []core.Posting{{
					Source:      "world",
					Destination: "bank",
					Asset:       "ETH/18",
					Amount:      amount,
				}},
			}))
		require.NoError(t, err)
		require.NoError(t, logs.Wait(context.Background()))

		txFromDB, err := l.GetTransaction(context.Background(), res.ID)
		require.NoError(t, err)
		require.Equal(t, txFromDB.Postings[0].Amount, amount)
	})
}

func BenchmarkTransaction1(b *testing.B) {
	runOnLedger(b, func(l *ledger.Ledger) {
		for n := 0; n < b.N; n++ {
			_, logs, err := l.ProcessScript(context.Background(), true, false, core.TxToScriptData(core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "benchmark",
						Asset:       "COIN",
						Amount:      core.NewMonetaryInt(10),
					},
				},
			}))
			require.NoError(b, err)
			require.NoError(b, logs.Wait(context.Background()))
		}
	})
}

func BenchmarkTransaction_20_1k(b *testing.B) {
	runOnLedger(b, func(l *ledger.Ledger) {
		for n := 0; n < b.N; n++ {
			for i := 0; i < 20; i++ {
				txs := []core.TransactionData{}

				for j := 0; j < 1e3; j++ {
					txs = append(txs, core.TransactionData{
						Postings: []core.Posting{
							{
								Source:      "world",
								Destination: "benchmark",
								Asset:       "COIN",
								Amount:      core.NewMonetaryInt(10),
							},
						},
					})
				}

				for _, script := range core.TxsToScriptsData(txs...) {
					_, logs, err := l.ProcessScript(context.Background(), true, false, script)
					require.NoError(b, err)
					require.NoError(b, logs.Wait(context.Background()))
				}
			}
		}
	})
}

func BenchmarkGetAccount(b *testing.B) {
	runOnLedger(b, func(l *ledger.Ledger) {
		for i := 0; i < b.N; i++ {
			_, err := l.GetAccount(context.Background(), "users:013")
			require.NoError(b, err)
		}
	})
}

func BenchmarkGetTransactions(b *testing.B) {
	runOnLedger(b, func(l *ledger.Ledger) {
		for i := 0; i < b.N; i++ {
			_, err := l.GetTransactions(context.Background(), storage.TransactionsQuery{})
			require.NoError(b, err)
		}
	})
}
