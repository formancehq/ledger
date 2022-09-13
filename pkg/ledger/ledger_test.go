package ledger_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func withContainer(options ...fx.Option) {
	done := make(chan struct{})
	opts := append([]fx.Option{
		fx.NopLogger,
		ledgertesting.ProvideLedgerStorageDriver(),
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
	go func() {
		if err := app.Start(context.Background()); err != nil {
			panic(err)
		}
	}()

	<-done
	if app.Err() != nil {
		panic(app.Err())
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()

	if err := app.Stop(ctx); err != nil {
		panic(err)
	}
}

func runOnLedger(f func(l *ledger.Ledger), ledgerOptions ...ledger.LedgerOption) {
	withContainer(fx.Invoke(func(lc fx.Lifecycle, storageDriver storage.Driver[ledger.Store]) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				name := uuid.New()
				store, _, err := storageDriver.GetLedgerStore(context.Background(), name, true)
				if err != nil {
					return err
				}
				_, err = store.Initialize(context.Background())
				if err != nil {
					return err
				}
				l, err := ledger.NewLedger(store, ledger.NewInMemoryLocker(), ledger.NewNoOpMonitor(), ledgerOptions...)
				if err != nil {
					panic(err)
				}
				lc.Append(fx.Hook{
					OnStop: l.Close,
				})
				f(l)
				return nil
			},
		})
	}))
}

func TestMain(m *testing.M) {
	var code int
	defer func() {
		os.Exit(code) // os.Exit don't care about defer so defer the os.Exit allow us to execute other defer
	}()

	flag.Parse()
	if testing.Verbose() {
		logrus.StandardLogger().Level = logrus.DebugLevel
	}

	code = m.Run()
}

func TestTransaction(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
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

			_, err := l.Commit(context.Background(), batch)
			require.NoError(t, err)

			batch = []core.TransactionData{}
		}

		world, err := l.GetAccount(context.Background(), "world")
		require.NoError(t, err)

		expected := total.Neg()
		b := world.Balances["GEM"]
		assert.Equalf(t, expected, b,
			"wrong GEM balance for account world, expected: %d got: %d",
			expected, b)

		require.NoError(t, l.Close(context.Background()))
	})
}

func TestTransactionBatchWithIntermediateWrongState(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		batch := []core.TransactionData{
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player2",
						Asset:       "GEM",
						Amount:      core.NewMonetaryInt(100),
					},
				},
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
			},
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player",
						Asset:       "GEM",
						Amount:      core.NewMonetaryInt(100),
					},
				},
			},
		}

		_, err := l.Commit(context.Background(), batch)
		assert.Error(t, err)
		assert.IsType(t, new(ledger.TransactionCommitError), err)
		assert.IsType(t, new(ledger.InsufficientFundError), errors.Unwrap(err))
	})
}

func TestTransactionBatchWithConflictingReference(t *testing.T) {
	t.Run("With conflict reference on transaction set", func(t *testing.T) {
		runOnLedger(func(l *ledger.Ledger) {
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

			_, err := l.Commit(context.Background(), batch)
			assert.Error(t, err)
			assert.IsType(t, new(ledger.ConflictError), err)
		})
	})
	t.Run("with conflict reference on database", func(t *testing.T) {
		runOnLedger(func(l *ledger.Ledger) {
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
			_, err := l.Commit(context.Background(), []core.TransactionData{txData})
			require.NoError(t, err)

			_, err = l.Commit(context.Background(), []core.TransactionData{txData})
			assert.Error(t, err)
			assert.IsType(t, new(ledger.ConflictError), err)
		})
	})
}

func TestTransactionExpectedVolumes(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		batch := []core.TransactionData{
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

		res, err := l.Commit(context.Background(), batch)
		assert.NoError(t, err)

		assert.EqualValues(t, core.AccountsAssetsVolumes{
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
		}, res.PostCommitVolumes)
	})
}

func TestBalance(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		_, err := l.Commit(context.Background(), []core.TransactionData{
			{
				Postings: []core.Posting{
					{
						Source:      "empty_wallet",
						Destination: "world",
						Amount:      core.NewMonetaryInt(1),
						Asset:       "COIN",
					},
				},
			},
		})
		assert.Error(t, err,
			"balance was insufficient yet the transaction was committed")
	})
}

func TestReference(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
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

		_, err := l.Commit(context.Background(), []core.TransactionData{tx})
		require.NoError(t, err)

		_, err = l.Commit(context.Background(), []core.TransactionData{tx})
		assert.Error(t, err)
	})
}

func TestAccountMetadata(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {

		err := l.SaveMeta(context.Background(), core.MetaTargetTypeAccount, "users:001", core.Metadata{
			"a random metadata": "old value",
		})
		assert.NoError(t, err)

		err = l.SaveMeta(context.Background(), core.MetaTargetTypeAccount, "users:001", core.Metadata{
			"a random metadata": "new value",
		})
		assert.NoError(t, err)

		{
			acc, err := l.GetAccount(context.Background(), "users:001")
			require.NoError(t, err)

			meta, ok := acc.Metadata["a random metadata"]
			require.True(t, ok)

			assert.Equalf(t, meta, "new value",
				"metadata entry did not match in get: expected \"new value\", got %v", meta)
		}

		{
			// We have to create at least one transaction to retrieve an account from GetAccounts store method
			_, err := l.Commit(context.Background(), []core.TransactionData{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Amount:      core.NewMonetaryInt(100),
							Asset:       "USD",
							Destination: "users:001",
						},
					},
				},
			})
			assert.NoError(t, err)

			acc, err := l.GetAccount(context.Background(), "users:001")
			assert.NoError(t, err)
			require.True(t, acc.Address == "users:001", "no account returned by get account")

			meta, ok := acc.Metadata["a random metadata"]
			assert.True(t, ok)
			assert.Equalf(t, meta, "new value",
				"metadata entry did not match in find: expected \"new value\", got %v", meta)
		}
	})
}

func TestTransactionMetadata(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		_, err := l.Commit(context.Background(), []core.TransactionData{{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "payments:001",
					Amount:      core.NewMonetaryInt(100),
					Asset:       "COIN",
				},
			},
		}})
		require.NoError(t, err)

		tx, err := l.GetLedgerStore().GetLastTransaction(context.Background())
		require.NoError(t, err)

		err = l.SaveMeta(context.Background(), core.MetaTargetTypeTransaction, tx.ID, core.Metadata{
			"a random metadata": "old value",
		})
		require.NoError(t, err)

		err = l.SaveMeta(context.Background(), core.MetaTargetTypeTransaction, tx.ID, core.Metadata{
			"a random metadata": "new value",
		})
		require.NoError(t, err)

		tx, err = l.GetLedgerStore().GetLastTransaction(context.Background())
		require.NoError(t, err)

		meta, ok := tx.Metadata["a random metadata"]
		require.True(t, ok)

		require.Equalf(t, meta, "new value",
			"metadata entry did not match: expected \"new value\", got %v", meta)
	})
}

func TestSaveTransactionMetadata(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		_, err := l.Commit(context.Background(), []core.TransactionData{{
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
		}})
		require.NoError(t, err)

		tx, err := l.GetLedgerStore().GetLastTransaction(context.Background())
		require.NoError(t, err)

		meta, ok := tx.Metadata["a metadata"]
		require.True(t, ok)

		assert.Equalf(t, meta, "a value",
			"metadata entry did not match: expected \"a value\", got %v", meta)
	})
}

func TestGetTransaction(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		_, err := l.Commit(context.Background(), []core.TransactionData{{
			Reference: "bar",
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "payments:001",
					Amount:      core.NewMonetaryInt(100),
					Asset:       "COIN",
				},
			},
		}})
		require.NoError(t, err)

		last, err := l.GetLedgerStore().GetLastTransaction(context.Background())
		require.NoError(t, err)

		tx, err := l.GetTransaction(context.Background(), last.ID)
		require.NoError(t, err)

		assert.True(t, reflect.DeepEqual(tx, last))
	})
}

func TestGetTransactions(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
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

		_, err := l.Commit(context.Background(), []core.TransactionData{tx})
		require.NoError(t, err)

		res, err := l.GetTransactions(context.Background(), *ledger.NewTransactionsQuery())
		require.NoError(t, err)

		assert.Equal(t, "test_get_transactions", res.Data[0].Postings[0].Destination)
	})
}

func TestRevertTransaction(t *testing.T) {
	runOnLedger(func(l *ledger.Ledger) {
		revertAmt := core.NewMonetaryInt(100)

		res, err := l.Commit(context.Background(), []core.TransactionData{{
			Reference: "foo",
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "payments:001",
					Amount:      revertAmt,
					Asset:       "COIN",
				},
			},
		}})
		require.NoError(t, err)

		world, err := l.GetAccount(context.Background(), "world")
		require.NoError(t, err)

		originalBal := world.Balances["COIN"]

		revertTx, err := l.RevertTransaction(context.Background(), res.GeneratedTransactions[0].ID)
		require.NoError(t, err)

		require.Equal(t, core.Postings{
			{
				Source:      "payments:001",
				Destination: "world",
				Amount:      core.NewMonetaryInt(100),
				Asset:       "COIN",
			},
		}, revertTx.TransactionData.Postings)

		require.EqualValues(t, fmt.Sprintf("%d", res.GeneratedTransactions[0].ID),
			revertTx.Metadata[core.RevertMetadataSpecKey()])

		tx, err := l.GetTransaction(context.Background(), res.GeneratedTransactions[0].ID)
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
	runOnLedger(func(l *Ledger) {

		amount, err := core.ParseMonetaryInt("199999999999999999992919191919192929292939847477171818284637291884661818183647392936472918836161728274766266161728493736383838")
		require.NoError(t, err)

		tx, err := l.Commit(context.Background(), []core.TransactionData{core.TransactionData{
			Postings: []core.Posting{{
				Source:      "world",
				Destination: "bank",
				Asset:       "ETH/18",
				Amount:      amount,
			}},
		}})
		require.NoError(t, err)

		txFromDB, err := l.GetTransaction(context.Background(), tx.GeneratedTransactions[0].ID)
		require.NoError(t, err)
		require.Equal(t, txFromDB.Postings[0].Amount, amount)
	})
}

func BenchmarkTransaction1(b *testing.B) {
	runOnLedger(func(l *ledger.Ledger) {
		for n := 0; n < b.N; n++ {
			txs := []core.TransactionData{}

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

			_, err := l.Commit(context.Background(), txs)
			require.NoError(b, err)
		}
	})
}

func BenchmarkTransaction_20_1k(b *testing.B) {
	runOnLedger(func(l *ledger.Ledger) {
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

				_, err := l.Commit(context.Background(), txs)
				require.NoError(b, err)
			}
		}
	})
}

func BenchmarkGetAccount(b *testing.B) {
	runOnLedger(func(l *ledger.Ledger) {
		for i := 0; i < b.N; i++ {
			_, err := l.GetAccount(context.Background(), "users:013")
			require.NoError(b, err)
		}
	})
}

func BenchmarkGetTransactions(b *testing.B) {
	runOnLedger(func(l *ledger.Ledger) {
		for i := 0; i < b.N; i++ {
			_, err := l.GetTransactions(context.Background(), ledger.TransactionsQuery{})
			require.NoError(b, err)
		}
	})
}
