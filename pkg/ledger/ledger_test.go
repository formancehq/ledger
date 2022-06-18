package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/numary/ledger/pkg/core"
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
		ledgertesting.ProvideStorageDriver(),
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

func runOnLedger(f func(l *Ledger)) {
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
				l, err := NewLedger(name, store, NewInMemoryLocker(), &noOpMonitor{})
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
	runOnLedger(func(l *Ledger) {
		testsize := 1e4
		total := 0
		batch := []core.TransactionData{}

		for i := 1; i <= int(testsize); i++ {
			user := fmt.Sprintf("users:%03d", 1+rand.Intn(100))
			amount := 100
			total += amount

			batch = append(batch, core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "mint",
						Asset:       "GEM",
						Amount:      int64(amount),
					},
					{
						Source:      "mint",
						Destination: user,
						Asset:       "GEM",
						Amount:      int64(amount),
					},
				},
			})

			if i%int(1e3) != 0 {
				continue
			}

			_, _, err := l.Commit(context.Background(), batch)
			require.NoError(t, err)

			batch = []core.TransactionData{}
		}

		world, err := l.GetAccount(context.Background(), "world")
		require.NoError(t, err)

		expected := int64(-1 * total)
		b := world.Balances["GEM"]
		assert.Equalf(t, expected, b,
			"wrong GEM balance for account world, expected: %d got: %d",
			expected, b)

		require.NoError(t, l.Close(context.Background()))
	})
}

func TestTransactionBatchWithIntermediateWrongState(t *testing.T) {
	runOnLedger(func(l *Ledger) {
		batch := []core.TransactionData{
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player2",
						Asset:       "GEM",
						Amount:      int64(100),
					},
				},
			},
			{
				Postings: []core.Posting{
					{
						Source:      "player",
						Destination: "game",
						Asset:       "GEM",
						Amount:      int64(100),
					},
				},
			},
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player",
						Asset:       "GEM",
						Amount:      int64(100),
					},
				},
			},
		}

		_, _, err := l.Commit(context.Background(), batch)
		assert.Error(t, err)
		assert.IsType(t, new(TransactionCommitError), err)
		assert.IsType(t, new(InsufficientFundError), errors.Unwrap(err))
	})
}

func TestTransactionBatchWithConflictingReference(t *testing.T) {
	runOnLedger(func(l *Ledger) {
		batch := []core.TransactionData{
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player",
						Asset:       "GEM",
						Amount:      int64(100),
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
						Amount:      int64(100),
					},
				},
				Reference: "ref2",
			},
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player",
						Asset:       "GEM",
						Amount:      int64(100),
					},
				},
				Reference: "ref1",
			},
		}

		_, _, err := l.Commit(context.Background(), batch)
		assert.Error(t, err)
		assert.IsType(t, new(ConflictError), err)
	})
}

func TestTransactionExpectedVolumes(t *testing.T) {
	runOnLedger(func(l *Ledger) {
		batch := []core.TransactionData{
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player",
						Asset:       "USD",
						Amount:      int64(100),
					},
				},
			},
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player",
						Asset:       "EUR",
						Amount:      int64(100),
					},
				},
			},
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player2",
						Asset:       "EUR",
						Amount:      int64(100),
					},
				},
			},
			{
				Postings: []core.Posting{
					{
						Source:      "player",
						Destination: "player2",
						Asset:       "EUR",
						Amount:      int64(50),
					},
				},
			},
		}

		volumes, _, err := l.Commit(context.Background(), batch)
		assert.NoError(t, err)

		assert.EqualValues(t, volumes, core.AggregatedVolumes{
			"world": core.Volumes{
				"USD": {
					Output: 100,
				},
				"EUR": {
					Output: 200,
				},
			},
			"player": core.Volumes{
				"USD": {
					Input: 100,
				},
				"EUR": {
					Input:  100,
					Output: 50,
				},
			},
			"player2": core.Volumes{
				"EUR": {
					Input: 150,
				},
			},
		})
	})
}

func TestBalance(t *testing.T) {
	runOnLedger(func(l *Ledger) {
		_, _, err := l.Commit(context.Background(), []core.TransactionData{
			{
				Postings: []core.Posting{
					{
						Source:      "empty_wallet",
						Destination: "world",
						Amount:      1,
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
	runOnLedger(func(l *Ledger) {
		tx := core.TransactionData{
			Reference: "payment_processor_id_01",
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "payments:001",
					Amount:      100,
					Asset:       "COIN",
				},
			},
		}

		_, _, err := l.Commit(context.Background(), []core.TransactionData{tx})
		require.NoError(t, err)

		_, _, err = l.Commit(context.Background(), []core.TransactionData{tx})
		assert.Error(t, err)
	})
}

func TestAccountMetadata(t *testing.T) {
	runOnLedger(func(l *Ledger) {

		err := l.SaveMeta(context.Background(), core.MetaTargetTypeAccount, "users:001", core.Metadata{
			"a random metadata": json.RawMessage(`"old value"`),
		})
		assert.NoError(t, err)

		err = l.SaveMeta(context.Background(), core.MetaTargetTypeAccount, "users:001", core.Metadata{
			"a random metadata": json.RawMessage(`"new value"`),
		})
		assert.NoError(t, err)

		{
			acc, err := l.GetAccount(context.Background(), "users:001")
			require.NoError(t, err)

			meta, ok := acc.Metadata["a random metadata"]
			require.True(t, ok)

			var value string
			require.NoError(t, json.Unmarshal(meta, &value))
			assert.Equalf(t, value, "new value",
				"metadata entry did not match in get: expected \"new value\", got %v", value)
		}

		{
			// We have to create at least one transaction to retrieve an account from GetAccounts store method
			_, _, err := l.Commit(context.Background(), []core.TransactionData{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Amount:      100,
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
			var value string
			require.NoError(t, json.Unmarshal(meta, &value))
			assert.Equalf(t, value, "new value",
				"metadata entry did not match in find: expected \"new value\", got %v", value)
		}
	})
}

func TestTransactionMetadata(t *testing.T) {
	runOnLedger(func(l *Ledger) {
		_, _, err := l.Commit(context.Background(), []core.TransactionData{{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "payments:001",
					Amount:      100,
					Asset:       "COIN",
				},
			},
		}})
		require.NoError(t, err)

		tx, err := l.store.GetLastTransaction(context.Background())
		require.NoError(t, err)

		err = l.SaveMeta(context.Background(), core.MetaTargetTypeTransaction, tx.ID, core.Metadata{
			"a random metadata": json.RawMessage(`"old value"`),
		})
		require.NoError(t, err)

		err = l.SaveMeta(context.Background(), core.MetaTargetTypeTransaction, tx.ID, core.Metadata{
			"a random metadata": json.RawMessage(`"new value"`),
		})
		require.NoError(t, err)

		tx, err = l.store.GetLastTransaction(context.Background())
		require.NoError(t, err)

		meta, ok := tx.Metadata["a random metadata"]
		assert.True(t, ok)

		var value string
		assert.NoError(t, json.Unmarshal(meta, &value))
		assert.Equalf(t, value, "new value",
			"metadata entry did not match: expected \"new value\", got %v", value)
	})
}

func TestSaveTransactionMetadata(t *testing.T) {
	runOnLedger(func(l *Ledger) {
		_, _, err := l.Commit(context.Background(), []core.TransactionData{{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "payments:001",
					Amount:      100,
					Asset:       "COIN",
				},
			},
			Metadata: core.Metadata{
				"a metadata": json.RawMessage(`"a value"`),
			},
		}})
		require.NoError(t, err)

		tx, err := l.store.GetLastTransaction(context.Background())
		require.NoError(t, err)

		meta, ok := tx.Metadata["a metadata"]
		require.True(t, ok)

		var value string
		require.NoError(t, json.Unmarshal(meta, &value))

		assert.Equalf(t, value, "a value",
			"metadata entry did not match: expected \"a value\", got %v", value)
	})
}

func TestGetTransaction(t *testing.T) {
	runOnLedger(func(l *Ledger) {
		_, _, err := l.Commit(context.Background(), []core.TransactionData{{
			Reference: "bar",
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "payments:001",
					Amount:      100,
					Asset:       "COIN",
				},
			},
		}})
		require.NoError(t, err)

		last, err := l.store.GetLastTransaction(context.Background())
		require.NoError(t, err)

		tx, err := l.GetTransaction(context.Background(), last.ID)
		require.NoError(t, err)

		assert.True(t, reflect.DeepEqual(tx, *last))
	})
}

func TestGetTransactions(t *testing.T) {
	runOnLedger(func(l *Ledger) {
		tx := core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "test_get_transactions",
					Amount:      100,
					Asset:       "COIN",
				},
			},
		}

		_, _, err := l.Commit(context.Background(), []core.TransactionData{tx})
		require.NoError(t, err)

		res, err := l.GetTransactions(context.Background())
		require.NoError(t, err)

		assert.Equal(t, "test_get_transactions", res.Data[0].Postings[0].Destination)
	})
}

func TestRevertTransaction(t *testing.T) {
	runOnLedger(func(l *Ledger) {
		revertAmt := int64(100)

		_, txs, err := l.Commit(context.Background(), []core.TransactionData{{
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

		revertTx, err := l.RevertTransaction(context.Background(), txs[0].ID)
		require.NoError(t, err)

		assert.Equal(t, core.Postings{
			{
				Source:      "payments:001",
				Destination: "world",
				Amount:      100,
				Asset:       "COIN",
			},
		}, revertTx.TransactionData.Postings)

		assert.EqualValues(t, fmt.Sprintf(`"%d"`, txs[0].ID),
			string(revertTx.Metadata[core.RevertMetadataSpecKey()]))

		tx, err := l.GetTransaction(context.Background(), txs[0].ID)
		assert.NoError(t, err)

		v := core.RevertedMetadataSpecValue{}
		assert.NoError(t, json.Unmarshal(tx.Metadata[core.RevertedMetadataSpecKey()], &v))

		assert.Equal(t, core.RevertedMetadataSpecValue{
			By: fmt.Sprint(revertTx.ID),
		}, v)

		world, err = l.GetAccount(context.Background(), "world")
		assert.NoError(t, err)

		newBal := world.Balances["COIN"]
		expectedBal := originalBal + revertAmt
		assert.Equalf(t, expectedBal, newBal,
			"COIN world balances expected %d, got %d", expectedBal, newBal)
	})
}

func BenchmarkTransaction1(b *testing.B) {
	runOnLedger(func(l *Ledger) {
		for n := 0; n < b.N; n++ {
			txs := []core.TransactionData{}

			txs = append(txs, core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "benchmark",
						Asset:       "COIN",
						Amount:      10,
					},
				},
			})

			_, _, err := l.Commit(context.Background(), txs)
			require.NoError(b, err)
		}
	})
}

func BenchmarkTransaction_20_1k(b *testing.B) {
	runOnLedger(func(l *Ledger) {
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
								Amount:      10,
							},
						},
					})
				}

				_, _, err := l.Commit(context.Background(), txs)
				require.NoError(b, err)
			}
		}
	})
}

func BenchmarkGetAccount(b *testing.B) {
	runOnLedger(func(l *Ledger) {
		for i := 0; i < b.N; i++ {
			_, err := l.GetAccount(context.Background(), "users:013")
			require.NoError(b, err)
		}
	})
}

func BenchmarkGetTransactions(b *testing.B) {
	runOnLedger(func(l *Ledger) {
		for i := 0; i < b.N; i++ {
			_, err := l.GetTransactions(context.Background())
			require.NoError(b, err)
		}
	})
}

func TestLedger_processTx(t *testing.T) {
	runOnLedger(func(l *Ledger) {
		t.Run("multi assets", func(t *testing.T) {
			var (
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

			expectedPreCommitVol := core.AggregatedVolumes{
				"alice": core.Volumes{
					"USD": {},
					"EUR": {},
				},
				"toto": core.Volumes{
					"USD": {},
					"EUR": {},
				},
				"world": core.Volumes{
					"USD": {},
					"EUR": {},
				},
			}

			expectedPostCommitVol := core.AggregatedVolumes{
				"alice": core.Volumes{
					"USD": {
						Input:  worldAliceUSD,
						Output: aliceTotoUSD,
					},
					"EUR": {
						Input: worldAliceEUR + totoAliceEUR,
					},
				},
				"toto": core.Volumes{
					"USD": {
						Input: worldTotoUSD + aliceTotoUSD,
					},
					"EUR": {
						Input:  worldTotoEUR,
						Output: totoAliceEUR,
					},
				},
				"world": core.Volumes{
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
						PreCommitVolumes: core.AggregatedVolumes{
							"toto":  core.Volumes{"USD": core.Volume{Input: 0, Output: 0}},
							"world": core.Volumes{"USD": core.Volume{Input: 0, Output: 0}}},
						PostCommitVolumes: core.AggregatedVolumes{
							"toto":  core.Volumes{"USD": core.Volume{Input: worldTotoUSD, Output: 0}},
							"world": core.Volumes{"USD": core.Volume{Input: 0, Output: worldTotoUSD}}},
					},
					{
						TransactionData: core.TransactionData{Postings: core.Postings{postings[1]}},
						ID:              1,
						Timestamp:       time.Now().UTC().Format(time.RFC3339),
						PreCommitVolumes: core.AggregatedVolumes{
							"world": core.Volumes{"USD": core.Volume{Input: 0, Output: worldTotoUSD}},
							"alice": core.Volumes{"USD": core.Volume{Input: 0, Output: 0}},
						},
						PostCommitVolumes: core.AggregatedVolumes{
							"world": core.Volumes{"USD": core.Volume{Input: 0, Output: worldTotoUSD + worldAliceUSD}},
							"alice": core.Volumes{"USD": core.Volume{Input: worldAliceUSD, Output: 0}},
						},
					},
					{
						TransactionData: core.TransactionData{Postings: core.Postings{postings[2]}},
						ID:              2,
						Timestamp:       time.Now().UTC().Format(time.RFC3339),
						PreCommitVolumes: core.AggregatedVolumes{
							"alice": core.Volumes{"USD": core.Volume{Input: worldAliceUSD, Output: 0}},
							"toto":  core.Volumes{"USD": core.Volume{Input: worldTotoUSD, Output: 0}},
						},
						PostCommitVolumes: core.AggregatedVolumes{
							"alice": core.Volumes{"USD": core.Volume{Input: worldAliceUSD, Output: aliceTotoUSD}},
							"toto":  core.Volumes{"USD": core.Volume{Input: worldTotoUSD + aliceTotoUSD, Output: 0}},
						},
					},
					{
						TransactionData: core.TransactionData{Postings: core.Postings{postings[3]}},
						ID:              3,
						Timestamp:       time.Now().UTC().Format(time.RFC3339),
						PreCommitVolumes: core.AggregatedVolumes{
							"world": core.Volumes{"EUR": core.Volume{Input: 0, Output: 0}},
							"toto":  core.Volumes{"EUR": core.Volume{Input: 0, Output: 0}},
						},
						PostCommitVolumes: core.AggregatedVolumes{
							"world": core.Volumes{"EUR": core.Volume{Input: 0, Output: worldTotoEUR}},
							"toto":  core.Volumes{"EUR": core.Volume{Input: worldTotoEUR, Output: 0}},
						},
					},
					{
						TransactionData: core.TransactionData{Postings: core.Postings{postings[4]}},
						ID:              4,
						Timestamp:       time.Now().UTC().Format(time.RFC3339),
						PreCommitVolumes: core.AggregatedVolumes{
							"world": core.Volumes{"EUR": core.Volume{Input: 0, Output: worldTotoEUR}},
							"alice": core.Volumes{"EUR": core.Volume{Input: 0, Output: 0}},
						},
						PostCommitVolumes: core.AggregatedVolumes{
							"world": core.Volumes{"EUR": core.Volume{Input: 0, Output: worldTotoEUR + worldAliceEUR}},
							"alice": core.Volumes{"EUR": core.Volume{Input: worldAliceEUR, Output: 0}},
						},
					},
					{
						TransactionData: core.TransactionData{Postings: core.Postings{postings[5]}},
						ID:              5,
						Timestamp:       time.Now().UTC().Format(time.RFC3339),
						PreCommitVolumes: core.AggregatedVolumes{
							"toto":  core.Volumes{"EUR": core.Volume{Input: worldTotoEUR, Output: 0}},
							"alice": core.Volumes{"EUR": core.Volume{Input: worldAliceEUR, Output: 0}},
						},
						PostCommitVolumes: core.AggregatedVolumes{
							"toto":  core.Volumes{"EUR": core.Volume{Input: worldTotoEUR, Output: totoAliceEUR}},
							"alice": core.Volumes{"EUR": core.Volume{Input: worldAliceEUR + totoAliceEUR, Output: 0}},
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
			assert.Equal(t, &CommitResult{
				PreCommitVolumes:      core.AggregatedVolumes{},
				PostCommitVolumes:     core.AggregatedVolumes{},
				GeneratedTransactions: []core.Transaction{},
				GeneratedLogs:         []core.Log{},
			}, result)
		})
	})
}
