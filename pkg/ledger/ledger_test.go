package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
	"go.uber.org/fx"
)

var driver storage.Driver

func with(f func(l *Ledger)) {
	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() storage.Driver {
			return driver
		}),
		fx.Invoke(func(d storage.Driver) error {
			return d.Initialize(context.Background())
		}),
		fx.Provide(storage.NewDefaultFactory),
		fx.Provide(
			func(storageFactory storage.Factory) (*Ledger, error) {
				store, err := storageFactory.GetStore("test")
				if err != nil {
					return nil, err
				}
				err = store.Initialize(context.Background())
				if err != nil {
					return nil, err
				}
				l, err := NewLedger("test", store, NewInMemoryLocker())
				if err != nil {
					panic(err)
				}
				return l, nil
			},
		),
		fx.Invoke(f),
		fx.Invoke(func(l *Ledger) {
			err := l.Close(context.Background())
			if err != nil {
				logrus.Error(err)
			}
		}),
		// Closing the driver after each test cause a test to fail
		// Tests seems not independent
		//fx.Invoke(func(d storage.Driver) error {
		//	return d.Close(context.Background())
		//}),
	)
	if app.Err() != nil {
		panic(app.Err())
	}
}

func TestMain(m *testing.M) {

	var (
		code int
	)
	defer func() {
		os.Exit(code) // os.Exit don't care about defer so defer the os.Exit allow us to execute other defer
	}()

	flag.Parse()
	if testing.Verbose() {
		logrus.StandardLogger().Level = logrus.DebugLevel
	}

	switch os.Getenv("NUMARY_STORAGE_DRIVER") {
	case "sqlite", "":
		driver = sqlstorage.NewInMemorySQLiteDriver()
	case "postgres":
		pgServer, err := ledgertesting.PostgresServer()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer pgServer.Close()

		driver = sqlstorage.NewOpenCloseDBDriver(
			"postgres",
			sqlstorage.PostgreSQL,
			func(name string) string {
				return pgServer.ConnString()
			},
		)
	}

	code = m.Run()
}

func TestTransaction(t *testing.T) {
	with(func(l *Ledger) {
		testsize := 1e4
		total := 0
		batch := []core.Transaction{}

		for i := 1; i <= int(testsize); i++ {
			user := fmt.Sprintf("users:%03d", 1+rand.Intn(100))
			amount := 100
			total += amount

			batch = append(batch, core.Transaction{
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

			logrus.Debugln(i)

			_, _, err := l.Commit(context.Background(), batch)

			if err != nil {
				t.Error(err)
			}

			batch = []core.Transaction{}
		}

		world, err := l.GetAccount(context.Background(), "world")

		if err != nil {
			t.Error(err)
		}

		expected := int64(-1 * total)
		if b := world.Balances["GEM"]; b != expected {
			t.Error(fmt.Sprintf(
				"wrong GEM balance for account world, expected: %d got: %d",
				expected,
				b,
			))
		}

		l.Close(context.Background())
	})
}

func TestTransactionWithIntermediateWrongState(t *testing.T) {
	with(func(l *Ledger) {
		batch := []core.Transaction{
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
		assert.IsType(t, new(CommitError), err)
		assert.Equal(t, 1, err.(*CommitError).TXIndex)
		assert.IsType(t, new(InsufficientFundError), err.(*CommitError).Err)

		l.Close(context.Background())
	})
}

func TestTransactionExpectedBalances(t *testing.T) {
	with(func(l *Ledger) {
		batch := []core.Transaction{
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player",
						Asset:       "TOK1",
						Amount:      int64(100),
					},
				},
			},
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player",
						Asset:       "TOK2",
						Amount:      int64(100),
					},
				},
			},
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player2",
						Asset:       "TOK2",
						Amount:      int64(100),
					},
				},
			},
			{
				Postings: []core.Posting{
					{
						Source:      "player",
						Destination: "player2",
						Asset:       "TOK2",
						Amount:      int64(50),
					},
				},
			},
		}

		balances, _, err := l.Commit(context.Background(), batch)
		assert.NoError(t, err)

		assert.EqualValues(t, balances, Balances{
			"player": map[string]int64{
				"TOK1": 100,
				"TOK2": 50,
			},
			"player2": map[string]int64{
				"TOK2": 150,
			},
		})

	})
}

func TestBalance(t *testing.T) {
	with(func(l *Ledger) {
		_, _, err := l.Commit(context.Background(), []core.Transaction{
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

		if err == nil {
			t.Error(errors.New(
				"balance was insufficient yet the transation was commited",
			))
		}
	})
}

func TestReference(t *testing.T) {
	with(func(l *Ledger) {
		tx := core.Transaction{
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

		_, _, err := l.Commit(context.Background(), []core.Transaction{tx})

		if err != nil {
			t.Error(err)
		}

		_, _, err = l.Commit(context.Background(), []core.Transaction{tx})

		if err == nil {
			t.Fail()
		}
	})
}

func TestLast(t *testing.T) {
	with(func(l *Ledger) {
		_, err := l.GetLastTransaction(context.Background())

		if err != nil {
			t.Error(err)
		}
	})
}

func TestAccountMetadata(t *testing.T) {
	with(func(l *Ledger) {
		err := l.SaveMeta(context.Background(), "account", "users:001", core.Metadata{
			"a random metadata": json.RawMessage(`"old value"`),
		})
		assert.NoError(t, err)

		err = l.SaveMeta(context.Background(), "account", "users:001", core.Metadata{
			"a random metadata": json.RawMessage(`"new value"`),
		})
		assert.NoError(t, err)

		{
			acc, err := l.GetAccount(context.Background(), "users:001")
			if err != nil {
				t.Fatal(err)
			}

			if meta, ok := acc.Metadata["a random metadata"]; ok {
				var value string
				err := json.Unmarshal(meta, &value)
				if err != nil {
					t.Fatal(err)
				}
				if value != "new value" {
					t.Fatalf("metadata entry did not match in get: expected \"new value\", got %v", value)
				}
			}
		}

		{
			cursor, err := l.FindAccounts(context.Background(), query.Account("users:001"))
			if err != nil {
				t.Fatal(err)
			}

			accounts, ok := cursor.Data.([]core.Account)
			if !ok {
				t.Fatalf("wrong cursor type: %v", reflect.TypeOf(cursor.Data))
			}
			if len(accounts) == 0 {
				t.Fatal("no accounts returned by find")
			}

			if meta, ok := accounts[0].Metadata["a random metadata"]; ok {
				var value string
				err := json.Unmarshal(meta, &value)
				if err != nil {
					t.Fatal(err)
				}
				if value != "new value" {
					t.Fatalf("metadata entry did not match in find: expected \"new value\", got %v", value)
				}
			}
		}
	})
}

func TestTransactionMetadata(t *testing.T) {
	with(func(l *Ledger) {
		l.Commit(context.Background(), []core.Transaction{{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "payments:001",
					Amount:      100,
					Asset:       "COIN",
				},
			},
		}})

		tx, err := l.GetLastTransaction(context.Background())
		if err != nil {
			t.Error(err)
		}

		l.SaveMeta(context.Background(), "transaction", fmt.Sprintf("%d", tx.ID), core.Metadata{
			"a random metadata": json.RawMessage(`"old value"`),
		})
		l.SaveMeta(context.Background(), "transaction", fmt.Sprintf("%d", tx.ID), core.Metadata{
			"a random metadata": json.RawMessage(`"new value"`),
		})

		tx, err = l.GetLastTransaction(context.Background())
		if err != nil {
			t.Error(err)
		}

		if meta, ok := tx.Metadata["a random metadata"]; ok {
			var value string
			err := json.Unmarshal(meta, &value)
			if err != nil {
				t.Fatal(err)
			}
			if value != "new value" {
				t.Fatalf("metadata entry did not match: expected \"new value\", got %v", value)
			}
		}
	})
}

func TestSaveTransactionMetadata(t *testing.T) {
	with(func(l *Ledger) {

		l.Commit(context.Background(), []core.Transaction{{
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

		tx, err := l.GetLastTransaction(context.Background())
		if err != nil {
			t.Error(err)
		}

		if meta, ok := tx.Metadata["a metadata"]; ok {
			var value string
			err := json.Unmarshal(meta, &value)
			if err != nil {
				t.Fatal(err)
			}
			if value != "a value" {
				t.Fatalf("metadata entry did not match: expected \"a value\", got %v", value)
			}
		}
	})
}

func TestGetTransaction(t *testing.T) {
	with(func(l *Ledger) {
		l.Commit(context.Background(), []core.Transaction{{
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

		last, err := l.GetLastTransaction(context.Background())
		if err != nil {
			t.Error(err)
		}

		tx, err := l.GetTransaction(context.Background(), fmt.Sprint(last.ID))
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(tx, last) {
			t.Fail()
		}
	})
}

func TestFindTransactions(t *testing.T) {
	with(func(l *Ledger) {
		tx := core.Transaction{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "test_find_transactions",
					Amount:      100,
					Asset:       "COIN",
				},
			},
		}

		l.Commit(context.Background(), []core.Transaction{tx})

		res, err := l.FindTransactions(context.Background())

		if err != nil {
			t.Error(err)
		}

		txs := res.Data.([]core.Transaction)

		if txs[0].Postings[0].Destination != "test_find_transactions" {
			t.Error()
		}
	})
}

func TestRevertTransaction(t *testing.T) {
	with(func(l *Ledger) {
		revertAmt := int64(100)

		_, txs, err := l.Commit(context.Background(), []core.Transaction{{
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

		if err != nil {
			t.Fatal(err)
		}

		world, err := l.GetAccount(context.Background(), "world")
		if err != nil {
			t.Fatal(err)
		}
		originalBal := world.Balances["COIN"]

		err = l.RevertTransaction(context.Background(), fmt.Sprint(txs[0].ID))
		if err != nil {
			t.Fatal(err)
		}

		revertTx, err := l.GetLastTransaction(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		expectedPosting := core.Posting{
			Source:      "payments:001",
			Destination: "world",
			Amount:      100,
			Asset:       "COIN",
		}

		if diff := cmp.Diff(revertTx.Postings[0], expectedPosting); diff != "" {
			t.Errorf("RevertTransaction() reverted posting mismatch (-want +got):\n%s", diff)
		}

		world, err = l.GetAccount(context.Background(), "world")
		if err != nil {
			t.Fatal(err)
		}

		newBal := world.Balances["COIN"]
		expectedBal := originalBal + revertAmt
		if newBal != expectedBal {
			t.Fatalf("COIN world balances expected %d, got %d", expectedBal, newBal)
		}
	})
}

func BenchmarkTransaction1(b *testing.B) {
	with(func(l *Ledger) {
		for n := 0; n < b.N; n++ {
			txs := []core.Transaction{}

			txs = append(txs, core.Transaction{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "benchmark",
						Asset:       "COIN",
						Amount:      10,
					},
				},
			})

			l.Commit(context.Background(), txs)
		}
	})
}

func BenchmarkTransaction_20_1k(b *testing.B) {
	with(func(l *Ledger) {
		for n := 0; n < b.N; n++ {
			for i := 0; i < 20; i++ {
				txs := []core.Transaction{}

				for j := 0; j < 1e3; j++ {
					txs = append(txs, core.Transaction{
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

				l.Commit(context.Background(), txs)
			}
		}
	})
}

func BenchmarkGetAccount(b *testing.B) {
	with(func(l *Ledger) {
		for i := 0; i < b.N; i++ {
			l.GetAccount(context.Background(), "users:013")
		}
	})
}

func BenchmarkFindTransactions(b *testing.B) {
	with(func(l *Ledger) {
		for i := 0; i < b.N; i++ {
			l.FindTransactions(context.Background())
		}
	})
}
