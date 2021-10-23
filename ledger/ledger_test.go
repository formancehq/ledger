package ledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/numary/ledger/config"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger/query"
	"github.com/numary/ledger/storage/postgres"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

func with(f func(l *Ledger)) {
	fx.New(
		fx.Option(
			fx.NopLogger,
		),
		fx.Provide(
			func(lc fx.Lifecycle) (*Ledger, error) {
				l, err := NewLedger("test", lc)

				if err != nil {
					panic(err)
				}

				return l, nil
			},
		),
		fx.Invoke(f),
		fx.Invoke(func(l *Ledger) {
			l.Close()
		}),
	)
}

func TestMain(m *testing.M) {
	config.Init()

	viper.Set("storage.dir", os.TempDir())
	switch viper.GetString("storage.driver") {
	case "sqlite":
		viper.Set("storage.sqlite.db_name", "ledger")
		os.Remove(path.Join(os.TempDir(), "ledger_test.db"))
	case "postgres":
		store, err := postgres.NewStore("test")
		if err != nil {
			panic(err)
		}
		store.DropTest()
	}
	fmt.Println(viper.AllSettings())

	m.Run()
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

			fmt.Println(i)

			err := l.Commit(batch)

			if err != nil {
				t.Error(err)
			}

			batch = []core.Transaction{}
		}

		world, err := l.GetAccount("world")

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

		l.Close()
	})
}

func TestBalance(t *testing.T) {
	with(func(l *Ledger) {
		err := l.Commit([]core.Transaction{
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

		err := l.Commit([]core.Transaction{tx})

		if err != nil {
			t.Error(err)
		}

		err = l.Commit([]core.Transaction{tx})

		if err == nil {
			t.Fail()
		}
	})
}

func TestLast(t *testing.T) {
	with(func(l *Ledger) {
		_, err := l.GetLastTransaction()

		if err != nil {
			t.Error(err)
		}
	})
}

func TestAccountMetadata(t *testing.T) {
	with(func(l *Ledger) {
		l.SaveMeta("account", "users:001", core.Metadata{
			"a random metadata": json.RawMessage(`"old value"`),
		})
		l.SaveMeta("account", "users:001", core.Metadata{
			"a random metadata": json.RawMessage(`"new value"`),
		})

		{
			acc, err := l.GetAccount("users:001")
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
			cursor, err := l.FindAccounts(query.Account("users:001"))

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

		l.Commit([]core.Transaction{{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "payments:001",
					Amount:      100,
					Asset:       "COIN",
				},
			},
		}})

		tx, err := l.GetLastTransaction()
		if err != nil {
			t.Error(err)
		}

		l.SaveMeta("transaction", fmt.Sprintf("%d", tx.ID), core.Metadata{
			"a random metadata": json.RawMessage(`"old value"`),
		})
		l.SaveMeta("transaction", fmt.Sprintf("%d", tx.ID), core.Metadata{
			"a random metadata": json.RawMessage(`"new value"`),
		})

		tx, err = l.GetLastTransaction()
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

		l.Commit([]core.Transaction{{
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

		tx, err := l.GetLastTransaction()
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

func (l *Ledger) TestGetTransaction(t *testing.T) {
	with(func(l *Ledger) {
		l.Commit([]core.Transaction{{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "payments:001",
					Amount:      100,
					Asset:       "COIN",
				},
			},
		}})

		last, err := l.GetLastTransaction()
		if err != nil {
			t.Error(err)
		}

		tx, err := l.GetTransaction(fmt.Sprint(last.ID))
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(tx, last) {
			t.Fail()
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

			l.Commit(txs)
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

				l.Commit(txs)
			}
		}
	})
}

func BenchmarkGetAccount(b *testing.B) {
	with(func(l *Ledger) {
		for i := 0; i < b.N; i++ {
			l.GetAccount("users:013")
		}
	})
}

func BenchmarkFindTransactions(b *testing.B) {
	with(func(l *Ledger) {
		for i := 0; i < b.N; i++ {
			l.FindTransactions()
		}
	})
}
