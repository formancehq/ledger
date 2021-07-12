package ledger

import (
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"

	"github.com/numary/ledger/core"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

var driver = flag.String("storage-driver", "sqlite", "name of the storage driver to use")

func with(f func(l *Ledger)) {
	fx.New(
		fx.Option(
			fx.NopLogger,
		),
		fx.Provide(
			func(lc fx.Lifecycle) *Ledger {
				l, _ := NewLedger("test", lc)
				return l
			},
		),
		fx.Invoke(func() {
			viper.Set("storage.driver", *driver)
			viper.Set("storage.dir", os.TempDir())
			viper.Set("storage.sqlite.db_name", "ledger")
		}),
		fx.Invoke(f),
		fx.Invoke(func(l *Ledger) {
			l.Close()
		}),
	)
}

func TestMain(m *testing.M) {
	os.Remove(path.Join(os.TempDir(), "ledger.db"))
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

func TestTransactionInvalidScript(t *testing.T) {
	with(func(l *Ledger) {
		err := l.Commit([]core.Transaction{
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "users:001",
						Asset:       "GEM",
						Amount:      100,
					},
				},
				Script: "this is not a valid script",
			},
		})

		if err == nil {
			t.Error(errors.New(
				"script was invalid yet the transaction was commited",
			))
		}
	})
}

func TestTransactionFail(t *testing.T) {
	with(func(l *Ledger) {
		err := l.Commit([]core.Transaction{
			{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "users:001",
						Asset:       "GEM",
						Amount:      100,
					},
				},
				Script: "fail",
			},
		})

		if err == nil {
			t.Error(errors.New(
				"script failed yet the transaction was commited",
			))
		}
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
