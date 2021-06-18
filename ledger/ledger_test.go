package ledger

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"go.uber.org/fx"
	"numary.io/ledger/config"
	"numary.io/ledger/core"
)

func with(f func(l *Ledger)) {
	fx.New(
		fx.Provide(
			func() config.Config {
				c := config.DefaultConfig()
				c.Storage.SQLiteOpts.Directory = "/tmp"
				c.Storage.SQLiteOpts.DBName = "ledger"
				return c
			},
			NewLedger,
		),
		fx.Invoke(f),
		fx.Invoke(func(l *Ledger) {
			l.Close()
		}),
	)
}

func TestMain(m *testing.M) {
	os.Remove("/tmp/ledger.db")
	m.Run()
}

func TestTransaction(t *testing.T) {
	with(func(l *Ledger) {

		total := 0

		testsize := 1e5
		for i := 0; i < int(testsize); i++ {
			if i%int(testsize/10) == 0 && i > 0 {
				fmt.Println(i)
			}

			user := fmt.Sprintf("users:%03d", 1+rand.Intn(100))
			amount := 1 + rand.Intn(100)
			amount = 100
			total += amount

			err := l.Commit(core.Transaction{
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

			if err != nil {
				fmt.Println(err)
				t.Error(err)
			}
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
		err := l.Commit(core.Transaction{
			Postings: []core.Posting{
				{
					Source:      "empty_wallet",
					Destination: "world",
					Amount:      1,
					Asset:       "COIN",
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

		err := l.Commit(tx)

		if err != nil {
			t.Error(err)
		}

		err = l.Commit(tx)

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

func BenchmarkLedger(b *testing.B) {
	with(func(l *Ledger) {
		for i := 0; i < b.N; i++ {
			l.Commit(core.Transaction{
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
