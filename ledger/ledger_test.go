package ledger

import (
	"fmt"
	"math/rand"
	"testing"

	"go.uber.org/fx"
	"numary.io/ledger/core"
	"numary.io/ledger/ledger/query"
)

func with(f func(l *Ledger)) {
	fx.New(
		fx.Provide(
			NewLedger,
		),
		fx.Invoke(f),
	)
}

func debug(l *Ledger) error {
	txs, err := l.FindTransactions(query.Limit(10))

	fmt.Printf("ledger has %d transactions\n", len(txs))

	for _, tx := range txs {
		fmt.Println(tx)
	}

	ps, err := l.FindPostings()

	for _, p := range ps {
		fmt.Println(p)
	}

	return err
}

func TestMain(m *testing.M) {
	fmt.Println("prepare test")
	m.Run()
}

func TestTransaction(t *testing.T) {
	with(func(l *Ledger) {
		err := debug(l)

		if err != nil {
			t.Error(err)
		}

		total := 0

		for i := 0; i < 100; i++ {
			user := fmt.Sprintf("users:%03d", 1+rand.Intn(100))
			amount := 1 + rand.Intn(100)
			total += amount

			err = l.Commit(core.Transaction{
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
				t.Error(err)
			}
		}

		err = debug(l)

		if err != nil {
			t.Error(err)
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
