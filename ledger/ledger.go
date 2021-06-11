package ledger

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.uber.org/fx"
	"numary.io/ledger/core"
	"numary.io/ledger/ledger/query"
	"numary.io/ledger/storage"
	"numary.io/ledger/storage/sqlite"
)

type Ledger struct {
	sync.Mutex
	store storage.Store
}

func NewLedger(lc fx.Lifecycle) (*Ledger, error) {
	store, err := sqlite.NewStore()
	store.Initialize()

	if err != nil {
		return nil, err
	}

	l := &Ledger{
		store: store,
	}

	lc.Append(fx.Hook{
		OnStart: func(c context.Context) error {
			fmt.Println("starting ledger")
			return nil
		},
		OnStop: func(c context.Context) error {
			fmt.Println("closing ledger")
			l.Close()
			return nil
		},
	})

	return l, nil
}

func (l *Ledger) Close() {
	l.store.Close()
}

func (l *Ledger) Commit(t core.Transaction) error {
	l.Lock()

	count, _ := l.store.CountTransactions()
	t.ID = count

	if t.Timestamp == "" {
		t.Timestamp = time.Now().Format(time.RFC3339)
	}

	err := l.store.AppendTransaction(t)

	l.Unlock()

	return err
}

func (l *Ledger) FindTransactions(m ...query.QueryModifier) ([]core.Transaction, error) {
	q := query.New(m)

	return l.store.FindTransactions(q)
}

func (l *Ledger) FindAccounts() ([]core.Account, error) {
	return l.store.FindAccounts()
}

func (l *Ledger) GetAccount(address string) (core.Account, error) {
	account := core.Account{
		Address:  address,
		Contract: "default",
	}

	balances, err := l.store.AggregateBalances(address)

	if err != nil {
		return account, err
	}

	account.Balances = balances

	return account, nil
}

func (l *Ledger) FindPostings() ([]core.Posting, error) {
	q := query.Query{}
	q.Modify(query.Limit(10))

	res, err := l.store.FindPostings(q)

	if err != nil {
		log.Println(err)
	}

	return res, err
}
