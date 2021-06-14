package ledger

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"go.uber.org/fx"
	"numary.io/ledger/config"
	"numary.io/ledger/core"
	"numary.io/ledger/ledger/query"
	"numary.io/ledger/storage"
	"numary.io/ledger/storage/sqlite"
)

type Ledger struct {
	sync.Mutex
	store  storage.Store
	config config.Config
}

func NewLedger(lc fx.Lifecycle, c config.Config) (*Ledger, error) {
	store, err := sqlite.NewStore(c)
	store.Initialize()

	if err != nil {
		return nil, err
	}

	l := &Ledger{
		store:  store,
		config: c,
	}

	lc.Append(fx.Hook{
		OnStart: func(c context.Context) error {
			fmt.Println("starting ledger")
			fmt.Println(l.config)
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

	rf := map[string]map[string]int64{}

	for _, p := range t.Postings {
		if _, ok := rf[p.Source]; !ok {
			rf[p.Source] = map[string]int64{}
		}

		rf[p.Source][p.Asset] += p.Amount

		if _, ok := rf[p.Destination]; !ok {
			rf[p.Destination] = map[string]int64{}
		}

		rf[p.Destination][p.Asset] -= p.Amount
	}

	for addr := range rf {
		if addr == "world" {
			continue
		}

		checks := map[string]int64{}

		for asset := range rf[addr] {
			if rf[addr][asset] <= 0 {
				continue
			}

			checks[asset] = rf[addr][asset]
		}

		if len(checks) == 0 {
			continue
		}

		balances, err := l.store.AggregateBalances(addr)

		if err != nil {
			return err
		}

		for asset := range checks {
			balance, ok := balances[asset]

			if !ok || balance < checks[asset] {
				return errors.New(fmt.Sprintf(
					"balance.insufficient.%s",
					asset,
				))
			}
		}
	}

	err := l.store.AppendTransaction(t)

	l.Unlock()

	return err
}

func (l *Ledger) FindTransactions(m ...query.QueryModifier) ([]core.Transaction, error) {
	q := query.New(m)

	return l.store.FindTransactions(q)
}

func (l *Ledger) FindAccounts(m ...query.QueryModifier) ([]core.Account, error) {
	q := query.New(m)

	return l.store.FindAccounts(q)
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

func (l *Ledger) FindPostings(m ...query.QueryModifier) ([]core.Posting, error) {
	q := query.New()
	q.Apply(m)

	res, err := l.store.FindPostings(q)

	if err != nil {
		log.Println(err)
	}

	return res, err
}
