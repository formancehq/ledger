package ledger

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/numary/ledger/config"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger/query"
	"github.com/numary/ledger/storage"
	"go.uber.org/fx"
)

type Ledger struct {
	sync.Mutex
	name        string
	store       storage.Store
	_last       *core.Transaction
	_lastMetaID int
}

func NewLedger(name string, lc fx.Lifecycle) (*Ledger, error) {
	store, err := storage.GetStore(name)

	if err != nil {
		return nil, err
	}

	err = store.Initialize()

	if err != nil {
		err = fmt.Errorf("failed to initialize store: %w", err)
		log.Println(err)
		return nil, err
	}

	l := &Ledger{
		store: store,
		name:  name,
	}

	lc.Append(fx.Hook{
		OnStart: func(c context.Context) error {
			log.Printf("starting ledger %s\n", l.name)
			return nil
		},
		OnStop: func(c context.Context) error {
			log.Printf("closing ledger %s\n", l.name)
			l.Close()
			return nil
		},
	})

	return l, nil
}

func (l *Ledger) Close() {
	l.store.Close()
}

func (l *Ledger) Commit(ts []core.Transaction) error {
	defer config.Remember(l.name)

	l.Lock()
	defer l.Unlock()

	count, _ := l.store.CountTransactions()
	rf := map[string]map[string]int64{}
	timestamp := time.Now().Format(time.RFC3339)

	if l._last == nil {
		last, err := l.GetLastTransaction()

		if err != nil {
			return err
		}

		l._last = &last
	}

	last := l._last

	for i := range ts {

		if len(ts[i].Postings) == 0 {
			return errors.New("transaction has no postings")
		}

		ts[i].ID = count + int64(i)
		ts[i].Timestamp = timestamp

		ts[i].Hash = core.Hash(last, &ts[i])
		last = &ts[i]

		for _, p := range ts[i].Postings {
			if _, ok := rf[p.Source]; !ok {
				rf[p.Source] = map[string]int64{}
			}

			rf[p.Source][p.Asset] += p.Amount

			if _, ok := rf[p.Destination]; !ok {
				rf[p.Destination] = map[string]int64{}
			}

			rf[p.Destination][p.Asset] -= p.Amount
		}
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
				return fmt.Errorf(
					"balance.insufficient.%s",
					asset,
				)
			}
		}
	}

	err := l.store.SaveTransactions(ts)

	l._last = &ts[len(ts)-1]

	return err
}

func (l *Ledger) GetLastTransaction() (core.Transaction, error) {
	var tx core.Transaction

	q := query.New()
	q.Modify(query.Limit(1))

	c, err := l.store.FindTransactions(q)

	if err != nil {
		return tx, err
	}

	txs := (c.Data).([]core.Transaction)

	if len(txs) == 0 {
		return tx, nil
	}

	tx = txs[0]

	return tx, nil
}

func (l *Ledger) FindTransactions(m ...query.QueryModifier) (query.Cursor, error) {
	q := query.New(m)
	c, err := l.store.FindTransactions(q)

	return c, err
}

func (l *Ledger) GetTransaction(id string) (core.Transaction, error) {
	tx, err := l.store.GetTransaction(id)

	return tx, err
}

func (l *Ledger) RevertTransaction(id string) error {
	tx, err := l.store.GetTransaction(id)
	if err != nil {
		return err
	}

	if l._last == nil {
		last, err := l.GetLastTransaction()

		if err != nil {
			return err
		}

		l._last = &last
	}

	rt := tx.Reverse()
	rt.Metadata = core.Metadata{}
	rt.Metadata.MarkRevertedBy(fmt.Sprint(l._last.ID))
	err = l.Commit([]core.Transaction{rt})
	if err != nil {
		return err
	}

	return err
}

func (l *Ledger) FindAccounts(m ...query.QueryModifier) (query.Cursor, error) {
	q := query.New(m)

	c, err := l.store.FindAccounts(q)

	return c, err
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

	volumes, err := l.store.AggregateVolumes(address)

	if err != nil {
		return account, err
	}

	account.Volumes = volumes

	meta, err := l.store.GetMeta("account", address)
	if err != nil {
		return account, err
	}
	account.Metadata = meta

	return account, nil
}

func (l *Ledger) SaveMeta(targetType string, targetID string, m core.Metadata) error {
	l.Lock()
	defer l.Unlock()

	if l._lastMetaID == 0 {
		count, err := l.store.CountMeta()
		if err != nil {
			return err
		}
		l._lastMetaID = int(count) - 1
	}

	timestamp := time.Now().Format(time.RFC3339)

	for key, value := range m {
		metaRowID := fmt.Sprint(l._lastMetaID + 1)

		err := l.store.SaveMeta(
			metaRowID,
			timestamp,
			targetType,
			targetID,
			key,
			string(value),
		)
		if err == nil {
			l._lastMetaID++
		} else {
			return err
		}
	}
	return nil
}
