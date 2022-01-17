package ledger

import (
	"context"
	"fmt"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
	"time"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
)

const (
	targetTypeAccount     = "account"
	targetTypeTransaction = "transaction"
)

var DefaultContracts = []core.Contract{
	{
		Expr: &core.ExprGte{
			Op1: core.VariableExpr{
				Name: "balance",
			},
			Op2: core.ConstantExpr{
				Value: float64(0),
			},
		},
		Account: "*", // world still an exception
	},
}

type Ledger struct {
	locker Locker
	name   string
	store  storage.Store
}

func NewLedger(name string, store storage.Store, locker Locker) (*Ledger, error) {
	return &Ledger{
		store:  store,
		name:   name,
		locker: locker,
	}, nil
}

func (l *Ledger) Close(ctx context.Context) error {
	err := l.store.Close(ctx)
	if err != nil {
		return errors.Wrap(err, "closing store")
	}
	return nil
}

func (l *Ledger) Commit(ctx context.Context, ts []core.Transaction) ([]core.Transaction, error) {
	unlock, err := l.locker.Lock(l.name)
	if err != nil {
		return nil, errors.Wrap(err, "unable to acquire lock")
	}
	defer unlock()

	count, _ := l.store.CountTransactions(ctx)
	rf := map[string]map[string]int64{}
	timestamp := time.Now().Format(time.RFC3339)

	last, err := l.store.LastTransaction(ctx)
	if err != nil {
		return nil, err
	}

	for i := range ts {

		if len(ts[i].Postings) == 0 {
			return ts, NewValidationError("transaction has no postings")
		}

		ts[i].ID = count + int64(i)
		ts[i].Timestamp = timestamp

		ts[i].Hash = core.Hash(last, &ts[i])
		last = &ts[i]

		for _, p := range ts[i].Postings {
			if p.Amount < 0 {
				return ts, NewValidationError("negative amount")
			}
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

	contracts, err := l.store.FindContracts(ctx)
	if err != nil {
		return nil, err
	}
	if len(contracts) == 0 { // Keep default behavior
		contracts = DefaultContracts
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

		balances, err := l.store.AggregateBalances(ctx, addr)
		if err != nil {
			return ts, err
		}

		for asset := range checks {
			expectedBalance := balances[asset] - checks[asset]
			for _, contract := range contracts {
				if contract.Match(addr) {
					meta, err := l.store.GetMeta(ctx, "account", addr)
					if err != nil {
						return nil, err
					}
					ok := contract.Expr.Eval(core.EvalContext{
						Variables: map[string]interface{}{
							"balance": float64(expectedBalance),
						},
						Metadata: meta,
						Asset:    asset,
					})
					if !ok {
						return nil, NewInsufficientFundError(asset)
					}
				}
			}
		}
	}

	err = l.store.SaveTransactions(ctx, ts)
	if err != nil {
		return nil, err
	}

	return ts, err
}

func (l *Ledger) GetLastTransaction(ctx context.Context) (core.Transaction, error) {
	var tx core.Transaction

	q := query.New()
	q.Modify(query.Limit(1))

	c, err := l.store.FindTransactions(ctx, q)

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

func (l *Ledger) FindTransactions(ctx context.Context, m ...query.QueryModifier) (query.Cursor, error) {
	q := query.New(m)
	c, err := l.store.FindTransactions(ctx, q)

	return c, err
}

func (l *Ledger) GetTransaction(ctx context.Context, id string) (core.Transaction, error) {
	tx, err := l.store.GetTransaction(ctx, id)

	return tx, err
}

func (l *Ledger) RevertTransaction(ctx context.Context, id string) error {
	tx, err := l.store.GetTransaction(ctx, id)
	if err != nil {
		return err
	}

	lastTransaction, err := l.store.LastTransaction(ctx)
	if err != nil {
		return err
	}

	rt := tx.Reverse()
	rt.Metadata = core.Metadata{}
	rt.Metadata.MarkRevertedBy(fmt.Sprint(lastTransaction.ID))
	_, err = l.Commit(ctx, []core.Transaction{rt})

	return err
}

func (l *Ledger) FindAccounts(ctx context.Context, m ...query.QueryModifier) (query.Cursor, error) {
	q := query.New(m)

	c, err := l.store.FindAccounts(ctx, q)

	return c, err
}

func (l *Ledger) GetAccount(ctx context.Context, address string) (core.Account, error) {
	account := core.Account{
		Address: address,
	}

	balances, err := l.store.AggregateBalances(ctx, address)

	if err != nil {
		return account, err
	}

	account.Balances = balances

	volumes, err := l.store.AggregateVolumes(ctx, address)

	if err != nil {
		return account, err
	}

	account.Volumes = volumes

	meta, err := l.store.GetMeta(ctx, "account", address)
	if err != nil {
		return account, err
	}
	account.Metadata = meta

	return account, nil
}

func (l *Ledger) SaveMeta(ctx context.Context, targetType string, targetID string, m core.Metadata) error {
	unlock, err := l.locker.Lock(l.name)
	if err != nil {
		return errors.Wrap(err, "unable to acquire lock")
	}
	defer unlock()

	if targetType == "" {
		return NewValidationError("empty target type")
	}
	if targetType != targetTypeTransaction && targetType != targetTypeAccount {
		return NewValidationError(fmt.Sprintf("unknown target type '%s'", targetType))
	}
	if targetID == "" {
		return NewValidationError("empty target id")
	}

	lastMetaID, err := l.store.LastMetaID(ctx)
	if err != nil {
		return err
	}

	timestamp := time.Now().Format(time.RFC3339)

	for key, value := range m {
		lastMetaID++

		err := l.store.SaveMeta(
			ctx,
			lastMetaID,
			timestamp,
			targetType,
			targetID,
			key,
			string(value),
		)

		if err != nil {
			return err
		}
	}
	return nil
}
