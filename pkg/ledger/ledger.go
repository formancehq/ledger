package ledger

import (
	"context"
	"fmt"
	"time"

	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"

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

type Balances map[string]map[string]int64

type CommitTransactionResult struct {
	core.Transaction
	Err *TransactionCommitError `json:"error,omitempty"`
}

func (l *Ledger) processTx(ctx context.Context, ts []core.TransactionData) (Balances, []CommitTransactionResult, error) {
	timestamp := time.Now().Format(time.RFC3339)

	startId := int64(0)
	last, err := l.store.LastTransaction(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "loading last transaction")
	}
	if last != nil {
		startId = last.ID + 1
	}

	mapping, err := l.store.LoadMapping(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "loading mapping")
	}

	contracts := make([]core.Contract, 0)
	if mapping != nil {
		contracts = append(contracts, mapping.Contracts...)
	}
	contracts = append(contracts, DefaultContracts...)

	ret := make([]CommitTransactionResult, 0)
	aggregatedBalances := make(map[string]map[string]int64)
	hasError := false

	txs := make([]core.Transaction, 0)

txLoop:
	for i := range ts {

		tx := core.Transaction{
			TransactionData: ts[i],
		}

		tx.ID = startId + int64(i)
		tx.Timestamp = timestamp

		tx.Hash = core.Hash(last, &tx)
		last = &tx
		txs = append(txs, tx)

		commitError := func(err *TransactionCommitError) {
			ret = append(ret, CommitTransactionResult{
				Transaction: tx,
				Err:         err,
			})
			hasError = true
		}

		if len(ts[i].Postings) == 0 {
			commitError(NewTransactionCommitError(i, NewValidationError("transaction has no postings")))
			continue txLoop
		}

		rf := map[string]map[string]int64{}
		for _, p := range ts[i].Postings {
			if p.Amount < 0 {
				commitError(NewTransactionCommitError(i, NewValidationError("negative amount")))
				continue txLoop
			}
			if !core.ValidateAddress(p.Source) {
				commitError(NewTransactionCommitError(i, NewValidationError("invalid source address")))
				continue txLoop
			}
			if !core.ValidateAddress(p.Destination) {
				commitError(NewTransactionCommitError(i, NewValidationError("invalid destination address")))
				continue txLoop
			}
			if !core.AssetIsValid(p.Asset) {
				commitError(NewTransactionCommitError(i, NewValidationError("invalid asset")))
				continue txLoop
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

		for addr := range rf {
			if addr == "world" {
				continue
			}

			balances, ok := aggregatedBalances[addr]
			if !ok {
				balances, err = l.store.AggregateBalances(ctx, addr)
				if err != nil {
					return nil, nil, err
				}
				aggregatedBalances[addr] = balances
			}

			for asset, amount := range rf[addr] {
				expectedBalance := balances[asset] - amount
				for _, contract := range contracts {
					if contract.Match(addr) {
						meta, err := l.store.GetMeta(ctx, "account", addr)
						if err != nil {
							return nil, nil, err
						}
						ok := contract.Expr.Eval(core.EvalContext{
							Variables: map[string]interface{}{
								"balance": float64(expectedBalance),
							},
							Metadata: meta,
							Asset:    asset,
						})
						if !ok {
							commitError(NewTransactionCommitError(i, NewInsufficientFundError(asset)))
							continue txLoop
						}
						break
					}
				}
				balances[asset] = expectedBalance
			}
		}
		ret = append(ret, CommitTransactionResult{
			Transaction: tx,
		})
	}

	if hasError {
		return nil, ret, ErrCommitError
	}

	return aggregatedBalances, ret, nil
}

func (l *Ledger) Commit(ctx context.Context, ts []core.TransactionData) (Balances, []CommitTransactionResult, error) {
	unlock, err := l.locker.Lock(ctx, l.name)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to acquire lock")
	}
	defer unlock(ctx)

	balances, ret, err := l.processTx(ctx, ts)
	if err != nil {
		return nil, ret, err
	}

	txs := make([]core.Transaction, 0)
	for _, v := range ret {
		txs = append(txs, v.Transaction)
	}

	commitErrors, err := l.store.SaveTransactions(ctx, txs)
	if err != nil {
		switch err {
		case storage.ErrAborted:
			for ind, err := range commitErrors {
				switch eerr := err.(type) {
				case *storage.Error:
					switch eerr.Code {
					case storage.ConstraintFailed:
						ret[ind].Err = NewTransactionCommitError(ind, NewConflictError(ts[ind].Reference))
					default:
						return nil, nil, err
					}
				default:
					return nil, nil, err
				}
			}
			return nil, ret, ErrCommitError
		default:
			return nil, nil, errors.Wrap(err, "committing transactions")
		}
	}
	return balances, ret, nil
}

func (l *Ledger) Preview(ctx context.Context, ts []core.TransactionData) (Balances, []CommitTransactionResult, error) {
	unlock, err := l.locker.Lock(ctx, l.name)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to acquire lock")
	}
	defer unlock(ctx)

	return l.processTx(ctx, ts)
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

func (l *Ledger) SaveMapping(ctx context.Context, mapping core.Mapping) error {
	return l.store.SaveMapping(ctx, mapping)
}

func (l *Ledger) LoadMapping(ctx context.Context) (*core.Mapping, error) {
	return l.store.LoadMapping(ctx)
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
	rt.Metadata.MarkReverts(fmt.Sprint(lastTransaction.ID))
	_, ret, err := l.Commit(ctx, []core.TransactionData{rt})
	switch err {
	case ErrCommitError:
		return ret[0].Err
	default:
		return err
	}

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
	unlock, err := l.locker.Lock(ctx, l.name)
	if err != nil {
		return errors.Wrap(err, "unable to acquire lock")
	}
	defer unlock(ctx)

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
