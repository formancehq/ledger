package ledger

import (
	"context"
	"fmt"
	"github.com/numary/go-libs/sharedapi"
	"github.com/pborman/uuid"
	"strings"
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
	// TODO: We could remove this field since it is present in store
	name    string
	store   storage.Store
	monitor Monitor
}

func NewLedger(name string, store storage.Store, locker Locker, monitor Monitor) (*Ledger, error) {
	return &Ledger{
		store:   store,
		name:    name,
		locker:  locker,
		monitor: monitor,
	}, nil
}

func (l *Ledger) Close(ctx context.Context) error {
	err := l.store.Close(ctx)
	if err != nil {
		return errors.Wrap(err, "closing store")
	}
	return nil
}

type Volumes map[string]map[string]map[string]int64 // Account/Asset/"input"|"output"

type CommitTransactionResult struct {
	core.Transaction
	Err *TransactionCommitError `json:"error,omitempty"`
}

func (l *Ledger) processTx(ctx context.Context, ts []core.TransactionData) (Volumes, []CommitTransactionResult, []core.Log, error) {

	timestamp := time.Now()

	mapping, err := l.store.LoadMapping(ctx)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "loading mapping")
	}

	contracts := make([]core.Contract, 0)
	if mapping != nil {
		contracts = append(contracts, mapping.Contracts...)
	}
	contracts = append(contracts, DefaultContracts...)

	ret := make([]CommitTransactionResult, 0)
	aggregatedVolumes := Volumes{}
	hasError := false

	lastLog, err := l.store.LastLog(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	lastLogId := uint64(0)
	if lastLog != nil {
		lastLogId = lastLog.ID + 1
	}

	txs := make([]core.Transaction, 0)
	logs := make([]core.Log, 0)

txLoop:
	for i := range ts {

		tx := core.Transaction{
			TransactionData: ts[i],
		}

		tx.ID = uuid.New()
		tx.Timestamp = timestamp.Format(time.RFC3339)

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

		rf := Volumes{}
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
				rf[p.Source] = map[string]map[string]int64{}
			}
			if _, ok := rf[p.Source][p.Asset]; !ok {
				rf[p.Source][p.Asset] = map[string]int64{"input": 0, "output": 0}
			}

			rf[p.Source][p.Asset]["output"] += p.Amount

			if _, ok := rf[p.Destination]; !ok {
				rf[p.Destination] = map[string]map[string]int64{}
			}
			if _, ok := rf[p.Destination][p.Asset]; !ok {
				rf[p.Destination][p.Asset] = map[string]int64{"input": 0, "output": 0}
			}

			rf[p.Destination][p.Asset]["input"] += p.Amount
		}

		for addr := range rf {
			if addr == "world" {
				continue
			}

			_, ok := aggregatedVolumes[addr]
			if !ok {
				aggregatedVolumes[addr], err = l.store.AggregateVolumes(ctx, addr)
				if err != nil {
					return nil, nil, nil, err
				}
			}

			for asset, volumes := range rf[addr] {
				if _, ok := aggregatedVolumes[addr][asset]; !ok {
					aggregatedVolumes[addr][asset] = map[string]int64{
						"input":  0,
						"output": 0,
					}
				}
				expectedBalance := aggregatedVolumes[addr][asset]["input"] - aggregatedVolumes[addr][asset]["output"] + volumes["input"] - volumes["output"]
				for _, contract := range contracts {
					if contract.Match(addr) {
						account, err := l.store.GetAccount(ctx, addr)
						if err != nil {
							return nil, nil, nil, err
						}
						ok := contract.Expr.Eval(core.EvalContext{
							Variables: map[string]interface{}{
								"balance": float64(expectedBalance),
							},
							Metadata: account.Metadata,
							Asset:    asset,
						})
						if !ok {
							commitError(NewTransactionCommitError(i, NewInsufficientFundError(asset)))
							continue txLoop
						}
						break
					}
				}
				aggregatedVolumes[addr][asset]["input"] += volumes["input"]
				aggregatedVolumes[addr][asset]["output"] += volumes["output"]
			}
		}
		ret = append(ret, CommitTransactionResult{
			Transaction: tx,
		})
		lastLogId++
		logs = append(logs, core.Log{
			ID:   lastLogId,
			Type: "NEW_TRANSACTION",
			Data: tx,
			Hash: "",
			Date: timestamp,
		})
	}
	if hasError {
		return nil, ret, logs, ErrCommitError
	}

	return aggregatedVolumes, ret, logs, nil
}

func (l *Ledger) Commit(ctx context.Context, ts []core.TransactionData) (Volumes, []CommitTransactionResult, error) {
	unlock, err := l.locker.Lock(ctx, l.name)
	if err != nil {
		return nil, nil, NewLockError(err)
	}
	defer unlock(ctx)

	volumes, ret, logs, err := l.processTx(ctx, ts)
	if err != nil {
		return nil, ret, err
	}

	txs := make([]core.Transaction, 0)
	for _, v := range ret {
		txs = append(txs, v.Transaction)
	}

	commitErrors, err := l.store.AppendLog(ctx, logs...)
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

	l.monitor.CommittedTransactions(ctx, l.name, ret, volumes)

	return volumes, ret, nil
}

func (l *Ledger) CommitPreview(ctx context.Context, ts []core.TransactionData) (Volumes, []CommitTransactionResult, error) {
	unlock, err := l.locker.Lock(ctx, l.name)
	if err != nil {
		return nil, nil, NewLockError(err)
	}
	defer unlock(ctx)

	balances, ret, _, err := l.processTx(ctx, ts)
	return balances, ret, err
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

func (l *Ledger) FindTransactions(ctx context.Context, m ...query.QueryModifier) (sharedapi.Cursor, error) {
	q := query.New(m)
	c, err := l.store.FindTransactions(ctx, q)

	return c, err
}

func (l *Ledger) GetTransaction(ctx context.Context, id string) (core.Transaction, error) {
	tx, err := l.store.GetTransaction(ctx, id)

	return tx, err
}

func (l *Ledger) SaveMapping(ctx context.Context, mapping core.Mapping) error {
	err := l.store.SaveMapping(ctx, mapping)
	if err != nil {
		return err
	}
	l.monitor.UpdatedMapping(ctx, l.name, mapping)
	return nil
}

func (l *Ledger) LoadMapping(ctx context.Context) (*core.Mapping, error) {
	return l.store.LoadMapping(ctx)
}

func (l *Ledger) RevertTransaction(ctx context.Context, id string) (*core.Transaction, error) {
	tx, err := l.store.GetTransaction(ctx, id)
	if err != nil {
		return nil, err
	}

	rt := tx.Reverse()
	rt.Metadata = core.Metadata{}
	rt.Metadata.MarkReverts(tx.ID)
	_, ret, err := l.Commit(ctx, []core.TransactionData{rt})
	switch err {
	case ErrCommitError:
		return &ret[0].Transaction, ret[0].Err
	case nil:
		l.monitor.RevertedTransaction(ctx, l.name, tx, ret[0].Transaction)
		return &ret[0].Transaction, err
	default:
		return &ret[0].Transaction, err
	}
}

func (l *Ledger) FindAccounts(ctx context.Context, m ...query.QueryModifier) (sharedapi.Cursor, error) {
	q := query.New(m)

	c, err := l.store.FindAccounts(ctx, q)

	return c, err
}

func (l *Ledger) GetAccount(ctx context.Context, address string) (core.Account, error) {
	account, err := l.store.GetAccount(ctx, address)
	if err != nil {
		return core.Account{}, err
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

	return account, nil
}

func (l *Ledger) SaveMeta(ctx context.Context, targetType string, targetID string, m core.Metadata) error {
	unlock, err := l.locker.Lock(ctx, l.name)
	if err != nil {
		return NewLockError(err)
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

	lastId := uint64(0)
	lastLog, err := l.store.LastLog(ctx)
	if err != nil {
		return err
	}
	if lastLog != nil {
		lastId = lastLog.ID + 1
	}

	_, err = l.store.AppendLog(ctx, core.Log{
		ID:   lastId,
		Type: "SET_METADATA",
		Data: core.SetMetadata{
			TargetType: strings.ToUpper(targetType),
			TargetID:   targetID,
			Metadata:   m,
		},
		Hash: "",
		Date: time.Now(),
	})
	if err != nil {
		return err
	}
	return nil
}
