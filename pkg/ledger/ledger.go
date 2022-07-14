package ledger

import (
	"context"
	"fmt"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
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
	locker  Locker
	store   storage.Store
	monitor Monitor
}

func NewLedger(store storage.Store, locker Locker, monitor Monitor) (*Ledger, error) {
	return &Ledger{
		store:   store,
		locker:  locker,
		monitor: monitor,
	}, nil
}

func (l *Ledger) Close(ctx context.Context) error {
	if err := l.store.Close(ctx); err != nil {
		return errors.Wrap(err, "closing store")
	}
	return nil
}

type CommitResult struct {
	PreCommitVolumes      core.AccountsAssetsVolumes
	PostCommitVolumes     core.AccountsAssetsVolumes
	GeneratedTransactions []core.Transaction
}

func (l *Ledger) Commit(ctx context.Context, txsData []core.TransactionData) (*CommitResult, error) {
	unlock, err := l.locker.Lock(ctx, l.store.Name())
	if err != nil {
		return nil, NewLockError(err)
	}
	defer unlock(ctx)

	result, err := l.processTx(ctx, txsData)
	if err != nil {
		return nil, err
	}

	if err = l.store.Commit(ctx, result.GeneratedTransactions...); err != nil {
		spew.Dump(err)
		switch {
		case storage.IsErrorCode(err, storage.ConstraintFailed):
			return nil, NewConflictError()
		default:
			return nil, err
		}
	}

	l.monitor.CommittedTransactions(ctx, l.store.Name(), result)
	return result, nil
}

func (l *Ledger) CommitPreview(ctx context.Context, txsData []core.TransactionData) (*CommitResult, error) {
	unlock, err := l.locker.Lock(ctx, l.store.Name())
	if err != nil {
		return nil, NewLockError(err)
	}
	defer unlock(ctx)

	return l.processTx(ctx, txsData)
}

func (l *Ledger) GetTransactions(ctx context.Context, q storage.TransactionsQuery) (sharedapi.Cursor[core.Transaction], error) {

	return l.store.GetTransactions(ctx, q)
}

func (l *Ledger) CountTransactions(ctx context.Context, q storage.TransactionsQuery) (uint64, error) {

	return l.store.CountTransactions(ctx, q)
}

func (l *Ledger) GetTransaction(ctx context.Context, id uint64) (*core.Transaction, error) {
	tx, err := l.store.GetTransaction(ctx, id)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, NewNotFoundError("transaction not found")
	}

	return tx, nil
}

func (l *Ledger) SaveMapping(ctx context.Context, mapping core.Mapping) error {
	if err := l.store.SaveMapping(ctx, mapping); err != nil {
		return err
	}

	l.monitor.UpdatedMapping(ctx, l.store.Name(), mapping)
	return nil
}

func (l *Ledger) LoadMapping(ctx context.Context) (*core.Mapping, error) {
	return l.store.LoadMapping(ctx)
}

func (l *Ledger) RevertTransaction(ctx context.Context, id uint64) (*core.Transaction, error) {
	tx, err := l.store.GetTransaction(ctx, id)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, NewNotFoundError("transaction not found")
	}
	if tx.IsReverted() {
		return nil, NewValidationError("transaction already reverted")
	}

	rt := tx.Reverse()
	rt.Metadata = core.Metadata{}
	rt.Metadata.MarkReverts(tx.ID)

	unlock, err := l.locker.Lock(ctx, l.store.Name())
	if err != nil {
		return nil, NewLockError(err)
	}
	defer unlock(ctx)

	result, err := l.processTx(ctx, []core.TransactionData{rt})
	if err != nil {
		return nil, err
	}

	if err = l.store.CommitRevert(ctx, *tx, result.GeneratedTransactions[0]); err != nil {
		return nil, err
	}

	l.monitor.RevertedTransaction(ctx, l.store.Name(), tx, &result.GeneratedTransactions[0])
	return &result.GeneratedTransactions[0], nil
}

func (l *Ledger) CountAccounts(ctx context.Context, a storage.AccountsQuery) (uint64, error) {

	return l.store.CountAccounts(ctx, a)
}

func (l *Ledger) GetAccounts(ctx context.Context, a storage.AccountsQuery) (sharedapi.Cursor[core.Account], error) {

	return l.store.GetAccounts(ctx, a)
}

func (l *Ledger) GetAccount(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	account, err := l.store.GetAccount(ctx, address)
	if err != nil {
		return nil, err
	}

	volumes, err := l.store.GetAssetsVolumes(ctx, address)
	if err != nil {
		return nil, err
	}

	return &core.AccountWithVolumes{
		Account:  *account,
		Volumes:  volumes,
		Balances: volumes.Balances(),
	}, nil
}

func (l *Ledger) GetBalances(ctx context.Context, q storage.BalancesQuery) (sharedapi.Cursor[core.AccountsBalances], error) {
	return l.store.GetBalances(ctx, q)
}

func (l *Ledger) GetBalancesAggregated(ctx context.Context, q storage.BalancesQuery) (core.AssetsBalances, error) {
	return l.store.GetBalancesAggregated(ctx, q)
}

func (l *Ledger) SaveMeta(ctx context.Context, targetType string, targetID interface{}, m core.Metadata) error {
	unlock, err := l.locker.Lock(ctx, l.store.Name())
	if err != nil {
		return NewLockError(err)
	}
	defer unlock(ctx)

	if targetType == "" {
		return NewValidationError("empty target type")
	}
	if targetType != core.MetaTargetTypeTransaction && targetType != core.MetaTargetTypeAccount {
		return NewValidationError(fmt.Sprintf("unknown target type '%s'", targetType))
	}
	if targetID == "" {
		return NewValidationError("empty target id")
	}

	switch targetType {
	case core.MetaTargetTypeTransaction:
		err = l.store.UpdateTransactionMetadata(ctx, targetID.(uint64), m, time.Now().Round(time.Second).UTC())
	case core.MetaTargetTypeAccount:
		err = l.store.UpdateAccountMetadata(ctx, targetID.(string), m, time.Now().Round(time.Second).UTC())
	}
	if err != nil {
		return err
	}

	l.monitor.SavedMetadata(ctx, l.store.Name(), targetType, fmt.Sprint(targetID), m)
	return nil
}
