package ledger

import (
	"context"
	"fmt"
	"time"

	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
)

var DefaultContracts = []core.Contract{
	{
		Name:    "default",
		Account: "*", // world still an exception
	},
}

type Ledger struct {
	locker              Locker
	store               storage.Store
	monitor             Monitor
	allowPastTimestamps bool
}

type LedgerOption = func(*Ledger)

func WithPastTimestamps(l *Ledger) {
	l.allowPastTimestamps = true
}

func NewLedger(
	store storage.Store,
	locker Locker,
	monitor Monitor,
	options ...LedgerOption,
) (*Ledger, error) {
	l := &Ledger{
		store:   store,
		locker:  locker,
		monitor: monitor,
	}

	for _, option := range options {
		option(l)
	}

	return l, nil
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
	GeneratedTransactions []core.ExpandedTransaction
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

	if err := l.store.WithTX(ctx, func(api storage.API) error {
		return l.store.Commit(ctx, result.GeneratedTransactions...)
	}); err != nil {
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

func (l *Ledger) GetTransactions(ctx context.Context, q storage.TransactionsQuery) (sharedapi.Cursor[core.ExpandedTransaction], error) {
	return l.store.GetTransactions(ctx, q)
}

func (l *Ledger) CountTransactions(ctx context.Context, q storage.TransactionsQuery) (uint64, error) {
	return l.store.CountTransactions(ctx, q)
}

func (l *Ledger) GetTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, error) {
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

func (l *Ledger) RevertTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, error) {
	revertedTx, err := l.store.GetTransaction(ctx, id)
	if err != nil {
		return nil, err
	}
	if revertedTx == nil {
		return nil, NewNotFoundError("transaction not found")
	}
	if revertedTx.IsReverted() {
		return nil, NewValidationError("transaction already reverted")
	}

	rt := revertedTx.Reverse()
	rt.Metadata = core.Metadata{}
	rt.Metadata.MarkReverts(revertedTx.ID)

	unlock, err := l.locker.Lock(ctx, l.store.Name())
	if err != nil {
		return nil, NewLockError(err)
	}
	defer unlock(ctx)

	result, err := l.processTx(ctx, []core.TransactionData{rt})
	if err != nil {
		return nil, err
	}
	revert := result.GeneratedTransactions[0]

	err = l.store.WithTX(ctx, func(api storage.API) error {
		err := api.Commit(ctx, revert)
		if err != nil {
			return err
		}

		return api.UpdateTransactionMetadata(ctx, revertedTx.ID, core.RevertedMetadata(revert.ID), revert.Timestamp)
	})
	if err != nil {
		return nil, err
	}

	if revertedTx.Metadata == nil {
		revertedTx.Metadata = core.Metadata{}
	}
	revertedTx.Metadata.Merge(core.RevertedMetadata(revert.ID))

	l.monitor.RevertedTransaction(ctx, l.store.Name(), revertedTx, &result.GeneratedTransactions[0])
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
	switch targetType {
	case core.MetaTargetTypeTransaction:
	case core.MetaTargetTypeAccount:
	default:
		return NewValidationError(fmt.Sprintf("unknown target type '%s'", targetType))
	}
	if targetID == "" {
		return NewValidationError("empty target id")
	}

	err = l.store.WithTX(ctx, func(api storage.API) error {
		switch targetType {
		case core.MetaTargetTypeTransaction:
			return l.store.UpdateTransactionMetadata(ctx, targetID.(uint64), m, time.Now().Round(time.Second).UTC())
		case core.MetaTargetTypeAccount:
			return l.store.UpdateAccountMetadata(ctx, targetID.(string), m, time.Now().Round(time.Second).UTC())
		}
		panic("can not happen")
	})
	if err != nil {
		return err
	}

	l.monitor.SavedMetadata(ctx, l.store.Name(), targetType, fmt.Sprint(targetID), m)
	return nil
}
