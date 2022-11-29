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
		Expr: &core.ExprGte{
			Op1: core.VariableExpr{
				Name: "balance",
			},
			Op2: core.ConstantExpr{
				Value: core.NewMonetaryInt(0),
			},
		},
	},
}

type Ledger struct {
	store               Store
	monitor             Monitor
	allowPastTimestamps bool
}

type LedgerOption = func(*Ledger)

func WithPastTimestamps(l *Ledger) {
	l.allowPastTimestamps = true
}

func NewLedger(
	store Store,
	monitor Monitor,
	options ...LedgerOption,
) (*Ledger, error) {
	l := &Ledger{
		store:   store,
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

func (l *Ledger) GetLedgerStore() Store {
	return l.store
}

type CommitResult struct {
	PreCommitVolumes      core.AccountsAssetsVolumes
	PostCommitVolumes     core.AccountsAssetsVolumes
	GeneratedTransactions []core.ExpandedTransaction
}

func (l *Ledger) Commit(ctx context.Context, txsData ...core.TransactionData) (*CommitResult, error) {
	commitRes, err := l.ProcessTx(ctx, txsData...)
	if err != nil {
		return nil, err
	}

	if err := l.store.Commit(ctx, commitRes.GeneratedTransactions...); err != nil {
		switch {
		case storage.IsErrorCode(err, storage.ConstraintFailed):
			return nil, NewConflictError()
		default:
			return nil, err
		}
	}

	for _, t := range txsData {
		if t.AddOps != nil && t.AddOps.SetAccountMeta != nil {
			for addr, m := range t.AddOps.SetAccountMeta {
				if err := l.store.UpdateAccountMetadata(ctx,
					addr, m, time.Now().Round(time.Second).UTC()); err != nil {
					return nil, err
				}
			}
		}
	}

	l.monitor.CommittedTransactions(ctx, l.store.Name(), commitRes)
	for _, t := range txsData {
		if t.AddOps != nil && t.AddOps.SetAccountMeta != nil {
			for addr, m := range t.AddOps.SetAccountMeta {
				l.monitor.SavedMetadata(ctx,
					l.store.Name(), core.MetaTargetTypeAccount, addr, m)
			}
		}
	}

	return commitRes, nil
}

func (l *Ledger) CommitPreview(ctx context.Context, txsData ...core.TransactionData) (*CommitResult, error) {
	return l.ProcessTx(ctx, txsData...)
}

func (l *Ledger) GetTransactions(ctx context.Context, q TransactionsQuery) (sharedapi.Cursor[core.ExpandedTransaction], error) {
	return l.store.GetTransactions(ctx, q)
}

func (l *Ledger) CountTransactions(ctx context.Context, q TransactionsQuery) (uint64, error) {
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

	result, err := l.ProcessTx(ctx, rt)
	if err != nil {
		return nil, err
	}
	revert := result.GeneratedTransactions[0]

	if err := l.store.Commit(ctx, revert); err != nil {
		return nil, err
	}
	if err := l.store.UpdateTransactionMetadata(ctx, revertedTx.ID, core.RevertedMetadata(revert.ID), revert.Timestamp); err != nil {
		return nil, err
	}

	if revertedTx.Metadata == nil {
		revertedTx.Metadata = core.Metadata{}
	}
	revertedTx.Metadata.Merge(core.RevertedMetadata(revert.ID))

	l.monitor.RevertedTransaction(ctx, l.store.Name(), revertedTx, &result.GeneratedTransactions[0])
	return &result.GeneratedTransactions[0], nil
}

func (l *Ledger) CountAccounts(ctx context.Context, a AccountsQuery) (uint64, error) {
	return l.store.CountAccounts(ctx, a)
}

func (l *Ledger) GetAccounts(ctx context.Context, a AccountsQuery) (sharedapi.Cursor[core.Account], error) {
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

func (l *Ledger) GetBalances(ctx context.Context, q BalancesQuery) (sharedapi.Cursor[core.AccountsBalances], error) {
	return l.store.GetBalances(ctx, q)
}

func (l *Ledger) GetBalancesAggregated(ctx context.Context, q BalancesQuery) (core.AssetsBalances, error) {
	return l.store.GetBalancesAggregated(ctx, q)
}

func (l *Ledger) SaveMeta(ctx context.Context, targetType string, targetID interface{}, m core.Metadata) error {

	if targetType == "" {
		return NewValidationError("empty target type")
	}

	if targetID == "" {
		return NewValidationError("empty target id")
	}

	var err error
	switch targetType {
	case core.MetaTargetTypeTransaction:
		err = l.store.UpdateTransactionMetadata(ctx, targetID.(uint64), m, time.Now().Round(time.Second).UTC())
	case core.MetaTargetTypeAccount:
		err = l.store.UpdateAccountMetadata(ctx, targetID.(string), m, time.Now().Round(time.Second).UTC())
	default:
		return NewValidationError(fmt.Sprintf("unknown target type '%s'", targetType))
	}
	if err != nil {
		return err
	}

	l.monitor.SavedMetadata(ctx, l.store.Name(), targetType, fmt.Sprint(targetID), m)
	return nil
}
