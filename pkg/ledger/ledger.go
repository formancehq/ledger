package ledger

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/formancehq/go-libs/api"
	"github.com/numary/ledger/pkg/core"
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
	cache               *ristretto.Cache
}

type LedgerOption = func(*Ledger)

func WithPastTimestamps(l *Ledger) {
	l.allowPastTimestamps = true
}

func NewLedger(store Store, monitor Monitor, cache *ristretto.Cache, options ...LedgerOption) (*Ledger, error) {
	l := &Ledger{
		store:   store,
		monitor: monitor,
		cache:   cache,
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

func (l *Ledger) GetTransactions(ctx context.Context, q TransactionsQuery) (api.Cursor[core.ExpandedTransaction], error) {
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
		return nil, errors.Wrap(err, fmt.Sprintf("getting transaction %d", id))
	}
	if revertedTx == nil {
		return nil, NewNotFoundError(fmt.Sprintf("transaction %d not found", id))
	}
	if revertedTx.IsReverted() {
		return nil, NewValidationError(fmt.Sprintf("transaction %d already reverted", id))
	}

	rt := revertedTx.Reverse()
	rt.Metadata = core.Metadata{}
	rt.Metadata.MarkReverts(revertedTx.ID)

	txData := core.TransactionData{
		Postings:  rt.Postings,
		Timestamp: rt.Timestamp,
		Reference: rt.Reference,
		Metadata:  rt.Metadata,
	}
	res, err := l.Execute(ctx, false, false,
		core.TxsToScriptsData(txData)...)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(
			"executing revert script for transaction %d", id))
	}
	revertTx := res[0]

	if err := l.store.UpdateTransactionMetadata(ctx,
		revertedTx.ID, core.RevertedMetadata(revertTx.ID), revertTx.Timestamp); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf(
			"updating transaction %d metadata while reverting", id))
	}

	if revertedTx.Metadata == nil {
		revertedTx.Metadata = core.Metadata{}
	}
	revertedTx.Metadata.Merge(core.RevertedMetadata(revertTx.ID))

	l.monitor.RevertedTransaction(ctx, l.store.Name(), revertedTx, &revertTx)
	return &revertTx, nil
}

func (l *Ledger) CountAccounts(ctx context.Context, a AccountsQuery) (uint64, error) {
	return l.store.CountAccounts(ctx, a)
}

func (l *Ledger) GetAccounts(ctx context.Context, a AccountsQuery) (api.Cursor[core.Account], error) {
	return l.store.GetAccounts(ctx, a)
}

func (l *Ledger) GetAccount(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	return l.store.GetAccountWithVolumes(ctx, address)
}

func (l *Ledger) GetBalances(ctx context.Context, q BalancesQuery) (api.Cursor[core.AccountsBalances], error) {
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

func (l *Ledger) GetLogs(ctx context.Context, q *LogsQuery) (api.Cursor[core.Log], error) {
	return l.store.GetLogs(ctx, q)
}
