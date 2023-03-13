package ledger

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/pkg/errors"
)

type waitLogsAndPostProcessing func(context.Context) error

type Ledger struct {
	store               Store
	monitor             Monitor
	allowPastTimestamps bool
	cache               *ristretto.Cache
	locker              Locker
}

type LedgerOption = func(*Ledger)

func WithPastTimestamps(l *Ledger) {
	l.allowPastTimestamps = true
}

func WithLocker(locker Locker) LedgerOption {
	return func(ledger *Ledger) {
		ledger.locker = locker
	}
}

var defaultLedgerOptions = []LedgerOption{
	WithLocker(NewInMemoryLocker()),
}

func NewLedger(store Store, monitor Monitor, cache *ristretto.Cache, options ...LedgerOption) (*Ledger, error) {
	l := &Ledger{
		store:   store,
		monitor: monitor,
		cache:   cache,
	}

	for _, option := range append(
		defaultLedgerOptions,
		options...,
	) {
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

func (l *Ledger) RevertTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, waitLogsAndPostProcessing) {
	revertedTx, err := l.store.GetTransaction(ctx, id)
	if err != nil {
		return nil, func(_ context.Context) error { return errors.Wrap(err, fmt.Sprintf("getting transaction %d", id)) }
	}
	if revertedTx == nil {
		return nil, func(_ context.Context) error { return NewNotFoundError(fmt.Sprintf("transaction %d not found", id)) }
	}
	if revertedTx.IsReverted() {
		return nil, func(_ context.Context) error {
			return NewValidationError(fmt.Sprintf("transaction %d already reverted", id))
		}
	}

	rt := revertedTx.Reverse()
	rt.Metadata = core.Metadata{}
	rt.Metadata.MarkReverts(revertedTx.ID)

	scriptData := core.TxsToScriptsData(core.TransactionData{
		Postings:  rt.Postings,
		Timestamp: rt.Timestamp,
		Reference: rt.Reference,
		Metadata:  rt.Metadata,
	})

	revertTx, waitAndPostProcess := l.ExecuteScript(ctx, false, scriptData[0])
	// TODO(polo): merge next logs with the one in ExecuteScript function
	// when CQRS is implemented

	if err := l.store.UpdateTransactionMetadata(ctx,
		revertedTx.ID, core.RevertedMetadata(revertTx.ID)); err != nil {
		return nil, func(_ context.Context) error {
			return errors.Wrap(err, fmt.Sprintf(
				"updating transaction %d metadata while reverting", id))
		}
	}

	// TODO(polo): Merge thoses logs with the one in ExecuteScript function
	logs := make([]core.Log, 0, 1)
	logs = append(logs, core.NewSetMetadataLog(revertTx.Timestamp, core.SetMetadata{
		TargetType: core.MetaTargetTypeTransaction,
		TargetID:   revertedTx.ID,
		Metadata:   core.RevertedMetadata(revertTx.ID),
	}))

	errChan := l.store.AppendLogs(ctx, logs...)

	// TODO(polo): remove this when the CQRS is implemented
	// Move the monitor save metadat to the CQRS part
	return &revertTx, func(ctx context.Context) error {
		if err := waitAndPostProcess(ctx); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errChan:
			if err != nil {
				return err
			}
		}

		if revertedTx.Metadata == nil {
			revertedTx.Metadata = core.Metadata{}
		}
		revertedTx.Metadata.Merge(core.RevertedMetadata(revertTx.ID))

		l.monitor.RevertedTransaction(ctx, l.store.Name(), revertedTx, &revertTx)

		return nil
	}
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

func (l *Ledger) SaveMeta(ctx context.Context, targetType string, targetID interface{}, m core.Metadata) waitLogsAndPostProcessing {

	if targetType == "" {
		return func(_ context.Context) error { return NewValidationError("empty target type") }
	}

	if targetID == "" {
		return func(_ context.Context) error { return NewValidationError("empty target id") }
	}

	logs := make([]core.Log, 0)
	var err error
	switch targetType {
	case core.MetaTargetTypeTransaction:
		at := time.Now().Round(time.Second).UTC()
		err = l.store.UpdateTransactionMetadata(ctx, targetID.(uint64), m)
		logs = append(logs, core.NewSetMetadataLog(at, core.SetMetadata{
			TargetType: core.MetaTargetTypeTransaction,
			TargetID:   targetID.(uint64),
			Metadata:   m,
		}))
	case core.MetaTargetTypeAccount:
		at := time.Now().Round(time.Second).UTC()
		err = l.store.UpdateAccountMetadata(ctx, targetID.(string), m)
		logs = append(logs, core.NewSetMetadataLog(at, core.SetMetadata{
			TargetType: core.MetaTargetTypeAccount,
			TargetID:   targetID.(string),
			Metadata:   m,
		}))
	default:
		return func(_ context.Context) error {
			return NewValidationError(fmt.Sprintf("unknown target type '%s'", targetType))
		}
	}
	if err != nil {
		return func(_ context.Context) error { return err }
	}

	errChan := l.store.AppendLogs(ctx, logs...)

	// TODO(polo): remove this when the CQRS is implemented
	// Move the monitor save metadat to the CQRS part
	return func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errChan:
			if err != nil {
				return err
			}
		}

		l.monitor.SavedMetadata(ctx, l.store.Name(), targetType, fmt.Sprint(targetID), m)

		return nil
	}
}

func (l *Ledger) GetLogs(ctx context.Context, q *LogsQuery) (api.Cursor[core.Log], error) {
	return l.store.GetLogs(ctx, q)
}
