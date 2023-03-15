package ledger

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/pkg/errors"
)

type Ledger struct {
	store               storage.LedgerStore
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

func NewLedger(store storage.LedgerStore, monitor Monitor, cache *ristretto.Cache, options ...LedgerOption) (*Ledger, error) {
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

func (l *Ledger) GetLedgerStore() storage.LedgerStore {
	return l.store
}

func (l *Ledger) GetTransactions(ctx context.Context, q storage.TransactionsQuery) (api.Cursor[core.ExpandedTransaction], error) {
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

func (l *Ledger) RevertTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, *Logs, error) {
	revertedTx, err := l.store.GetTransaction(ctx, id)
	if err != nil {
		return nil, nil, errors.Wrap(err, fmt.Sprintf("getting transaction %d", id))
	}
	if revertedTx == nil {
		return nil, nil, NewNotFoundError(fmt.Sprintf("transaction %d not found", id))
	}
	if revertedTx.IsReverted() {
		return nil, nil,
			NewValidationError(fmt.Sprintf("transaction %d already reverted", id))
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

	revertTx, logs, err := l.ProcessScript(ctx, false, false, scriptData[0])
	if err != nil {
		return nil, nil, errors.Wrap(err, fmt.Sprintf(
			"updating transaction %d metadata while reverting", id))
	}

	if err := l.store.UpdateTransactionMetadata(ctx,
		revertedTx.ID, core.RevertedMetadata(revertTx.ID)); err != nil {
		return nil, nil,
			errors.Wrap(err, fmt.Sprintf(
				"updating transaction %d metadata while reverting", id))
	}

	// TODO(polo/gfyrag): combine both logs into one (maybe protobuf)
	logs.AddLog(core.NewSetMetadataLog(revertTx.Timestamp, core.SetMetadata{
		TargetType: core.MetaTargetTypeTransaction,
		TargetID:   revertedTx.ID,
		Metadata:   core.RevertedMetadata(revertTx.ID),
	}))

	logs.AddPostProcessing(func(ctx context.Context) error {
		if revertedTx.Metadata == nil {
			revertedTx.Metadata = core.Metadata{}
		}
		revertedTx.Metadata.Merge(core.RevertedMetadata(revertTx.ID))

		l.monitor.RevertedTransaction(ctx, l.store.Name(), revertedTx, &revertTx)

		return nil
	})

	if err := logs.Write(ctx); err != nil {
		return nil, nil, errors.Wrap(err, "writing logs")
	}

	return &revertTx, logs, nil
}

func (l *Ledger) CountAccounts(ctx context.Context, a storage.AccountsQuery) (uint64, error) {
	return l.store.CountAccounts(ctx, a)
}

func (l *Ledger) GetAccounts(ctx context.Context, a storage.AccountsQuery) (api.Cursor[core.Account], error) {
	return l.store.GetAccounts(ctx, a)
}

func (l *Ledger) GetAccount(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	return l.store.GetAccountWithVolumes(ctx, address)
}

func (l *Ledger) GetBalances(ctx context.Context, q storage.BalancesQuery) (api.Cursor[core.AccountsBalances], error) {
	return l.store.GetBalances(ctx, q)
}

func (l *Ledger) GetBalancesAggregated(ctx context.Context, q storage.BalancesQuery) (core.AssetsBalances, error) {
	return l.store.GetBalancesAggregated(ctx, q)
}

func (l *Ledger) SaveMeta(ctx context.Context, targetType string, targetID interface{}, m core.Metadata) (*Logs, error) {

	if targetType == "" {
		return nil, NewValidationError("empty target type")
	}

	if targetID == "" {
		return nil, NewValidationError("empty target id")
	}

	logs := NewLogs(l.store.AppendLogs, nil, nil)
	var err error
	switch targetType {
	case core.MetaTargetTypeTransaction:
		at := time.Now().Round(time.Second).UTC()
		err = l.store.UpdateTransactionMetadata(ctx, targetID.(uint64), m)
		logs.AddLog(core.NewSetMetadataLog(at, core.SetMetadata{
			TargetType: core.MetaTargetTypeTransaction,
			TargetID:   targetID.(uint64),
			Metadata:   m,
		}))
	case core.MetaTargetTypeAccount:
		at := time.Now().Round(time.Second).UTC()
		err = l.store.UpdateAccountMetadata(ctx, targetID.(string), m)
		logs.AddLog(core.NewSetMetadataLog(at, core.SetMetadata{
			TargetType: core.MetaTargetTypeAccount,
			TargetID:   targetID.(string),
			Metadata:   m,
		}))
	default:
		return nil,
			NewValidationError(fmt.Sprintf("unknown target type '%s'", targetType))
	}
	if err != nil {
		return nil, err
	}

	logs.AddPostProcessing(func(ctx context.Context) error {
		l.monitor.SavedMetadata(ctx, l.store.Name(), targetType, fmt.Sprint(targetID), m)
		return nil
	})

	if err := logs.Write(ctx); err != nil {
		return nil, err
	}

	return logs, nil
}

func (l *Ledger) GetLogs(ctx context.Context, q *storage.LogsQuery) (api.Cursor[core.Log], error) {
	return l.store.GetLogs(ctx, q)
}
