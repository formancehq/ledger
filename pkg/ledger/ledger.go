package ledger

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/ledger/pkg/ledger/runner"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
)

type Ledger struct {
	runner          *runner.Runner
	store           storage.LedgerStore
	locker          lock.Locker
	dbCache         *cache.Cache
	queryWorker     *query.Worker
	metricsRegistry metrics.PerLedgerMetricsRegistry
}

func New(
	store storage.LedgerStore,
	dbCache *cache.Cache,
	runner *runner.Runner,
	locker lock.Locker,
	queryWorker *query.Worker,
	metricsRegistry metrics.PerLedgerMetricsRegistry,
) *Ledger {
	return &Ledger{
		store:           store,
		dbCache:         dbCache,
		runner:          runner,
		locker:          locker,
		queryWorker:     queryWorker,
		metricsRegistry: metricsRegistry,
	}
}

func (l *Ledger) Close(ctx context.Context) error {
	if err := l.store.Close(ctx); err != nil {
		return errors.Wrap(err, "closing store")
	}

	if err := l.queryWorker.Stop(ctx); err != nil {
		return errors.Wrap(err, "stopping query worker")
	}

	return nil
}

func (l *Ledger) GetLedgerStore() storage.LedgerStore {
	return l.store
}

func (l *Ledger) writeLog(ctx context.Context, logHolder *core.LogHolder) error {
	l.queryWorker.QueueLog(logHolder)
	// Wait for CQRS ingestion
	// TODO(polo/gfyrag): add possiblity to disable this via request param
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-logHolder.Ingested:
		return nil
	}
}

func (l *Ledger) CreateTransaction(ctx context.Context, dryRun bool, script core.RunScript) (*core.ExpandedTransaction, error) {
	tx, logHolder, err := l.runner.Execute(ctx, script, dryRun, func(expandedTx core.ExpandedTransaction, accountMetadata map[string]metadata.Metadata) core.Log {
		return core.NewTransactionLog(expandedTx.Transaction, accountMetadata)
	})

	if err == nil && !dryRun {
		if err := l.writeLog(ctx, logHolder); err != nil {
			return nil, errors.Wrap(err, "writing log")
		}
	}

	return tx, errors.Wrap(err, "create transactions")
}

func (l *Ledger) GetTransactions(ctx context.Context, q storage.TransactionsQuery) (*api.Cursor[core.ExpandedTransaction], error) {
	txs, err := l.store.GetTransactions(ctx, q)
	return txs, errors.Wrap(err, "getting transactions")
}

func (l *Ledger) CountTransactions(ctx context.Context, q storage.TransactionsQuery) (uint64, error) {
	count, err := l.store.CountTransactions(ctx, q)
	return count, errors.Wrap(err, "counting transactions")
}

func (l *Ledger) GetTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, error) {
	tx, err := l.store.GetTransaction(ctx, id)
	return tx, errors.Wrap(err, "getting transaction")
}

func (l *Ledger) RevertTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, error) {
	revertedTx, err := l.store.GetTransaction(ctx, id)
	if err != nil && !storage.IsNotFoundError(err) {
		return nil, errors.Wrap(err, "get transaction before revert")
	}
	if storage.IsNotFoundError(err) {
		return nil, errorsutil.NewError(err, errors.Errorf("transaction %d not found", id))
	}
	if revertedTx.IsReverted() {
		return nil, errorsutil.NewError(ErrValidation,
			errors.Errorf("transaction %d already reverted", id))
	}

	rt := revertedTx.Reverse()
	rt.Metadata = core.MarkReverts(metadata.Metadata{}, revertedTx.ID)

	scriptData := core.TxsToScriptsData(core.TransactionData{
		Postings:  rt.Postings,
		Reference: rt.Reference,
		Metadata:  rt.Metadata,
	})
	tx, log, err := l.runner.Execute(ctx, scriptData[0], false, func(expandedTx core.ExpandedTransaction, accountMetadata map[string]metadata.Metadata) core.Log {
		return core.NewRevertedTransactionLog(expandedTx.Timestamp, revertedTx.ID, expandedTx.Transaction)
	})
	if err != nil {
		return nil, errors.Wrap(err, "revert transaction")
	}

	if err == nil {
		if err := l.writeLog(ctx, log); err != nil {
			return nil, errors.Wrap(err, "writing log")
		}
	}

	return tx, nil
}

func (l *Ledger) CountAccounts(ctx context.Context, a storage.AccountsQuery) (uint64, error) {
	count, err := l.store.CountAccounts(ctx, a)
	return count, errors.Wrap(err, "counting accounts")
}

func (l *Ledger) GetAccounts(ctx context.Context, a storage.AccountsQuery) (*api.Cursor[core.Account], error) {
	accounts, err := l.store.GetAccounts(ctx, a)
	return accounts, errors.Wrap(err, "getting accounts")
}

func (l *Ledger) GetAccount(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	accounts, err := l.store.GetAccountWithVolumes(ctx, address)
	return accounts, errors.Wrap(err, "getting account")
}

func (l *Ledger) GetBalances(ctx context.Context, q storage.BalancesQuery) (*api.Cursor[core.AccountsBalances], error) {
	balances, err := l.store.GetBalances(ctx, q)
	return balances, errors.Wrap(err, "getting balances")
}

func (l *Ledger) GetBalancesAggregated(ctx context.Context, q storage.BalancesQuery) (core.AssetsBalances, error) {
	balances, err := l.store.GetBalancesAggregated(ctx, q)
	return balances, errors.Wrap(err, "getting balances aggregated")
}

// TODO(gfyrag): maybe we should check transaction exists on the log store before set a metadata ? (accounts always exists even if never used)
func (l *Ledger) SaveMeta(ctx context.Context, targetType string, targetID interface{}, m metadata.Metadata) error {
	if m == nil {
		return nil
	}

	if targetType == "" {
		return errorsutil.NewError(ErrValidation, errors.New("empty target type"))
	}

	if targetID == "" {
		return errorsutil.NewError(ErrValidation, errors.New("empty target id"))
	}

	at := core.Now()
	var (
		err error
		log core.Log
	)
	switch targetType {
	case core.MetaTargetTypeTransaction:
		_, err = l.GetTransaction(ctx, targetID.(uint64))
		if err != nil {
			return err
		}

		log = core.NewSetMetadataLog(at, core.SetMetadataLogPayload{
			TargetType: core.MetaTargetTypeTransaction,
			TargetID:   targetID.(uint64),
			Metadata:   m,
		})
	case core.MetaTargetTypeAccount:
		release, err := l.dbCache.LockAccounts(ctx, targetID.(string))
		if err != nil {
			return errors.Wrap(err, "lock account")
		}
		defer release()

		// Machine can access account metadata, so store the metadata until CQRS compute final of the account
		// The cache can still evict the account entry before CQRS part compute the view
		unlock, err := l.locker.Lock(ctx, l.store.Name(), targetID.(string))
		if err != nil {
			return errors.Wrap(err, "lock account")
		}
		defer unlock(context.Background())

		err = l.dbCache.UpdateAccountMetadata(targetID.(string), m)
		if err != nil {
			return errors.Wrap(err, "update account metadata")
		}

		unlock(context.Background())

		log = core.NewSetMetadataLog(at, core.SetMetadataLogPayload{
			TargetType: core.MetaTargetTypeAccount,
			TargetID:   targetID.(string),
			Metadata:   m,
		})
	default:
		return errorsutil.NewError(ErrValidation, errors.Errorf("unknown target type '%s'", targetType))
	}
	if err != nil {
		return err
	}

	err = l.store.AppendLog(ctx, &log)
	logHolder := core.NewLogHolder(&log)
	if err == nil {
		if err := l.writeLog(ctx, logHolder); err != nil {
			return err
		}
	}

	return err
}

func (l *Ledger) GetLogs(ctx context.Context, q storage.LogsQuery) (*api.Cursor[core.Log], error) {
	logs, err := l.store.GetLogs(ctx, q)
	return logs, errors.Wrap(err, "getting logs")
}
