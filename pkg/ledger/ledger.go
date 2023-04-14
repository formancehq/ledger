package ledger

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/ledger/pkg/ledger/revert"
	"github.com/formancehq/ledger/pkg/ledger/runner"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
)

type Ledger struct {
	*revert.Reverter
	runner          *runner.Runner
	store           storage.LedgerStore
	locker          *lock.Locker
	dbCache         *cache.Cache
	queryWorker     *query.Worker
	metricsRegistry metrics.PerLedgerMetricsRegistry
}

func New(
	store storage.LedgerStore,
	dbCache *cache.Cache,
	runner *runner.Runner,
	locker *lock.Locker,
	queryWorker *query.Worker,
	metricsRegistry metrics.PerLedgerMetricsRegistry,
) *Ledger {
	return &Ledger{
		Reverter:        revert.NewReverter(store, runner, queryWorker),
		store:           store,
		dbCache:         dbCache,
		runner:          runner,
		locker:          locker,
		queryWorker:     queryWorker,
		metricsRegistry: metricsRegistry,
	}
}

func (l *Ledger) Close(ctx context.Context) error {

	if err := l.queryWorker.Stop(ctx); err != nil {
		return errors.Wrap(err, "stopping query worker")
	}

	if err := l.locker.Stop(ctx); err != nil {
		return errors.Wrap(err, "stopping cache")
	}

	if err := l.dbCache.Stop(ctx); err != nil {
		return errors.Wrap(err, "stopping cache")
	}

	if err := l.store.Close(ctx); err != nil {
		return errors.Wrap(err, "closing store")
	}

	return nil
}

func (l *Ledger) GetLedgerStore() storage.LedgerStore {
	return l.store
}

func (l *Ledger) CreateTransaction(ctx context.Context, dryRun, async bool, script core.RunScript) (*core.ExpandedTransaction, error) {
	tx, logHolder, err := l.runner.Execute(ctx, script, dryRun, func(expandedTx core.ExpandedTransaction, accountMetadata map[string]metadata.Metadata) core.Log {
		return core.NewTransactionLog(expandedTx.Transaction, accountMetadata)
	})

	if err == nil && !dryRun {
		if err := l.queryWorker.QueueLog(ctx, logHolder, async); err != nil {
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
func (l *Ledger) SaveMeta(ctx context.Context, targetType string, targetID interface{}, m metadata.Metadata, async bool) error {
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
		err     error
		log     core.Log
		release cache.Release = func() {}
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
		unlock, err := l.locker.Lock(ctx, lock.Accounts{
			Write: []string{targetID.(string)},
		})
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
	if err == nil {
		logHolder := core.NewLogHolder(&log)
		if err := l.queryWorker.QueueLog(ctx, logHolder, async); err != nil {
			release()
			return errors.Wrap(err, "writing log")
		}
		go func() {
			<-logHolder.Ingested
			release()
		}()
	} else {
		release()
	}

	return err
}

func (l *Ledger) GetLogs(ctx context.Context, q storage.LogsQuery) (*api.Cursor[core.Log], error) {
	logs, err := l.store.GetLogs(ctx, q)
	return logs, errors.Wrap(err, "getting logs")
}
