package ledger

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/ledger/command"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/pkg/errors"
)

type Ledger struct {
	*command.Commander
	store       *ledgerstore.Store
	queryWorker *query.Worker
	locker      *command.DefaultLocker
	cache       *cache.Cache
}

func New(
	store *ledgerstore.Store,
	cache *cache.Cache,
	locker *command.DefaultLocker,
	queryWorker *query.Worker,
	state *command.State,
	compiler *command.Compiler,
	metricsRegistry metrics.PerLedgerMetricsRegistry,
) *Ledger {
	store.OnLogWrote(func(logs []*ledgerstore.AppendedLog) {
		if err := queryWorker.QueueLog(context.Background(), logs...); err != nil {
			panic(err)
		}
	})
	return &Ledger{
		Commander:   command.New(store, cache, locker, state, compiler, metricsRegistry),
		store:       store,
		queryWorker: queryWorker,
		locker:      locker,
		cache:       cache,
	}
}

func (l *Ledger) Close(ctx context.Context) error {

	l.Commander.Wait()

	if err := l.store.Stop(ctx); err != nil {
		return errors.Wrap(err, "stopping ledger store")
	}

	if err := l.queryWorker.Stop(ctx); err != nil {
		return errors.Wrap(err, "stopping query worker")
	}

	if err := l.locker.Stop(ctx); err != nil {
		return errors.Wrap(err, "stopping cache")
	}

	if err := l.cache.Stop(ctx); err != nil {
		return errors.Wrap(err, "stopping cache")
	}

	return nil
}

func (l *Ledger) GetTransactions(ctx context.Context, q ledgerstore.TransactionsQuery) (*api.Cursor[core.ExpandedTransaction], error) {
	txs, err := l.store.GetTransactions(ctx, q)
	return txs, errors.Wrap(err, "getting transactions")
}

func (l *Ledger) CountTransactions(ctx context.Context, q ledgerstore.TransactionsQuery) (uint64, error) {
	count, err := l.store.CountTransactions(ctx, q)
	return count, errors.Wrap(err, "counting transactions")
}

func (l *Ledger) GetTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, error) {
	tx, err := l.store.GetTransaction(ctx, id)
	return tx, errors.Wrap(err, "getting transaction")
}

func (l *Ledger) CountAccounts(ctx context.Context, a ledgerstore.AccountsQuery) (uint64, error) {
	count, err := l.store.CountAccounts(ctx, a)
	return count, errors.Wrap(err, "counting accounts")
}

func (l *Ledger) GetAccounts(ctx context.Context, a ledgerstore.AccountsQuery) (*api.Cursor[core.Account], error) {
	accounts, err := l.store.GetAccounts(ctx, a)
	return accounts, errors.Wrap(err, "getting accounts")
}

func (l *Ledger) GetAccount(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	accounts, err := l.store.GetAccountWithVolumes(ctx, address)
	return accounts, errors.Wrap(err, "getting account")
}

func (l *Ledger) GetBalances(ctx context.Context, q ledgerstore.BalancesQuery) (*api.Cursor[core.AccountsBalances], error) {
	balances, err := l.store.GetBalances(ctx, q)
	return balances, errors.Wrap(err, "getting balances")
}

func (l *Ledger) GetBalancesAggregated(ctx context.Context, q ledgerstore.BalancesQuery) (core.AssetsBalances, error) {
	balances, err := l.store.GetBalancesAggregated(ctx, q)
	return balances, errors.Wrap(err, "getting balances aggregated")
}

func (l *Ledger) GetLogs(ctx context.Context, q ledgerstore.LogsQuery) (*api.Cursor[core.PersistedLog], error) {
	logs, err := l.store.GetLogs(ctx, q)
	return logs, errors.Wrap(err, "getting logs")
}
