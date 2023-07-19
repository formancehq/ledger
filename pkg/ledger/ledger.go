package ledger

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/command"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"
)

type Ledger struct {
	*command.Commander
	store     *ledgerstore.Store
	projector *query.Projector
	locker    *command.DefaultLocker
}

func New(
	store *ledgerstore.Store,
	locker *command.DefaultLocker,
	queryWorker *query.Projector,
	compiler *command.Compiler,
	metricsRegistry metrics.PerLedgerRegistry,
) *Ledger {
	store.OnLogWrote(func(logs []*core.ActiveLog) {
		if err := queryWorker.QueueLog(logs...); err != nil {
			panic(err)
		}
	})
	return &Ledger{
		Commander: command.New(store, locker, compiler, command.NewReferencer(), metricsRegistry),
		store:     store,
		projector: queryWorker,
		locker:    locker,
	}
}

func (l *Ledger) Close(ctx context.Context) error {

	logging.FromContext(ctx).Debugf("Close commander")
	l.Commander.Wait()

	logging.FromContext(ctx).Debugf("Close storage worker")
	if err := l.store.Stop(logging.ContextWithField(ctx, "component", "store")); err != nil {
		return errors.Wrap(err, "stopping ledger store")
	}

	logging.FromContext(ctx).Debugf("Close projector")
	l.projector.Stop(logging.ContextWithField(ctx, "component", "projector"))

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

func (l *Ledger) GetBalances(ctx context.Context, q ledgerstore.BalancesQuery) (*api.Cursor[core.BalancesByAssetsByAccounts], error) {
	balances, err := l.store.GetBalances(ctx, q)
	return balances, errors.Wrap(err, "getting balances")
}

func (l *Ledger) GetBalancesAggregated(ctx context.Context, q ledgerstore.BalancesQuery) (core.BalancesByAssets, error) {
	balances, err := l.store.GetBalancesAggregated(ctx, q)
	return balances, errors.Wrap(err, "getting balances aggregated")
}

func (l *Ledger) GetLogs(ctx context.Context, q ledgerstore.LogsQuery) (*api.Cursor[core.ChainedLog], error) {
	logs, err := l.store.GetLogs(ctx, q)
	return logs, errors.Wrap(err, "getting logs")
}
