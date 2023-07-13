package ledger

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/ledger/pkg/bus"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/command"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
)

type Ledger struct {
	commander *command.Commander
	store     *ledgerstore.Store
	projector *query.Projector
}

func (l *Ledger) CreateTransaction(ctx context.Context, parameters command.Parameters, data core.RunScript) (*core.Transaction, error) {
	return l.commander.CreateTransaction(ctx, parameters, data)
}

func (l *Ledger) RevertTransaction(ctx context.Context, parameters command.Parameters, id uint64) (*core.Transaction, error) {
	return l.commander.RevertTransaction(ctx, parameters, id)
}

func (l *Ledger) SaveMeta(ctx context.Context, parameters command.Parameters, targetType string, targetID any, m metadata.Metadata) error {
	return l.commander.SaveMeta(ctx, parameters, targetType, targetID, m)
}

func New(
	name string,
	store *ledgerstore.Store,
	publisher message.Publisher,
	compiler *command.Compiler,
) *Ledger {
	var monitor query.Monitor = query.NewNoOpMonitor()
	if publisher != nil {
		monitor = bus.NewLedgerMonitor(publisher, name)
	}
	metricsRegistry, err := metrics.RegisterPerLedgerMetricsRegistry(name)
	if err != nil {
		panic(err)
	}
	projector := query.NewProjector(store, monitor, metricsRegistry)
	return &Ledger{
		commander: command.New(
			store,
			command.NewDefaultLocker(),
			compiler,
			command.NewReferencer(),
			metricsRegistry,
			projector.QueueLog,
		),
		store:     store,
		projector: projector,
	}
}

func (l *Ledger) Start(ctx context.Context) {
	go l.commander.Run(logging.ContextWithField(ctx, "component", "commander"))
	l.projector.Start(logging.ContextWithField(ctx, "component", "projector"))
}

func (l *Ledger) Close(ctx context.Context) {
	logging.FromContext(ctx).Debugf("Close commander")
	l.commander.Close()

	logging.FromContext(ctx).Debugf("Close projector")
	l.projector.Stop(logging.ContextWithField(ctx, "component", "projector"))
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
