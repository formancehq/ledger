package engine

import (
	"context"
	"math/big"
	"sync"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/ledger/internal/engine/chain"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/ledger/internal/storage/systemstore"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/bus"
	"github.com/formancehq/ledger/internal/engine/command"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
)

type Ledger struct {
	commander   *command.Commander
	systemStore *systemstore.Store
	store       *ledgerstore.Store
	mu          sync.Mutex
	config      LedgerConfig
	chain       *chain.Chain
}

type GlobalLedgerConfig struct {
	batchSize int
}

type LedgerConfig struct {
	GlobalLedgerConfig
	driver.LedgerState
	isSchemaUpToDate bool
}

var (
	defaultLedgerConfig = GlobalLedgerConfig{
		batchSize: 50,
	}
)

func New(
	systemStore *systemstore.Store,
	store *ledgerstore.Store,
	publisher message.Publisher,
	compiler *command.Compiler,
	ledgerConfig LedgerConfig,
) *Ledger {
	var monitor bus.Monitor = bus.NewNoOpMonitor()
	if publisher != nil {
		monitor = bus.NewLedgerMonitor(publisher, store.Name())
	}
	chain := chain.New(store)
	ret := &Ledger{
		commander: command.New(
			store,
			command.NewDefaultLocker(),
			compiler,
			command.NewReferencer(),
			monitor,
			chain,
			ledgerConfig.batchSize,
		),
		store:       store,
		config:      ledgerConfig,
		systemStore: systemStore,
		chain:       chain,
	}
	return ret
}

func (l *Ledger) Start(ctx context.Context) {
	if err := l.chain.Init(ctx); err != nil {
		panic(err)
	}
	go l.commander.Run(logging.ContextWithField(ctx, "component", "commander"))
}

func (l *Ledger) Close(ctx context.Context) {
	logging.FromContext(ctx).Debugf("Close commander")
	l.commander.Close()
}

func (l *Ledger) GetTransactions(ctx context.Context, q ledgerstore.GetTransactionsQuery) (*bunpaginate.Cursor[ledger.ExpandedTransaction], error) {
	txs, err := l.store.GetTransactions(ctx, q)
	return txs, newStorageError(err, "getting transactions")
}

func (l *Ledger) CountTransactions(ctx context.Context, q ledgerstore.GetTransactionsQuery) (int, error) {
	count, err := l.store.CountTransactions(ctx, q)
	return count, newStorageError(err, "counting transactions")
}

func (l *Ledger) GetTransactionWithVolumes(ctx context.Context, query ledgerstore.GetTransactionQuery) (*ledger.ExpandedTransaction, error) {
	tx, err := l.store.GetTransactionWithVolumes(ctx, query)
	return tx, newStorageError(err, "getting transaction")
}

func (l *Ledger) CountAccounts(ctx context.Context, a ledgerstore.GetAccountsQuery) (int, error) {
	count, err := l.store.CountAccounts(ctx, a)
	return count, newStorageError(err, "counting accounts")
}

func (l *Ledger) GetAccountsWithVolumes(ctx context.Context, a ledgerstore.GetAccountsQuery) (*bunpaginate.Cursor[ledger.ExpandedAccount], error) {
	accounts, err := l.store.GetAccountsWithVolumes(ctx, a)
	return accounts, newStorageError(err, "getting accounts")
}

func (l *Ledger) GetAccountWithVolumes(ctx context.Context, q ledgerstore.GetAccountQuery) (*ledger.ExpandedAccount, error) {
	accounts, err := l.store.GetAccountWithVolumes(ctx, q)
	return accounts, newStorageError(err, "getting account")
}

func (l *Ledger) GetAggregatedBalances(ctx context.Context, q ledgerstore.GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {
	balances, err := l.store.GetAggregatedBalances(ctx, q)
	return balances, newStorageError(err, "getting balances aggregated")
}

func (l *Ledger) GetLogs(ctx context.Context, q ledgerstore.GetLogsQuery) (*bunpaginate.Cursor[ledger.ChainedLog], error) {
	logs, err := l.store.GetLogs(ctx, q)
	return logs, newStorageError(err, "getting logs")
}

func (l *Ledger) markInUseIfNeeded(ctx context.Context) {
	if l.config.LedgerState.State == systemstore.StateInitializing {
		if err := l.systemStore.UpdateLedgerState(ctx, l.store.Name(), systemstore.StateInUse); err != nil {
			logging.FromContext(ctx).Error("Unable to declare ledger as in use")
			return
		}
		l.config.LedgerState.State = systemstore.StateInUse
	}
}

func (l *Ledger) CreateTransaction(ctx context.Context, parameters command.Parameters, data ledger.RunScript) (*ledger.Transaction, error) {
	ret, err := l.commander.CreateTransaction(ctx, parameters, data)
	if err != nil {
		return nil, NewCommandError(err)
	}
	l.markInUseIfNeeded(ctx)
	return ret, nil
}

func (l *Ledger) RevertTransaction(ctx context.Context, parameters command.Parameters, id *big.Int, force, atEffectiveDate bool) (*ledger.Transaction, error) {
	ret, err := l.commander.RevertTransaction(ctx, parameters, id, force, atEffectiveDate)
	if err != nil {
		return nil, NewCommandError(err)
	}
	l.markInUseIfNeeded(ctx)
	return ret, nil
}

func (l *Ledger) SaveMeta(ctx context.Context, parameters command.Parameters, targetType string, targetID any, m metadata.Metadata) error {
	if err := l.commander.SaveMeta(ctx, parameters, targetType, targetID, m); err != nil {
		return NewCommandError(err)
	}

	l.markInUseIfNeeded(ctx)
	return nil
}

func (l *Ledger) DeleteMetadata(ctx context.Context, parameters command.Parameters, targetType string, targetID any, key string) error {
	if err := l.commander.DeleteMetadata(ctx, parameters, targetType, targetID, key); err != nil {
		return NewCommandError(err)
	}

	l.markInUseIfNeeded(ctx)
	return nil
}

func (l *Ledger) IsDatabaseUpToDate(ctx context.Context) (bool, error) {
	if l.config.isSchemaUpToDate {
		return true, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.config.isSchemaUpToDate {
		return true, nil
	}

	var err error
	l.config.isSchemaUpToDate, err = l.store.IsUpToDate(ctx)

	return l.config.isSchemaUpToDate, err
}

func (l *Ledger) GetVolumesWithBalances(ctx context.Context, q ledgerstore.GetVolumesWithBalancesQuery) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	volumes, err := l.store.GetVolumesWithBalances(ctx, q)
	return volumes, newStorageError(err, "getting Volumes with balances")
}
