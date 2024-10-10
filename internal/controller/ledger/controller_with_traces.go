package ledger

import (
	"context"
	"github.com/formancehq/go-libs/migrations"
	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
)

type ControllerWithTraces struct {
	underlying Controller
}

func NewControllerWithTraces(underlying Controller) *ControllerWithTraces {
	return &ControllerWithTraces{
		underlying: underlying,
	}
}

func (ctrl *ControllerWithTraces) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return ctrl.underlying.GetMigrationsInfo(ctx)
}

func (ctrl *ControllerWithTraces) ListTransactions(ctx context.Context, q ListTransactionsQuery) (*bunpaginate.Cursor[ledger.Transaction], error) {
	return tracing.Trace(ctx, "ListTransactions", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Transaction], error) {
		return ctrl.underlying.ListTransactions(ctx, q)
	})
}

func (ctrl *ControllerWithTraces) CountTransactions(ctx context.Context, q ListTransactionsQuery) (int, error) {
	return tracing.Trace(ctx, "CountTransactions", func(ctx context.Context) (int, error) {
		return ctrl.underlying.CountTransactions(ctx, q)
	})
}

func (ctrl *ControllerWithTraces) GetTransaction(ctx context.Context, query GetTransactionQuery) (*ledger.Transaction, error) {
	return tracing.Trace(ctx, "GetTransaction", func(ctx context.Context) (*ledger.Transaction, error) {
		return ctrl.underlying.GetTransaction(ctx, query)
	})
}

func (ctrl *ControllerWithTraces) CountAccounts(ctx context.Context, a ListAccountsQuery) (int, error) {
	return tracing.Trace(ctx, "CountAccounts", func(ctx context.Context) (int, error) {
		return ctrl.underlying.CountAccounts(ctx, a)
	})
}

func (ctrl *ControllerWithTraces) ListAccounts(ctx context.Context, a ListAccountsQuery) (*bunpaginate.Cursor[ledger.Account], error) {
	return tracing.Trace(ctx, "ListAccounts", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Account], error) {
		return ctrl.underlying.ListAccounts(ctx, a)
	})
}

func (ctrl *ControllerWithTraces) GetAccount(ctx context.Context, q GetAccountQuery) (*ledger.Account, error) {
	return tracing.Trace(ctx, "GetAccount", func(ctx context.Context) (*ledger.Account, error) {
		return ctrl.underlying.GetAccount(ctx, q)
	})
}

func (ctrl *ControllerWithTraces) GetAggregatedBalances(ctx context.Context, q GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {
	return tracing.Trace(ctx, "GetAggregatedBalances", func(ctx context.Context) (ledger.BalancesByAssets, error) {
		return ctrl.underlying.GetAggregatedBalances(ctx, q)
	})
}

func (ctrl *ControllerWithTraces) ListLogs(ctx context.Context, q GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error) {
	return tracing.Trace(ctx, "ListLogs", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Log], error) {
		return ctrl.underlying.ListLogs(ctx, q)
	})
}

func (ctrl *ControllerWithTraces) Import(ctx context.Context, stream chan ledger.Log) error {
	return tracing.SkipResult(tracing.Trace(ctx, "Import", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.underlying.Import(ctx, stream)
	})))
}

func (ctrl *ControllerWithTraces) Export(ctx context.Context, w ExportWriter) error {
	return tracing.SkipResult(tracing.Trace(ctx, "Export", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.underlying.Export(ctx, w)
	})))
}

func (ctrl *ControllerWithTraces) IsDatabaseUpToDate(ctx context.Context) (bool, error) {
	return tracing.Trace(ctx, "IsDatabaseUpToDate", func(ctx context.Context) (bool, error) {
		return ctrl.underlying.IsDatabaseUpToDate(ctx)
	})
}

func (ctrl *ControllerWithTraces) GetVolumesWithBalances(ctx context.Context, q GetVolumesWithBalancesQuery) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	return tracing.Trace(ctx, "GetVolumesWithBalances", func(ctx context.Context) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
		return ctrl.underlying.GetVolumesWithBalances(ctx, q)
	})
}

func (ctrl *ControllerWithTraces) CreateTransaction(ctx context.Context, parameters Parameters[RunScript]) (*ledger.CreatedTransaction, error) {
	return tracing.Trace(ctx, "CreateTransaction", func(ctx context.Context) (*ledger.CreatedTransaction, error) {
		return ctrl.underlying.CreateTransaction(ctx, parameters)
	})
}

func (ctrl *ControllerWithTraces) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.RevertedTransaction, error) {
	return tracing.Trace(ctx, "RevertTransaction", func(ctx context.Context) (*ledger.RevertedTransaction, error) {
		return ctrl.underlying.RevertTransaction(ctx, parameters)
	})
}

func (ctrl *ControllerWithTraces) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) error {
	return tracing.SkipResult(tracing.Trace(ctx, "SaveTransactionMetadata", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.underlying.SaveTransactionMetadata(ctx, parameters)
	})))
}

func (ctrl *ControllerWithTraces) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) error {
	return tracing.SkipResult(tracing.Trace(ctx, "SaveAccountMetadata", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.underlying.SaveAccountMetadata(ctx, parameters)
	})))
}

func (ctrl *ControllerWithTraces) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) error {
	return tracing.SkipResult(tracing.Trace(ctx, "DeleteTransactionMetadata", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.underlying.DeleteTransactionMetadata(ctx, parameters)
	})))
}

func (ctrl *ControllerWithTraces) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) error {
	return tracing.SkipResult(tracing.Trace(ctx, "DeleteAccountMetadata", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.underlying.DeleteAccountMetadata(ctx, parameters)
	})))
}

func (ctrl *ControllerWithTraces) GetStats(ctx context.Context) (Stats, error) {
	return tracing.Trace(ctx, "GetStats", func(ctx context.Context) (Stats, error) {
		return ctrl.underlying.GetStats(ctx)
	})
}

var _ Controller = (*ControllerWithTraces)(nil)
