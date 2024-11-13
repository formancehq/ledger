package ledger

import (
	"context"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/formancehq/ledger/internal/tracing"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
)

type ControllerWithTraces struct {
	underlying Controller
	tracer     trace.Tracer
}

func NewControllerWithTraces(underlying Controller, tracer trace.Tracer) *ControllerWithTraces {
	return &ControllerWithTraces{
		underlying: underlying,
		tracer:     tracer,
	}
}

func (ctrl *ControllerWithTraces) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return ctrl.underlying.GetMigrationsInfo(ctx)
}

func (ctrl *ControllerWithTraces) ListTransactions(ctx context.Context, q ListTransactionsQuery) (*bunpaginate.Cursor[ledger.Transaction], error) {
	return tracing.Trace(ctx, ctrl.tracer, "ListTransactions", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Transaction], error) {
		return ctrl.underlying.ListTransactions(ctx, q)
	})
}

func (ctrl *ControllerWithTraces) CountTransactions(ctx context.Context, q ListTransactionsQuery) (int, error) {
	return tracing.Trace(ctx, ctrl.tracer, "CountTransactions", func(ctx context.Context) (int, error) {
		return ctrl.underlying.CountTransactions(ctx, q)
	})
}

func (ctrl *ControllerWithTraces) GetTransaction(ctx context.Context, query GetTransactionQuery) (*ledger.Transaction, error) {
	return tracing.Trace(ctx, ctrl.tracer, "GetTransaction", func(ctx context.Context) (*ledger.Transaction, error) {
		return ctrl.underlying.GetTransaction(ctx, query)
	})
}

func (ctrl *ControllerWithTraces) CountAccounts(ctx context.Context, a ListAccountsQuery) (int, error) {
	return tracing.Trace(ctx, ctrl.tracer, "CountAccounts", func(ctx context.Context) (int, error) {
		return ctrl.underlying.CountAccounts(ctx, a)
	})
}

func (ctrl *ControllerWithTraces) ListAccounts(ctx context.Context, a ListAccountsQuery) (*bunpaginate.Cursor[ledger.Account], error) {
	return tracing.Trace(ctx, ctrl.tracer, "ListAccounts", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Account], error) {
		return ctrl.underlying.ListAccounts(ctx, a)
	})
}

func (ctrl *ControllerWithTraces) GetAccount(ctx context.Context, q GetAccountQuery) (*ledger.Account, error) {
	return tracing.Trace(ctx, ctrl.tracer, "GetAccount", func(ctx context.Context) (*ledger.Account, error) {
		return ctrl.underlying.GetAccount(ctx, q)
	})
}

func (ctrl *ControllerWithTraces) GetAggregatedBalances(ctx context.Context, q GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {
	return tracing.Trace(ctx, ctrl.tracer, "GetAggregatedBalances", func(ctx context.Context) (ledger.BalancesByAssets, error) {
		return ctrl.underlying.GetAggregatedBalances(ctx, q)
	})
}

func (ctrl *ControllerWithTraces) ListLogs(ctx context.Context, q GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error) {
	return tracing.Trace(ctx, ctrl.tracer, "ListLogs", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Log], error) {
		return ctrl.underlying.ListLogs(ctx, q)
	})
}

func (ctrl *ControllerWithTraces) Import(ctx context.Context, stream chan ledger.Log) error {
	return tracing.SkipResult(tracing.Trace(ctx, ctrl.tracer, "Import", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.underlying.Import(ctx, stream)
	})))
}

func (ctrl *ControllerWithTraces) Export(ctx context.Context, w ExportWriter) error {
	return tracing.SkipResult(tracing.Trace(ctx, ctrl.tracer, "Export", tracing.NoResult(func(ctx context.Context) error {
		return ctrl.underlying.Export(ctx, w)
	})))
}

func (ctrl *ControllerWithTraces) IsDatabaseUpToDate(ctx context.Context) (bool, error) {
	return tracing.Trace(ctx, ctrl.tracer, "IsDatabaseUpToDate", func(ctx context.Context) (bool, error) {
		return ctrl.underlying.IsDatabaseUpToDate(ctx)
	})
}

func (ctrl *ControllerWithTraces) GetVolumesWithBalances(ctx context.Context, q GetVolumesWithBalancesQuery) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	return tracing.Trace(ctx, ctrl.tracer, "GetVolumesWithBalances", func(ctx context.Context) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
		return ctrl.underlying.GetVolumesWithBalances(ctx, q)
	})
}

func (ctrl *ControllerWithTraces) CreateTransaction(ctx context.Context, parameters Parameters[RunScript]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	ctx, span := ctrl.tracer.Start(ctx, "CreateTransaction")
	defer span.End()

	return ctrl.underlying.CreateTransaction(ctx, parameters)
}

func (ctrl *ControllerWithTraces) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	ctx, span := ctrl.tracer.Start(ctx, "RevertTransaction")
	defer span.End()

	return ctrl.underlying.RevertTransaction(ctx, parameters)
}

func (ctrl *ControllerWithTraces) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	ctx, span := ctrl.tracer.Start(ctx, "SaveTransactionMetadata")
	defer span.End()

	return ctrl.underlying.SaveTransactionMetadata(ctx, parameters)
}

func (ctrl *ControllerWithTraces) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	ctx, span := ctrl.tracer.Start(ctx, "SaveAccountMetadata")
	defer span.End()

	return ctrl.underlying.SaveAccountMetadata(ctx, parameters)
}

func (ctrl *ControllerWithTraces) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	ctx, span := ctrl.tracer.Start(ctx, "DeleteTransactionMetadata")
	defer span.End()

	return ctrl.underlying.DeleteTransactionMetadata(ctx, parameters)
}

func (ctrl *ControllerWithTraces) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	ctx, span := ctrl.tracer.Start(ctx, "DeleteAccountMetadata")
	defer span.End()

	return ctrl.underlying.DeleteAccountMetadata(ctx, parameters)
}

func (ctrl *ControllerWithTraces) GetStats(ctx context.Context) (Stats, error) {
	return tracing.Trace(ctx, ctrl.tracer, "GetStats", func(ctx context.Context) (Stats, error) {
		return ctrl.underlying.GetStats(ctx)
	})
}

var _ Controller = (*ControllerWithTraces)(nil)
