package ledger

import (
	"context"
	"database/sql"
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

func (c *ControllerWithTraces) BeginTX(ctx context.Context, options *sql.TxOptions) (Controller, error) {
	return tracing.Trace(ctx, c.tracer, "BeginTX", func(ctx context.Context) (Controller, error) {
		ctrl, err := c.underlying.BeginTX(ctx, options)
		if err != nil {
			return nil, err
		}

		return &ControllerWithTraces{
			underlying: ctrl,
			tracer:     c.tracer,
		}, nil
	})
}

func (c *ControllerWithTraces) Commit(ctx context.Context) error {
	return tracing.SkipResult(tracing.Trace(ctx, c.tracer, "BeginTX", tracing.NoResult(func(ctx context.Context) error {
		return c.underlying.Commit(ctx)
	})))
}

func (c *ControllerWithTraces) Rollback(ctx context.Context) error {
	return tracing.SkipResult(tracing.Trace(ctx, c.tracer, "BeginTX", tracing.NoResult(func(ctx context.Context) error {
		return c.underlying.Rollback(ctx)
	})))
}

func (c *ControllerWithTraces) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return c.underlying.GetMigrationsInfo(ctx)
}

func (c *ControllerWithTraces) ListTransactions(ctx context.Context, q ListTransactionsQuery) (*bunpaginate.Cursor[ledger.Transaction], error) {
	return tracing.Trace(ctx, c.tracer, "ListTransactions", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Transaction], error) {
		return c.underlying.ListTransactions(ctx, q)
	})
}

func (c *ControllerWithTraces) CountTransactions(ctx context.Context, q ListTransactionsQuery) (int, error) {
	return tracing.Trace(ctx, c.tracer, "CountTransactions", func(ctx context.Context) (int, error) {
		return c.underlying.CountTransactions(ctx, q)
	})
}

func (c *ControllerWithTraces) GetTransaction(ctx context.Context, query GetTransactionQuery) (*ledger.Transaction, error) {
	return tracing.Trace(ctx, c.tracer, "GetTransaction", func(ctx context.Context) (*ledger.Transaction, error) {
		return c.underlying.GetTransaction(ctx, query)
	})
}

func (c *ControllerWithTraces) CountAccounts(ctx context.Context, a ListAccountsQuery) (int, error) {
	return tracing.Trace(ctx, c.tracer, "CountAccounts", func(ctx context.Context) (int, error) {
		return c.underlying.CountAccounts(ctx, a)
	})
}

func (c *ControllerWithTraces) ListAccounts(ctx context.Context, a ListAccountsQuery) (*bunpaginate.Cursor[ledger.Account], error) {
	return tracing.Trace(ctx, c.tracer, "ListAccounts", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Account], error) {
		return c.underlying.ListAccounts(ctx, a)
	})
}

func (c *ControllerWithTraces) GetAccount(ctx context.Context, q GetAccountQuery) (*ledger.Account, error) {
	return tracing.Trace(ctx, c.tracer, "GetAccount", func(ctx context.Context) (*ledger.Account, error) {
		return c.underlying.GetAccount(ctx, q)
	})
}

func (c *ControllerWithTraces) GetAggregatedBalances(ctx context.Context, q GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {
	return tracing.Trace(ctx, c.tracer, "GetAggregatedBalances", func(ctx context.Context) (ledger.BalancesByAssets, error) {
		return c.underlying.GetAggregatedBalances(ctx, q)
	})
}

func (c *ControllerWithTraces) ListLogs(ctx context.Context, q GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error) {
	return tracing.Trace(ctx, c.tracer, "ListLogs", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Log], error) {
		return c.underlying.ListLogs(ctx, q)
	})
}

func (c *ControllerWithTraces) Import(ctx context.Context, stream chan ledger.Log) error {
	return tracing.SkipResult(tracing.Trace(ctx, c.tracer, "Import", tracing.NoResult(func(ctx context.Context) error {
		return c.underlying.Import(ctx, stream)
	})))
}

func (c *ControllerWithTraces) Export(ctx context.Context, w ExportWriter) error {
	return tracing.SkipResult(tracing.Trace(ctx, c.tracer, "Export", tracing.NoResult(func(ctx context.Context) error {
		return c.underlying.Export(ctx, w)
	})))
}

func (c *ControllerWithTraces) IsDatabaseUpToDate(ctx context.Context) (bool, error) {
	return tracing.Trace(ctx, c.tracer, "IsDatabaseUpToDate", func(ctx context.Context) (bool, error) {
		return c.underlying.IsDatabaseUpToDate(ctx)
	})
}

func (c *ControllerWithTraces) GetVolumesWithBalances(ctx context.Context, q GetVolumesWithBalancesQuery) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	return tracing.Trace(ctx, c.tracer, "GetVolumesWithBalances", func(ctx context.Context) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
		return c.underlying.GetVolumesWithBalances(ctx, q)
	})
}

func (c *ControllerWithTraces) CreateTransaction(ctx context.Context, parameters Parameters[RunScript]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	ctx, span := c.tracer.Start(ctx, "CreateTransaction")
	defer span.End()

	return c.underlying.CreateTransaction(ctx, parameters)
}

func (c *ControllerWithTraces) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	ctx, span := c.tracer.Start(ctx, "RevertTransaction")
	defer span.End()

	return c.underlying.RevertTransaction(ctx, parameters)
}

func (c *ControllerWithTraces) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	ctx, span := c.tracer.Start(ctx, "SaveTransactionMetadata")
	defer span.End()

	return c.underlying.SaveTransactionMetadata(ctx, parameters)
}

func (c *ControllerWithTraces) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	ctx, span := c.tracer.Start(ctx, "SaveAccountMetadata")
	defer span.End()

	return c.underlying.SaveAccountMetadata(ctx, parameters)
}

func (c *ControllerWithTraces) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	ctx, span := c.tracer.Start(ctx, "DeleteTransactionMetadata")
	defer span.End()

	return c.underlying.DeleteTransactionMetadata(ctx, parameters)
}

func (c *ControllerWithTraces) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	ctx, span := c.tracer.Start(ctx, "DeleteAccountMetadata")
	defer span.End()

	return c.underlying.DeleteAccountMetadata(ctx, parameters)
}

func (c *ControllerWithTraces) GetStats(ctx context.Context) (Stats, error) {
	return tracing.Trace(ctx, c.tracer, "GetStats", func(ctx context.Context) (Stats, error) {
		return c.underlying.GetStats(ctx)
	})
}

var _ Controller = (*ControllerWithTraces)(nil)
