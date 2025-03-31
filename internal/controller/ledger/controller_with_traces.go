package ledger

import (
	"context"
	"database/sql"
	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/internal/tracing"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
)

type ControllerWithTraces struct {
	underlying Controller
	tracer     trace.Tracer

	beginTxHistogram                   metric.Int64Histogram
	commitHistogram                    metric.Int64Histogram
	rollbackHistogram                  metric.Int64Histogram
	listTransactionsHistogram          metric.Int64Histogram
	countTransactionsHistogram         metric.Int64Histogram
	getTransactionHistogram            metric.Int64Histogram
	countAccountsHistogram             metric.Int64Histogram
	listAccountsHistogram              metric.Int64Histogram
	getAccountHistogram                metric.Int64Histogram
	getAggregatedBalancesHistogram     metric.Int64Histogram
	listLogsHistogram                  metric.Int64Histogram
	importHistogram                    metric.Int64Histogram
	exportHistogram                    metric.Int64Histogram
	isDatabaseUpToDateHistogram        metric.Int64Histogram
	getVolumesWithBalancesHistogram    metric.Int64Histogram
	getStatsHistogram                  metric.Int64Histogram
	createTransactionHistogram         metric.Int64Histogram
	revertTransactionHistogram         metric.Int64Histogram
	saveTransactionMetadataHistogram   metric.Int64Histogram
	saveAccountMetadataHistogram       metric.Int64Histogram
	deleteTransactionMetadataHistogram metric.Int64Histogram
	deleteAccountMetadataHistogram     metric.Int64Histogram
}

func NewControllerWithTraces(underlying Controller, tracer trace.Tracer, meter metric.Meter) *ControllerWithTraces {
	ret := &ControllerWithTraces{
		underlying: underlying,
		tracer:     tracer,
	}

	var err error
	ret.beginTxHistogram, err = meter.Int64Histogram("BeginTX")
	if err != nil {
		panic(err)
	}
	ret.listTransactionsHistogram, err = meter.Int64Histogram("ListTransactions")
	if err != nil {
		panic(err)
	}
	ret.commitHistogram, err = meter.Int64Histogram("Commit")
	if err != nil {
		panic(err)
	}
	ret.rollbackHistogram, err = meter.Int64Histogram("Rollback")
	if err != nil {
		panic(err)
	}
	ret.countTransactionsHistogram, err = meter.Int64Histogram("CountTransactions")
	if err != nil {
		panic(err)
	}
	ret.getTransactionHistogram, err = meter.Int64Histogram("GetTransaction")
	if err != nil {
		panic(err)
	}
	ret.countAccountsHistogram, err = meter.Int64Histogram("CountAccounts")
	if err != nil {
		panic(err)
	}
	ret.listAccountsHistogram, err = meter.Int64Histogram("ListAccounts")
	if err != nil {
		panic(err)
	}
	ret.getAccountHistogram, err = meter.Int64Histogram("GetAccount")
	if err != nil {
		panic(err)
	}
	ret.getAggregatedBalancesHistogram, err = meter.Int64Histogram("GetAggregatedBalances")
	if err != nil {
		panic(err)
	}
	ret.listLogsHistogram, err = meter.Int64Histogram("ListLogs")
	if err != nil {
		panic(err)
	}
	ret.importHistogram, err = meter.Int64Histogram("Import")
	if err != nil {
		panic(err)
	}
	ret.exportHistogram, err = meter.Int64Histogram("Export")
	if err != nil {
		panic(err)
	}
	ret.isDatabaseUpToDateHistogram, err = meter.Int64Histogram("IsDatabaseUpToDate")
	if err != nil {
		panic(err)
	}
	ret.getVolumesWithBalancesHistogram, err = meter.Int64Histogram("GetVolumesWithBalances")
	if err != nil {
		panic(err)
	}
	ret.getStatsHistogram, err = meter.Int64Histogram("GetStats")
	if err != nil {
		panic(err)
	}
	ret.createTransactionHistogram, err = meter.Int64Histogram("CreateTransaction")
	if err != nil {
		panic(err)
	}
	ret.revertTransactionHistogram, err = meter.Int64Histogram("RevertTransaction")
	if err != nil {
		panic(err)
	}
	ret.saveTransactionMetadataHistogram, err = meter.Int64Histogram("SaveTransactionMetadata")
	if err != nil {
		panic(err)
	}
	ret.saveAccountMetadataHistogram, err = meter.Int64Histogram("SaveAccountMetadata")
	if err != nil {
		panic(err)
	}
	ret.deleteTransactionMetadataHistogram, err = meter.Int64Histogram("DeleteTransactionMetadata")
	if err != nil {
		panic(err)
	}
	ret.deleteAccountMetadataHistogram, err = meter.Int64Histogram("DeleteAccountMetadata")
	if err != nil {
		panic(err)
	}

	return ret
}

func (c *ControllerWithTraces) BeginTX(ctx context.Context, options *sql.TxOptions) (Controller, *bun.Tx, error) {
	var (
		ctrl Controller
		tx   *bun.Tx
		err  error
	)
	ctrl, err = tracing.TraceWithMetric(
		ctx,
		"BeginTX",
		c.tracer,
		c.beginTxHistogram,
		func(ctx context.Context) (Controller, error) {
			ctrl, tx, err = c.underlying.BeginTX(ctx, options)
			if err != nil {
				return nil, err
			}

			ret := *c
			ret.underlying = ctrl

			return &ret, nil
		},
	)
	if err != nil {
		return nil, nil, err
	}
	return ctrl, tx, nil
}

func (c *ControllerWithTraces) Commit(ctx context.Context) error {
	return tracing.SkipResult(tracing.TraceWithMetric(
		ctx,
		"Commit",
		c.tracer,
		c.commitHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			return c.underlying.Commit(ctx)
		}),
	))
}

func (c *ControllerWithTraces) Rollback(ctx context.Context) error {
	return tracing.SkipResult(tracing.TraceWithMetric(
		ctx,
		"Rollback",
		c.tracer,
		c.rollbackHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			return c.underlying.Rollback(ctx)
		}),
	))
}

func (c *ControllerWithTraces) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return tracing.TraceWithMetric(
		ctx,
		"GetMigrationsInfo",
		c.tracer,
		c.listTransactionsHistogram,
		func(ctx context.Context) ([]migrations.Info, error) {
			return c.underlying.GetMigrationsInfo(ctx)
		},
	)
}

func (c *ControllerWithTraces) ListTransactions(ctx context.Context, q common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Transaction], error) {
	return tracing.TraceWithMetric(
		ctx,
		"ListTransactions",
		c.tracer,
		c.listTransactionsHistogram,
		func(ctx context.Context) (*bunpaginate.Cursor[ledger.Transaction], error) {
			return c.underlying.ListTransactions(ctx, q)
		},
	)
}

func (c *ControllerWithTraces) CountTransactions(ctx context.Context, q common.ResourceQuery[any]) (int, error) {
	return tracing.TraceWithMetric(
		ctx,
		"CountTransactions",
		c.tracer,
		c.countTransactionsHistogram,
		func(ctx context.Context) (int, error) {
			return c.underlying.CountTransactions(ctx, q)
		},
	)
}

func (c *ControllerWithTraces) GetTransaction(ctx context.Context, query common.ResourceQuery[any]) (*ledger.Transaction, error) {
	return tracing.TraceWithMetric(
		ctx,
		"GetTransaction",
		c.tracer,
		c.getTransactionHistogram,
		func(ctx context.Context) (*ledger.Transaction, error) {
			return c.underlying.GetTransaction(ctx, query)
		},
	)
}

func (c *ControllerWithTraces) CountAccounts(ctx context.Context, a common.ResourceQuery[any]) (int, error) {
	return tracing.TraceWithMetric(
		ctx,
		"CountAccounts",
		c.tracer,
		c.countAccountsHistogram,
		func(ctx context.Context) (int, error) {
			return c.underlying.CountAccounts(ctx, a)
		},
	)
}

func (c *ControllerWithTraces) ListAccounts(ctx context.Context, a common.OffsetPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Account], error) {
	return tracing.TraceWithMetric(
		ctx,
		"ListAccounts",
		c.tracer,
		c.listAccountsHistogram,
		func(ctx context.Context) (*bunpaginate.Cursor[ledger.Account], error) {
			return c.underlying.ListAccounts(ctx, a)
		},
	)
}

func (c *ControllerWithTraces) GetAccount(ctx context.Context, q common.ResourceQuery[any]) (*ledger.Account, error) {
	return tracing.TraceWithMetric(
		ctx,
		"GetAccount",
		c.tracer,
		c.getAccountHistogram,
		func(ctx context.Context) (*ledger.Account, error) {
			return c.underlying.GetAccount(ctx, q)
		},
	)
}

func (c *ControllerWithTraces) GetAggregatedBalances(ctx context.Context, q common.ResourceQuery[GetAggregatedVolumesOptions]) (ledger.BalancesByAssets, error) {
	return tracing.TraceWithMetric(
		ctx,
		"GetAggregatedBalances",
		c.tracer,
		c.getAggregatedBalancesHistogram,
		func(ctx context.Context) (ledger.BalancesByAssets, error) {
			return c.underlying.GetAggregatedBalances(ctx, q)
		},
	)
}

func (c *ControllerWithTraces) ListLogs(ctx context.Context, q common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
	return tracing.TraceWithMetric(
		ctx,
		"ListLogs",
		c.tracer,
		c.listLogsHistogram,
		func(ctx context.Context) (*bunpaginate.Cursor[ledger.Log], error) {
			return c.underlying.ListLogs(ctx, q)
		},
	)
}

func (c *ControllerWithTraces) Import(ctx context.Context, stream chan ledger.Log) error {
	return tracing.SkipResult(tracing.TraceWithMetric(
		ctx,
		"Import",
		c.tracer,
		c.importHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			return c.underlying.Import(ctx, stream)
		}),
	))
}

func (c *ControllerWithTraces) Export(ctx context.Context, w ExportWriter) error {
	return tracing.SkipResult(tracing.TraceWithMetric(
		ctx,
		"Export",
		c.tracer,
		c.exportHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			return c.underlying.Export(ctx, w)
		}),
	))
}

func (c *ControllerWithTraces) IsDatabaseUpToDate(ctx context.Context) (bool, error) {
	return tracing.TraceWithMetric(
		ctx,
		"IsDatabaseUpToDate",
		c.tracer,
		c.isDatabaseUpToDateHistogram,
		func(ctx context.Context) (bool, error) {
			return c.underlying.IsDatabaseUpToDate(ctx)
		},
	)
}

func (c *ControllerWithTraces) GetVolumesWithBalances(ctx context.Context, q common.OffsetPaginatedQuery[GetVolumesOptions]) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	return tracing.TraceWithMetric(
		ctx,
		"GetVolumesWithBalances",
		c.tracer,
		c.getVolumesWithBalancesHistogram,
		func(ctx context.Context) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
			return c.underlying.GetVolumesWithBalances(ctx, q)
		},
	)
}

func (c *ControllerWithTraces) CreateTransaction(ctx context.Context, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	var (
		createdTransaction *ledger.CreatedTransaction
		log                *ledger.Log
		err                error
	)
	_, err = tracing.TraceWithMetric(
		ctx,
		"CreateTransaction",
		c.tracer,
		c.createTransactionHistogram,
		func(ctx context.Context) (any, error) {
			log, createdTransaction, err = c.underlying.CreateTransaction(ctx, parameters)
			return nil, err
		},
	)
	if err != nil {
		return nil, nil, err
	}

	return log, createdTransaction, nil
}

func (c *ControllerWithTraces) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	var (
		revertedTransaction *ledger.RevertedTransaction
		log                 *ledger.Log
		err                 error
	)
	_, err = tracing.TraceWithMetric(
		ctx,
		"RevertTransaction",
		c.tracer,
		c.revertTransactionHistogram,
		func(ctx context.Context) (any, error) {
			log, revertedTransaction, err = c.underlying.RevertTransaction(ctx, parameters)
			return nil, err
		},
	)
	if err != nil {
		return nil, nil, err
	}

	return log, revertedTransaction, nil
}

func (c *ControllerWithTraces) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	return tracing.TraceWithMetric(
		ctx,
		"SaveTransactionMetadata",
		c.tracer,
		c.saveTransactionMetadataHistogram,
		func(ctx context.Context) (*ledger.Log, error) {
			return c.underlying.SaveTransactionMetadata(ctx, parameters)
		},
	)
}

func (c *ControllerWithTraces) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	return tracing.TraceWithMetric(
		ctx,
		"SaveAccountMetadata",
		c.tracer,
		c.saveAccountMetadataHistogram,
		func(ctx context.Context) (*ledger.Log, error) {
			return c.underlying.SaveAccountMetadata(ctx, parameters)
		},
	)
}

func (c *ControllerWithTraces) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	return tracing.TraceWithMetric(
		ctx,
		"DeleteTransactionMetadata",
		c.tracer,
		c.deleteTransactionMetadataHistogram,
		func(ctx context.Context) (*ledger.Log, error) {
			return c.underlying.DeleteTransactionMetadata(ctx, parameters)
		},
	)
}

func (c *ControllerWithTraces) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	return tracing.TraceWithMetric(
		ctx,
		"DeleteAccountMetadata",
		c.tracer,
		c.deleteAccountMetadataHistogram,
		func(ctx context.Context) (*ledger.Log, error) {
			return c.underlying.DeleteAccountMetadata(ctx, parameters)
		},
	)
}

func (c *ControllerWithTraces) GetStats(ctx context.Context) (Stats, error) {
	return tracing.TraceWithMetric(
		ctx,
		"GetStats",
		c.tracer,
		c.getStatsHistogram,
		func(ctx context.Context) (Stats, error) {
			return c.underlying.GetStats(ctx)
		},
	)
}

var _ Controller = (*ControllerWithTraces)(nil)
