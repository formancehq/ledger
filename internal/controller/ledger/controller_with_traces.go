package ledger

import (
	"context"
	"database/sql"

	"github.com/formancehq/go-libs/v3/migrations"
	"github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/formancehq/ledger/internal/tracing"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
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
	lockLedgerHistogram                metric.Int64Histogram
}

func (c *ControllerWithTraces) Info() ledger.Ledger {
	return c.underlying.Info()
}

func NewControllerWithTraces(underlying Controller, tracer trace.Tracer, meter metric.Meter) *ControllerWithTraces {
	ret := &ControllerWithTraces{
		underlying: underlying,
		tracer:     tracer,
	}

	var err error
	ret.beginTxHistogram, err = meter.Int64Histogram("controller.begin_tx")
	if err != nil {
		panic(err)
	}
	ret.listTransactionsHistogram, err = meter.Int64Histogram("controller.list_transactions")
	if err != nil {
		panic(err)
	}
	ret.commitHistogram, err = meter.Int64Histogram("controller.commit")
	if err != nil {
		panic(err)
	}
	ret.rollbackHistogram, err = meter.Int64Histogram("controller.rollback")
	if err != nil {
		panic(err)
	}
	ret.countTransactionsHistogram, err = meter.Int64Histogram("controller.count_transactions")
	if err != nil {
		panic(err)
	}
	ret.getTransactionHistogram, err = meter.Int64Histogram("controller.get_transaction")
	if err != nil {
		panic(err)
	}
	ret.countAccountsHistogram, err = meter.Int64Histogram("controller.count_accounts")
	if err != nil {
		panic(err)
	}
	ret.listAccountsHistogram, err = meter.Int64Histogram("controller.list_accounts")
	if err != nil {
		panic(err)
	}
	ret.getAccountHistogram, err = meter.Int64Histogram("controller.get_account")
	if err != nil {
		panic(err)
	}
	ret.getAggregatedBalancesHistogram, err = meter.Int64Histogram("controller.get_aggregated_balances")
	if err != nil {
		panic(err)
	}
	ret.listLogsHistogram, err = meter.Int64Histogram("controller.list_logs")
	if err != nil {
		panic(err)
	}
	ret.importHistogram, err = meter.Int64Histogram("controller.import")
	if err != nil {
		panic(err)
	}
	ret.exportHistogram, err = meter.Int64Histogram("controller.export")
	if err != nil {
		panic(err)
	}
	ret.isDatabaseUpToDateHistogram, err = meter.Int64Histogram("controller.is_database_up_to_date")
	if err != nil {
		panic(err)
	}
	ret.getVolumesWithBalancesHistogram, err = meter.Int64Histogram("controller.get_volumes_with_balances")
	if err != nil {
		panic(err)
	}
	ret.getStatsHistogram, err = meter.Int64Histogram("controller.get_stats")
	if err != nil {
		panic(err)
	}
	ret.createTransactionHistogram, err = meter.Int64Histogram("controller.create_transaction")
	if err != nil {
		panic(err)
	}
	ret.revertTransactionHistogram, err = meter.Int64Histogram("controller.revert_transaction")
	if err != nil {
		panic(err)
	}
	ret.saveTransactionMetadataHistogram, err = meter.Int64Histogram("controller.save_transaction_metadata")
	if err != nil {
		panic(err)
	}
	ret.saveAccountMetadataHistogram, err = meter.Int64Histogram("controller.save_account_metadata")
	if err != nil {
		panic(err)
	}
	ret.deleteTransactionMetadataHistogram, err = meter.Int64Histogram("controller.delete_transaction_metadata")
	if err != nil {
		panic(err)
	}
	ret.deleteAccountMetadataHistogram, err = meter.Int64Histogram("controller.delete_account_metadata")
	if err != nil {
		panic(err)
	}
	ret.lockLedgerHistogram, err = meter.Int64Histogram("controller.lock_ledger")
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
	ctrl, err = tracing.TraceWithMetricWithAttributes(
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
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
	if err != nil {
		return nil, nil, err
	}
	return ctrl, tx, nil
}

func (c *ControllerWithTraces) Commit(ctx context.Context) error {
	return tracing.SkipResult(tracing.TraceWithMetricWithAttributes(
		ctx,
		"Commit",
		c.tracer,
		c.commitHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			return c.underlying.Commit(ctx)
		}),
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	))
}

func (c *ControllerWithTraces) Rollback(ctx context.Context) error {
	return tracing.SkipResult(tracing.TraceWithMetricWithAttributes(
		ctx,
		"Rollback",
		c.tracer,
		c.rollbackHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			return c.underlying.Rollback(ctx)
		}),
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	))
}

func (c *ControllerWithTraces) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return tracing.TraceWithMetricWithAttributes(
		ctx,
		"GetMigrationsInfo",
		c.tracer,
		c.listTransactionsHistogram,
		func(ctx context.Context) ([]migrations.Info, error) {
			return c.underlying.GetMigrationsInfo(ctx)
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
}

func (c *ControllerWithTraces) ListTransactions(ctx context.Context, q common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Transaction], error) {
	return tracing.TraceWithMetricWithAttributes(
		ctx,
		"ListTransactions",
		c.tracer,
		c.listTransactionsHistogram,
		func(ctx context.Context) (*bunpaginate.Cursor[ledger.Transaction], error) {
			return c.underlying.ListTransactions(ctx, q)
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
}

func (c *ControllerWithTraces) CountTransactions(ctx context.Context, q common.ResourceQuery[any]) (int, error) {
	return tracing.TraceWithMetricWithAttributes(
		ctx,
		"CountTransactions",
		c.tracer,
		c.countTransactionsHistogram,
		func(ctx context.Context) (int, error) {
			return c.underlying.CountTransactions(ctx, q)
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
}

func (c *ControllerWithTraces) GetTransaction(ctx context.Context, query common.ResourceQuery[any]) (*ledger.Transaction, error) {
	return tracing.TraceWithMetricWithAttributes(
		ctx,
		"GetTransaction",
		c.tracer,
		c.getTransactionHistogram,
		func(ctx context.Context) (*ledger.Transaction, error) {
			return c.underlying.GetTransaction(ctx, query)
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
}

func (c *ControllerWithTraces) CountAccounts(ctx context.Context, a common.ResourceQuery[any]) (int, error) {
	return tracing.TraceWithMetricWithAttributes(
		ctx,
		"CountAccounts",
		c.tracer,
		c.countAccountsHistogram,
		func(ctx context.Context) (int, error) {
			return c.underlying.CountAccounts(ctx, a)
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
}

func (c *ControllerWithTraces) ListAccounts(ctx context.Context, a common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Account], error) {
	return tracing.TraceWithMetricWithAttributes(
		ctx,
		"ListAccounts",
		c.tracer,
		c.listAccountsHistogram,
		func(ctx context.Context) (*bunpaginate.Cursor[ledger.Account], error) {
			return c.underlying.ListAccounts(ctx, a)
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
}

func (c *ControllerWithTraces) GetAccount(ctx context.Context, q common.ResourceQuery[any]) (*ledger.Account, error) {
	return tracing.TraceWithMetricWithAttributes(
		ctx,
		"GetAccount",
		c.tracer,
		c.getAccountHistogram,
		func(ctx context.Context) (*ledger.Account, error) {
			return c.underlying.GetAccount(ctx, q)
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
}

func (c *ControllerWithTraces) GetAggregatedBalances(ctx context.Context, q common.ResourceQuery[ledgerstore.GetAggregatedVolumesOptions]) (ledger.BalancesByAssets, error) {
	return tracing.TraceWithMetricWithAttributes(
		ctx,
		"GetAggregatedBalances",
		c.tracer,
		c.getAggregatedBalancesHistogram,
		func(ctx context.Context) (ledger.BalancesByAssets, error) {
			return c.underlying.GetAggregatedBalances(ctx, q)
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
}

func (c *ControllerWithTraces) ListLogs(ctx context.Context, q common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
	return tracing.TraceWithMetricWithAttributes(
		ctx,
		"ListLogs",
		c.tracer,
		c.listLogsHistogram,
		func(ctx context.Context) (*bunpaginate.Cursor[ledger.Log], error) {
			return c.underlying.ListLogs(ctx, q)
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
}

func (c *ControllerWithTraces) Import(ctx context.Context, stream chan ledger.Log) error {
	return tracing.SkipResult(tracing.TraceWithMetricWithAttributes(
		ctx,
		"Import",
		c.tracer,
		c.importHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			return c.underlying.Import(ctx, stream)
		}),
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	))
}

func (c *ControllerWithTraces) Export(ctx context.Context, w ExportWriter) error {
	return tracing.SkipResult(tracing.TraceWithMetricWithAttributes(
		ctx,
		"Export",
		c.tracer,
		c.exportHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			return c.underlying.Export(ctx, w)
		}),
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	))
}

func (c *ControllerWithTraces) IsDatabaseUpToDate(ctx context.Context) (bool, error) {
	return tracing.TraceWithMetricWithAttributes(
		ctx,
		"IsDatabaseUpToDate",
		c.tracer,
		c.isDatabaseUpToDateHistogram,
		func(ctx context.Context) (bool, error) {
			return c.underlying.IsDatabaseUpToDate(ctx)
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
}

func (c *ControllerWithTraces) GetVolumesWithBalances(ctx context.Context, q common.PaginatedQuery[ledgerstore.GetVolumesOptions]) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	return tracing.TraceWithMetricWithAttributes(
		ctx,
		"GetVolumesWithBalances",
		c.tracer,
		c.getVolumesWithBalancesHistogram,
		func(ctx context.Context) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
			return c.underlying.GetVolumesWithBalances(ctx, q)
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
}

func (c *ControllerWithTraces) CreateTransaction(ctx context.Context, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	var (
		createdTransaction *ledger.CreatedTransaction
		log                *ledger.Log
		err                error
	)
	_, err = tracing.TraceWithMetricWithAttributes(
		ctx,
		"CreateTransaction",
		c.tracer,
		c.createTransactionHistogram,
		func(ctx context.Context) (any, error) {
			log, createdTransaction, err = c.underlying.CreateTransaction(ctx, parameters)
			return nil, err
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
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
	_, err = tracing.TraceWithMetricWithAttributes(
		ctx,
		"RevertTransaction",
		c.tracer,
		c.revertTransactionHistogram,
		func(ctx context.Context) (any, error) {
			log, revertedTransaction, err = c.underlying.RevertTransaction(ctx, parameters)
			return nil, err
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
	if err != nil {
		return nil, nil, err
	}

	return log, revertedTransaction, nil
}

func (c *ControllerWithTraces) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	return tracing.TraceWithMetricWithAttributes(
		ctx,
		"SaveTransactionMetadata",
		c.tracer,
		c.saveTransactionMetadataHistogram,
		func(ctx context.Context) (*ledger.Log, error) {
			return c.underlying.SaveTransactionMetadata(ctx, parameters)
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
}

func (c *ControllerWithTraces) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	return tracing.TraceWithMetricWithAttributes(
		ctx,
		"SaveAccountMetadata",
		c.tracer,
		c.saveAccountMetadataHistogram,
		func(ctx context.Context) (*ledger.Log, error) {
			return c.underlying.SaveAccountMetadata(ctx, parameters)
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
}

func (c *ControllerWithTraces) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	return tracing.TraceWithMetricWithAttributes(
		ctx,
		"DeleteTransactionMetadata",
		c.tracer,
		c.deleteTransactionMetadataHistogram,
		func(ctx context.Context) (*ledger.Log, error) {
			return c.underlying.DeleteTransactionMetadata(ctx, parameters)
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
}

func (c *ControllerWithTraces) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	return tracing.TraceWithMetricWithAttributes(
		ctx,
		"DeleteAccountMetadata",
		c.tracer,
		c.deleteAccountMetadataHistogram,
		func(ctx context.Context) (*ledger.Log, error) {
			return c.underlying.DeleteAccountMetadata(ctx, parameters)
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
}

func (c *ControllerWithTraces) GetStats(ctx context.Context) (Stats, error) {
	return tracing.TraceWithMetricWithAttributes(
		ctx,
		"GetStats",
		c.tracer,
		c.getStatsHistogram,
		func(ctx context.Context) (Stats, error) {
			return c.underlying.GetStats(ctx)
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
}

func (c *ControllerWithTraces) LockLedger(ctx context.Context) (Controller, bun.IDB, func() error, error) {
	var (
		controller Controller
		release    func() error
		conn       bun.IDB
		err        error
	)
	_, err = tracing.TraceWithMetricWithAttributes(
		ctx,
		"LockLedger",
		c.tracer,
		c.lockLedgerHistogram,
		func(ctx context.Context) (any, error) {
			controller, conn, release, err = c.underlying.LockLedger(ctx)
			return nil, err
		},
		[]attribute.KeyValue{
			attribute.String("ledger", c.underlying.Info().Name),
		},
	)
	if err != nil {
		return nil, nil, nil, err
	}

	cp := *c
	cp.underlying = controller

	return &cp, conn, release, nil
}

var _ Controller = (*ControllerWithTraces)(nil)
