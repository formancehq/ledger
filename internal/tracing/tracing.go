package tracing

import (
	"context"
	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/go-libs/v3/time"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"go.opentelemetry.io/otel/trace"
)

func LegacyMetricsName(operationName string) string {
	switch operationName {
	case "controller.numscript_run":
		return "numscript.run"
	case "controller.deadlocks":
		return "deadlocks"
	case "controller.begin_tx":
		return "BeginTX"
	case "controller.list_transactions":
		return "ListTransactions"
	case "controller.commit":
		return "Commit"
	case "controller.rollback":
		return "Rollback"
	case "controller.count_transactions":
		return "CountTransactions"
	case "controller.get_transaction":
		return "GetTransaction"
	case "controller.count_accounts":
		return "CountAccounts"
	case "controller.list_accounts":
		return "ListAccounts"
	case "controller.get_account":
		return "GetAccount"
	case "controller.get_aggregated_balances":
		return "GetAggregatedBalances"
	case "controller.list_logs":
		return "ListLogs"
	case "controller.import":
		return "Import"
	case "controller.export":
		return "Export"
	case "controller.is_database_up_to_date":
		return "IsDatabaseUpToDate"
	case "controller.get_volumes_with_balances":
		return "GetVolumesWithBalances"
	case "controller.get_stats":
		return "GetStats"
	case "controller.create_transaction":
		return "CreateTransaction"
	case "controller.revert_transaction":
		return "RevertTransaction"
	case "controller.save_transaction_metadata":
		return "SaveTransactionMetadata"
	case "controller.save_account_metadata":
		return "SaveAccountMetadata"
	case "controller.delete_transaction_metadata":
		return "DeleteTransactionMetadata"
	case "controller.delete_account_metadata":
		return "DeleteAccountMetadata"
	case "controller.lock_ledger":
		return "LockLedger"
	case "store.check_bucket_schema":
		return "store.checkBucketSchema"
	case "store.check_ledger_schema":
		return "store.checkLedgerSchema"
	case "store.update_accounts_metadata":
		return "store.updateAccountsMetadata"
	case "store.delete_account_metadata":
		return "store.deleteAccountMetadata"
	case "store.upsert_accounts":
		return "store.upsertAccounts"
	case "store.get_balances":
		return "store.getBalances"
	case "store.insert_log":
		return "store.insertLog"
	case "store.read_log_with_idempotency_key":
		return "store.readLogWithIdempotencyKey"
	case "store.insert_moves":
		return "store.insertMoves"
	case "store.insert_transaction":
		return "store.insertTransaction"
	case "store.revert_transaction":
		return "store.revertTransaction"
	case "store.update_transaction_metadata":
		return "store.updateTransactionMetadata"
	case "store.delete_transaction_metadata":
		return "store.deleteTransactionMetadata"
	case "store.update_balances":
		return "store.updateBalances"
	case "store.get_volumes_with_balances":
		return "store.getVolumesWithBalances"
	default:
		return operationName
	}
}

func TraceWithMetric[RET any](
	ctx context.Context,
	operationName string,
	tracer trace.Tracer,
	histogram metric.Int64Histogram,
	fn func(ctx context.Context) (RET, error),
	finalizers ...func(ctx context.Context, ret RET),
) (RET, error) {
	var zeroRet RET

	return Trace(ctx, tracer, operationName, func(ctx context.Context) (RET, error) {
		now := time.Now()
		ret, err := fn(ctx)
		if err != nil {
			otlp.RecordError(ctx, err)
			return zeroRet, err
		}

		latency := time.Since(now)
		histogram.Record(ctx, latency.Milliseconds())
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("latency", latency.String()))

		for _, finalizer := range finalizers {
			finalizer(ctx, ret)
		}

		return ret, nil
	})
}

func Trace[RET any](
	ctx context.Context,
	tracer trace.Tracer,
	name string,
	fn func(ctx context.Context) (RET, error),
	spanOptions ...trace.SpanStartOption,
) (RET, error) {
	ctx, span := tracer.Start(ctx, name, spanOptions...)
	defer span.End()

	ret, err := fn(ctx)
	if err != nil {
		otlp.RecordError(ctx, err)
		return ret, err
	}

	return ret, nil
}

func NoResult(fn func(ctx context.Context) error) func(ctx context.Context) (any, error) {
	return func(ctx context.Context) (any, error) {
		return nil, fn(ctx)
	}
}

func SkipResult[RET any](_ RET, err error) error {
	return err
}
