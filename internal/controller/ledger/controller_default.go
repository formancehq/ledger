package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/formancehq/ledger/internal/storage/common"
	"math/big"
	"reflect"

	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/metric"
	noopmetrics "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"

	"github.com/formancehq/go-libs/v3/migrations"
	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/ledger/internal/machine"

	"github.com/formancehq/go-libs/v3/platform/postgres"

	"errors"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/google/uuid"

	ledger "github.com/formancehq/ledger/internal"
)

type DefaultController struct {
	store  Store
	parser NumscriptParser
	ledger ledger.Ledger

	tracer trace.Tracer
	meter  metric.Meter

	executeMachineHistogram metric.Int64Histogram
	deadLockCounter         metric.Int64Counter

	createTransactionLp         *logProcessor[CreateTransaction, ledger.CreatedTransaction]
	revertTransactionLp         *logProcessor[RevertTransaction, ledger.RevertedTransaction]
	saveTransactionMetadataLp   *logProcessor[SaveTransactionMetadata, ledger.SavedMetadata]
	saveAccountMetadataLp       *logProcessor[SaveAccountMetadata, ledger.SavedMetadata]
	deleteTransactionMetadataLp *logProcessor[DeleteTransactionMetadata, ledger.DeletedMetadata]
	deleteAccountMetadataLp     *logProcessor[DeleteAccountMetadata, ledger.DeletedMetadata]
}

func (ctrl *DefaultController) BeginTX(ctx context.Context, options *sql.TxOptions) (Controller, *bun.Tx, error) {
	cp := *ctrl
	var (
		err error
		tx  *bun.Tx
	)
	cp.store, tx, err = ctrl.store.BeginTX(ctx, options)
	if err != nil {
		return nil, nil, err
	}

	return &cp, tx, nil
}

func (ctrl *DefaultController) Commit(_ context.Context) error {
	return ctrl.store.Commit()
}

func (ctrl *DefaultController) Rollback(_ context.Context) error {
	return ctrl.store.Rollback()
}

func NewDefaultController(
	l ledger.Ledger,
	store Store,
	numscriptParser NumscriptParser,
	opts ...DefaultControllerOption,
) *DefaultController {
	ret := &DefaultController{
		store:  store,
		ledger: l,
		parser: numscriptParser,
	}

	for _, opt := range append(defaultOptions, opts...) {
		opt(ret)
	}

	var err error
	ret.executeMachineHistogram, err = ret.meter.Int64Histogram("numscript.run")
	if err != nil {
		panic(err)
	}
	ret.deadLockCounter, err = ret.meter.Int64Counter("deadlocks")
	if err != nil {
		panic(err)
	}

	ret.createTransactionLp = newLogProcessor[CreateTransaction, ledger.CreatedTransaction]("CreateTransaction", ret.deadLockCounter)
	ret.revertTransactionLp = newLogProcessor[RevertTransaction, ledger.RevertedTransaction]("RevertTransaction", ret.deadLockCounter)
	ret.saveTransactionMetadataLp = newLogProcessor[SaveTransactionMetadata, ledger.SavedMetadata]("SaveTransactionMetadata", ret.deadLockCounter)
	ret.saveAccountMetadataLp = newLogProcessor[SaveAccountMetadata, ledger.SavedMetadata]("SaveAccountMetadata", ret.deadLockCounter)
	ret.deleteTransactionMetadataLp = newLogProcessor[DeleteTransactionMetadata, ledger.DeletedMetadata]("DeleteTransactionMetadata", ret.deadLockCounter)
	ret.deleteAccountMetadataLp = newLogProcessor[DeleteAccountMetadata, ledger.DeletedMetadata]("DeleteAccountMetadata", ret.deadLockCounter)

	return ret
}

func (ctrl *DefaultController) IsDatabaseUpToDate(ctx context.Context) (bool, error) {
	return ctrl.store.IsUpToDate(ctx)
}

func (ctrl *DefaultController) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return ctrl.store.GetMigrationsInfo(ctx)
}

func (ctrl *DefaultController) ListTransactions(ctx context.Context, q common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Transaction], error) {
	return ctrl.store.Transactions().Paginate(ctx, q)
}

func (ctrl *DefaultController) CountTransactions(ctx context.Context, q common.ResourceQuery[any]) (int, error) {
	return ctrl.store.Transactions().Count(ctx, q)
}

func (ctrl *DefaultController) GetTransaction(ctx context.Context, q common.ResourceQuery[any]) (*ledger.Transaction, error) {
	return ctrl.store.Transactions().GetOne(ctx, q)
}

func (ctrl *DefaultController) CountAccounts(ctx context.Context, q common.ResourceQuery[any]) (int, error) {
	return ctrl.store.Accounts().Count(ctx, q)
}

func (ctrl *DefaultController) ListAccounts(ctx context.Context, q common.OffsetPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Account], error) {
	return ctrl.store.Accounts().Paginate(ctx, q)
}

func (ctrl *DefaultController) GetAccount(ctx context.Context, q common.ResourceQuery[any]) (*ledger.Account, error) {
	return ctrl.store.Accounts().GetOne(ctx, q)
}

func (ctrl *DefaultController) GetAggregatedBalances(ctx context.Context, q common.ResourceQuery[GetAggregatedVolumesOptions]) (ledger.BalancesByAssets, error) {
	ret, err := ctrl.store.AggregatedBalances().GetOne(ctx, q)
	if err != nil {
		return nil, err
	}
	return ret.Aggregated.Balances(), nil
}

func (ctrl *DefaultController) ListLogs(ctx context.Context, q common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
	return ctrl.store.Logs().Paginate(ctx, q)
}

func (ctrl *DefaultController) GetVolumesWithBalances(ctx context.Context, q common.OffsetPaginatedQuery[GetVolumesOptions]) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	return ctrl.store.Volumes().Paginate(ctx, q)
}

func (ctrl *DefaultController) Import(ctx context.Context, stream chan ledger.Log) error {

	var lastLogID *int

	// We can import only if the ledger is empty.
	logs, err := ctrl.store.Logs().Paginate(ctx, common.ColumnPaginatedQuery[any]{
		PageSize: 1,
	})
	if err != nil {
		return fmt.Errorf("error listing logs: %w", err)
	}

	if len(logs.Data) > 0 {
		lastLogID = logs.Data[0].ID
	}

	for log := range stream {
		if lastLogID != nil && *log.ID <= *lastLogID {
			return fmt.Errorf("log %d already exists", *log.ID)
		}
		lastLogID = nil

		if err := ctrl.importLog(ctx, log); err != nil {
			switch {
			case errors.Is(err, postgres.ErrSerialization) ||
				errors.Is(err, ErrConcurrentTransaction{}):
				return NewErrImport(errors.New("concurrent transaction occur" +
					"red, cannot import the ledger"))
			}
			return fmt.Errorf("importing log %d: %w", *log.ID, err)
		}
	}

	return err
}

func (ctrl *DefaultController) importLog(ctx context.Context, log ledger.Log) error {
	_, err := tracing.Trace(ctx, ctrl.tracer, "ImportLog", func(ctx context.Context) (any, error) {
		switch payload := log.Data.(type) {
		case ledger.CreatedTransaction:
			logging.FromContext(ctx).Debugf("Importing transaction %d", *payload.Transaction.ID)
			if err := ctrl.store.CommitTransaction(ctx, &payload.Transaction, payload.AccountMetadata); err != nil {
				return nil, fmt.Errorf("failed to commit transaction: %w", err)
			}
			logging.FromContext(ctx).Debugf("Imported transaction %d", *payload.Transaction.ID)
		case ledger.RevertedTransaction:
			logging.FromContext(ctx).Debugf("Reverting transaction %d", *payload.RevertedTransaction.ID)
			_, _, err := ctrl.store.RevertTransaction(
				ctx,
				*payload.RevertedTransaction.ID,
				*payload.RevertedTransaction.RevertedAt,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to revert transaction: %w", err)
			}
			if err := ctrl.store.CommitTransaction(ctx, &payload.RevertTransaction, nil); err != nil {
				return nil, fmt.Errorf("failed to commit transaction: %w", err)
			}
		case ledger.SavedMetadata:
			switch payload.TargetType {
			case ledger.MetaTargetTypeTransaction:
				logging.FromContext(ctx).Debugf("Saving metadata of transaction %d", payload.TargetID)
				if _, _, err := ctrl.store.UpdateTransactionMetadata(ctx, payload.TargetID.(int), payload.Metadata, log.Date); err != nil {
					return nil, fmt.Errorf("failed to update transaction metadata: %w", err)
				}
			case ledger.MetaTargetTypeAccount:
				logging.FromContext(ctx).Debugf("Saving metadata of account %s", payload.TargetID)
				if err := ctrl.store.UpdateAccountsMetadata(ctx, ledger.AccountMetadata{
					payload.TargetID.(string): payload.Metadata,
				}); err != nil {
					return nil, fmt.Errorf("failed to update account metadata: %w", err)
				}
			}
		case ledger.DeletedMetadata:
			switch payload.TargetType {
			case ledger.MetaTargetTypeTransaction:
				logging.FromContext(ctx).Debugf("Deleting metadata of transaction %d", payload.TargetID)
				if _, _, err := ctrl.store.DeleteTransactionMetadata(ctx, payload.TargetID.(int), payload.Key, log.Date); err != nil {
					return nil, fmt.Errorf("failed to delete transaction metadata: %w", err)
				}
			case ledger.MetaTargetTypeAccount:
				logging.FromContext(ctx).Debugf("Deleting metadata of account %s", payload.TargetID)
				if err := ctrl.store.DeleteAccountMetadata(ctx, payload.TargetID.(string), payload.Key); err != nil {
					return nil, fmt.Errorf("failed to delete account metadata: %w", err)
				}
			}
		}

		logCopy := log
		logging.FromContext(ctx).Debugf("Inserting log %d", *log.ID)
		if err := ctrl.store.InsertLog(ctx, &log); err != nil {
			return nil, fmt.Errorf("failed to insert log: %w", err)
		}

		if ctrl.ledger.HasFeature(features.FeatureHashLogs, "SYNC") {
			if !reflect.DeepEqual(log.Hash, logCopy.Hash) {
				return nil, newErrInvalidHash(*log.ID, logCopy.Hash, log.Hash)
			}
		}

		return nil, nil
	})
	return err
}

func (ctrl *DefaultController) Export(ctx context.Context, w ExportWriter) error {
	return bunpaginate.Iterate(
		ctx,
		common.ColumnPaginatedQuery[any]{
			PageSize: 100,
			Order:    pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
		},
		func(ctx context.Context, q common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
			return ctrl.store.Logs().Paginate(ctx, q)
		},
		func(cursor *bunpaginate.Cursor[ledger.Log]) error {
			for _, data := range cursor.Data {
				if err := w.Write(ctx, data); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func (ctrl *DefaultController) createTransaction(ctx context.Context, store Store, parameters Parameters[CreateTransaction]) (*ledger.CreatedTransaction, error) {

	logger := logging.FromContext(ctx).WithField("req", uuid.NewString()[:8])
	ctx = logging.ContextWithLogger(ctx, logger)

	m, err := ctrl.parser.Parse(parameters.Input.Plain)
	if err != nil {
		return nil, fmt.Errorf("failed to compile script: %w", err)
	}

	result, err := tracing.TraceWithMetric(
		ctx,
		"ExecuteMachine",
		ctrl.tracer,
		ctrl.executeMachineHistogram,
		func(ctx context.Context) (*NumscriptExecutionResult, error) {
			return m.Execute(ctx, store, parameters.Input.Vars)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to execute program: %w", err)
	}

	if len(result.Postings) == 0 {
		return nil, ErrNoPostings
	}

	finalMetadata := result.Metadata
	if finalMetadata == nil {
		finalMetadata = metadata.Metadata{}
	}
	for k, v := range parameters.Input.Metadata {
		if finalMetadata[k] != "" {
			return nil, newErrMetadataOverride(k)
		}
		finalMetadata[k] = v
	}

	accountMetadata := result.AccountMetadata
	if accountMetadata == nil {
		accountMetadata = make(map[string]metadata.Metadata)
	}
	if parameters.Input.AccountMetadata != nil {
		for account, values := range parameters.Input.AccountMetadata {
			if accountMetadata[account] == nil {
				accountMetadata[account] = metadata.Metadata{}
			}
			for k, v := range values {
				accountMetadata[account][k] = v
			}
		}
	}

	transaction := ledger.NewTransaction().
		WithPostings(result.Postings...).
		WithMetadata(finalMetadata).
		WithTimestamp(parameters.Input.Timestamp).
		WithReference(parameters.Input.Reference)
	err = store.CommitTransaction(ctx, &transaction, accountMetadata)
	if err != nil {
		return nil, err
	}

	return &ledger.CreatedTransaction{
		Transaction:     transaction,
		AccountMetadata: accountMetadata,
	}, err
}

func (ctrl *DefaultController) CreateTransaction(ctx context.Context, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	return ctrl.createTransactionLp.forgeLog(ctx, ctrl.store, parameters, ctrl.createTransaction)
}

func (ctrl *DefaultController) revertTransaction(ctx context.Context, store Store, parameters Parameters[RevertTransaction]) (*ledger.RevertedTransaction, error) {
	var (
		hasBeenReverted bool
		err             error
	)
	originalTransaction, hasBeenReverted, err := store.RevertTransaction(ctx, parameters.Input.TransactionID, time.Time{})
	if err != nil {
		return nil, err
	}
	if !hasBeenReverted {
		return nil, newErrAlreadyReverted(parameters.Input.TransactionID)
	}

	bq := originalTransaction.InvolvedDestinations()

	balances, err := store.GetBalances(ctx, bq)
	if err != nil {
		return nil, fmt.Errorf("failed to get balances: %w", err)
	}

	reversedTx := originalTransaction.Reverse()
	if parameters.Input.AtEffectiveDate {
		reversedTx = reversedTx.WithTimestamp(originalTransaction.Timestamp)
	} else {
		reversedTx = reversedTx.WithTimestamp(*originalTransaction.RevertedAt)
	}
	reversedTx.Metadata = ledger.MarkReverts(metadata.Metadata{}, *originalTransaction.ID)

	// Check balances after the revert, all balances must be greater than 0
	if !parameters.Input.Force {
		for _, posting := range reversedTx.Postings {
			balances[posting.Source][posting.Asset] = balances[posting.Source][posting.Asset].Add(
				balances[posting.Source][posting.Asset],
				big.NewInt(0).Neg(posting.Amount),
			)
			if _, ok := balances[posting.Destination]; ok {
				// if destination is also a source in some posting, since balances should only contain posting sources
				balances[posting.Destination][posting.Asset] = balances[posting.Destination][posting.Asset].Add(
					balances[posting.Destination][posting.Asset],
					posting.Amount,
				)
			}
		}

		for account, forAccount := range balances {
			for asset, finalBalance := range forAccount {
				if finalBalance.Cmp(new(big.Int)) < 0 && account != "world" {
					// todo(waiting): break dependency on machine package
					// notes(gfyrag): wait for the new interpreter
					return nil, machine.NewErrInsufficientFund("insufficient fund for %s/%s", account, asset)
				}
			}
		}
	}

	err = store.CommitTransaction(ctx, &reversedTx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to insert transaction: %w", err)
	}

	return &ledger.RevertedTransaction{
		RevertedTransaction: *originalTransaction,
		RevertTransaction:   reversedTx,
	}, nil
}

func (ctrl *DefaultController) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	return ctrl.revertTransactionLp.forgeLog(ctx, ctrl.store, parameters, ctrl.revertTransaction)
}

func (ctrl *DefaultController) saveTransactionMetadata(ctx context.Context, store Store, parameters Parameters[SaveTransactionMetadata]) (*ledger.SavedMetadata, error) {
	if _, _, err := store.UpdateTransactionMetadata(ctx, parameters.Input.TransactionID, parameters.Input.Metadata, time.Time{}); err != nil {
		return nil, err
	}

	return &ledger.SavedMetadata{
		TargetType: ledger.MetaTargetTypeTransaction,
		TargetID:   parameters.Input.TransactionID,
		Metadata:   parameters.Input.Metadata,
	}, nil
}

func (ctrl *DefaultController) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	log, _, err := ctrl.saveTransactionMetadataLp.forgeLog(ctx, ctrl.store, parameters, ctrl.saveTransactionMetadata)
	return log, err
}

func (ctrl *DefaultController) saveAccountMetadata(ctx context.Context, store Store, parameters Parameters[SaveAccountMetadata]) (*ledger.SavedMetadata, error) {
	if err := store.UpsertAccounts(ctx, &ledger.Account{
		Address:  parameters.Input.Address,
		Metadata: parameters.Input.Metadata,
	}); err != nil {
		return nil, err
	}

	return &ledger.SavedMetadata{
		TargetType: ledger.MetaTargetTypeAccount,
		TargetID:   parameters.Input.Address,
		Metadata:   parameters.Input.Metadata,
	}, nil
}

func (ctrl *DefaultController) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	log, _, err := ctrl.saveAccountMetadataLp.forgeLog(ctx, ctrl.store, parameters, ctrl.saveAccountMetadata)

	return log, err
}

func (ctrl *DefaultController) deleteTransactionMetadata(ctx context.Context, store Store, parameters Parameters[DeleteTransactionMetadata]) (*ledger.DeletedMetadata, error) {
	_, modified, err := store.DeleteTransactionMetadata(ctx, parameters.Input.TransactionID, parameters.Input.Key, time.Time{})
	if err != nil {
		return nil, err
	}

	if !modified {
		return nil, postgres.ErrNotFound
	}

	return &ledger.DeletedMetadata{
		TargetType: ledger.MetaTargetTypeTransaction,
		TargetID:   parameters.Input.TransactionID,
		Key:        parameters.Input.Key,
	}, nil
}

func (ctrl *DefaultController) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	log, _, err := ctrl.deleteTransactionMetadataLp.forgeLog(ctx, ctrl.store, parameters, ctrl.deleteTransactionMetadata)
	return log, err
}

func (ctrl *DefaultController) deleteAccountMetadata(ctx context.Context, store Store, parameters Parameters[DeleteAccountMetadata]) (*ledger.DeletedMetadata, error) {
	err := store.DeleteAccountMetadata(ctx, parameters.Input.Address, parameters.Input.Key)
	if err != nil {
		return nil, err
	}

	return &ledger.DeletedMetadata{
		TargetType: ledger.MetaTargetTypeAccount,
		TargetID:   parameters.Input.Address,
		Key:        parameters.Input.Key,
	}, nil
}

func (ctrl *DefaultController) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	log, _, err := ctrl.deleteAccountMetadataLp.forgeLog(ctx, ctrl.store, parameters, ctrl.deleteAccountMetadata)
	return log, err
}

var _ Controller = (*DefaultController)(nil)

type DefaultControllerOption func(controller *DefaultController)

var defaultOptions = []DefaultControllerOption{
	WithMeter(noopmetrics.Meter{}),
	WithTracer(nooptracer.Tracer{}),
}

func WithMeter(meter metric.Meter) DefaultControllerOption {
	return func(controller *DefaultController) {
		controller.meter = meter
	}
}
func WithTracer(tracer trace.Tracer) DefaultControllerOption {
	return func(controller *DefaultController) {
		controller.tracer = tracer
	}
}
