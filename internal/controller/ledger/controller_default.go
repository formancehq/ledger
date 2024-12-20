package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/time"
	"github.com/formancehq/ledger/pkg/features"
	"math/big"
	"reflect"

	. "github.com/formancehq/go-libs/v2/collectionutils"
	"go.opentelemetry.io/otel/metric"
	noopmetrics "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"

	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/ledger/internal/machine"

	"github.com/formancehq/go-libs/v2/platform/postgres"

	"errors"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/metadata"
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

	createTransactionLp         *logProcessor[RunScript, ledger.CreatedTransaction]
	revertTransactionLp         *logProcessor[RevertTransaction, ledger.RevertedTransaction]
	saveTransactionMetadataLp   *logProcessor[SaveTransactionMetadata, ledger.SavedMetadata]
	saveAccountMetadataLp       *logProcessor[SaveAccountMetadata, ledger.SavedMetadata]
	deleteTransactionMetadataLp *logProcessor[DeleteTransactionMetadata, ledger.DeletedMetadata]
	deleteAccountMetadataLp     *logProcessor[DeleteAccountMetadata, ledger.DeletedMetadata]
}

func (ctrl *DefaultController) BeginTX(ctx context.Context, options *sql.TxOptions) (Controller, error) {
	cp := *ctrl
	var err error
	cp.store, err = ctrl.store.BeginTX(ctx, options)
	if err != nil {
		return nil, err
	}

	return &cp, nil
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

	ret.createTransactionLp = newLogProcessor[RunScript, ledger.CreatedTransaction]("CreateTransaction", ret.deadLockCounter)
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

func (ctrl *DefaultController) ListTransactions(ctx context.Context, q ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Transaction], error) {
	return ctrl.store.Transactions().Paginate(ctx, q)
}

func (ctrl *DefaultController) CountTransactions(ctx context.Context, q ResourceQuery[any]) (int, error) {
	return ctrl.store.Transactions().Count(ctx, q)
}

func (ctrl *DefaultController) GetTransaction(ctx context.Context, q ResourceQuery[any]) (*ledger.Transaction, error) {
	return ctrl.store.Transactions().GetOne(ctx, q)
}

func (ctrl *DefaultController) CountAccounts(ctx context.Context, q ResourceQuery[any]) (int, error) {
	return ctrl.store.Accounts().Count(ctx, q)
}

func (ctrl *DefaultController) ListAccounts(ctx context.Context, q OffsetPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Account], error) {
	return ctrl.store.Accounts().Paginate(ctx, q)
}

func (ctrl *DefaultController) GetAccount(ctx context.Context, q ResourceQuery[any]) (*ledger.Account, error) {
	return ctrl.store.Accounts().GetOne(ctx, q)
}

func (ctrl *DefaultController) GetAggregatedBalances(ctx context.Context, q ResourceQuery[GetAggregatedVolumesOptions]) (ledger.BalancesByAssets, error) {
	ret, err := ctrl.store.AggregatedBalances().GetOne(ctx, q)
	if err != nil {
		return nil, err
	}
	return ret.Aggregated.Balances(), nil
}

func (ctrl *DefaultController) ListLogs(ctx context.Context, q ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
	return ctrl.store.Logs().Paginate(ctx, q)
}

func (ctrl *DefaultController) GetVolumesWithBalances(ctx context.Context, q OffsetPaginatedQuery[GetVolumesOptions]) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	return ctrl.store.Volumes().Paginate(ctx, q)
}

func (ctrl *DefaultController) Import(ctx context.Context, stream chan ledger.Log) error {
	// Use serializable isolation level to ensure no concurrent request use the store.
	// If a concurrent transactions is made while we are importing some logs, the transaction importing logs will
	// be canceled with serialization error.
	store, err := ctrl.store.BeginTX(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	if err := ctrl.importLogs(ctx, store, stream); err != nil {
		if rollbackErr := store.Rollback(); rollbackErr != nil {
			logging.FromContext(ctx).Errorf("failed to rollback transaction: %v", rollbackErr)
		}
		return err
	}

	if err := store.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (ctrl *DefaultController) importLogs(ctx context.Context, store Store, stream chan ledger.Log) error {

	// Due to the serializable isolation level, and since we explicitly ask for the ledger state in the sql transaction context
	// if the state change, the sql transaction will be aborted with a serialization error
	if err := store.LockLedger(ctx); err != nil {
		return fmt.Errorf("failed to lock ledger: %w", err)
	}

	// We can import only if the ledger is empty.
	logs, err := store.Logs().Paginate(ctx, ColumnPaginatedQuery[any]{
		PageSize: 1,
	})
	if err != nil {
		return fmt.Errorf("error listing logs: %w", err)
	}

	if len(logs.Data) > 0 {
		return newErrImport(errors.New("ledger must be empty"))
	}

	for log := range stream {
		if err := ctrl.importLog(ctx, store, log); err != nil {
			if errors.Is(err, postgres.ErrSerialization) {
				return newErrImport(errors.New("concurrent transaction occur" +
					"red, cannot import the ledger"))
			}
			return fmt.Errorf("importing log %d: %w", log.ID, err)
		}
	}

	return err
}

// todo: as ids are generated by the database, the exported ids can not match imported ones
// it can cause issues with actions referencing other objects
func (ctrl *DefaultController) importLog(ctx context.Context, store Store, log ledger.Log) error {
	switch payload := log.Data.(type) {
	case ledger.CreatedTransaction:
		logging.FromContext(ctx).Debugf("Importing transaction %d", payload.Transaction.ID)
		if err := store.CommitTransaction(ctx, &payload.Transaction); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		logging.FromContext(ctx).Debugf("Imported transaction %d", payload.Transaction.ID)

		if len(payload.AccountMetadata) > 0 {
			logging.FromContext(ctx).Debugf("Importing metadata of accounts '%s'", Keys(payload.AccountMetadata))
			if err := store.UpdateAccountsMetadata(ctx, payload.AccountMetadata); err != nil {
				return fmt.Errorf("updating metadata of accounts '%s': %w", Keys(payload.AccountMetadata), err)
			}
		}
	case ledger.RevertedTransaction:
		logging.FromContext(ctx).Debugf("Reverting transaction %d", payload.RevertedTransaction.ID)
		_, _, err := store.RevertTransaction(
			ctx,
			payload.RevertedTransaction.ID,
			*payload.RevertedTransaction.RevertedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to revert transaction: %w", err)
		}
		if err := store.CommitTransaction(ctx, &payload.RevertTransaction); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
	case ledger.SavedMetadata:
		switch payload.TargetType {
		case ledger.MetaTargetTypeTransaction:
			logging.FromContext(ctx).Debugf("Saving metadata of transaction %d", payload.TargetID)
			if _, _, err := store.UpdateTransactionMetadata(ctx, payload.TargetID.(int), payload.Metadata); err != nil {
				return fmt.Errorf("failed to update transaction metadata: %w", err)
			}
		case ledger.MetaTargetTypeAccount:
			logging.FromContext(ctx).Debugf("Saving metadata of account %s", payload.TargetID)
			if err := store.UpdateAccountsMetadata(ctx, ledger.AccountMetadata{
				payload.TargetID.(string): payload.Metadata,
			}); err != nil {
				return fmt.Errorf("failed to update account metadata: %w", err)
			}
		}
	case ledger.DeletedMetadata:
		switch payload.TargetType {
		case ledger.MetaTargetTypeTransaction:
			logging.FromContext(ctx).Debugf("Deleting metadata of transaction %d", payload.TargetID)
			if _, _, err := store.DeleteTransactionMetadata(ctx, payload.TargetID.(int), payload.Key); err != nil {
				return fmt.Errorf("failed to delete transaction metadata: %w", err)
			}
		case ledger.MetaTargetTypeAccount:
			logging.FromContext(ctx).Debugf("Deleting metadata of account %s", payload.TargetID)
			if err := store.DeleteAccountMetadata(ctx, payload.TargetID.(string), payload.Key); err != nil {
				return fmt.Errorf("failed to delete account metadata: %w", err)
			}
		}
	}

	logCopy := log
	logging.FromContext(ctx).Debugf("Inserting log %d", log.ID)
	if err := store.InsertLog(ctx, &log); err != nil {
		return fmt.Errorf("failed to insert log: %w", err)
	}

	if ctrl.ledger.HasFeature(features.FeatureHashLogs, "SYNC") {
		if !reflect.DeepEqual(log.Hash, logCopy.Hash) {
			return newErrInvalidHash(log.ID, logCopy.Hash, log.Hash)
		}
	}

	return nil
}

func (ctrl *DefaultController) Export(ctx context.Context, w ExportWriter) error {
	return bunpaginate.Iterate(
		ctx,
		ColumnPaginatedQuery[any]{
			PageSize: 100,
			Order: pointer.For(bunpaginate.Order(bunpaginate.OrderAsc)),
		},
		func(ctx context.Context, q ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
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

func (ctrl *DefaultController) createTransaction(ctx context.Context, store Store, parameters Parameters[RunScript]) (*ledger.CreatedTransaction, error) {

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

	transaction := ledger.NewTransaction().
		WithPostings(result.Postings...).
		WithMetadata(finalMetadata).
		WithTimestamp(parameters.Input.Timestamp).
		WithReference(parameters.Input.Reference)
	err = store.CommitTransaction(ctx, &transaction)
	if err != nil {
		return nil, err
	}

	if len(result.AccountMetadata) > 0 {
		if err := store.UpdateAccountsMetadata(ctx, result.AccountMetadata); err != nil {
			return nil, fmt.Errorf("updating metadata of account '%s': %w", Keys(result.AccountMetadata), err)
		}
	}

	return &ledger.CreatedTransaction{
		Transaction:     transaction,
		AccountMetadata: result.AccountMetadata,
	}, err
}

func (ctrl *DefaultController) CreateTransaction(ctx context.Context, parameters Parameters[RunScript]) (*ledger.Log, *ledger.CreatedTransaction, error) {
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

	// Check balances after the revert, all balances must be greater than 0
	if !parameters.Input.Force {
		for _, posting := range reversedTx.Postings {
			balances[posting.Source][posting.Asset] = balances[posting.Source][posting.Asset].Add(
				balances[posting.Source][posting.Asset],
				big.NewInt(0).Neg(posting.Amount),
			)
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

	err = store.CommitTransaction(ctx, &reversedTx)
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
	if _, _, err := store.UpdateTransactionMetadata(ctx, parameters.Input.TransactionID, parameters.Input.Metadata); err != nil {
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
	_, modified, err := store.DeleteTransactionMetadata(ctx, parameters.Input.TransactionID, parameters.Input.Key)
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
