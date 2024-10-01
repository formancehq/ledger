package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/formancehq/go-libs/time"
	"math/big"
	"reflect"

	"github.com/formancehq/go-libs/migrations"
	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/ledger/internal/machine"

	. "github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/platform/postgres"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/pointer"
	"github.com/google/uuid"
	// todo: remove as it is in maintenance mode
	"errors"

	ledger "github.com/formancehq/ledger/internal"
)

type DefaultController struct {
	store          Store
	machineFactory MachineFactory
	ledger         ledger.Ledger
}

func NewDefaultController(
	ledger ledger.Ledger,
	store Store,
	machineFactory MachineFactory,
) *DefaultController {
	ret := &DefaultController{
		store:          store,
		ledger:         ledger,
		machineFactory: machineFactory,
	}

	return ret
}

func (ctrl *DefaultController) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return ctrl.store.GetMigrationsInfo(ctx)
}

func (ctrl *DefaultController) ListTransactions(ctx context.Context, q ListTransactionsQuery) (*bunpaginate.Cursor[ledger.Transaction], error) {
	return ctrl.store.ListTransactions(ctx, q)
}

func (ctrl *DefaultController) CountTransactions(ctx context.Context, q ListTransactionsQuery) (int, error) {
	return ctrl.store.CountTransactions(ctx, q)
}

func (ctrl *DefaultController) GetTransaction(ctx context.Context, query GetTransactionQuery) (*ledger.Transaction, error) {
	return ctrl.store.GetTransaction(ctx, query)
}

func (ctrl *DefaultController) CountAccounts(ctx context.Context, a ListAccountsQuery) (int, error) {
	return ctrl.store.CountAccounts(ctx, a)
}

func (ctrl *DefaultController) ListAccounts(ctx context.Context, a ListAccountsQuery) (*bunpaginate.Cursor[ledger.Account], error) {
	return ctrl.store.ListAccounts(ctx, a)
}

func (ctrl *DefaultController) GetAccount(ctx context.Context, q GetAccountQuery) (*ledger.Account, error) {
	return ctrl.store.GetAccount(ctx, q)
}

func (ctrl *DefaultController) GetAggregatedBalances(ctx context.Context, q GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {
	return ctrl.store.GetAggregatedBalances(ctx, q)
}

func (ctrl *DefaultController) ListLogs(ctx context.Context, q GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error) {
	return ctrl.store.ListLogs(ctx, q)
}

func (ctrl *DefaultController) Import(ctx context.Context, stream chan ledger.Log) error {
	// Use serializable isolation level to ensure no concurrent request use the store.
	// If a concurrent transactions is made while we are importing some logs, the transaction importing logs will
	// be canceled with serialization error.
	err := ctrl.store.WithTX(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}, func(sqlTx TX) (bool, error) {

		// Due to the serializable isolation level, and since we explicitly ask for the ledger state in the sql transaction context
		// if the state change, the sql transaction will be aborted with a serialization error
		if err := sqlTx.LockLedger(ctx); err != nil {
			return false, fmt.Errorf("failed to lock ledger: %w", err)
		}

		// We can import only if the ledger is empty.
		logs, err := sqlTx.ListLogs(ctx, NewListLogsQuery(PaginatedQueryOptions[any]{
			PageSize: 1,
		}))
		if err != nil {
			return false, fmt.Errorf("error listing logs: %w", err)
		}

		if len(logs.Data) > 0 {
			return false, newErrImport(fmt.Errorf("ledger must be empty"))
		}

		for log := range stream {
			if err := ctrl.importLog(ctx, sqlTx, log); err != nil {
				return false, fmt.Errorf("importing log %d: %w", log.ID, err)
			}
		}

		return true, nil
	})
	if err != nil {
		if errors.Is(err, postgres.ErrSerialization) {
			return newErrImport(errors.New("concurrent transaction occured, cannot import the ledger"))
		}
	}

	return err
}

func (ctrl *DefaultController) importLog(ctx context.Context, sqlTx TX, log ledger.Log) error {
	switch payload := log.Data.(type) {
	case ledger.CreatedTransaction:
		if err := sqlTx.CommitTransaction(ctx, &payload.Transaction); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		if len(payload.AccountMetadata) > 0 {
			if err := sqlTx.UpdateAccountsMetadata(ctx, payload.AccountMetadata); err != nil {
				return fmt.Errorf("updating metadata of accounts '%s': %w", Keys(payload.AccountMetadata), err)
			}
		}
	case ledger.RevertedTransaction:
		_, _, err := sqlTx.RevertTransaction(ctx, payload.RevertedTransaction.ID)
		if err != nil {
			return fmt.Errorf("failed to revert transaction: %w", err)
		}
	case ledger.SavedMetadata:
		switch payload.TargetType {
		case ledger.MetaTargetTypeTransaction:
			if _, _, err := sqlTx.UpdateTransactionMetadata(ctx, payload.TargetID.(int), payload.Metadata); err != nil {
				return fmt.Errorf("failed to update transaction metadata: %w", err)
			}
		case ledger.MetaTargetTypeAccount:
			if err := sqlTx.UpdateAccountsMetadata(ctx, ledger.AccountMetadata{
				payload.TargetID.(string): payload.Metadata,
			}); err != nil {
				return fmt.Errorf("failed to update account metadata: %w", err)
			}
		}
	case ledger.DeletedMetadata:
		switch payload.TargetType {
		case ledger.MetaTargetTypeTransaction:
			if _, _, err := sqlTx.DeleteTransactionMetadata(ctx, payload.TargetID.(int), payload.Key); err != nil {
				return fmt.Errorf("failed to delete transaction metadata: %w", err)
			}
		case ledger.MetaTargetTypeAccount:
			if err := sqlTx.DeleteAccountMetadata(ctx, payload.TargetID.(string), payload.Key); err != nil {
				return fmt.Errorf("failed to delete account metadata: %w", err)
			}
		}
	}

	logCopy := log
	if err := sqlTx.InsertLog(ctx, &log); err != nil {
		return fmt.Errorf("failed to insert log: %w", err)
	}

	if ctrl.ledger.HasFeature(ledger.FeatureHashLogs, "SYNC") {
		if !reflect.DeepEqual(log.Hash, logCopy.Hash) {
			return newErrInvalidHash(log.ID, logCopy.Hash, log.Hash)
		}
	}

	return nil
}

func (ctrl *DefaultController) Export(ctx context.Context, w ExportWriter) error {
	return bunpaginate.Iterate(
		ctx,
		NewListLogsQuery(NewPaginatedQueryOptions[any](nil).WithPageSize(100)).
			WithOrder(bunpaginate.OrderAsc),
		func(ctx context.Context, q GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error) {
			return ctrl.store.ListLogs(ctx, q)
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

func (ctrl *DefaultController) IsDatabaseUpToDate(ctx context.Context) (bool, error) {
	return ctrl.store.IsUpToDate(ctx)
}

func (ctrl *DefaultController) GetVolumesWithBalances(ctx context.Context, q GetVolumesWithBalancesQuery) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	return ctrl.store.GetVolumesWithBalances(ctx, q)
}

func (ctrl *DefaultController) CreateTransaction(ctx context.Context, parameters Parameters[RunScript]) (*ledger.CreatedTransaction, error) {

	logger := logging.FromContext(ctx).WithField("req", uuid.NewString()[:8])
	ctx = logging.ContextWithLogger(ctx, logger)

	m, err := ctrl.machineFactory.Make(parameters.Input.Plain)
	if err != nil {
		return nil, fmt.Errorf("failed to compile script: %w", err)
	}

	output, err := forgeLog(ctx, ctrl.store, parameters, func(ctx context.Context, sqlTX TX, input RunScript) (*ledger.Log, *ledger.CreatedTransaction, error) {
		result, err := tracing.TraceWithLatency(ctx, "ExecuteMachine", func(ctx context.Context) (*MachineResult, error) {
			return m.Execute(ctx, newVmStoreAdapter(sqlTX), input.Vars)
		})
		if err != nil {
			return nil, nil, fmt.Errorf("failed to execute program: %w", err)
		}

		if len(result.Postings) == 0 {
			return nil, nil, ErrNoPostings
		}

		finalMetadata := result.Metadata
		if finalMetadata == nil {
			finalMetadata = metadata.Metadata{}
		}
		for k, v := range input.Metadata {
			if finalMetadata[k] != "" {
				return nil, nil, newErrMetadataOverride(k)
			}
			finalMetadata[k] = v
		}

		now := time.Now()
		ts := input.Timestamp
		if ts.IsZero() {
			ts = now
		}

		transaction := ledger.NewTransaction().
			WithPostings(result.Postings...).
			WithMetadata(finalMetadata).
			WithTimestamp(ts).
			WithInsertedAt(now).
			WithReference(input.Reference)
		err = sqlTX.CommitTransaction(ctx, &transaction)
		if err != nil {
			return nil, nil, err
		}

		if len(result.AccountMetadata) > 0 {
			if err := sqlTX.UpdateAccountsMetadata(ctx, result.AccountMetadata); err != nil {
				return nil, nil, fmt.Errorf("updating metadata of account '%s': %w", Keys(result.AccountMetadata), err)
			}
		}

		createdTransaction := ledger.CreatedTransaction{
			Transaction:     transaction,
			AccountMetadata: result.AccountMetadata,
		}

		return pointer.For(ledger.NewTransactionLog(createdTransaction)), &createdTransaction, err
	})
	if err != nil {
		return nil, err
	}

	return output, nil
}

func (ctrl *DefaultController) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.RevertedTransaction, error) {
	return forgeLog(ctx, ctrl.store, parameters, func(ctx context.Context, sqlTX TX, input RevertTransaction) (*ledger.Log, *ledger.RevertedTransaction, error) {

		var (
			hasBeenReverted bool
			err             error
		)
		originalTransaction, hasBeenReverted, err := sqlTX.RevertTransaction(ctx, input.TransactionID)
		if err != nil {
			return nil, nil, err
		}
		if !hasBeenReverted {
			return nil, nil, newErrAlreadyReverted(input.TransactionID)
		}

		bq := originalTransaction.InvolvedAccountAndAssets()

		balances, err := sqlTX.GetBalances(ctx, bq)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get balances: %w", err)
		}

		reversedTx := originalTransaction.Reverse()
		if input.AtEffectiveDate {
			reversedTx = reversedTx.WithTimestamp(originalTransaction.Timestamp)
		} else {
			reversedTx = reversedTx.WithTimestamp(*originalTransaction.RevertedAt)
		}

		// Check balances after the revert, all balances must be greater than 0
		if !input.Force {
			for _, posting := range reversedTx.Postings {
				balances[posting.Source][posting.Asset] = balances[posting.Source][posting.Asset].Add(
					balances[posting.Source][posting.Asset],
					big.NewInt(0).Neg(posting.Amount),
				)
				balances[posting.Destination][posting.Destination] = balances[posting.Destination][posting.Asset].Add(
					balances[posting.Destination][posting.Asset],
					big.NewInt(0).Set(posting.Amount),
				)
			}

			for account, forAccount := range balances {
				for asset, finalBalance := range forAccount {
					if finalBalance.Cmp(new(big.Int)) < 0 {
						// todo(waiting): break dependency on machine package
						// notes(gfyrag): wait for the new interpreter
						return nil, nil, machine.NewErrInsufficientFund("insufficient fund for %s/%s", account, asset)
					}
				}
			}
		}

		err = sqlTX.CommitTransaction(ctx, &reversedTx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to insert transaction: %w", err)
		}

		return pointer.For(ledger.NewRevertedTransactionLog(*originalTransaction, reversedTx)), &ledger.RevertedTransaction{
			RevertedTransaction: *originalTransaction,
			RevertTransaction:   reversedTx,
		}, nil
	})
}

func (ctrl *DefaultController) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) error {
	_, err := forgeLog(ctx, ctrl.store, parameters, func(ctx context.Context, sqlTX TX, input SaveTransactionMetadata) (*ledger.Log, *ledger.SavedMetadata, error) {
		if _, _, err := sqlTX.UpdateTransactionMetadata(ctx, input.TransactionID, input.Metadata); err != nil {
			return nil, nil, err
		}

		setMetadata := ledger.SavedMetadata{
			TargetType: ledger.MetaTargetTypeTransaction,
			TargetID:   parameters.Input.TransactionID,
			Metadata:   parameters.Input.Metadata,
		}

		return pointer.For(ledger.NewSetMetadataOnTransactionLog(setMetadata)), &setMetadata, nil
	})
	return err
}

func (ctrl *DefaultController) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) error {
	now := time.Now()
	_, err := forgeLog(ctx, ctrl.store, parameters, func(ctx context.Context, sqlTX TX, input SaveAccountMetadata) (*ledger.Log, *ledger.SavedMetadata, error) {
		if _, err := sqlTX.UpsertAccount(ctx, &ledger.Account{
			Address:       input.Address,
			Metadata:      input.Metadata,
			FirstUsage:    now,
			InsertionDate: now,
			UpdatedAt:     now,
		}); err != nil {
			return nil, nil, err
		}

		setMetadata := ledger.SavedMetadata{
			TargetType: ledger.MetaTargetTypeAccount,
			TargetID:   parameters.Input.Address,
			Metadata:   parameters.Input.Metadata,
		}

		return pointer.For(ledger.NewSetMetadataOnAccountLog(setMetadata)), &setMetadata, nil
	})

	return err
}

func (ctrl *DefaultController) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) error {
	_, err := forgeLog(ctx, ctrl.store, parameters, func(ctx context.Context, sqlTX TX, input DeleteTransactionMetadata) (*ledger.Log, *ledger.DeletedMetadata, error) {
		_, modified, err := sqlTX.DeleteTransactionMetadata(ctx, input.TransactionID, input.Key)
		if err != nil {
			return nil, nil, err
		}

		if !modified {
			return nil, nil, postgres.ErrNotFound
		}

		return pointer.For(ledger.NewDeleteTransactionMetadataLog(input.TransactionID, input.Key)), &ledger.DeletedMetadata{
			TargetType: ledger.MetaTargetTypeTransaction,
			TargetID:   parameters.Input.TransactionID,
			Key:        parameters.Input.Key,
		}, nil
	})

	return err
}

func (ctrl *DefaultController) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) error {
	_, err := forgeLog(ctx, ctrl.store, parameters, func(ctx context.Context, sqlTX TX, input DeleteAccountMetadata) (*ledger.Log, *ledger.DeletedMetadata, error) {
		err := sqlTX.DeleteAccountMetadata(ctx, input.Address, input.Key)
		if err != nil {
			return nil, nil, err
		}

		return pointer.For(ledger.NewDeleteAccountMetadataLog(input.Address, input.Key)), &ledger.DeletedMetadata{
			TargetType: ledger.MetaTargetTypeAccount,
			TargetID:   parameters.Input.Address,
			Key:        parameters.Input.Key,
		}, nil
	})
	return err
}

var _ Controller = (*DefaultController)(nil)
