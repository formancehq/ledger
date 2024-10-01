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
	"github.com/pkg/errors"

	ledger "github.com/formancehq/ledger/internal"
)

type DefaultController struct {
	store          Store
	listener       Listener
	machineFactory MachineFactory
	ledger         ledger.Ledger
}

// todo: could decline controller as some kind of middlewares
func NewDefaultController(
	ledger ledger.Ledger,
	store Store,
	listener Listener,
	machineFactory MachineFactory,
) *DefaultController {
	ret := &DefaultController{
		store:          store,
		listener:       listener,
		ledger:         ledger,
		machineFactory: machineFactory,
	}
	return ret
}

func (ctrl *DefaultController) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return ctrl.store.GetMigrationsInfo(ctx)
}

func (ctrl *DefaultController) ListTransactions(ctx context.Context, q ListTransactionsQuery) (*bunpaginate.Cursor[ledger.Transaction], error) {
	return tracing.Trace(ctx, "ListTransactions", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Transaction], error) {
		txs, err := ctrl.store.ListTransactions(ctx, q)
		return txs, err
	})
}

func (ctrl *DefaultController) CountTransactions(ctx context.Context, q ListTransactionsQuery) (int, error) {
	return tracing.Trace(ctx, "CountTransactions", func(ctx context.Context) (int, error) {
		count, err := ctrl.store.CountTransactions(ctx, q)
		return count, err
	})
}

func (ctrl *DefaultController) GetTransaction(ctx context.Context, query GetTransactionQuery) (*ledger.Transaction, error) {
	return tracing.Trace(ctx, "GetTransaction", func(ctx context.Context) (*ledger.Transaction, error) {
		tx, err := ctrl.store.GetTransaction(ctx, query)
		return tx, err
	})
}

func (ctrl *DefaultController) CountAccounts(ctx context.Context, a ListAccountsQuery) (int, error) {
	return tracing.Trace(ctx, "CountAccounts", func(ctx context.Context) (int, error) {
		count, err := ctrl.store.CountAccounts(ctx, a)
		return count, err
	})
}

func (ctrl *DefaultController) ListAccounts(ctx context.Context, a ListAccountsQuery) (*bunpaginate.Cursor[ledger.Account], error) {
	return tracing.Trace(ctx, "ListAccounts", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Account], error) {
		accounts, err := ctrl.store.ListAccounts(ctx, a)
		return accounts, err
	})
}

func (ctrl *DefaultController) GetAccount(ctx context.Context, q GetAccountQuery) (*ledger.Account, error) {
	return tracing.Trace(ctx, "GetAccount", func(ctx context.Context) (*ledger.Account, error) {
		accounts, err := ctrl.store.GetAccount(ctx, q)
		return accounts, err
	})
}

func (ctrl *DefaultController) GetAggregatedBalances(ctx context.Context, q GetAggregatedBalanceQuery) (ledger.BalancesByAssets, error) {
	return tracing.Trace(ctx, "GetAggregatedBalances", func(ctx context.Context) (ledger.BalancesByAssets, error) {
		balances, err := ctrl.store.GetAggregatedBalances(ctx, q)
		return balances, err
	})
}

func (ctrl *DefaultController) ListLogs(ctx context.Context, q GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error) {
	return tracing.Trace(ctx, "ListLogs", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Log], error) {
		logs, err := ctrl.store.ListLogs(ctx, q)
		return logs, err
	})
}

func (ctrl *DefaultController) Import(ctx context.Context, stream chan ledger.Log) error {
	err := tracing.SkipResult(tracing.Trace(ctx, "Import", tracing.NoResult(func(ctx context.Context) error {
		// Use serializable isolation level to ensure no concurrent request use the store.
		// If a concurrent transactions is made while we are importing some logs, the transaction importing logs will
		// be canceled with serialization error.
		return ctrl.store.WithTX(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}, func(sqlTx TX) (bool, error) {

			// Due to the serializable isolation level, and since we explicitly ask for the ledger state in the sql transaction context
			// if the state change, the sql transaction will be aborted with a serialization error
			if err := sqlTx.LockLedger(ctx); err != nil {
				return false, errors.Wrap(err, "failed to lock ledger")
			}

			// We can import only if the ledger is empty.
			logs, err := sqlTx.ListLogs(ctx, NewListLogsQuery(PaginatedQueryOptions[any]{
				PageSize: 1,
			}))
			if err != nil {
				return false, errors.Wrap(err, "error listing logs")
			}

			if len(logs.Data) > 0 {
				return false, newErrImport(fmt.Errorf("ledger must be empty"))
			}

			for log := range stream {
				if err := ctrl.importLog(ctx, sqlTx, log); err != nil {
					return false, errors.Wrapf(err, "importing log %d", log.ID)
				}
			}

			return true, nil
		})
	})))
	if err != nil {
		if errors.Is(err, postgres.ErrSerialization) {
			return newErrImport(errors.New("concurrent transaction occured, cannot import the ledger"))
		}
	}

	return err
}

func (ctrl *DefaultController) importLog(ctx context.Context, sqlTx TX, log ledger.Log) error {
	switch payload := log.Data.(type) {
	case ledger.NewTransactionLogPayload:
		if err := sqlTx.CommitTransaction(ctx, &payload.Transaction); err != nil {
			return errors.Wrap(err, "failed to commit transaction")
		}
		if len(payload.AccountMetadata) > 0 {
			if err := sqlTx.UpdateAccountsMetadata(ctx, payload.AccountMetadata); err != nil {
				return errors.Wrapf(err, "updating metadata of accounts '%s'", Keys(payload.AccountMetadata))
			}
		}
	case ledger.RevertedTransactionLogPayload:
		_, _, err := sqlTx.RevertTransaction(ctx, payload.RevertedTransactionID)
		if err != nil {
			return errors.Wrap(err, "failed to revert transaction")
		}
	case ledger.SetMetadataLogPayload:
		switch payload.TargetType {
		case ledger.MetaTargetTypeTransaction:
			if _, _, err := sqlTx.UpdateTransactionMetadata(ctx, payload.TargetID.(int), payload.Metadata); err != nil {
				return errors.Wrap(err, "failed to update transaction metadata")
			}
		case ledger.MetaTargetTypeAccount:
			if err := sqlTx.UpdateAccountsMetadata(ctx, ledger.AccountMetadata{
				payload.TargetID.(string): payload.Metadata,
			}); err != nil {
				return errors.Wrap(err, "failed to update account metadata")
			}
		}
	case ledger.DeleteMetadataLogPayload:
		switch payload.TargetType {
		case ledger.MetaTargetTypeTransaction:
			if _, _, err := sqlTx.DeleteTransactionMetadata(ctx, payload.TargetID.(int), payload.Key); err != nil {
				return errors.Wrap(err, "failed to delete transaction metadata")
			}
		case ledger.MetaTargetTypeAccount:
			if err := sqlTx.DeleteAccountMetadata(ctx, payload.TargetID.(string), payload.Key); err != nil {
				return errors.Wrap(err, "failed to delete account metadata")
			}
		}
	}

	logCopy := log
	if err := sqlTx.InsertLog(ctx, &log); err != nil {
		return errors.Wrap(err, "failed to insert log")
	}

	if ctrl.ledger.HasFeature(ledger.FeatureHashLogs, "SYNC") {
		if !reflect.DeepEqual(log.Hash, logCopy.Hash) {
			return newErrInvalidHash(log.ID, logCopy.Hash, log.Hash)
		}
	}

	return nil
}

func (ctrl *DefaultController) Export(ctx context.Context, w ExportWriter) error {
	return tracing.SkipResult(tracing.Trace(ctx, "Export", tracing.NoResult(func(ctx context.Context) error {
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
	})))
}

func (ctrl *DefaultController) IsDatabaseUpToDate(ctx context.Context) (bool, error) {
	return tracing.Trace(ctx, "IsDatabaseUpToDate", func(ctx context.Context) (bool, error) {
		return ctrl.store.IsUpToDate(ctx)
	})
}

func (ctrl *DefaultController) GetVolumesWithBalances(ctx context.Context, q GetVolumesWithBalancesQuery) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
	return tracing.Trace(ctx, "GetVolumesWithBalances", func(ctx context.Context) (*bunpaginate.Cursor[ledger.VolumesWithBalanceByAssetByAccount], error) {
		volumes, err := ctrl.store.GetVolumesWithBalances(ctx, q)
		return volumes, err
	})
}

func (ctrl *DefaultController) CreateTransaction(ctx context.Context, parameters Parameters[RunScript]) (*ledger.Transaction, error) {

	log, err := tracing.TraceWithLatency(ctx, "CreateTransaction", func(ctx context.Context) (*ledger.Log, error) {
		logger := logging.FromContext(ctx).WithField("req", uuid.NewString()[:8])
		ctx = logging.ContextWithLogger(ctx, logger)

		m, err := ctrl.machineFactory.Make(parameters.Input.Plain)
		if err != nil {
			return nil, errors.Wrap(err, "failed to compile script")
		}

		return forgeLog(ctx, ctrl.store, parameters, func(ctx context.Context, sqlTX TX, input RunScript) (*ledger.Log, error) {
			result, err := tracing.TraceWithLatency(ctx, "ExecuteMachine", func(ctx context.Context) (*MachineResult, error) {
				return m.Execute(ctx, newVmStoreAdapter(sqlTX), input.Vars)
			})
			if err != nil {
				return nil, errors.Wrap(err, "failed to execute program")
			}

			if len(result.Postings) == 0 {
				return nil, ErrNoPostings
			}

			finalMetadata := result.Metadata
			if finalMetadata == nil {
				finalMetadata = metadata.Metadata{}
			}
			for k, v := range input.Metadata {
				if finalMetadata[k] != "" {
					return nil, newErrMetadataOverride(k)
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
				return nil, err
			}

			if len(result.AccountMetadata) > 0 {
				if err := sqlTX.UpdateAccountsMetadata(ctx, result.AccountMetadata); err != nil {
					return nil, errors.Wrapf(err, "updating metadata of account '%s'", Keys(result.AccountMetadata))
				}
			}

			return pointer.For(ledger.NewTransactionLog(transaction, result.AccountMetadata)), err
		})
	})
	if err != nil {
		return nil, err
	}

	transaction := log.Data.(ledger.NewTransactionLogPayload).Transaction
	accountMetadata := log.Data.(ledger.NewTransactionLogPayload).AccountMetadata

	ctrl.listener.CommittedTransactions(ctx, ctrl.ledger.Name, transaction, accountMetadata)

	return &transaction, nil
}

func (ctrl *DefaultController) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.Transaction, error) {
	var originalTransaction *ledger.Transaction
	ret, err := tracing.Trace(ctx, "RevertTransaction", func(ctx context.Context) (*ledger.Transaction, error) {
		log, err := forgeLog(ctx, ctrl.store, parameters, func(ctx context.Context, sqlTX TX, input RevertTransaction) (*ledger.Log, error) {

			var (
				hasBeenReverted bool
				err             error
			)
			originalTransaction, hasBeenReverted, err = sqlTX.RevertTransaction(ctx, input.TransactionID)
			if err != nil {
				return nil, err
			}
			if !hasBeenReverted {
				return nil, newErrAlreadyReverted(input.TransactionID)
			}

			bq := originalTransaction.InvolvedAccountAndAssets()

			balances, err := sqlTX.GetBalances(ctx, bq)
			if err != nil {
				return nil, errors.Wrap(err, "failed to get balances")
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
							return nil, machine.NewErrInsufficientFund("insufficient fund for %s/%s", account, asset)
						}
					}
				}
			}

			err = sqlTX.CommitTransaction(ctx, &reversedTx)
			if err != nil {
				return nil, errors.Wrap(err, "failed to insert transaction")
			}

			return pointer.For(ledger.NewRevertedTransactionLog(input.TransactionID, reversedTx)), nil
		})
		if err != nil {
			return nil, err
		}

		return pointer.For(log.Data.(ledger.RevertedTransactionLogPayload).RevertTransaction), nil
	})
	if err != nil {
		return nil, err
	}

	if ctrl.listener != nil {
		ctrl.listener.RevertedTransaction(ctx, ctrl.ledger.Name, originalTransaction, ret)
	}

	return ret, nil
}

func (ctrl *DefaultController) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) error {
	if err := tracing.SkipResult(tracing.Trace(ctx, "SaveTransactionMetadata", func(ctx context.Context) (*ledger.Log, error) {
		return forgeLog(ctx, ctrl.store, parameters, func(ctx context.Context, sqlTX TX, input SaveTransactionMetadata) (*ledger.Log, error) {
			if _, _, err := sqlTX.UpdateTransactionMetadata(ctx, input.TransactionID, input.Metadata); err != nil {
				return nil, err
			}

			return pointer.For(ledger.NewSetMetadataOnTransactionLog(input.TransactionID, input.Metadata)), nil
		})
	})); err != nil {
		return err
	}

	if ctrl.listener != nil {
		ctrl.listener.SavedMetadata(
			ctx,
			ctrl.ledger.Name,
			ledger.MetaTargetTypeTransaction,
			fmt.Sprint(parameters.Input.TransactionID),
			parameters.Input.Metadata,
		)
	}

	return nil
}

func (ctrl *DefaultController) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) error {
	if err := tracing.SkipResult(tracing.Trace(ctx, "SaveAccountMetadata", func(ctx context.Context) (*ledger.Log, error) {
		now := time.Now()
		return forgeLog(ctx, ctrl.store, parameters, func(ctx context.Context, sqlTX TX, input SaveAccountMetadata) (*ledger.Log, error) {
			if _, err := sqlTX.UpsertAccount(ctx, &ledger.Account{
				Address:       input.Address,
				Metadata:      input.Metadata,
				FirstUsage:    now,
				InsertionDate: now,
				UpdatedAt:     now,
			}); err != nil {
				return nil, err
			}

			return pointer.For(ledger.NewSetMetadataOnAccountLog(input.Address, input.Metadata)), nil
		})
	})); err != nil {
		return err
	}

	if ctrl.listener != nil {
		ctrl.listener.SavedMetadata(
			ctx,
			ctrl.ledger.Name,
			ledger.MetaTargetTypeAccount,
			parameters.Input.Address,
			parameters.Input.Metadata,
		)
	}

	return nil
}

func (ctrl *DefaultController) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) error {
	if err := tracing.SkipResult(tracing.Trace(ctx, "DeleteTransactionMetadata", func(ctx context.Context) (*ledger.Log, error) {
		return forgeLog(ctx, ctrl.store, parameters, func(ctx context.Context, sqlTX TX, input DeleteTransactionMetadata) (*ledger.Log, error) {
			_, modified, err := sqlTX.DeleteTransactionMetadata(ctx, input.TransactionID, input.Key)
			if err != nil {
				return nil, err
			}

			if !modified {
				return nil, postgres.ErrNotFound
			}

			return pointer.For(ledger.NewDeleteTransactionMetadataLog(input.TransactionID, input.Key)), nil
		})
	})); err != nil {
		return err
	}

	// todo: events should not be sent in dry run!
	if ctrl.listener != nil {
		ctrl.listener.DeletedMetadata(
			ctx,
			ctrl.ledger.Name,
			ledger.MetaTargetTypeTransaction,
			fmt.Sprint(parameters.Input.TransactionID),
			parameters.Input.Key,
		)
	}

	return nil
}

func (ctrl *DefaultController) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) error {
	if err := tracing.SkipResult(tracing.Trace(ctx, "DeleteAccountMetadata", func(ctx context.Context) (*ledger.Log, error) {
		return forgeLog(ctx, ctrl.store, parameters, func(ctx context.Context, sqlTX TX, input DeleteAccountMetadata) (*ledger.Log, error) {
			err := sqlTX.DeleteAccountMetadata(ctx, input.Address, input.Key)
			if err != nil {
				return nil, err
			}

			return pointer.For(ledger.NewDeleteAccountMetadataLog(input.Address, input.Key)), nil
		})
	})); err != nil {
		return err
	}

	if ctrl.listener != nil {
		ctrl.listener.DeletedMetadata(
			ctx,
			ctrl.ledger.Name,
			ledger.MetaTargetTypeAccount,
			parameters.Input.Address,
			parameters.Input.Key,
		)
	}

	return nil
}

var _ Controller = (*DefaultController)(nil)
