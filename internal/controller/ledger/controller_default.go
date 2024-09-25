package ledger

import (
	"context"
	"fmt"
	"math/big"
	"reflect"

	"github.com/formancehq/go-libs/time"

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

func (ctrl *DefaultController) runTx(ctx context.Context, parameters Parameters, fn func(sqlTX TX) (*ledger.Log, error)) (*ledger.Log, error) {
	var log *ledger.Log
	err := ctrl.store.WithTX(ctx, func(tx TX) (commit bool, err error) {
		log, err = fn(tx)
		if err != nil {
			return false, err
		}
		log.IdempotencyKey = parameters.IdempotencyKey

		_, err = tracing.TraceWithLatency(ctx, "InsertLog", func(ctx context.Context) (*struct{}, error) {
			return nil, tx.InsertLog(ctx, log)
		})
		if err != nil {
			return false, errors.Wrap(err, "failed to insert log")
		}
		logging.FromContext(ctx).Debugf("log inserted with id %d", log.ID)

		if parameters.DryRun {
			return false, nil
		}

		if ctrl.ledger.State == ledger.StateInitializing {
			if err := tx.SwitchLedgerState(ctx, ctrl.ledger.Name, ledger.StateInUse); err != nil {
				return false, errors.Wrap(err, "failed to switch ledger state")
			}
		}

		return true, nil
	})
	return log, err
}

func (ctrl *DefaultController) forgeLog(ctx context.Context, parameters Parameters, fn func(sqlTX TX) (*ledger.Log, error)) (*ledger.Log, error) {

	if parameters.IdempotencyKey != "" {
		log, err := ctrl.store.ReadLogWithIdempotencyKey(ctx, parameters.IdempotencyKey)
		if err != nil && !errors.Is(err, postgres.ErrNotFound) {
			return nil, err
		}
		if err == nil {
			return log, nil
		}
	}

	for {
		log, err := ctrl.runTx(ctx, parameters, fn)
		if err != nil {
			switch {
			case errors.Is(err, postgres.ErrDeadlockDetected):
				logging.FromContext(ctx).Info("deadlock detected, retrying...")
				continue
			// A log with the IK could have been inserted in the meantime, read again the database to retrieve it
			case errors.Is(err, ErrIdempotencyKeyConflict{}):
				log, err := ctrl.store.ReadLogWithIdempotencyKey(ctx, parameters.IdempotencyKey)
				if err != nil && !errors.Is(err, postgres.ErrNotFound) {
					return nil, err
				}
				if errors.Is(err, postgres.ErrNotFound) {
					logging.FromContext(ctx).Errorf("incoherent error, received duplicate IK but log not found in database")
					return nil, err
				}

				return log, nil
			default:
				return nil, errors.Wrap(err, "unexpected error while forging log")
			}
		}

		return log, nil
	}
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

func (ctrl *DefaultController) ListAccounts(ctx context.Context, a ListAccountsQuery) (*bunpaginate.Cursor[ledger.ExpandedAccount], error) {
	return tracing.Trace(ctx, "ListAccounts", func(ctx context.Context) (*bunpaginate.Cursor[ledger.ExpandedAccount], error) {
		accounts, err := ctrl.store.ListAccounts(ctx, a)
		return accounts, err
	})
}

func (ctrl *DefaultController) GetAccount(ctx context.Context, q GetAccountQuery) (*ledger.ExpandedAccount, error) {
	return tracing.Trace(ctx, "GetAccount", func(ctx context.Context) (*ledger.ExpandedAccount, error) {
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
	return tracing.SkipResult(tracing.Trace(ctx, "Import", tracing.NoResult(func(ctx context.Context) error {
		// todo: need to write to in-use when creating a new transaction
		// maybe take a lock to avoid a concurrent transaction to change the ledger state?
		if ctrl.ledger.State != ledger.StateInitializing {
			return newErrInvalidState(ledger.StateInitializing, ctrl.ledger.State)
		}

		return ctrl.store.WithTX(ctx, func(sqlTx TX) (bool, error) {
			for log := range stream {
				switch payload := log.Data.(type) {
				case ledger.NewTransactionLogPayload:
					if err := sqlTx.CommitTransaction(ctx, &payload.Transaction); err != nil {
						return false, errors.Wrap(err, "failed to commit transaction")
					}
					if len(payload.AccountMetadata) > 0 {
						if err := sqlTx.UpdateAccountsMetadata(ctx, payload.AccountMetadata); err != nil {
							return false, errors.Wrapf(err, "updating metadata of accounts '%s'", Keys(payload.AccountMetadata))
						}
					}
				case ledger.RevertedTransactionLogPayload:
					_, _, err := sqlTx.RevertTransaction(ctx, payload.RevertedTransactionID)
					if err != nil {
						return false, err
					}
				case ledger.SetMetadataLogPayload:
					switch payload.TargetType {
					case ledger.MetaTargetTypeTransaction:
						if _, _, err := sqlTx.UpdateTransactionMetadata(ctx, payload.TargetID.(int), payload.Metadata); err != nil {
							return false, err
						}
					case ledger.MetaTargetTypeAccount:
						if err := sqlTx.UpdateAccountsMetadata(ctx, ledger.AccountMetadata{
							payload.TargetID.(string): payload.Metadata,
						}); err != nil {
							return false, err
						}
					}
				case ledger.DeleteMetadataLogPayload:
					switch payload.TargetType {
					case ledger.MetaTargetTypeTransaction:
						if _, _, err := sqlTx.DeleteTransactionMetadata(ctx, payload.TargetID.(int), payload.Key); err != nil {
							return false, err
						}
					case ledger.MetaTargetTypeAccount:
						if err := sqlTx.DeleteAccountMetadata(ctx, payload.TargetID.(string), payload.Key); err != nil {
							return false, err
						}
					}
				}

				logCopy := log
				if err := sqlTx.InsertLog(ctx, &log); err != nil {
					return false, errors.Wrap(err, "failed to insert log")
				}

				if !reflect.DeepEqual(log.Hash, logCopy.Hash) {
					return false, newErrInvalidHash(log.ID, logCopy.Hash, log.Hash)
				}
			}

			return true, nil
		})
	})))
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

func (ctrl *DefaultController) CreateTransaction(ctx context.Context, parameters Parameters, runScript ledger.RunScript) (*ledger.Transaction, error) {

	log, err := tracing.TraceWithLatency(ctx, "CreateTransaction", func(ctx context.Context) (*ledger.Log, error) {
		logger := logging.FromContext(ctx).WithField("req", uuid.NewString()[:8])
		ctx = logging.ContextWithLogger(ctx, logger)

		m, err := ctrl.machineFactory.Make(runScript.Plain)
		if err != nil {
			return nil, errors.Wrap(err, "failed to compile script")
		}

		return ctrl.forgeLog(ctx, parameters, func(sqlTX TX) (*ledger.Log, error) {

			result, err := tracing.TraceWithLatency(ctx, "ExecuteMachine", func(ctx context.Context) (*MachineResult, error) {
				return m.Execute(ctx, newVmStoreAdapter(sqlTX), runScript.Vars)
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
			for k, v := range runScript.Metadata {
				if finalMetadata[k] != "" {
					return nil, newErrMetadataOverride(k)
				}
				finalMetadata[k] = v
			}

			now := time.Now()
			ts := runScript.Timestamp
			if ts.IsZero() {
				ts = now
			}

			transaction := ledger.NewTransaction().
				WithPostings(result.Postings...).
				WithMetadata(finalMetadata).
				WithTimestamp(ts).
				WithInsertedAt(now).
				WithReference(runScript.Reference)
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

func (ctrl *DefaultController) RevertTransaction(ctx context.Context, parameters Parameters, id int, force, atEffectiveDate bool) (*ledger.Transaction, error) {
	var originalTransaction *ledger.Transaction
	ret, err := tracing.Trace(ctx, "RevertTransaction", func(ctx context.Context) (*ledger.Transaction, error) {
		log, err := ctrl.forgeLog(ctx, parameters, func(sqlTX TX) (*ledger.Log, error) {

			var (
				hasBeenReverted bool
				err             error
			)
			originalTransaction, hasBeenReverted, err = sqlTX.RevertTransaction(ctx, id)
			if err != nil {
				return nil, err
			}
			if !hasBeenReverted {
				return nil, newErrAlreadyReverted(id)
			}

			bq := originalTransaction.InvolvedAccountAndAssets()

			balances, err := sqlTX.GetBalances(ctx, bq)
			if err != nil {
				return nil, errors.Wrap(err, "failed to get balances")
			}

			reversedTx := originalTransaction.Reverse(atEffectiveDate)

			// Check balances after the revert
			// must be greater than 0
			if !force {
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

			return pointer.For(ledger.NewRevertedTransactionLog(id, reversedTx)), nil
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

func (ctrl *DefaultController) SaveTransactionMetadata(ctx context.Context, parameters Parameters, id int, m metadata.Metadata) error {
	if err := tracing.SkipResult(tracing.Trace(ctx, "SaveTransactionMetadata", tracing.NoResult(func(ctx context.Context) error {
		_, err := ctrl.forgeLog(ctx, parameters, func(sqlTX TX) (*ledger.Log, error) {
			if _, _, err := sqlTX.UpdateTransactionMetadata(ctx, id, m); err != nil {
				return nil, err
			}

			return pointer.For(ledger.NewSetMetadataOnTransactionLog(id, m)), nil
		})
		return err
	}))); err != nil {
		return err
	}

	if ctrl.listener != nil {
		ctrl.listener.SavedMetadata(ctx, ctrl.ledger.Name, ledger.MetaTargetTypeTransaction, fmt.Sprint(id), m)
	}

	return nil
}

func (ctrl *DefaultController) SaveAccountMetadata(ctx context.Context, parameters Parameters, address string, m metadata.Metadata) error {
	if err := tracing.SkipResult(tracing.Trace(ctx, "SaveAccountMetadata", tracing.NoResult(func(ctx context.Context) error {
		now := time.Now()
		_, err := ctrl.forgeLog(ctx, parameters, func(sqlTX TX) (*ledger.Log, error) {
			if err := sqlTX.UpsertAccount(ctx, &ledger.Account{
				Address:       address,
				Metadata:      m,
				FirstUsage:    now,
				InsertionDate: now,
				UpdatedAt:     now,
			}); err != nil {
				return nil, err
			}

			return pointer.For(ledger.NewSetMetadataOnAccountLog(address, m)), nil
		})
		return err
	}))); err != nil {
		return err
	}

	if ctrl.listener != nil {
		ctrl.listener.SavedMetadata(ctx, ctrl.ledger.Name, ledger.MetaTargetTypeAccount, address, m)
	}

	return nil
}

func (ctrl *DefaultController) DeleteTransactionMetadata(ctx context.Context, parameters Parameters, targetID int, key string) error {
	if err := tracing.SkipResult(tracing.Trace(ctx, "DeleteTransactionMetadata", func(ctx context.Context) (*ledger.Log, error) {
		return ctrl.forgeLog(ctx, parameters, func(sqlTX TX) (*ledger.Log, error) {
			_, modified, err := sqlTX.DeleteTransactionMetadata(ctx, targetID, key)
			if err != nil {
				return nil, err
			}

			if !modified {
				return nil, postgres.ErrNotFound
			}

			return pointer.For(ledger.NewDeleteTransactionMetadataLog(targetID, key)), nil
		})
	})); err != nil {
		return err
	}

	if ctrl.listener != nil {
		ctrl.listener.DeletedMetadata(ctx, ctrl.ledger.Name, ledger.MetaTargetTypeTransaction, fmt.Sprint(targetID), key)
	}

	return nil
}

func (ctrl *DefaultController) DeleteAccountMetadata(ctx context.Context, parameters Parameters, targetID string, key string) error {
	if err := tracing.SkipResult(tracing.Trace(ctx, "DeleteAccountMetadata", func(ctx context.Context) (*ledger.Log, error) {
		return ctrl.forgeLog(ctx, parameters, func(sqlTX TX) (*ledger.Log, error) {
			err := sqlTX.DeleteAccountMetadata(ctx, targetID, key)
			if err != nil {
				return nil, err
			}

			return pointer.For(ledger.NewDeleteAccountMetadataLog(targetID, key)), nil
		})
	})); err != nil {
		return err
	}

	if ctrl.listener != nil {
		ctrl.listener.DeletedMetadata(ctx, ctrl.ledger.Name, ledger.MetaTargetTypeAccount, targetID, key)
	}

	return nil
}

var _ Controller = (*DefaultController)(nil)
