package command

import (
	"context"
	"fmt"
	"math/big"
	"sync"

	"github.com/formancehq/ledger/v2/internal/machine/vm/program"
	"github.com/formancehq/ledger/v2/internal/opentelemetry/tracer"

	"github.com/formancehq/go-libs/time"

	storageerrors "github.com/formancehq/ledger/v2/internal/storage/sqlutils"

	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/v2/internal"
	"github.com/formancehq/ledger/v2/internal/bus"
	"github.com/formancehq/ledger/v2/internal/engine/utils/batching"
	"github.com/formancehq/ledger/v2/internal/machine/vm"
	"github.com/pkg/errors"
)

type Parameters struct {
	DryRun         bool
	IdempotencyKey string
}

type Chainer interface {
	ChainLog(log *ledger.Log) *ledger.ChainedLog
	AllocateNewTxID() *big.Int
	PredictNextTxID() *big.Int
}

type Commander struct {
	*batching.Batcher[*ledger.ChainedLog]
	store      Store
	locker     Locker
	compiler   *Compiler
	running    sync.WaitGroup
	referencer *Referencer

	monitor bus.Monitor
	chain   Chainer
}

func New(
	store Store,
	locker Locker,
	compiler *Compiler,
	referencer *Referencer,
	monitor bus.Monitor,
	chain Chainer,
	batchSize int,
) *Commander {
	return &Commander{
		store:      store,
		locker:     locker,
		compiler:   compiler,
		chain:      chain,
		referencer: referencer,
		Batcher:    batching.NewBatcher(store.InsertLogs, 1, batchSize),
		monitor:    monitor,
	}
}

func (commander *Commander) GetLedgerStore() Store {
	return commander.store
}

func (commander *Commander) exec(ctx context.Context, parameters Parameters, script ledger.RunScript,
	logComputer func(tx *ledger.Transaction, accountMetadata map[string]metadata.Metadata) *ledger.Log) (*ledger.ChainedLog, error) {

	if script.Script.Plain == "" {
		return nil, NewErrNoScript()
	}

	if script.Timestamp.IsZero() {
		script.Timestamp = time.Now()
	}

	execContext := newExecutionContext(commander, parameters)
	return execContext.run(ctx, func(executionContext *executionContext) (*ledger.ChainedLog, error) {
		if script.Reference != "" {
			if err := commander.referencer.take(referenceTxReference, script.Reference); err != nil {
				return nil, NewErrConflict()
			}
			defer commander.referencer.release(referenceTxReference, script.Reference)

			err := func() error {
				ctx, span := tracer.Start(ctx, "CheckReference")
				defer span.End()

				_, err := commander.store.GetTransactionByReference(ctx, script.Reference)
				if err == nil {
					return NewErrConflict()
				}
				if err != nil && !storageerrors.IsNotFoundError(err) {
					return err
				}
				return nil
			}()
			if err != nil {
				return nil, err
			}
		}

		program, err := func() (*program.Program, error) {
			_, span := tracer.Start(ctx, "CompileNumscript")
			defer span.End()

			program, err := commander.compiler.Compile(script.Plain)
			if err != nil {
				return nil, NewErrCompilationFailed(err)
			}

			return program, nil
		}()
		if err != nil {
			return nil, err
		}

		m := vm.NewMachine(*program)
		if err := m.SetVarsFromJSON(script.Vars); err != nil {
			return nil, NewErrCompilationFailed(err)
		}

		readLockAccounts, writeLockAccounts, err := m.ResolveResources(ctx, commander.store)
		if err != nil {
			return nil, NewErrCompilationFailed(err)
		}
		lockAccounts := Accounts{
			Read:  readLockAccounts,
			Write: writeLockAccounts,
		}

		unlock, err := func() (Unlock, error) {
			_, span := tracer.Start(ctx, "Lock")
			defer span.End()

			unlock, err := commander.locker.Lock(ctx, lockAccounts)
			if err != nil {
				return nil, errors.Wrap(err, "locking accounts for tx processing")
			}

			return unlock, nil
		}()
		if err != nil {
			return nil, err
		}
		defer unlock(ctx)

		err = func() error {
			ctx, span := tracer.Start(ctx, "ResolveBalances")
			defer span.End()

			err = m.ResolveBalances(ctx, commander.store)
			if err != nil {
				return errors.Wrap(err, "could not resolve balances")
			}

			return nil
		}()
		if err != nil {
			return nil, err
		}
		result, err := func() (*vm.Result, error) {
			_, span := tracer.Start(ctx, "RunNumscript")
			defer span.End()

			result, err := vm.Run(m, script)
			if err != nil {
				return nil, NewErrMachine(err)
			}

			return result, nil
		}()
		if err != nil {
			return nil, err
		}

		if len(result.Postings) == 0 {
			return nil, NewErrNoPostings()
		}

		txID := commander.chain.PredictNextTxID()
		if !parameters.DryRun {
			txID = commander.chain.AllocateNewTxID()
		}

		tx := ledger.NewTransaction().
			WithPostings(result.Postings...).
			WithMetadata(result.Metadata).
			WithDate(script.Timestamp).
			WithID(txID).
			WithReference(script.Reference)

		log := logComputer(tx, result.AccountMetadata)
		if parameters.IdempotencyKey != "" {
			log = log.WithIdempotencyKey(parameters.IdempotencyKey)
		}

		return executionContext.AppendLog(ctx, log)
	})
}

func (commander *Commander) CreateTransaction(ctx context.Context, parameters Parameters, script ledger.RunScript) (*ledger.Transaction, error) {

	ctx, span := tracer.Start(ctx, "CreateTransaction")
	defer span.End()

	log, err := commander.exec(ctx, parameters, script, ledger.NewTransactionLog)
	if err != nil {

		return nil, err
	}

	commander.monitor.CommittedTransactions(ctx, *log.Data.(ledger.NewTransactionLogPayload).Transaction, log.Data.(ledger.NewTransactionLogPayload).AccountMetadata)

	return log.Data.(ledger.NewTransactionLogPayload).Transaction, nil
}

func (commander *Commander) SaveMeta(ctx context.Context, parameters Parameters, targetType string, targetID interface{}, m metadata.Metadata) error {
	execContext := newExecutionContext(commander, parameters)
	_, err := execContext.run(ctx, func(executionContext *executionContext) (*ledger.ChainedLog, error) {
		var (
			log *ledger.Log
			at  = time.Now()
		)
		switch targetType {
		case ledger.MetaTargetTypeTransaction:
			_, err := commander.store.GetTransaction(ctx, targetID.(*big.Int))
			if err != nil {
				if storageerrors.IsNotFoundError(err) {
					return nil, newErrSaveMetadataTransactionNotFound()
				}
			}
			log = ledger.NewSetMetadataLog(at, ledger.SetMetadataLogPayload{
				TargetType: ledger.MetaTargetTypeTransaction,
				TargetID:   targetID.(*big.Int),
				Metadata:   m,
			})
		case ledger.MetaTargetTypeAccount:
			log = ledger.NewSetMetadataLog(at, ledger.SetMetadataLogPayload{
				TargetType: ledger.MetaTargetTypeAccount,
				TargetID:   targetID.(string),
				Metadata:   m,
			})
		default:
			panic(errors.Errorf("unknown target type '%s'", targetType))
		}

		return executionContext.AppendLog(ctx, log)
	})
	if err != nil {
		return err
	}

	commander.monitor.SavedMetadata(ctx, targetType, fmt.Sprint(targetID), m)
	return nil
}

func (commander *Commander) RevertTransaction(ctx context.Context, parameters Parameters, id *big.Int, force, atEffectiveDate bool) (*ledger.Transaction, error) {

	if err := commander.referencer.take(referenceReverts, id); err != nil {
		return nil, NewErrRevertTransactionOccurring()
	}
	defer commander.referencer.release(referenceReverts, id)

	transactionToRevert, err := commander.store.GetTransaction(ctx, id)
	if err != nil {
		if storageerrors.IsNotFoundError(err) {
			return nil, NewErrRevertTransactionNotFound()
		}
		return nil, err
	}
	if transactionToRevert.Reverted {
		return nil, NewErrRevertTransactionAlreadyReverted()
	}

	rt := transactionToRevert.Reverse()
	rt.Metadata = ledger.MarkReverts(metadata.Metadata{}, transactionToRevert.ID)

	script := ledger.TxToScriptData(ledger.TransactionData{
		Postings: rt.Postings,
		Metadata: rt.Metadata,
	}, force)
	if atEffectiveDate {
		script.Timestamp = transactionToRevert.Timestamp
	}

	log, err := commander.exec(ctx, parameters, script,
		func(tx *ledger.Transaction, accountMetadata map[string]metadata.Metadata) *ledger.Log {
			return ledger.NewRevertedTransactionLog(tx.Timestamp, transactionToRevert.ID, tx)
		})
	if err != nil {
		return nil, err
	}

	commander.monitor.RevertedTransaction(ctx, log.Data.(ledger.RevertedTransactionLogPayload).RevertTransaction, transactionToRevert)

	return log.Data.(ledger.RevertedTransactionLogPayload).RevertTransaction, nil
}

func (commander *Commander) Close() {
	commander.Batcher.Close()
	commander.running.Wait()
}

func (commander *Commander) DeleteMetadata(ctx context.Context, parameters Parameters, targetType string, targetID any, key string) error {
	execContext := newExecutionContext(commander, parameters)
	_, err := execContext.run(ctx, func(executionContext *executionContext) (*ledger.ChainedLog, error) {
		var (
			log *ledger.Log
			at  = time.Now()
		)
		switch targetType {
		case ledger.MetaTargetTypeTransaction:
			_, err := commander.store.GetTransaction(ctx, targetID.(*big.Int))
			if err != nil {
				return nil, newErrDeleteMetadataTransactionNotFound()
			}
			log = ledger.NewDeleteMetadataLog(at, ledger.DeleteMetadataLogPayload{
				TargetType: ledger.MetaTargetTypeTransaction,
				TargetID:   targetID.(*big.Int),
				Key:        key,
			})
		case ledger.MetaTargetTypeAccount:
			log = ledger.NewDeleteMetadataLog(at, ledger.DeleteMetadataLogPayload{
				TargetType: ledger.MetaTargetTypeAccount,
				TargetID:   targetID.(string),
				Key:        key,
			})
		default:
			panic(errors.Errorf("unknown target type '%s'", targetType))
		}

		return executionContext.AppendLog(ctx, log)
	})
	if err != nil {
		return err
	}

	commander.monitor.DeletedMetadata(ctx, targetType, targetID, key)

	return nil
}
