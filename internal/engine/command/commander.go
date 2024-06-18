package command

import (
	"context"
	"fmt"
	"math/big"
	"sync"

	"github.com/formancehq/stack/libs/go-libs/time"

	storageerrors "github.com/formancehq/ledger/internal/storage/sqlutils"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/bus"
	"github.com/formancehq/ledger/internal/engine/utils/batching"
	"github.com/formancehq/ledger/internal/machine/vm"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
)

type Parameters struct {
	DryRun         bool
	IdempotencyKey string
}

type Commander struct {
	*batching.Batcher[*ledger.ChainedLog]
	store      Store
	locker     Locker
	compiler   *Compiler
	running    sync.WaitGroup
	lastTXID   *big.Int
	referencer *Referencer
	mu         sync.Mutex

	lastLog *ledger.ChainedLog
	monitor bus.Monitor
}

func New(store Store, locker Locker, compiler *Compiler, referencer *Referencer, monitor bus.Monitor, batchSize int) *Commander {
	return &Commander{
		store:      store,
		locker:     locker,
		compiler:   compiler,
		lastTXID:   big.NewInt(-1),
		referencer: referencer,
		Batcher:    batching.NewBatcher(store.InsertLogs, 1, batchSize),
		monitor:    monitor,
	}
}

func (commander *Commander) Init(ctx context.Context) error {
	lastTx, err := commander.store.GetLastTransaction(ctx)
	if err != nil && !storageerrors.IsNotFoundError(err) {
		return err
	}
	if lastTx != nil {
		commander.lastTXID = lastTx.ID
	}

	commander.lastLog, err = commander.store.GetLastLog(ctx)
	if err != nil && !storageerrors.IsNotFoundError(err) {
		return err
	}
	return nil
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

			_, err := commander.store.GetTransactionByReference(ctx, script.Reference)
			if err == nil {
				return nil, NewErrConflict()
			}
			if err != nil && !storageerrors.IsNotFoundError(err) {
				return nil, err
			}
		}

		program, err := commander.compiler.Compile(script.Plain)
		if err != nil {
			return nil, NewErrCompilationFailed(err)
		}

		m := vm.NewMachine(*program)

		if err := m.SetVarsFromJSON(script.Vars); err != nil {
			return nil, NewErrCompilationFailed(err)
		}

		involvedAccounts, involvedSources, err := m.ResolveResources(ctx, commander.store)
		if err != nil {
			return nil, NewErrCompilationFailed(err)
		}

		worldFilter := collectionutils.FilterNot(collectionutils.FilterEq("world"))
		lockAccounts := Accounts{
			Read:  collectionutils.Filter(involvedAccounts, worldFilter),
			Write: collectionutils.Filter(involvedSources, worldFilter),
		}

		unlock, err := commander.locker.Lock(ctx, lockAccounts)
		if err != nil {
			return nil, errors.Wrap(err, "locking accounts for tx processing")
		}
		defer unlock(ctx)

		err = m.ResolveBalances(ctx, commander.store)
		if err != nil {
			return nil, errors.Wrap(err, "could not resolve balances")
		}

		result, err := vm.Run(m, script)
		if err != nil {
			return nil, NewErrMachine(err)
		}

		if len(result.Postings) == 0 {
			return nil, NewErrNoPostings()
		}

		txID := commander.predictNextTxID()
		if !parameters.DryRun {
			txID = commander.allocateNewTxID()
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

func (commander *Commander) chainLog(log *ledger.Log) *ledger.ChainedLog {
	commander.mu.Lock()
	defer commander.mu.Unlock()

	commander.lastLog = log.ChainLog(commander.lastLog)
	return commander.lastLog
}

func (commander *Commander) allocateNewTxID() *big.Int {
	commander.mu.Lock()
	defer commander.mu.Unlock()

	commander.lastTXID = commander.predictNextTxID()

	return commander.lastTXID
}

func (commander *Commander) predictNextTxID() *big.Int {
	return big.NewInt(0).Add(commander.lastTXID, big.NewInt(1))
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
