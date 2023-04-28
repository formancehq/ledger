package command

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/machine"
	"github.com/formancehq/ledger/pkg/machine/vm"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	storageerrors "github.com/formancehq/ledger/pkg/storage/errors"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/pkg/errors"
)

type Parameters struct {
	DryRun         bool
	Async          bool
	IdempotencyKey string
}

type Commander struct {
	store           Store
	locker          Locker
	metricsRegistry metrics.PerLedgerMetricsRegistry
	compiler        *Compiler
	running         sync.WaitGroup
	lastTXID        *atomic.Int64
	referencer      *Referencer
}

func New(
	store Store,
	locker Locker,
	compiler *Compiler,
	referencer *Referencer,
	metricsRegistry metrics.PerLedgerMetricsRegistry,
) *Commander {
	log, err := store.ReadLastLogWithType(context.Background(), core.NewTransactionLogType, core.RevertedTransactionLogType)
	if err != nil && !storageerrors.IsNotFoundError(err) {
		panic(err)
	}
	var lastTxID *uint64
	if err == nil {
		switch payload := log.Data.(type) {
		case core.NewTransactionLogPayload:
			lastTxID = &payload.Transaction.ID
		case core.RevertedTransactionLogPayload:
			lastTxID = &payload.RevertTransaction.ID
		default:
			panic(fmt.Sprintf("unhandled payload type: %T", payload))
		}
	}
	lastTXID := &atomic.Int64{}
	if lastTxID != nil {
		lastTXID.Add(int64(*lastTxID))
	} else {
		lastTXID.Add(-1)
	}
	return &Commander{
		store:           store,
		locker:          locker,
		metricsRegistry: metricsRegistry,
		compiler:        compiler,
		referencer:      referencer,
		lastTXID:        lastTXID,
	}
}

func (commander *Commander) GetLedgerStore() Store {
	return commander.store
}

func (commander *Commander) exec(ctx context.Context, parameters Parameters, script core.RunScript,
	logComputer func(tx *core.Transaction, accountMetadata map[string]metadata.Metadata) *core.Log) (*core.PersistedLog, error) {

	if script.Script.Plain == "" {
		return nil, ErrNoScript
	}

	if script.Timestamp.IsZero() {
		script.Timestamp = core.Now()
	}

	execContext := newExecutionContext(commander, parameters)
	tracker, err := execContext.run(ctx, func(executionContext *executionContext) (*core.LogPersistenceTracker, error) {
		if script.Reference != "" {
			if err := commander.referencer.take(referenceTxReference, script.Reference); err != nil {
				return nil, ErrConflictError
			}
			defer commander.referencer.release(referenceTxReference, script.Reference)

			_, err := commander.store.ReadLogForCreatedTransactionWithReference(ctx, script.Reference)
			if err == nil {
				return nil, ErrConflictError
			}
			if err != storageerrors.ErrNotFound && err != nil {
				return nil, err
			}
		}

		program, err := commander.compiler.Compile(ctx, script.Plain)
		if err != nil {
			return nil, errorsutil.NewError(ErrCompilationFailed, errors.Wrap(err, "compiling numscript"))
		}

		m := vm.NewMachine(*program)

		if err := m.SetVarsFromJSON(script.Vars); err != nil {
			return nil, errorsutil.NewError(ErrCompilationFailed,
				errors.Wrap(err, "could not set variables"))
		}

		involvedAccounts, involvedSources, err := m.ResolveResources(ctx, commander.store)
		if err != nil {
			return nil, errorsutil.NewError(ErrCompilationFailed,
				errors.Wrap(err, "could not resolve program resources"))
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
		unlock(ctx)

		err = m.ResolveBalances(ctx, commander.store)
		if err != nil {
			return nil, errorsutil.NewError(ErrCompilationFailed,
				errors.Wrap(err, "could not resolve balances"))
		}

		result, err := machine.Run(m, script)
		if err != nil {
			return nil, errors.Wrap(err, "running numscript")
		}

		if len(result.Postings) == 0 {
			return nil, ErrNoPostings
		}

		tx := core.NewTransaction().
			WithPostings(result.Postings...).
			WithMetadata(result.Metadata).
			WithTimestamp(script.Timestamp).
			WithID(uint64(commander.lastTXID.Add(1))).
			WithReference(script.Reference)

		log := logComputer(tx, result.AccountMetadata)
		if parameters.IdempotencyKey != "" {
			log = log.WithIdempotencyKey(parameters.IdempotencyKey)
		}

		return executionContext.AppendLog(ctx, core.NewActiveLog(log))
	})
	if err != nil {
		return nil, err
	}
	return tracker.Result(), nil
}

func (commander *Commander) CreateTransaction(ctx context.Context, parameters Parameters, script core.RunScript) (*core.Transaction, error) {
	log, err := commander.exec(ctx, parameters, script, core.NewTransactionLog)
	if err != nil {
		return nil, err
	}
	return log.Data.(core.NewTransactionLogPayload).Transaction, nil
}

func (commander *Commander) SaveMeta(ctx context.Context, parameters Parameters, targetType string, targetID interface{}, m metadata.Metadata) error {
	if m == nil {
		return nil
	}

	if targetType == "" {
		return errorsutil.NewError(ErrValidation, errors.New("empty target type"))
	}
	if targetID == "" {
		return errorsutil.NewError(ErrValidation, errors.New("empty target id"))
	}

	execContext := newExecutionContext(commander, parameters)
	_, err := execContext.run(ctx, func(executionContext *executionContext) (*core.LogPersistenceTracker, error) {
		var (
			log *core.Log
			at  = core.Now()
		)
		switch targetType {
		case core.MetaTargetTypeTransaction:
			_, err := commander.store.ReadLogForCreatedTransaction(ctx, targetID.(uint64))
			if err != nil {
				return nil, err
			}
			log = core.NewSetMetadataLog(at, core.SetMetadataLogPayload{
				TargetType: core.MetaTargetTypeTransaction,
				TargetID:   targetID.(uint64),
				Metadata:   m,
			})
		case core.MetaTargetTypeAccount:
			log = core.NewSetMetadataLog(at, core.SetMetadataLogPayload{
				TargetType: core.MetaTargetTypeAccount,
				TargetID:   targetID.(string),
				Metadata:   m,
			})
		default:
			return nil, errorsutil.NewError(ErrValidation, errors.Errorf("unknown target type '%s'", targetType))
		}

		return executionContext.AppendLog(ctx, core.NewActiveLog(log))
	})
	return err
}

func (commander *Commander) RevertTransaction(ctx context.Context, parameters Parameters, id uint64) (*core.Transaction, error) {

	if err := commander.referencer.take(referenceReverts, id); err != nil {
		return nil, ErrRevertOccurring
	}
	defer commander.referencer.release(referenceReverts, id)

	_, err := commander.store.ReadLogForRevertedTransaction(ctx, id)
	if err == nil {
		return nil, ErrAlreadyReverted
	}
	if err != nil && !errors.Is(err, storageerrors.ErrNotFound) {
		return nil, err
	}

	transactionToRevertLog, err := commander.store.ReadLogForCreatedTransaction(ctx, id)
	if storageerrors.IsNotFoundError(err) {
		return nil, errorsutil.NewError(err, errors.Errorf("transaction %d not found", id))
	}
	if err != nil {
		return nil, err
	}

	transactionToRevert := transactionToRevertLog.Data.(core.NewTransactionLogPayload).Transaction

	rt := transactionToRevert.Reverse()
	rt.Metadata = core.MarkReverts(metadata.Metadata{}, transactionToRevert.ID)

	log, err := commander.exec(ctx, parameters,
		core.TxToScriptData(core.TransactionData{
			Postings: rt.Postings,
			Metadata: rt.Metadata,
		}),
		func(tx *core.Transaction, accountMetadata map[string]metadata.Metadata) *core.Log {
			return core.NewRevertedTransactionLog(tx.Timestamp, transactionToRevert.ID, tx)
		})
	if err != nil {
		return nil, err
	}

	return log.Data.(core.RevertedTransactionLogPayload).RevertTransaction, nil
}

func (commander *Commander) Wait() {
	commander.running.Wait()
}
