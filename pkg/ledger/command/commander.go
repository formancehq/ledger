package command

import (
	"context"
	"sync"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/machine"
	"github.com/formancehq/ledger/pkg/machine/vm"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage"
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

type Cache interface {
	GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error)
	LockAccounts(ctx context.Context, accounts ...string) (cache.Release, error)
	UpdateVolumeWithTX(tx *core.Transaction)
	UpdateAccountMetadata(s string, m metadata.Metadata) error
}

type LogIngester interface {
	QueueLog(ctx context.Context, log *core.LogHolder) error
}
type LogIngesterFn func(ctx context.Context, log *core.LogHolder) error

func (fn LogIngesterFn) QueueLog(ctx context.Context, log *core.LogHolder) error {
	return fn(ctx, log)
}

var NoOpIngester = LogIngesterFn(func(ctx context.Context, log *core.LogHolder) error {
	log.SetIngested()
	return nil
})

type Commander struct {
	//TODO(gfyrag): Move to state
	inflightReverts sync.Map
	store           Store
	locker          Locker
	cache           Cache
	logIngester     LogIngester
	metricsRegistry metrics.PerLedgerMetricsRegistry
	state           *State
	compiler        *Compiler
}

func New(
	store Store,
	cache Cache,
	locker Locker,
	logIngester LogIngester,
	state *State,
	metricsRegistry metrics.PerLedgerMetricsRegistry,
) *Commander {
	return &Commander{
		store:           store,
		locker:          locker,
		cache:           cache,
		logIngester:     logIngester,
		metricsRegistry: metricsRegistry,
		state:           state,
		compiler:        NewCompiler(),
	}
}

func (c *Commander) GetLedgerStore() Store {
	return c.store
}

func (c *Commander) executeTransaction(ctx context.Context, parameters Parameters, script core.RunScript,
	logComputer func(tx *core.Transaction, result *machine.Result) core.Log) (*core.Log, error) {
	if script.Plain == "" {
		return nil, ErrNoScript
	}

	reserve, ts, err := c.state.Reserve(ctx, ReserveRequest{
		Timestamp: script.Timestamp,
		Reference: script.Reference,
	})
	if err != nil {
		return nil, errorsutil.NewError(ErrState, err)
	}

	var newTx *core.Transaction

	log, err := c.runCommand(ctx, parameters, func(ctx *executionContext) (*core.Log, error) {
		script.Timestamp = *ts

		program, err := c.compiler.Compile(ctx, script.Plain)
		if err != nil {
			return nil, errorsutil.NewError(ErrCompilationFailed, errors.Wrap(err, "compiling numscript"))
		}

		m := vm.NewMachine(*program)

		if err := m.SetVarsFromJSON(script.Vars); err != nil {
			return nil, errorsutil.NewError(ErrCompilationFailed,
				errors.Wrap(err, "could not set variables"))
		}

		involvedAccounts, involvedSources, err := m.ResolveResources(ctx, c.cache)
		if err != nil {
			return nil, errorsutil.NewError(ErrCompilationFailed,
				errors.Wrap(err, "could not resolve program resources"))
		}

		if err := ctx.RetainAccount(involvedAccounts...); err != nil {
			return nil, err
		}

		worldFilter := collectionutils.FilterNot(collectionutils.FilterEq("world"))
		lockAccounts := Accounts{
			Read:  collectionutils.Filter(involvedAccounts, worldFilter),
			Write: collectionutils.Filter(involvedSources, worldFilter),
		}

		unlock, err := c.locker.Lock(ctx, lockAccounts)
		if err != nil {
			return nil, errors.Wrap(err, "locking accounts for tx processing")
		}
		defer unlock(ctx)

		err = m.ResolveBalances(ctx, c.cache)
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
			WithID(c.state.GetNextTXID()).
			WithReference(script.Reference)

		newTx = &tx

		if !parameters.DryRun {
			c.cache.UpdateVolumeWithTX(newTx)
		}

		log := logComputer(newTx, result)

		return &log, nil
	})
	if err != nil {
		reserve.Clear(nil)
		return nil, err
	}

	reserve.Clear(newTx)

	return log, nil
}

func (c *Commander) CreateTransaction(ctx context.Context, parameters Parameters, script core.RunScript) (*core.Transaction, error) {
	log, err := c.executeTransaction(ctx, parameters, script, func(tx *core.Transaction, result *machine.Result) core.Log {
		return core.NewTransactionLog(*tx, result.AccountMetadata)
	})
	if err != nil {
		return nil, err
	}
	tx := log.Data.(core.NewTransactionLogPayload).Transaction
	return &tx, nil
}

func (c *Commander) SaveMeta(ctx context.Context, parameters Parameters, targetType string, targetID interface{}, m metadata.Metadata) error {
	if m == nil {
		return nil
	}

	if targetType == "" {
		return errorsutil.NewError(ErrValidation, errors.New("empty target type"))
	}

	if targetID == "" {
		return errorsutil.NewError(ErrValidation, errors.New("empty target id"))
	}

	_, err := c.runCommand(ctx, parameters, func(ctx *executionContext) (*core.Log, error) {
		at := core.Now()
		var (
			err error
			log core.Log
		)
		switch targetType {
		case core.MetaTargetTypeTransaction:
			_, err := c.store.ReadLogForCreatedTransaction(ctx, targetID.(uint64))
			if err != nil {
				return nil, err
			}
			log = core.NewSetMetadataLog(at, core.SetMetadataLogPayload{
				TargetType: core.MetaTargetTypeTransaction,
				TargetID:   targetID.(uint64),
				Metadata:   m,
			})
		case core.MetaTargetTypeAccount:
			if err := ctx.RetainAccount(targetID.(string)); err != nil {
				return nil, err
			}
			if err = c.cache.UpdateAccountMetadata(targetID.(string), m); err != nil {
				return nil, errors.Wrap(err, "update account metadata")
			}
			log = core.NewSetMetadataLog(at, core.SetMetadataLogPayload{
				TargetType: core.MetaTargetTypeAccount,
				TargetID:   targetID.(string),
				Metadata:   m,
			})
		default:
			return nil, errorsutil.NewError(ErrValidation, errors.Errorf("unknown target type '%s'", targetType))
		}
		if err != nil {
			return nil, err
		}

		return &log, nil
	})
	return err
}

func (c *Commander) RevertTransaction(ctx context.Context, parameters Parameters, id uint64) (*core.Transaction, error) {
	_, loaded := c.inflightReverts.LoadOrStore(id, struct{}{})
	if loaded {
		return nil, ErrRevertOccurring
	}
	defer func() {
		c.inflightReverts.Delete(id)
	}()

	_, err := c.store.ReadLogForRevertedTransaction(ctx, id)
	if err == nil {
		return nil, ErrAlreadyReverted
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	transactionToRevertLog, err := c.store.ReadLogForCreatedTransaction(ctx, id)
	if storage.IsNotFoundError(err) {
		return nil, errorsutil.NewError(err, errors.Errorf("transaction %d not found", id))
	}
	if err != nil {
		return nil, err
	}

	transactionToRevert := transactionToRevertLog.Data.(core.NewTransactionLogPayload).Transaction

	rt := transactionToRevert.Reverse()
	rt.Metadata = core.MarkReverts(metadata.Metadata{}, transactionToRevert.ID)

	log, err := c.executeTransaction(ctx, parameters,
		core.TxToScriptData(core.TransactionData{
			Postings: rt.Postings,
			Metadata: rt.Metadata,
		}),
		func(tx *core.Transaction, result *machine.Result) core.Log {
			return core.NewRevertedTransactionLog(tx.Timestamp, transactionToRevert.ID, *tx)
		})
	if err != nil {
		return nil, err
	}
	tx := log.Data.(core.RevertedTransactionLogPayload).RevertTransaction

	return &tx, nil
}

func (c *Commander) runCommand(ctx context.Context, parameters Parameters, exec func(ctx *executionContext) (*core.Log, error)) (*core.Log, error) {
	if parameters.IdempotencyKey != "" {
		log, err := c.store.ReadLogWithIdempotencyKey(ctx, parameters.IdempotencyKey)
		if err != nil && err != storage.ErrNotFound {
			return nil, err
		}
		if err == nil {
			return log, nil
		}
	}
	execContext := newExecutionContext(ctx, c.cache)
	l, err := exec(execContext)
	if err != nil {
		close(execContext.ingested)
		return nil, err
	}
	log := l.WithIdempotencyKey(parameters.IdempotencyKey)
	if parameters.DryRun {
		close(execContext.ingested)
		return &log, nil
	}
	if err := c.store.AppendLog(ctx, &log); err != nil {
		close(execContext.ingested)
		return nil, err
	}
	logHolder := core.NewLogHolder(&log)
	if err := c.logIngester.QueueLog(ctx, logHolder); err != nil {
		close(execContext.ingested)
		return nil, err
	}
	if parameters.Async {
		go func() {
			<-logHolder.Ingested
			execContext.SetIngested()
		}()
	} else {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-logHolder.Ingested:
		}
		execContext.SetIngested()
	}

	return &log, nil
}
