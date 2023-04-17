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

type executionContext struct {
	context.Context
	cache Cache
	close chan struct{}
}

// TODO(gfyrag): Explicit retain is not required
// A call to a GetAccountWithVolumes should automatically retain accounts until execution context completion
func (ctx *executionContext) RetainAccount(accounts ...string) error {
	release, err := ctx.cache.LockAccounts(ctx, accounts...)
	if err != nil {
		return errors.Wrap(err, "locking accounts into cache")
	}

	go func() {
		<-ctx.close
		release()
	}()

	return nil
}

type Parameters struct {
	DryRun bool
	Async  bool
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

type Locker interface {
	Lock(ctx context.Context, accounts Accounts) (Unlock, error)
}
type LockerFn func(ctx context.Context, accounts Accounts) (Unlock, error)

func (fn LockerFn) Lock(ctx context.Context, accounts Accounts) (Unlock, error) {
	return fn(ctx, accounts)
}

var NoOpLocker = LockerFn(func(ctx context.Context, accounts Accounts) (Unlock, error) {
	return func(ctx context.Context) {}, nil
})

type Commander struct {
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

func (l *Commander) GetLedgerStore() Store {
	return l.store
}

func (l *Commander) executeTransaction(ctx context.Context, parameters Parameters, script core.RunScript,
	logComputer func(tx *core.Transaction, result *machine.Result) core.Log) (*core.Transaction, error) {
	if script.Plain == "" {
		return nil, ErrNoScript
	}
	var tx *core.Transaction

	err := l.runCommand(ctx, parameters, func(ctx executionContext) (*core.Log, error) {
		reserve, ts, err := l.state.Reserve(ctx, ReserveRequest{
			Timestamp: script.Timestamp,
			Reference: script.Reference,
		})
		if err != nil {
			return nil, errorsutil.NewError(ErrState, err)
		}
		defer func() {
			reserve.Clear(tx)
		}()

		script.Timestamp = *ts

		program, err := l.compiler.Compile(ctx, script.Plain)
		if err != nil {
			return nil, errorsutil.NewError(ErrCompilationFailed, errors.Wrap(err, "compiling numscript"))
		}

		m := vm.NewMachine(*program)

		if err := m.SetVarsFromJSON(script.Vars); err != nil {
			return nil, errorsutil.NewError(ErrCompilationFailed,
				errors.Wrap(err, "could not set variables"))
		}

		involvedAccounts, involvedSources, err := m.ResolveResources(ctx, l.cache)
		if err != nil {
			return nil, errorsutil.NewError(ErrCompilationFailed,
				errors.Wrap(err, "could not resolve program resources"))
		}

		if err := ctx.RetainAccount(involvedAccounts...); err != nil {
			return nil, err
		}

		worldFilter := collectionutils.Not(collectionutils.Eq("world"))
		lockAccounts := Accounts{
			Read:  collectionutils.Filter(involvedAccounts, worldFilter),
			Write: collectionutils.Filter(involvedSources, worldFilter),
		}

		unlock, err := l.locker.Lock(ctx, lockAccounts)
		if err != nil {
			return nil, errors.Wrap(err, "locking accounts for tx processing")
		}
		defer unlock(ctx)

		err = m.ResolveBalances(ctx, l.cache)
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

		newTx := core.NewTransaction().
			WithPostings(result.Postings...).
			WithReference(script.Reference).
			WithMetadata(result.Metadata).
			WithTimestamp(script.Timestamp).
			WithID(l.state.GetNextTXID())

		tx = &newTx

		if !parameters.DryRun {
			l.cache.UpdateVolumeWithTX(tx)
		}

		log := logComputer(tx, result)
		if script.Reference != "" {
			log = log.WithReference(script.Reference)
		}

		return &log, nil
	})
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func (l *Commander) CreateTransaction(ctx context.Context, parameters Parameters, script core.RunScript) (*core.Transaction, error) {
	return l.executeTransaction(ctx, parameters, script, func(tx *core.Transaction, result *machine.Result) core.Log {
		return core.NewTransactionLog(*tx, result.AccountMetadata)
	})
}

func (l *Commander) SaveMeta(ctx context.Context, targetType string, targetID interface{}, m metadata.Metadata, async bool) error {
	if m == nil {
		return nil
	}

	if targetType == "" {
		return errorsutil.NewError(ErrValidation, errors.New("empty target type"))
	}

	if targetID == "" {
		return errorsutil.NewError(ErrValidation, errors.New("empty target id"))
	}

	return l.runCommand(ctx, Parameters{
		DryRun: false,
		Async:  async,
	}, func(ctx executionContext) (*core.Log, error) {
		at := core.Now()
		var (
			err error
			log core.Log
		)
		switch targetType {
		case core.MetaTargetTypeTransaction:
			_, err = l.store.GetTransaction(ctx, targetID.(uint64))
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
			if err = l.cache.UpdateAccountMetadata(targetID.(string), m); err != nil {
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
}

func (r *Commander) RevertTransaction(ctx context.Context, id uint64, async bool) (*core.Transaction, error) {
	_, loaded := r.inflightReverts.LoadOrStore(id, struct{}{})
	if loaded {
		return nil, ErrRevertOccurring
	}
	defer func() {
		//TODO(gfyrag): Should not be deleted until log ingestion
		r.inflightReverts.Delete(id)
	}()

	transactionToRevert, err := r.store.GetTransaction(ctx, id)
	if err != nil {
		return nil, err
	}
	if err != nil && !storage.IsNotFoundError(err) {
		return nil, errors.Wrap(err, "get transaction before revert")
	}

	if storage.IsNotFoundError(err) {
		return nil, errorsutil.NewError(err, errors.Errorf("transaction %d not found", id))
	}

	if transactionToRevert.IsReverted() {
		return nil, errorsutil.NewError(ErrAlreadyReverted, errors.Errorf("transaction %d already reverted", id))
	}

	rt := transactionToRevert.Reverse()
	rt.Metadata = core.MarkReverts(metadata.Metadata{}, transactionToRevert.ID)

	return r.executeTransaction(ctx,
		Parameters{
			Async: async,
		},
		core.TxToScriptData(core.TransactionData{
			Postings:  rt.Postings,
			Reference: rt.Reference,
			Metadata:  rt.Metadata,
		}),
		func(tx *core.Transaction, result *machine.Result) core.Log {
			return core.NewRevertedTransactionLog(tx.Timestamp, transactionToRevert.ID, *tx)
		})
}

func (l *Commander) runCommand(ctx context.Context, parameters Parameters, exec func(ctx executionContext) (*core.Log, error)) error {
	execContext := executionContext{
		Context: ctx,
		cache:   l.cache,
		close:   make(chan struct{}),
	}
	log, err := exec(execContext)
	if err != nil {
		close(execContext.close)
		return err
	}
	if parameters.DryRun {
		close(execContext.close)
		return nil
	}
	if err := l.store.AppendLog(ctx, log); err != nil {
		close(execContext.close)
		return err
	}
	logHolder := core.NewLogHolder(log)
	if err := l.logIngester.QueueLog(ctx, logHolder); err != nil {
		close(execContext.close)
		return err
	}
	if parameters.Async {
		go func() {
			<-logHolder.Ingested
			close(execContext.close)
		}()
	} else {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logHolder.Ingested:
		}
		close(execContext.close)
	}

	return nil
}
