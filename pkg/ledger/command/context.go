package command

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	storageerrors "github.com/formancehq/ledger/pkg/storage/errors"
)

type executionContext struct {
	commander  *Commander
	parameters Parameters
}

func (e *executionContext) AppendLog(ctx context.Context, log *core.ActiveLog) (*core.LogPersistenceTracker, error) {
	if e.parameters.DryRun {
		return core.NewResolvedLogPersistenceTracker(log, log.ComputePersistentLog(nil)), nil
	}
	return e.commander.store.AppendLog(ctx, log)
}

func (e *executionContext) run(ctx context.Context, executor func(e *executionContext) (*core.LogPersistenceTracker, error)) (*core.LogPersistenceTracker, error) {
	if ik := e.parameters.IdempotencyKey; ik != "" {
		if err := e.commander.referencer.take(referenceIks, ik); err != nil {
			return nil, err
		}
		defer e.commander.referencer.release(referenceIks, ik)

		persistedLog, err := e.commander.store.ReadLogWithIdempotencyKey(ctx, ik)
		if err == nil {
			return core.NewResolvedLogPersistenceTracker(nil, persistedLog), nil
		}
		if err != storageerrors.ErrNotFound && err != nil {
			return nil, err
		}
	}
	tracker, err := executor(e)
	if err != nil {
		return nil, err
	}
	<-tracker.Done()
	if !e.parameters.Async {
		<-tracker.ActiveLog().Ingested
	}
	return tracker, nil
}

func newExecutionContext(commander *Commander, parameters Parameters) *executionContext {
	return &executionContext{
		commander:  commander,
		parameters: parameters,
	}
}
