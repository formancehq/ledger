package command

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	storageerrors "github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/logging"
)

type executionContext struct {
	commander  *Commander
	parameters Parameters
}

func (e *executionContext) AppendLog(ctx context.Context, log *core.Log) (*core.LogPersistenceTracker, error) {
	if e.parameters.DryRun {
		chainedLog := log.ChainLog(nil)
		return core.NewResolvedLogPersistenceTracker(core.NewActiveLog(chainedLog)), nil
	}

	activeLog := core.NewActiveLog(e.commander.chainLog(log))
	logging.FromContext(ctx).WithFields(map[string]any{
		"id": activeLog.ChainedLog.ID,
	}).Debugf("Appending log")
	return e.commander.store.AppendLog(ctx, activeLog)
}

func (e *executionContext) run(ctx context.Context, executor func(e *executionContext) (*core.LogPersistenceTracker, error)) (*core.LogPersistenceTracker, error) {
	if ik := e.parameters.IdempotencyKey; ik != "" {
		if err := e.commander.referencer.take(referenceIks, ik); err != nil {
			return nil, err
		}
		defer e.commander.referencer.release(referenceIks, ik)

		chainedLog, err := e.commander.store.ReadLogWithIdempotencyKey(ctx, ik)
		if err == nil {
			return core.NewResolvedLogPersistenceTracker(core.NewActiveLog(chainedLog)), nil
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
	logger := logging.FromContext(ctx).WithFields(map[string]any{
		"id": tracker.ActiveLog().ChainedLog.ID,
	})
	logger.Debugf("Log inserted in database")
	if !e.parameters.Async {
		<-tracker.ActiveLog().Projected
		logger.Debugf("Log fully ingested")
	}
	return tracker, nil
}

func newExecutionContext(commander *Commander, parameters Parameters) *executionContext {
	return &executionContext{
		commander:  commander,
		parameters: parameters,
	}
}
