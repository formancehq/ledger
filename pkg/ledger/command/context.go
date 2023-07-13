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

func (e *executionContext) AppendLog(ctx context.Context, log *core.Log) (*core.ActiveLog, chan struct{}, error) {
	if e.parameters.DryRun {
		ret := make(chan struct{})
		close(ret)
		return core.NewActiveLog(log.ChainLog(nil)), ret, nil
	}

	activeLog := core.NewActiveLog(e.commander.chainLog(log))
	logging.FromContext(ctx).WithFields(map[string]any{
		"id": activeLog.ChainedLog.ID,
	}).Debugf("Appending log")
	done := make(chan struct{})
	e.commander.Append(activeLog, func() {
		close(done)
	})
	return activeLog, done, nil
}

func (e *executionContext) run(ctx context.Context, executor func(e *executionContext) (*core.ActiveLog, chan struct{}, error)) (*core.ChainedLog, error) {
	if ik := e.parameters.IdempotencyKey; ik != "" {
		if err := e.commander.referencer.take(referenceIks, ik); err != nil {
			return nil, err
		}
		defer e.commander.referencer.release(referenceIks, ik)

		chainedLog, err := e.commander.store.ReadLogWithIdempotencyKey(ctx, ik)
		if err == nil {
			return chainedLog, nil
		}
		if err != storageerrors.ErrNotFound && err != nil {
			return nil, err
		}
	}
	activeLog, done, err := executor(e)
	if err != nil {
		return nil, err
	}
	<-done
	logger := logging.FromContext(ctx).WithFields(map[string]any{
		"id": activeLog.ID,
	})
	logger.Debugf("Log inserted in database")
	if !e.parameters.Async {
		<-activeLog.Projected
		logger.Debugf("Log fully ingested")
	}
	return activeLog.ChainedLog, nil
}

func newExecutionContext(commander *Commander, parameters Parameters) *executionContext {
	return &executionContext{
		commander:  commander,
		parameters: parameters,
	}
}
