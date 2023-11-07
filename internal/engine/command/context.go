package command

import (
	"context"

	ledger "github.com/formancehq/ledger/internal"
	storageerrors "github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/stack/libs/go-libs/logging"
)

type executionContext struct {
	commander  *Commander
	parameters Parameters
}

func (e *executionContext) AppendLog(ctx context.Context, log *ledger.Log) (*ledger.ChainedLog, chan struct{}, error) {
	if e.parameters.DryRun {
		ret := make(chan struct{})
		close(ret)
		return log.ChainLog(nil), ret, nil
	}

	chainedLog := e.commander.chainLog(log)
	logging.FromContext(ctx).WithFields(map[string]any{
		"id": chainedLog.ID,
	}).Debugf("Appending log")
	done := make(chan struct{})
	e.commander.Append(chainedLog, func() {
		close(done)
	})
	return chainedLog, done, nil
}

func (e *executionContext) run(ctx context.Context, executor func(e *executionContext) (*ledger.ChainedLog, chan struct{}, error)) (*ledger.ChainedLog, error) {
	if ik := e.parameters.IdempotencyKey; ik != "" {
		if err := e.commander.referencer.take(referenceIks, ik); err != nil {
			return nil, err
		}
		defer e.commander.referencer.release(referenceIks, ik)

		chainedLog, err := e.commander.store.ReadLogWithIdempotencyKey(ctx, ik)
		if err == nil {
			return chainedLog, nil
		}
		if err != nil && !storageerrors.IsNotFoundError(err) {
			return nil, err
		}
	}
	chainedLog, done, err := executor(e)
	if err != nil {
		return nil, err
	}
	<-done
	logger := logging.FromContext(ctx).WithFields(map[string]any{
		"id": chainedLog.ID,
	})
	logger.Debugf("Log inserted in database")
	return chainedLog, nil
}

func newExecutionContext(commander *Commander, parameters Parameters) *executionContext {
	return &executionContext{
		commander:  commander,
		parameters: parameters,
	}
}
