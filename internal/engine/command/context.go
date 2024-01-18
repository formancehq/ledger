package command

import (
	"context"

	storageerrors "github.com/formancehq/ledger/internal/storage/sqlutils"

	ledger "github.com/formancehq/ledger/internal"
)

type executionContext struct {
	commander  *Commander
	parameters Parameters
}

func (e *executionContext) AppendLog(ctx context.Context, log *ledger.Log) (*ledger.ChainedLog, error) {
	if e.parameters.DryRun {
		return log.ChainLog(nil), nil
	}

	chainedLog := e.commander.chainLog(log)
	done := make(chan struct{})
	e.commander.Append(chainedLog, func() {
		close(done)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-done:
	}
	return chainedLog, nil
}

func (e *executionContext) run(ctx context.Context, executor func(e *executionContext) (*ledger.ChainedLog, error)) (*ledger.ChainedLog, error) {
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
	return executor(e)
}

func newExecutionContext(commander *Commander, parameters Parameters) *executionContext {
	return &executionContext{
		commander:  commander,
		parameters: parameters,
	}
}
