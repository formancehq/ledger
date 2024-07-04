package command

import (
	"context"

	"github.com/formancehq/ledger/internal/opentelemetry/tracer"

	storageerrors "github.com/formancehq/ledger/internal/storage/sqlutils"

	ledger "github.com/formancehq/ledger/internal"
)

type executionContext struct {
	commander  *Commander
	parameters Parameters
}

func (e *executionContext) AppendLog(ctx context.Context, log *ledger.Log) (*ledger.ChainedLog, error) {
	ctx, span := tracer.Start(ctx, "AppendLog")
	defer span.End()

	if e.parameters.DryRun {
		return log.ChainLog(nil), nil
	}

	chainedLog := func() *ledger.ChainedLog {
		_, span := tracer.Start(ctx, "ChainLog")
		defer span.End()

		return e.commander.chain.ChainLog(log)
	}()

	done := make(chan struct{})
	func() {
		_, span := tracer.Start(ctx, "AppendLogToQueue")
		defer span.End()

		e.commander.Append(chainedLog, func() {
			close(done)
		})
	}()

	err := func() error {
		_, span := tracer.Start(ctx, "WaitLogAck")
		defer span.End()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return nil
		}
	}()
	if err != nil {
		return nil, err
	}

	return chainedLog, nil
}

func (e *executionContext) run(ctx context.Context, executor func(e *executionContext) (*ledger.ChainedLog, error)) (*ledger.ChainedLog, error) {
	if ik := e.parameters.IdempotencyKey; ik != "" {
		if err := e.commander.referencer.take(referenceIks, ik); err != nil {
			return nil, err
		}
		defer e.commander.referencer.release(referenceIks, ik)

		ctx, span := tracer.Start(ctx, "CheckIK")
		defer span.End()

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
