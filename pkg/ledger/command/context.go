package command

import (
	"context"

	"github.com/pkg/errors"
)

type executionContext struct {
	context.Context
	cache    Cache
	ingested chan struct{}
}

// TODO(gfyrag): Explicit retain is not required
// A call to a GetAccountWithVolumes should automatically retain accounts until execution context completion
func (ctx *executionContext) RetainAccount(accounts ...string) error {
	release, err := ctx.cache.LockAccounts(ctx, accounts...)
	if err != nil {
		return errors.Wrap(err, "locking accounts into cache")
	}

	go func() {
		<-ctx.ingested
		release()
	}()

	return nil
}

func (ctx *executionContext) SetIngested() {
	close(ctx.ingested)
}

func newExecutionContext(ctx context.Context, cache Cache) *executionContext {
	return &executionContext{
		Context:  ctx,
		cache:    cache,
		ingested: make(chan struct{}),
	}
}
