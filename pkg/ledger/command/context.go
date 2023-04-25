package command

import (
	"context"

	"github.com/pkg/errors"
)

type executionContext struct {
	context.Context
	cache               Cache
	onCompleteCallbacks []func()
	onCommitCallbacks   []func()
}

// TODO(gfyrag): Explicit retain is not required
// A call to a GetAccountWithVolumes should automatically retain accounts until execution context completion
func (ctx *executionContext) retainAccount(accounts ...string) error {
	release, err := ctx.cache.LockAccounts(ctx, accounts...)
	if err != nil {
		return errors.Wrap(err, "locking accounts into cache")
	}

	ctx.onComplete(release)

	return nil
}

func (ctx *executionContext) onComplete(fn func()) {
	ctx.onCompleteCallbacks = append(ctx.onCompleteCallbacks, fn)
}

func (ctx *executionContext) complete() {
	for _, callback := range ctx.onCompleteCallbacks {
		callback()
	}
}

func (ctx *executionContext) onCommit(fn func()) {
	ctx.onCompleteCallbacks = append(ctx.onCompleteCallbacks, fn)
}

func (ctx *executionContext) commit() {
	for _, callback := range ctx.onCommitCallbacks {
		callback()
	}
}

func newExecutionContext(ctx context.Context, cache Cache) *executionContext {
	return &executionContext{
		Context: ctx,
		cache:   cache,
	}
}
