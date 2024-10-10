package tracing

import "context"

func Trace[RET any](ctx context.Context, name string, fn func(ctx context.Context) (RET, error)) (RET, error) {
	ctx, trace := Start(ctx, name)
	defer trace.End()

	return fn(ctx)
}

func NoResult(fn func(ctx context.Context) error) func(ctx context.Context) (any, error) {
	return func(ctx context.Context) (any, error) {
		return nil, fn(ctx)
	}
}

func SkipResult[RET any](_ RET, err error) error {
	return err
}
