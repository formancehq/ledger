package ballast

import (
	"context"

	"go.uber.org/fx"
)

func Module(ballastSizeInBytes uint) fx.Option {
	if ballastSizeInBytes == 0 {
		return fx.Options()
	}

	return fx.Options(
		fx.Invoke(func(lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					Allocate(ballastSizeInBytes)
					return nil
				},
				OnStop: func(ctx context.Context) error {
					ReleaseForGC()
					return nil
				},
			})
		}),
	)
}
