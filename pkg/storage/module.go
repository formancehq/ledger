package storage

import "go.uber.org/fx"

func DefaultModule() fx.Option {
	return fx.Options(
		fx.Provide(NewDefaultFactory),
	)
}
