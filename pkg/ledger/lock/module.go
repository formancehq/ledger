package lock

import (
	"go.uber.org/fx"
)

func Module() fx.Option {
	return fx.Options(
		fx.Provide(fx.Annotate(NewInMemory, fx.As(new(Locker)))),
	)
}
