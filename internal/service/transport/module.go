package transport

import (
	"go.uber.org/fx"
)

// Module provides the transport connection pools
func Module() fx.Option {
	return fx.Options(
		fx.Provide(NewConnectionPool),
		fx.Provide(NewServiceConnectionPool),
	)
}
