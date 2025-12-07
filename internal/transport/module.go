package transport

import (
	"go.uber.org/fx"
)

// Module provides the transport connection pool
func Module() fx.Option {
	return fx.Options(
		fx.Provide(NewConnectionPool),
	)
}

