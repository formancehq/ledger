package middlewares

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewLedgerMiddleware),
	fx.Provide(func() Locker {
		return NewInMemoryLocker()
	}),
)
