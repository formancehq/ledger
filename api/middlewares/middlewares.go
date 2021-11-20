package middlewares

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewAuthMiddleware),
	fx.Provide(NewLedgerMiddleware),
)
