package middlewares

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(
		fx.Annotate(NewAuthMiddleware, fx.ParamTags(`name:"httpBasic"`)),
	),
	fx.Provide(NewLedgerMiddleware),
)
