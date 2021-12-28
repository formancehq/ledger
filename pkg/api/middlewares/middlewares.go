package middlewares

import (
	"go.uber.org/fx"
)

const HttpBasicKey = `name:"_apiHttpBasic"`

func ProvideHTTPBasic(provider interface{}) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(HttpBasicKey)),
	)
}

var Module = fx.Options(
	fx.Provide(
		fx.Annotate(NewAuthMiddleware, fx.ParamTags(HttpBasicKey)),
	),
	fx.Provide(NewLedgerMiddleware),
)
