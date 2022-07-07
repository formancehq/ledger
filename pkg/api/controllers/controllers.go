package controllers

import (
	"github.com/numary/ledger/pkg/health"
	"go.uber.org/fx"
)

const (
	versionKey = `name:"_apiVersion"`
)

func ProvideVersion(provider interface{}) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(versionKey)),
	)
}

var Module = fx.Options(
	fx.Provide(
		fx.Annotate(NewConfigController, fx.ParamTags(versionKey)),
	),
	fx.Provide(NewLedgerController),
	fx.Provide(NewScriptController),
	fx.Provide(NewAccountController),
	fx.Provide(NewTransactionController),
	fx.Provide(NewBalanceController),
	fx.Provide(NewMappingController),
	fx.Provide(
		fx.Annotate(NewHealthController, fx.ParamTags(health.HealthCheckKey)),
	),
)
