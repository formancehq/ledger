package controllers

import (
	"github.com/numary/ledger/pkg/health"
	"go.uber.org/fx"
)

const (
	versionKey       = `name:"_apiVersion"`
	storageDriverKey = `name:"_apiStorageDriver"`
	ledgerListerKey  = `name:"_apiLedgerLister"`
)

func ProvideVersion(provider interface{}) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(versionKey)),
	)
}

func ProvideStorageDriver(provider interface{}) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(storageDriverKey)),
	)
}

func ProvideLedgerLister(provider interface{}) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(ledgerListerKey)),
	)
}

var Module = fx.Options(
	fx.Provide(
		fx.Annotate(NewConfigController, fx.ParamTags(versionKey, storageDriverKey, ledgerListerKey)),
	),
	fx.Provide(NewLedgerController),
	fx.Provide(NewScriptController),
	fx.Provide(NewAccountController),
	fx.Provide(NewTransactionController),
	fx.Provide(NewMappingController),
	fx.Provide(
		fx.Annotate(NewHealthController, fx.ParamTags(health.HealthCheckKey)),
	),
)
