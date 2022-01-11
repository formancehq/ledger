package controllers

import (
	"go.uber.org/fx"
)

const (
	VersionKey       = `name:"_apiVersion"`
	StorageDriverKey = `name:"_apiStorageDriver"`
	LedgerListerKey  = `name:"_apiLedgerLister"`
)

func ProvideVersion(provider interface{}) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(VersionKey)),
	)
}

func ProvideStorageDriver(provider interface{}) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(StorageDriverKey)),
	)
}

func ProvideLedgerLister(provider interface{}) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(LedgerListerKey)),
	)
}

var Module = fx.Options(
	fx.Provide(
		fx.Annotate(NewConfigController, fx.ParamTags(VersionKey, StorageDriverKey, LedgerListerKey)),
	),
	fx.Provide(NewLedgerController),
	fx.Provide(NewScriptController),
	fx.Provide(NewAccountController),
	fx.Provide(NewTransactionController),
)
