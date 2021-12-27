package controllers

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(
		fx.Annotate(NewConfigController, fx.ParamTags(`name:"version"`, `name:"storageDriver"`, `name:"ledgerLister"`)),
	),
	fx.Provide(NewLedgerController),
	fx.Provide(NewScriptController),
	fx.Provide(NewAccountController),
	fx.Provide(NewTransactionController),
)
