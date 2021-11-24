package controllers

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewConfigController),
	fx.Provide(NewLedgerController),
	fx.Provide(NewScriptController),
	fx.Provide(NewAccountController),
	fx.Provide(NewTransactionController),
)
