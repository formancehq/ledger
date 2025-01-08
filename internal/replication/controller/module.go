package controller

import (
	"go.uber.org/fx"
)

func NewFXModule() fx.Option {
	return fx.Options(
		fx.Provide(New),
		fx.Provide(NewDefaultRunner),
	)
}
