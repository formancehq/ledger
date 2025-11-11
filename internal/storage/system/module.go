package system

import (
	"github.com/uptrace/bun"
	"go.uber.org/fx"
)

func NewFXModule() fx.Option {
	return fx.Options(
		fx.Provide(func(db *bun.DB) *DefaultStore {
			return New(db)
		}),
	)
}
