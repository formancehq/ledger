package workflow

import (
	"go.uber.org/fx"
)

func NewModule() fx.Option {
	return fx.Provide(NewManager)
}
