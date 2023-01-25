package publish

import (
	"github.com/ThreeDotsLabs/watermill"
	"go.uber.org/fx"
)

func DefaultLoggingModule() fx.Option {
	return fx.Supply(fx.Annotate(watermill.NopLogger{}, fx.As(new(watermill.LoggerAdapter))))
}
