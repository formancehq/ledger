package publish

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"go.uber.org/fx"
)

type watermillLoggerAdapter struct {
	logging.Logger
}

func (w watermillLoggerAdapter) Error(msg string, err error, fields watermill.LogFields) {
	w.WithFields(fields).WithFields(map[string]any{
		"err": err,
	}).Error(msg)
}

func (w watermillLoggerAdapter) Info(msg string, fields watermill.LogFields) {
	w.WithFields(fields).Info(msg)
}

func (w watermillLoggerAdapter) Debug(msg string, fields watermill.LogFields) {
	w.WithFields(fields).Debug(msg)
}

func (w watermillLoggerAdapter) Trace(msg string, fields watermill.LogFields) {
	w.WithFields(fields).Debug(msg)
}

func (w watermillLoggerAdapter) With(fields watermill.LogFields) watermill.LoggerAdapter {
	return watermillLoggerAdapter{
		Logger: w.Logger.WithFields(fields),
	}
}

var _ watermill.LoggerAdapter = &watermillLoggerAdapter{}

func defaultLoggingModule() fx.Option {
	return fx.Provide(func(logger logging.Logger) watermill.LoggerAdapter {
		return watermillLoggerAdapter{
			Logger: logger,
		}
	})
}
