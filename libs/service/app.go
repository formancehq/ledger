package service

import (
	"context"
	"io"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

const DebugFlag = "debug"
const JsonFormattingLoggerFlag = "json-formatting-logger"

type App struct {
	options []fx.Option
	output  io.Writer
}

func (a *App) Run(ctx context.Context) error {
	logger := GetDefaultLogger(a.output, viper.GetBool(DebugFlag), viper.GetBool(JsonFormattingLoggerFlag))
	app := a.newFxApp(logger)
	if err := app.Start(logging.ContextWithLogger(ctx, logger)); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
	case <-app.Done():
		// <-app.Done() is a signals channel, it means we have to call the
		// app.Stop in order to gracefully shutdown the app
	}

	return app.Stop(logging.ContextWithLogger(context.Background(), logger))
}

func (a *App) Start(ctx context.Context) error {
	logger := GetDefaultLogger(a.output, viper.GetBool(DebugFlag), viper.GetBool(JsonFormattingLoggerFlag))
	return a.newFxApp(logger).Start(ctx)
}

func (a *App) newFxApp(logger logging.Logger) *fx.App {
	return fx.New(append(a.options,
		fx.NopLogger,
		fx.Supply(fx.Annotate(logger, fx.As(new(logging.Logger)))),
	)...)
}

func New(output io.Writer, options ...fx.Option) *App {
	return &App{
		options: options,
		output:  output,
	}
}
