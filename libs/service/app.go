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
	app := a.newFxApp(ctx)
	if err := app.Start(ctx); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
	case <-app.Done():
		// <-app.Done() is a signals channel, it means we have to call the
		// app.Stop in order to gracefully shutdown the app
	}

	return app.Stop(context.Background())
}

func (a *App) Start(ctx context.Context) error {
	return a.newFxApp(ctx).Start(ctx)
}

func (a *App) newFxApp(ctx context.Context) *fx.App {
	ctx = defaultLoggingContext(ctx, a.output, viper.GetBool(DebugFlag), viper.GetBool(JsonFormattingLoggerFlag))
	options := append(a.options,
		fx.NopLogger,
		fx.Provide(func() logging.Logger {
			return logging.FromContext(ctx)
		}),
	)
	return fx.New(options...)
}

func New(output io.Writer, options ...fx.Option) *App {
	return &App{
		options: options,
		output:  output,
	}
}
