package service

import (
	"context"
	"io"
	"os"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

const DebugFlag = "debug"

func BindFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().Bool(DebugFlag, false, "Debug mode")
	cmd.PersistentFlags().Bool(logging.JsonFormattingLoggerFlag, false, "Format logs as json")

	if err := viper.BindPFlags(cmd.PersistentFlags()); err != nil {
		panic(err)
	}
}

func IsDebug() bool {
	return viper.GetBool(DebugFlag)
}

type App struct {
	options []fx.Option
	output  io.Writer
}

func (a *App) Run(ctx context.Context) error {
	logger := GetDefaultLogger(a.output)
	logger.Infof("Starting application")
	logger.Debugf("Environment variables")
	for _, v := range os.Environ() {
		logger.Debugf(v)
	}

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

	logger.Infof("Stopping app...")

	return app.Stop(logging.ContextWithLogger(contextWithLifecycle(
		context.Background(), // Don't reuse original context as it can have been cancelled, and we really need to properly stop the app
		lifecycleFromContext(ctx),
	), logger))
}

func (a *App) Start(ctx context.Context) error {
	logger := GetDefaultLogger(a.output)
	return a.newFxApp(logger).Start(ctx)
}

func (a *App) newFxApp(logger logging.Logger) *fx.App {
	options := append(a.options,
		fx.NopLogger,
		fx.Supply(fx.Annotate(logger, fx.As(new(logging.Logger)))),
		fx.Invoke(func(lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					markAsAppReady(ctx)

					return nil
				},
			})
		}),
	)
	options = append([]fx.Option{
		fx.Invoke(func(lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					markAsAppStopped(ctx)

					return nil
				},
			})
		}),
	}, options...)
	return fx.New(options...)
}

func New(output io.Writer, options ...fx.Option) *App {
	return &App{
		options: options,
		output:  output,
	}
}
