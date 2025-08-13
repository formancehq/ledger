package service

import (
	"context"
	"io"
	"os"

	"github.com/spf13/pflag"

	"github.com/formancehq/go-libs/otlp/otlptraces"
	"github.com/sirupsen/logrus"
	"github.com/uptrace/opentelemetry-go-extra/otellogrus"

	"github.com/formancehq/go-libs/errorsutils"
	"github.com/formancehq/go-libs/logging"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

const DebugFlag = "debug"

func AddFlags(flags *pflag.FlagSet) {
	flags.Bool(DebugFlag, false, "Debug mode")
	flags.Bool(logging.JsonFormattingLoggerFlag, false, "Format logs as json")
}

type App struct {
	options []fx.Option
	output  io.Writer
}

func (a *App) Run(cmd *cobra.Command) error {

	loggerHooks := make([]logrus.Hook, 0)
	otelTraces, _ := cmd.Flags().GetBool(otlptraces.OtelTracesFlag)
	if otelTraces {
		loggerHooks = append(loggerHooks, otellogrus.NewHook(otellogrus.WithLevels(
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
			logrus.WarnLevel,
		)))
	}

	jsonFormatting, _ := cmd.Flags().GetBool(logging.JsonFormattingLoggerFlag)
	logger := logging.NewDefaultLogger(
		a.output,
		IsDebug(cmd),
		jsonFormatting,
		loggerHooks...,
	)
	logger.Infof("Starting application")
	logger.Debugf("Environment variables")
	for _, v := range os.Environ() {
		logger.Debugf(v)
	}

	app := a.newFxApp(logger)
	if err := app.Start(logging.ContextWithLogger(cmd.Context(), logger)); err != nil {
		switch {
		case errorsutils.IsErrorWithExitCode(err):
			logger.Errorf("Error: %v", err)
			// We want to have a specific exit code for the error
			os.Exit(err.(*errorsutils.ErrorWithExitCode).ExitCode)
		default:
			return err
		}
	}

	var exitCode int
	select {
	case <-cmd.Context().Done():
	case shutdownSignal := <-app.Wait():
		// <-app.Done() is a signals channel, it means we have to call the
		// app.Stop in order to gracefully shutdown the app
		exitCode = shutdownSignal.ExitCode
	}

	logger.Infof("Stopping app...")
	defer func() {
		logger.Infof("App stopped!")
	}()

	if err := app.Stop(logging.ContextWithLogger(contextWithLifecycle(
		context.Background(), // Don't reuse original context as it can have been cancelled, and we really need to properly stop the app
		lifecycleFromContext(cmd.Context()),
	), logger)); err != nil {
		return err
	}

	if exitCode != 0 {
		os.Exit(exitCode)
	}

	return nil
}

func (a *App) newFxApp(logger logging.Logger) *fx.App {
	options := append(
		a.options,
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
