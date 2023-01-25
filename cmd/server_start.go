package cmd

import (
	"context"
	"net"
	"net/http"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/logging/logginglogrus"
	"github.com/formancehq/go-libs/otlp/otlptraces"
	"github.com/numary/ledger/pkg/api"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uptrace/opentelemetry-go-extra/otellogrus"
	"go.uber.org/fx"
)

func NewServerStart() *cobra.Command {
	return &cobra.Command{
		Use: "start",
		RunE: func(cmd *cobra.Command, args []string) error {
			l := logrus.New()
			if viper.GetBool(debugFlag) {
				l.Level = logrus.DebugLevel
			}
			if viper.GetBool(otlptraces.OtelTracesFlag) {
				l.AddHook(otellogrus.NewHook(otellogrus.WithLevels(
					logrus.PanicLevel,
					logrus.FatalLevel,
					logrus.ErrorLevel,
					logrus.WarnLevel,
				)))
			}
			loggerFactory := logging.StaticLoggerFactory(logginglogrus.New(l))
			logging.SetFactory(loggerFactory)

			app := NewContainer(
				viper.GetViper(),
				fx.Invoke(func(lc fx.Lifecycle, h *api.API) {
					var (
						err      error
						listener net.Listener
					)
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							listener, err = net.Listen("tcp", viper.GetString(serverHttpBindAddressFlag))
							if err != nil {
								return err
							}
							go func() {
								httpErr := http.Serve(listener, h)
								logging.Errorf("http.Serve: %s", httpErr)
							}()
							return nil
						},
						OnStop: func(ctx context.Context) error {
							return listener.Close()
						},
					})
				}),
			)
			errCh := make(chan error, 1)
			go func() {
				err := app.Start(cmd.Context())
				if err != nil {
					errCh <- err
				}
			}()
			select {
			case err := <-errCh:
				return err
			case <-cmd.Context().Done():
				return app.Stop(context.Background())
			case <-app.Done():
				return app.Err()
			}
		},
	}
}
